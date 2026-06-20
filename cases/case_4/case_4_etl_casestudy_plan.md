# Zepto Order Events Pipeline — ETL Case Study Plan
### Zepto REST API (Cursor Pagination) → Kafka → Cassandra | Streamcraft Execution Framework

---

## Overview

This case study models a real-world order event ingestion pipeline for a large Indian quick-commerce platform — modelled after **Zepto** — that exposes an internal REST API emitting order lifecycle events across its dark-store network. Each order progresses through five stages: ORDER_CREATED → ORDER_CONFIRMED → ORDER_PICKED → ORDER_DISPATCHED → ORDER_DELIVERED, and each stage transition fires an event carrying city, store, customer, and amount metadata.

The analytics and ops team needs all order lifecycle events stored in **Cassandra** so that city-level and store-level dashboards can run time-series queries over a rolling 90-day window without hitting the transactional order service.

The pipeline is built on the Streamcraft Execution framework and covers **two independent pipeline flows**:

- **Flow 1 — Cursor Ingestion Flow**: Polls the Zepto internal REST API using cursor-based pagination, reads order event records page by page, and publishes each event to a Kafka topic (`zepto.order.events`). Checkpoints the last committed cursor position in AuxDB so the flow can resume from the exact page boundary after a restart.
- **Flow 2 — Stream Storage Flow**: Consumes events from the Kafka topic (`zepto.order.events`), parses and validates each record, and writes it to Cassandra (`zepto_events.order_events`) with a per-row 90-day TTL. Checkpoints the last committed Kafka offset per partition in AuxDB.

The flows are deliberately decoupled. If Cassandra is slow or unavailable, Flow 1 continues publishing to Kafka uninterrupted — events accumulate in the topic buffer. Flow 2 can be restarted, debugged, or scaled without affecting the API ingestion layer.

The central engineering challenges of this case are:

1. **Cursor-based API pagination**: The API does not use offset; it uses an opaque cursor (`seq_<n>`) that encodes the position in the event stream. The connector must track and forward cursors correctly to avoid gaps or duplicate pages.
2. **Kafka as a durable buffer**: Publishing to Kafka before writing to Cassandra decouples the two failure domains. A Cassandra outage does not cause API polling to back up.
3. **Cassandra partition design for time-series**: `(city, store_id)` as the partition key co-locates all events for a store on the same Cassandra nodes — critical for the per-store time-series queries the dashboard runs. TTL is set per-row at write time so the 90-day rolling window is self-maintaining.
4. **Three distinct fault paths**: The seeder injects three failure types — silent drop (empty city), timestamp parse error, and fault-inject payload marker — so all three pipeline failure paths (records-dropped metric, Flow 2 storage backlog, Flow 1 ingestion backlog) are exercised in every test run.

---

## Part 1 — Source Architecture (Zepto REST API)

### 1.1 API Overview

The Zepto Order Events API is an internal HTTP service exposing a single paginated endpoint:

```
GET /api/v2/order-events?cursor=<seq>&limit=<n>
Host: zepto-order-api.internal
X-Internal-Token: <token>
```

| Parameter | Description |
|---|---|
| `cursor` | Opaque cursor string from the previous response's `next_cursor`. Empty string = start from the beginning. |
| `limit` | Number of events to return per page. Max: 1,000. Default: 500. |

**Response shape:**

```json
{
  "events": [ ... ],
  "next_cursor": "seq_500",
  "has_more": true
}
```

When `has_more` is `false`, the cursor feed is exhausted — Flow 1 terminates after processing the final page.

**Auth**: The `X-Internal-Token` header must be present and non-empty. Any non-empty value is accepted by the seeder (production uses a rotating internal token validated by the order service).

### 1.2 Cursor Format

The cursor encodes the integer position in the event pool as `seq_<index>`:

| Cursor | Meaning |
|---|---|
| `""` (empty) | Start from index 0 |
| `"seq_500"` | Resume from index 500 |
| `"seq_1500"` | Resume from index 1500 |

The connector extracts the index by stripping the `seq_` prefix and parsing the integer. An unparseable cursor safely resets to index 0 (avoids a crash but re-reads from the start — acceptable for idempotent Cassandra writes).

### 1.3 Event Schema

Each event in the `events` array carries the following fields:

| Field | Type | Description |
|---|---|---|
| `event_id` | `string` (UUID-like hex) | Globally unique event identifier |
| `order_id` | `string` | Order identifier (e.g., `ORD-00000001`) |
| `customer_id` | `string` | Customer identifier (e.g., `CUST-000001`) |
| `store_id` | `string` | Dark store identifier (e.g., `STR-BLR-001`) |
| `city` | `string` | City name in lowercase (see §1.4) |
| `event_type` | `string` | One of the five lifecycle stages (see §1.5) |
| `status` | `string` | Human-readable status matching `event_type` |
| `amount` | `float64` | Order value in INR |
| `created_at` | `string` | RFC3339 timestamp |
| `payload` | `object` | Event-type-specific metadata (see §1.6) |

### 1.4 Cities and Stores

The API covers **7 cities** across Zepto's dark-store network:

| City | Store IDs |
|---|---|
| bangalore | STR-BLR-001, STR-BLR-012, STR-BLR-023, STR-BLR-034, STR-BLR-042 |
| delhi | STR-DEL-001, STR-DEL-007, STR-DEL-015, STR-DEL-022 |
| mumbai | STR-MUM-001, STR-MUM-008, STR-MUM-019, STR-MUM-031 |
| hyderabad | STR-HYD-001, STR-HYD-005, STR-HYD-011 |
| chennai | STR-CHN-001, STR-CHN-004, STR-CHN-009 |
| pune | STR-PNE-001, STR-PNE-003 |
| kolkata | STR-KOL-001, STR-KOL-006 |

The `(city, store_id)` combination is the natural Cassandra partition key — all queries are scoped to a specific store.

### 1.5 Event Types and Status Mapping

| `event_type` | `status` | Description |
|---|---|---|
| `ORDER_CREATED` | `created` | Customer placed the order |
| `ORDER_CONFIRMED` | `confirmed` | Dark store accepted the order |
| `ORDER_PICKED` | `picked` | Items picked from shelves |
| `ORDER_DISPATCHED` | `dispatched` | Delivery rider departed the store |
| `ORDER_DELIVERED` | `delivered` | Order handed to customer |

### 1.6 Per-Event-Type Payload Fields

Each event carries a type-specific `payload` object:

| `event_type` | Payload Fields |
|---|---|
| `ORDER_CREATED` | `channel` (app/web/sms), `promo_code` |
| `ORDER_CONFIRMED` | `estimated_prep_mins` |
| `ORDER_PICKED` | `picker_id` |
| `ORDER_DISPATCHED` | `driver_id`, `eta_mins` |
| `ORDER_DELIVERED` | `rating` (1–5), `delivery_mins` |

### 1.7 Fault Injection

The seeder injects three fault types in round-robin at the configured `FAULT_RATE` percentage:

| Fault Type | Mutation | Pipeline Effect |
|---|---|---|
| **Type A** | `city = ""` (empty string) | `transformer_81` returns `nil` — record silently dropped, increments the `records-dropped` metric |
| **Type B** | `created_at = "INVALID_TIMESTAMP"` | `transformer_88` returns a parse error — record routed to `zepto_storage_backlog` (Flow 2 backlog) |
| **Type C** | `payload["_fault_inject"] = "error"` | `transformer_81` returns an error — record routed to `zepto_ingestion_backlog` (Flow 1 backlog) |

Fault types cycle in sequence so every failure path fires during a single test run. At the default `FAULT_RATE=5%`, one fault fires every 20 records.

---

## Part 2 — Intermediate Buffer (Kafka)

### 2.1 Kafka Configuration

Kafka runs in **KRaft mode** (no ZooKeeper) as a single-broker cluster:

| Parameter | Value |
|---|---|
| Image | `apache/kafka:3.9.0` |
| Container | `zepto_kafka` |
| Port | `9092` (PLAINTEXT) |
| Controller port | `9093` |
| Auto-create topics | enabled |
| Log retention | 168 hours (7 days) |
| Replication factor | 1 (single broker) |

### 2.2 Topic

| Topic | Partitions | Purpose |
|---|---|---|
| `zepto.order.events` | 3 | Durable buffer between Flow 1 (REST API ingest) and Flow 2 (Cassandra write) |

Three partitions allow Flow 2 to be scaled to three parallel consumers without rebalancing. The partition key for publish is derived from `(city + store_id)` to keep a store's events ordered within a partition.

### 2.3 Why Kafka in the Middle

Without Kafka, Flow 1 would have to block on every Cassandra write. If Cassandra is slow under load, API polling backs up, the cursor falls behind, and the pipeline risks missing events during a backoff window. With Kafka:

- Flow 1 publishes at full API speed regardless of Cassandra health.
- Flow 2 consumes at whatever rate Cassandra can sustain.
- Kafka's 7-day retention means events survive a multi-hour Cassandra outage without data loss.
- The two flows checkpoint independently — a Flow 2 restart replays only the unconsumed offsets, not the entire cursor history.

---

## Part 3 — Destination Architecture (Cassandra)

### 3.1 Keyspace

```cql
CREATE KEYSPACE IF NOT EXISTS zepto_events
  WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1}
  AND durable_writes = true;
```

| Setting | Value | Reason |
|---|---|---|
| Replication class | `NetworkTopologyStrategy` | Rack-aware; required for multi-DC production deployments even in single-DC local setup |
| Replication factor | 1 per DC | Single-node local cluster; production would use 3 |
| Durable writes | true | Commits to commit log before acknowledging — safe for financial event data |

### 3.2 Table Schema

```cql
CREATE TABLE IF NOT EXISTS zepto_events.order_events (
  city        text,
  store_id    text,
  event_type  text,
  created_at  timestamp,
  event_id    uuid,
  order_id    text,
  customer_id text,
  status      text,
  amount      decimal,
  payload     text,
  run_id      text,
  PRIMARY KEY ((city, store_id), event_type, created_at, event_id)
) WITH CLUSTERING ORDER BY (event_type ASC, created_at DESC, event_id ASC)
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_unit': 'DAYS',
                    'compaction_window_size': 1}
  AND comment = 'Order lifecycle events. TTL=90d set per-row by pipeline.';
```

### 3.3 Partition Key Design

The composite partition key `(city, store_id)` co-locates all events for a given dark store on the same Cassandra nodes. This is intentional:

- **Dashboard queries are always store-scoped** — "show me all events for STR-BLR-001 in the last 7 days" — so hitting a single partition is efficient.
- **Cardinality is bounded** — 23 stores across 7 cities = 23 partitions. No hotspot risk at this scale.
- **City alone would be too broad** — a `city=bangalore` partition would accumulate millions of rows per day across 5 stores.
- **store_id alone would lose city locality** — queries that compare stores within a city would scatter across nodes.

### 3.4 Clustering Columns

| Column | Direction | Reason |
|---|---|---|
| `event_type` | ASC | Groups lifecycle stages together within a store partition |
| `created_at` | DESC | Most-recent-first ordering — dashboards show latest events at the top |
| `event_id` | ASC | Tiebreaker for events with identical `created_at` — prevents overwrites |

The composite clustering key `(event_type, created_at, event_id)` means every row in a partition is uniquely addressed by its full primary key — an event can be rewritten idempotently by re-publishing the same `event_id`.

### 3.5 TTL and Compaction

**Per-row TTL of 90 days** is set by Flow 2 at write time (`USING TTL 7776000`). There is no separate cleanup job — Cassandra's TTL mechanism marks expired cells as tombstones and the compaction strategy removes them.

**TimeWindowCompactionStrategy (TWCS)** is used instead of the default LeveledCompaction:

| Strategy | Why chosen |
|---|---|
| `TimeWindowCompactionStrategy` | Optimised for time-series append workloads. Groups SSTables by time window (1 day here). Expired TTL windows compact cleanly into a single SSTable per window, then get dropped when all cells expire. Minimises write amplification compared to STCS/LCS for append-only event streams. |
| Window size: 1 day | Matches the 90-day TTL granularity — one SSTable per day per store partition is eligible for drop on day 91 |

### 3.6 Payload Serialisation

The `payload` column is stored as `text` (JSON string) rather than a frozen CQL map. This avoids Cassandra schema migrations when Zepto adds new payload fields to future event types — the pipeline JSON-encodes the `payload` map before writing and the consumer deserialises on read.

---

## Part 4 — Control Plane (AuxDB)

AuxDB is a dedicated PostgreSQL instance (port `5446`) that serves as the operational backbone for both flows. It holds checkpoint state and backlog records. Neither Kafka nor Cassandra is involved in control-plane operations.

### 4.1 AuxDB Tables

#### `zepto_ingestion_cursors` — Flow 1 cursor checkpoints

```sql
CREATE TABLE IF NOT EXISTS zepto_ingestion_cursors (
    pipeline    TEXT PRIMARY KEY,
    last_cursor TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);
```

One row per pipeline name. Flow 1 upserts after each successfully published page. On restart, Flow 1 reads `last_cursor` and resumes pagination from that position — no events are re-published, no pages are skipped.

#### `zepto_ingestion_backlog` — Flow 1 failed Kafka publishes

```sql
CREATE TABLE IF NOT EXISTS zepto_ingestion_backlog (
    id              BIGSERIAL PRIMARY KEY,
    order_id        TEXT,
    event_id        TEXT,
    failure_stage   TEXT,
    error_message   TEXT,
    record_payload  JSONB,
    pipeline_run_id TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Records routed here by:
- `transformer_81` when `payload["_fault_inject"]` is present (Fault Type C)
- Kafka publish timeout or broker unavailability (destination-stage failure)

`failure_stage` values: `transform`, `destination`.

#### `zepto_storage_offsets` — Flow 2 Kafka offset checkpoints

```sql
CREATE TABLE IF NOT EXISTS zepto_storage_offsets (
    topic       TEXT,
    partition   INT,
    last_offset BIGINT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (topic, partition)
);
```

One row per `(topic, partition)`. Flow 2 upserts after each successfully written Cassandra batch. On restart, Flow 2 reads per-partition offsets and resumes `CONSUME` from `last_offset + 1` — avoids re-writing records that already landed in Cassandra.

#### `zepto_storage_backlog` — Flow 2 failed Cassandra writes

```sql
CREATE TABLE IF NOT EXISTS zepto_storage_backlog (
    id              BIGSERIAL PRIMARY KEY,
    order_id        TEXT,
    event_id        TEXT,
    kafka_topic     TEXT,
    kafka_partition INT,
    kafka_offset    BIGINT,
    failure_stage   TEXT,
    error_message   TEXT,
    record_payload  JSONB,
    pipeline_run_id TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Includes `kafka_topic`, `kafka_partition`, and `kafka_offset` so a backlog replay tool can seek to the exact Kafka position and reprocess the failed record without scanning from the beginning.

Records routed here by:
- `transformer_88` when `created_at` cannot be parsed as RFC3339 (Fault Type B)
- `gocql` connection failure when Cassandra is unavailable (destination-stage failure)

`failure_stage` values: `transform`, `destination`.

---

## Part 5 — Pipeline Collections

### 5.1 Flow 1 — Cursor Ingestion Flow (REST API → Kafka)

**Purpose**: Poll the Zepto Order Events API using cursor-based pagination and publish each event to Kafka.

**Source connector**: `IClientRESTAPISource` — cursor mode  
**Destination**: Kafka topic `zepto.order.events`  
**Checkpoint**: `zepto_ingestion_cursors` in AuxDB (cursor position per pipeline name)

**Execution model**:
1. Read `last_cursor` from `zepto_ingestion_cursors` (empty string on first run).
2. Call `GET /api/v2/order-events?cursor=<last_cursor>&limit=500`.
3. For each event in the response, run the Flow 1 transformer chain.
4. Publish passing events to Kafka `zepto.order.events`.
5. On successful publish of the page, upsert `last_cursor = response.next_cursor` in AuxDB.
6. If `response.has_more = false`, terminate — cursor feed is exhausted.
7. On restart: read `last_cursor` from AuxDB and resume from step 2.

**Termination condition**: `has_more = false` in the API response.

**Flow 1 transformer chain**:

| Transformer | ID | Responsibility |
|---|---|---|
| `CityValidator` | transformer_81 | Validates `city` is non-empty. Empty city → `nil` return (silent drop, increments records-dropped). `_fault_inject` in payload → error return (routes to `zepto_ingestion_backlog`). |
| `AmountTypeCaster` | transformer_82 | Parses `amount` from `float64` to a typed decimal. Validates it is > 0. |
| `PayloadSerialiser` | transformer_83 | JSON-encodes the `payload` map to a string for Cassandra's `text` column. |

**Fault path — ingestion backlog**: `transformer_81` returns an error on Fault Type C records. The engine routes the record to `zepto_ingestion_backlog` with `failure_stage = transform`.

**Fault path — silent drop**: `transformer_81` returns `nil` on Fault Type A records (empty city). The engine increments the `records-dropped` pipeline metric and moves to the next record. No backlog entry is created — empty-city records are structurally unparseable for Cassandra's `(city, store_id)` partition key.

### 5.2 Flow 2 — Stream Storage Flow (Kafka → Cassandra)

**Purpose**: Consume order events from Kafka and write them to Cassandra with a 90-day TTL.

**Source connector**: Kafka consumer — topic `zepto.order.events`, 3 partitions  
**Destination**: Cassandra `zepto_events.order_events`  
**Checkpoint**: `zepto_storage_offsets` in AuxDB (per-partition Kafka offsets)

**Execution model**:
1. Read per-partition offsets from `zepto_storage_offsets` (start from partition beginning on first run).
2. Consume a batch of messages from Kafka (`CONSUME` from each partition's `last_offset + 1`).
3. For each message, run the Flow 2 transformer chain.
4. Write passing records to Cassandra `zepto_events.order_events` with `USING TTL 7776000` (90 days).
5. On successful Cassandra write, upsert `last_offset` per partition in AuxDB.
6. On restart: read offsets from AuxDB and resume consuming from the last committed position.

**Flow 2 transformer chain**:

| Transformer | ID | Responsibility |
|---|---|---|
| `TimestampParser` | transformer_88 | Parses `created_at` string (RFC3339) into a typed `time.Time`. Unparseable strings (e.g., `"INVALID_TIMESTAMP"`) → error return (routes to `zepto_storage_backlog`). |
| `EventIDParser` | transformer_89 | Parses `event_id` hex string into a Cassandra-compatible UUID. Malformed IDs → error return. |
| `RunIDStamper` | transformer_90 | Stamps `run_id` from pipeline context onto each record. Used for per-run traceability in Cassandra. |

**Fault path — storage backlog**: `transformer_88` returns an error on Fault Type B records. The engine routes the record to `zepto_storage_backlog` with `failure_stage = transform`, including the Kafka coordinates (`topic`, `partition`, `offset`) so the record can be replayed precisely.

---

## Part 6 — ICursorRESTAPISource Implementation

The Go connector package implements `coreinterface.IClientRESTAPISource` in cursor mode for the Zepto Order Events API.

### 6.1 Interface Method Mapping

| Interface Method | Zepto Usage |
|---|---|
| `GenerateCursorRequest` | Constructs `GET /api/v2/order-events?cursor=<cursor>&limit=500` with `X-Internal-Token` header |
| `FetchRecords` | Parses the JSON response body; extracts the `events` array; returns `[]map[string]any` |
| `NextCursor` | Reads `next_cursor` and `has_more` from the response body; returns `("", false)` when `has_more = false` |
| `GeneratePaginateRequest` | Not used — Zepto API is cursor-based, not offset-based |
| `GenerateWebhookRequest` | Not used — Zepto API has no push mechanism |
| `StreamRecords` | Not used — Flow 1 polls on demand |

### 6.2 `GenerateCursorRequest` — Parameter Mapping

```
Input (models.RESTAPISourceFetch):
  Cursor   string  -- "" on first call; "seq_500", "seq_1000", ... on subsequent pages

Output (RESTAPICursorTune):
  Path     = "/api/v2/order-events"
  Method   = "GET"
  Headers  = {"X-Internal-Token": "<token>"}
  Params   = {"cursor": Cursor, "limit": "500"}
  RecordsPath = "events"  -- dot-path into response JSON
```

### 6.3 `FetchRecords` — Response Parsing

The response body carries a top-level `events` array. `FetchRecords` unmarshals the JSON and returns the array as `[]map[string]any`. Each map key maps directly to the field names in §1.3.

The connector also extracts `next_cursor` and `has_more` from the response body and stores them in connector state for `NextCursor` to consume.

### 6.4 `NextCursor` — Cursor Forwarding

```
NextCursor(body, headers):
  read next_cursor and has_more from connector state (set by FetchRecords)
  if has_more == false:
    return ("", false)
  return (next_cursor, true)
```

The engine passes the returned cursor string back into `GenerateCursorRequest` as `Cursor` on the next iteration. The engine stops calling `GenerateCursorRequest` when `NextCursor` returns `false`.

---

## Part 7 — Orchestration Design (Streamcraft Framework Mapping)

### 7.1 Hierarchy

Two independent pipeline collections — one per flow. Each collection has a single flow with a single pipeline.

```
Collection  = Zepto Order Events Pipeline
  │
  ├── Flow 1 — Cursor Ingestion (pid=28)        [REST API → Kafka]
  │     └── pipeline_order_events_ingestion
  │           source:      Zepto REST API cursor connector
  │           transformers: transformer_81 → transformer_82 → transformer_83
  │           destination: Kafka topic zepto.order.events
  │           checkpoint:  zepto_ingestion_cursors (AuxDB)
  │           backlog:     zepto_ingestion_backlog (AuxDB)
  │
  └── Flow 2 — Stream Storage (pid=29)          [Kafka → Cassandra]
        └── pipeline_order_events_storage
              source:      Kafka topic zepto.order.events (3 partitions)
              transformers: transformer_88 → transformer_89 → transformer_90
              destination: Cassandra zepto_events.order_events
              checkpoint:  zepto_storage_offsets (AuxDB)
              backlog:     zepto_storage_backlog (AuxDB)
```

> **Start order**: Flow 2 (pid=29) must be started before Flow 1 (pid=28). If Flow 1 starts first and publishes events before Flow 2 has created its consumer group, the Kafka topic may auto-rotate offsets past the point where Flow 2 begins reading — events would be silently skipped. Starting the consumer before the producer is standard Kafka practice.

### 7.2 Connectors

| Connector | System | Interface | Used by |
|---|---|---|---|
| `connector_zepto_api` | Zepto REST API | `IClientRESTAPISource` (cursor mode) | Flow 1 source |
| `connector_kafka_producer` | Kafka `zepto.order.events` | `IClientKafkaDestination` | Flow 1 destination |
| `connector_kafka_consumer` | Kafka `zepto.order.events` | `IClientKafkaSource` | Flow 2 source |
| `connector_cassandra` | Cassandra `zepto_events` | `IClientCassandraDestination` | Flow 2 destination |

### 7.3 Concurrency Model

```
Flow 1:  1 cursor poller × 1 Kafka publisher = 1 active goroutine lane
Flow 2:  3 Kafka partitions × 1 consumer each = 3 active goroutine lanes
Aux:     1 AuxDB checkpoint writer (shared by both flows)
─────────────────────────────────────────────────────────
Peak: 5 concurrent goroutine lanes
```

Flow 2 processes the three Kafka partitions in parallel — one goroutine per partition — so the Cassandra write throughput scales linearly with partition count. Checkpoint writes are batched per-partition after each commit.

---

## Part 8 — Seeder Design

The seeder is a Go HTTP server that mimics the Zepto internal Order Events REST API. It serves deterministic, cursor-paginated responses from a pre-generated in-memory event pool.

### 8.1 Seeder Architecture

```
cmd/seeder/
  main.go                   -- starts HTTP server on :11334
  handlers/order_events.go  -- handles GET /api/v2/order-events
  generators/events.go      -- generates deterministic event pool at startup
```

### 8.2 Seeder Startup Sequence

1. Reads `SEEDER_PORT` (default `11334`), `TOTAL_EVENTS` (default `2000`), and `FAULT_RATE` (default `0`) from environment.
2. Calls `generators.GenerateMixed(TOTAL_EVENTS, FAULT_RATE)` — generates the full event pool in memory.
3. Starts HTTP server. All requests serve slices of the in-memory pool.
4. Events are deterministic — modular arithmetic rather than `math/rand` — so the same `TOTAL_EVENTS` and `FAULT_RATE` produce identical pools across restarts. The pipeline can resume from a checkpoint and receive the same data it would have seen on the original run.

### 8.3 Synthetic Data Parameters

| Parameter | Default | Description |
|---|---|---|
| Total events | 2,000 | Size of the in-memory event pool |
| Fault rate | 5% | Percentage of events replaced by fault records |
| Fault type cycle | A → B → C | Round-robin over the three fault types |
| Cities | 7 | bangalore, delhi, mumbai, hyderabad, chennai, pune, kolkata |
| Stores | 23 | 2–5 per city (see §1.4) |
| Event types | 5 | ORDER_CREATED through ORDER_DELIVERED |
| Amount range | ₹100–₹1,000 | Uniform within modular arithmetic bounds |
| Created at base | 25 days ago | Advances 1 minute per event; all events within 90-day TTL window |
| Page limit | max 1,000 | Enforced by handler; default 500 |

### 8.4 Cursor Mechanics

The handler slices the event pool by cursor position:

```
startIdx = parseCursor(cursor)  -- 0 if cursor is empty
end      = min(startIdx + limit, len(pool))
page     = pool[startIdx:end]
hasMore  = end < len(pool)
nextCursor = "seq_<end>"  if hasMore, else ""
```

This means cursor positions are stable across seeder restarts — `seq_500` always refers to index 500 of the same deterministic pool.

### 8.5 Backlog Seeder

`cmd/backlog_seeder` is a standalone tool that directly inserts synthetic failure records into both AuxDB backlog tables, bypassing the pipeline entirely. This allows the `make watch` metrics dashboard to show non-zero backlog counts immediately after `make setup`, without needing to run a full pipeline execution with fault injection.

```
make seed-backlog          # inserts 20 rows into each backlog table
make seed-backlog N=50     # inserts 50 rows into each backlog table
```

Ingestion backlog entries simulate:
- Kafka leader-not-available errors (`failure_stage = destination`)
- `transformer_81` JSON type errors (`failure_stage = transform`)
- Kafka request timeout (`failure_stage = destination`)

Storage backlog entries simulate:
- `transformer_88` timestamp parse failures (`failure_stage = transform`)
- `gocql` no-connections-available errors (`failure_stage = destination`)
- `transformer_88` missing `created_at` field (`failure_stage = transform`)

---

## Part 9 — Metrics Dashboard

`cmd/metrics_watcher` is a live terminal dashboard that polls AuxDB every few seconds and displays the state of both flows.

### 9.1 Dashboard Sections

| Section | AuxDB Table | What It Shows |
|---|---|---|
| **Flow 1 cursor checkpoints** | `zepto_ingestion_cursors` | `pipeline`, `last_cursor`, `updated_at` — confirms Flow 1 is advancing |
| **Flow 2 offset checkpoints** | `zepto_storage_offsets` | `topic`, `partition`, `last_offset`, `updated_at` per partition — confirms Flow 2 is consuming |
| **Backlog counts** | `zepto_ingestion_backlog`, `zepto_storage_backlog` | Row counts; recent ingestion backlog entries with stage and error |

### 9.2 Running It

```bash
go run ./cmd/metrics_watcher             # defaults: 5s interval, local AuxDB
make watch                               # same
make watch INTERVAL=10s                  # 10-second poll
```

### 9.3 Sample Output

```
=== Zepto Order Events Pipeline — Live Metrics [14:32:08] ===

── Flow 1: REST API → Kafka (cursor checkpoints) ────────────────────────────────
  pipeline=order_events_ingestion   cursor=seq_1500             updated=2026-06-17 14:32:05

── Flow 2: Kafka → Cassandra (offset checkpoints) ───────────────────────────────
  topic                           partition   last_offset  updated_at
  zepto.order.events                      0          4,821  2026-06-17 14:32:06
  zepto.order.events                      1          4,803  2026-06-17 14:32:06
  zepto.order.events                      2          4,810  2026-06-17 14:32:06

── Backlogs ──────────────────────────────────────────────────────────────────────
  zepto_ingestion_backlog (Flow 1 failed publishes) : 3
  zepto_storage_backlog   (Flow 2 failed writes)    : 7

  Recent ingestion backlog entries:
    ORD-00000020  event-id-...  transform  2026-06-17 14:31:50  transformer_81: ...

(refreshes every few seconds — Ctrl+C to exit)
```

### 9.4 Signals to Watch

| Signal | Threshold | Likely Cause |
|---|---|---|
| Cursor not advancing | 3+ consecutive ticks | Flow 1 stalled — API unreachable or Kafka publish failing |
| Offset not advancing | Any partition idle 3+ ticks | Flow 2 stalled — Cassandra unreachable or consumer paused |
| Ingestion backlog growing | Rate > 5% of published events | Fault rate high; or `transformer_81` config issue |
| Storage backlog growing | Rate > 5% of consumed events | Bad timestamps in source data; or Cassandra schema mismatch |

---

## Part 10 — Makefile Targets

```makefile
make up           # Start AuxDB + Kafka + Cassandra containers and wait for healthy
make deps         # Download Go module dependencies
make auxdb        # Create AuxDB tables (run once after 'up')
make cassandra    # Create Cassandra keyspace + order_events table (run once after 'up')
make seed         # Start Zepto REST API mock server on :11334 (background)
make seed-stop    # Stop the mock seeder
make seed-backlog # Insert synthetic backlog rows into AuxDB (N=20 per table by default)
make setup        # Full bootstrap: up → auxdb → cassandra
make watch        # Live metrics dashboard (polls AuxDB, Ctrl+C to exit)
make logs         # Tail all container logs
make status       # Show container health
make down         # Stop containers (keep volumes)
make reset        # Stop containers AND destroy volumes (DESTRUCTIVE)
make lint         # Run golangci-lint
```

**Seed overrides:**
```makefile
make seed TOTAL_EVENTS=5000 SEEDER_PORT=11334 FAULT_RATE=10
```

**Pipeline environment variables** (set before running streamcraftexecution):
```bash
ZEPTO_API_BASE_URL=http://localhost:11334
ZEPTO_API_TOKEN=dev-token        # seeder accepts any non-empty value
KAFKA_BROKERS=localhost:9092
CASSANDRA_HOSTS=localhost
AUXDB_DSN=postgresql://etl_user:etl_pass@localhost:5446/auxdb?sslmode=disable
```

---

## Part 11 — Step-by-Step Implementation Tasks

### Phase 1 — Infrastructure Setup

- [ ] **STEP-01** — Write `docker-compose.yml` with 3 services: `auxdb` (Postgres 15, port `5446`), `kafka` (apache/kafka:3.9.0 in KRaft mode, port `9092`), `cassandra` (cassandra:4.1, port `9042`). Add Docker healthchecks: `pg_isready` for AuxDB, `kafka-broker-api-versions.sh` for Kafka, `cqlsh -e 'DESCRIBE KEYSPACES'` for Cassandra. Kafka KRaft config: `KAFKA_PROCESS_ROLES=controller,broker`, `KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093`, `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`.
- [ ] **STEP-02** — Implement `cmd/auxdb_setup/main.go` — connects to AuxDB using the `-dsn` flag and creates all 4 control-plane tables: `zepto_ingestion_cursors`, `zepto_ingestion_backlog`, `zepto_storage_offsets`, `zepto_storage_backlog` (see §4.1). Idempotent: uses `CREATE TABLE IF NOT EXISTS` for all statements.
- [ ] **STEP-03** — Implement Cassandra keyspace and table creation in the Makefile via `docker exec zepto_cassandra cqlsh -e "..."` (see §3.1 and §3.2 for full DDL). Includes `NetworkTopologyStrategy` replication, `TimeWindowCompactionStrategy` compaction, and the `comment` annotation for the 90-day TTL reminder. No Go binary required for this step.
- [ ] **STEP-04** — Implement `internal/config/config.go` — loads all runtime connection parameters from environment variables with documented local-development defaults: `AUXDB_DSN` (port 5446), `KAFKA_BROKERS` (localhost:9092), `CASSANDRA_HOSTS` (localhost), `ZEPTO_API_BASE_URL` (http://localhost:11334), `ZEPTO_API_TOKEN` (dev-token). Single `Load()` function returns a `*Config` struct.

### Phase 2 — Data Seeder

- [ ] **STEP-05** — Implement `cmd/seeder/generators/events.go` — generates `n` deterministic order events. Uses modular arithmetic (no `math/rand`) so the pool is stable across restarts. Each event covers all fields in §1.3: `event_id` (hex UUID-like), `order_id` (ORD-XXXXXXXX), `customer_id` (CUST-XXXXXX), `store_id` (from per-city store list), `city`, `event_type`, `status`, `amount`, `created_at` (RFC3339, base 25 days ago advancing 1 minute per event), `payload` (per-event-type fields from §1.6).
- [ ] **STEP-06** — Implement `generators.GenerateMixed(n, faultPercent)` — extends `Generate(n)` with fault injection. At every `100/faultPercent`-th event, apply the next fault type in the A→B→C cycle: A sets `city=""`, B sets `created_at="INVALID_TIMESTAMP"`, C adds `payload["_fault_inject"]="error"`. `faultPercent=0` returns a clean pool identical to `Generate(n)`.
- [ ] **STEP-07** — Implement `cmd/seeder/handlers/order_events.go` — HTTP handler for `GET /api/v2/order-events`. Validates `X-Internal-Token` header (reject 401 if absent). Parses `cursor` (strip `seq_` prefix, parse integer; default 0) and `limit` (clamp to 1–1000; default 500) from query params. Slices the in-memory event pool and returns `{"events": [...], "next_cursor": "seq_<end>", "has_more": <bool>}` as JSON. Logs each request with cursor position, limit, result count, and `has_more`.
- [ ] **STEP-08** — Implement `cmd/seeder/main.go` — reads `SEEDER_PORT`, `TOTAL_EVENTS`, `FAULT_RATE` from environment. Calls `generators.GenerateMixed` at startup. Registers the order events handler at `/api/v2/order-events` and a health endpoint at `/health`. Starts `http.ListenAndServe`. Logs total events generated and fault count at startup.
- [ ] **STEP-09** — Implement `cmd/backlog_seeder/main.go` — standalone tool to seed both AuxDB backlog tables with `-n` synthetic failure records each. Generates events via `generators.Generate(n*2)`, then inserts `n` rows into `zepto_ingestion_backlog` (cycling through 3 failure-stage/error-message combinations) and `n` rows into `zepto_storage_backlog` (cycling through 3 Cassandra failure patterns). Timestamps stagger backwards in time so the metrics watcher shows realistic `created_at` spread.

### Phase 3 — Zepto API Source Connector

- [ ] **STEP-10** — Scaffold the cursor REST API connector package. Define its state struct: `baseURL string`, `token string`, `nextCursorVal string`, `hasMoreVal bool`. Set `hasMoreVal = true` on initialisation so the first call fires.
- [ ] **STEP-11** — Implement `GenerateCursorRequest` — constructs `GET <baseURL>/api/v2/order-events?cursor=<Cursor>&limit=500`. Sets `X-Internal-Token` header from config. Sets `RecordsPath = "events"` so the engine knows which JSON key contains the records array.
- [ ] **STEP-12** — Implement `FetchRecords` — unmarshals the response JSON body into `map[string]any`. Extracts the `events` array as `[]map[string]any`. Reads `next_cursor` and `has_more` from the response root and stores them in connector state for `NextCursor` to consume.
- [ ] **STEP-13** — Implement `NextCursor` — reads `hasMoreVal` and `nextCursorVal` from connector state. Returns `("", false)` when `hasMoreVal` is `false`. Returns `(nextCursorVal, true)` otherwise. The engine stops pagination when this method returns `false`.
- [ ] **STEP-14** — Stub `GeneratePaginateRequest`, `GenerateWebhookRequest`, and `StreamRecords` with panics: `"Zepto connector: offset pagination not supported"`, `"Zepto connector: webhook mode not supported"`, `"Zepto connector: streaming mode not supported"`. Prevents silent use of unimplemented paths.

### Phase 4 — Kafka Connector

- [ ] **STEP-15** — Implement `connector_kafka_producer` — wraps the Go Kafka client to publish `map[string]any` records to `zepto.order.events`. Partition key: SHA-256 of `city + store_id` modulo partition count, ensuring all events for a store go to the same partition. JSON-serialises each record before publishing. Returns an error on publish timeout or broker unavailability for the engine to route to `zepto_ingestion_backlog`.
- [ ] **STEP-16** — Implement `connector_kafka_consumer` — opens a Kafka consumer on `zepto.order.events` with 3 partitions. On startup, reads per-partition offsets from `zepto_storage_offsets` in AuxDB and seeks each partition to `last_offset + 1`. If no checkpoint exists, starts from the earliest available offset. Returns records as `map[string]any` with injected metadata fields: `_kafka_topic`, `_kafka_partition`, `_kafka_offset`.

### Phase 5 — Cassandra Destination Connector

- [ ] **STEP-17** — Implement `connector_cassandra` — opens a `gocql.Session` to the Cassandra cluster. Exposes `WriteOrderEvent(record map[string]any) error` which executes:
  ```cql
  INSERT INTO zepto_events.order_events
    (city, store_id, event_type, created_at, event_id, order_id, customer_id, status, amount, payload, run_id)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  USING TTL 7776000
  ```
  Returns `gocql` errors directly so the engine can route to `zepto_storage_backlog`. TTL is hardcoded to `7776000` seconds (90 days).
- [ ] **STEP-18** — Add `WriteBatch(records []map[string]any) error` to the Cassandra connector using `gocql` `UnloggedBatch` for throughput. Batch size: 50 records (avoids Cassandra's default 50KB batch warning threshold). On partial batch failure, falls back to single-row inserts so successful records land in Cassandra and only the failing rows go to the backlog.

### Phase 6 — Transformer Chains

- [ ] **STEP-19** — Implement `transformer_81` (`CityValidator`) for Flow 1:
  - If `city == ""`: return `nil` (silent drop — engine increments records-dropped metric).
  - If `payload["_fault_inject"] == "error"`: return error `"transformer_81: fault inject marker present"` — engine routes to `zepto_ingestion_backlog`.
  - Otherwise: pass record through unchanged.
- [ ] **STEP-20** — Implement `transformer_82` (`AmountTypeCaster`) for Flow 1: asserts `amount` is a `float64` > 0. Returns error for zero or negative amounts. Passes through unchanged on success. (Amount is already `float64` from JSON; this transformer validates rather than parses.)
- [ ] **STEP-21** — Implement `transformer_83` (`PayloadSerialiser`) for Flow 1: JSON-marshals `record["payload"]` (a `map[string]any`) to a JSON string and replaces the map with the string. Cassandra stores `payload` as `text`. Returns error only if `json.Marshal` fails (should not occur with a well-typed map).
- [ ] **STEP-22** — Implement `transformer_88` (`TimestampParser`) for Flow 2: parses `record["created_at"]` string as RFC3339 (`time.Parse(time.RFC3339, v)`). Returns error `"transformer_88: parse created_at <value>: ..."` on failure (Fault Type B records hit this path). Stores the parsed `time.Time` back onto the record for the Cassandra connector to use.
- [ ] **STEP-23** — Implement `transformer_89` (`EventIDParser`) for Flow 2: parses `record["event_id"]` hex string into a `gocql.UUID`. Returns error on malformed UUIDs. The Cassandra `event_id uuid` column requires a typed UUID, not a raw string.
- [ ] **STEP-24** — Implement `transformer_90` (`RunIDStamper`) for Flow 2: reads `pipeline_run_id` from pipeline context and stamps it onto the record as `run_id`. Pure enrichment — no error path.

### Phase 7 — Pipeline Control Plane

- [ ] **STEP-25** — Implement Flow 1 cursor checkpoint: after each successfully published page, upsert `zepto_ingestion_cursors` with `pipeline = <name>`, `last_cursor = <response.next_cursor>`, `updated_at = now()`. Use `INSERT ... ON CONFLICT (pipeline) DO UPDATE` (upsert). On startup, query `last_cursor` from this table before making the first API call.
- [ ] **STEP-26** — Implement Flow 2 offset checkpoint: after each batch of Cassandra writes, upsert `zepto_storage_offsets` for each `(topic, partition)` with the highest committed offset in that batch. On startup, query all rows from this table and seek each Kafka partition to `last_offset + 1`.
- [ ] **STEP-27** — Implement Flow 1 ingestion backlog writer: on any transformer or destination error in Flow 1, insert into `zepto_ingestion_backlog` with `order_id`, `event_id`, `failure_stage` (transform/destination), `error_message`, `record_payload` (JSONB-serialised record), `pipeline_run_id`.
- [ ] **STEP-28** — Implement Flow 2 storage backlog writer: on any transformer or destination error in Flow 2, insert into `zepto_storage_backlog` with the same fields as the ingestion backlog plus `kafka_topic`, `kafka_partition`, `kafka_offset` extracted from the `_kafka_*` metadata fields injected by the Kafka consumer connector.
- [ ] **STEP-29** — Implement `TerminateRule` evaluation for both flows. Rules:
  - `ERROR_RATE_BREACH` — backlog rate > 10% of records in the current batch → stop
  - `SOURCE_UNREACHABLE` — HTTP 5xx from API after 3 retries (Flow 1) or Kafka consumer error after 3 retries (Flow 2) → stop
  - `DESTINATION_UNREACHABLE` — Kafka broker unavailable after 3 retries (Flow 1) or Cassandra pool empty after 3 retries (Flow 2) → stop
  - `CURSOR_EXHAUSTED` — `has_more = false` and final page processed → clean stop for Flow 1
  - `IDLE_TIMEOUT` — no records consumed for 30 seconds → stop for Flow 2
  - `MANUAL_KILL` — `FORCE_STOP=true` environment variable set → graceful stop at next checkpoint

### Phase 8 — Observability

- [ ] **STEP-30** — Implement `cmd/metrics_watcher/main.go` — polls AuxDB on a configurable `--interval` (default 5s). Three sections: (1) Flow 1 cursor checkpoints from `zepto_ingestion_cursors`, (2) Flow 2 offset checkpoints from `zepto_storage_offsets`, (3) backlog counts from both tables with recent entries for `zepto_ingestion_backlog` (most recent 5 rows with order_id, event_id, stage, error). Uses `pgx/v5` for AuxDB queries. Clears screen each tick with ANSI escape `\033[H\033[2J`. Exits cleanly on `SIGINT` / `SIGTERM`.
- [ ] **STEP-31** — Add structured logging at key events: page fetched (cursor, count, has_more), Kafka publish batch committed (count, topic, partition), Kafka offset committed (partition, offset), Cassandra batch written (count, latency), backlog record inserted (stage, error), TerminateRule fired (rule name, flow), cursor checkpoint saved (pipeline, cursor), offset checkpoint saved (partition, offset).

### Phase 9 — End-to-End Test Run

- [ ] **STEP-32** — Run `make setup` (Docker up → wait healthy → `make auxdb` → `make cassandra`). Verify all 4 AuxDB tables exist via `psql`. Verify Cassandra keyspace and table exist via `cqlsh -e "DESCRIBE zepto_events.order_events"`.
- [ ] **STEP-33** — Run `make seed` with defaults (`TOTAL_EVENTS=2000 FAULT_RATE=5`). Verify seeder is reachable: `curl -H "X-Internal-Token: dev-token" "http://localhost:11334/api/v2/order-events?limit=10"`. Confirm response shape: `events` array, `next_cursor`, `has_more=true`.
- [ ] **STEP-34** — Run `make seed-backlog`. Verify both AuxDB backlog tables contain 20 rows each. Run `make watch` to confirm the metrics dashboard shows non-zero backlog counts.
- [ ] **STEP-35** — Start Flow 2 (pid=29) first. Verify it creates the Kafka consumer group and begins waiting for messages. Then start Flow 1 (pid=28). Verify Flow 1 begins publishing events and `zepto_ingestion_cursors` shows a non-empty `last_cursor` within the first poll interval.
- [ ] **STEP-36** — After 1–2 minutes of execution, query Cassandra: `SELECT COUNT(*) FROM zepto_events.order_events;`. Confirm rows are accumulating. Query `zepto_storage_offsets` and confirm `last_offset` is advancing on all 3 partitions.
- [ ] **STEP-37** — Verify fault handling: with `FAULT_RATE=5` and `TOTAL_EVENTS=2000`, expect ~33 fault records (5% of 2000, split across 3 fault types). Confirm:
  - ~11 Fault Type A records: silent drops — no backlog rows, but records-dropped counter incremented in pipeline metrics
  - ~11 Fault Type B records: appear in `zepto_storage_backlog` with `failure_stage=transform` and `transformer_88` error
  - ~11 Fault Type C records: appear in `zepto_ingestion_backlog` with `failure_stage=transform` and `transformer_81` error
- [ ] **STEP-38** — Test checkpoint/resume for Flow 1: kill Flow 1 mid-run. Note the `last_cursor` value in `zepto_ingestion_cursors`. Restart Flow 1. Confirm it resumes from that cursor and does not re-publish already-committed pages.
- [ ] **STEP-39** — Test checkpoint/resume for Flow 2: kill Flow 2 mid-run. Note the `last_offset` values in `zepto_storage_offsets`. Restart Flow 2. Confirm it resumes from `last_offset + 1` per partition and does not re-write already-committed records to Cassandra (idempotent upsert by primary key: `(city, store_id, event_type, created_at, event_id)`).
- [ ] **STEP-40** — Test TTL enforcement: insert a synthetic Cassandra row directly via `cqlsh` with `USING TTL 10` (10 seconds). Wait 12 seconds. Confirm the row is no longer returned by `SELECT`. This validates TWCS compaction and TTL are working correctly in the local cluster.
- [ ] **STEP-41** — Full pipeline run: `make reset` → `make setup` → `make seed TOTAL_EVENTS=2000 FAULT_RATE=5` → start Flow 2 → start Flow 1 → monitor via `make watch` until Flow 1 terminates (cursor exhausted). Verify final Cassandra row count is approximately `2000 - 11` (total events minus silent-drop Fault Type A records). Verify `zepto_ingestion_backlog` has ~11 rows and `zepto_storage_backlog` has ~11 rows.

---

## Summary Reference

| Dimension | Detail |
|---|---|
| Source | Zepto internal REST API — cursor-based pagination, `GET /api/v2/order-events`, auth via `X-Internal-Token` |
| Event types | 5: ORDER_CREATED, ORDER_CONFIRMED, ORDER_PICKED, ORDER_DISPATCHED, ORDER_DELIVERED |
| Cities / stores | 7 cities, 23 dark stores |
| Intermediate buffer | Kafka (KRaft, single broker), topic `zepto.order.events`, 3 partitions, 7-day retention |
| Destination | Cassandra 4.1, keyspace `zepto_events`, table `order_events`, TTL 90 days, TWCS compaction |
| Partition key | `(city, store_id)` — store-scoped time-series queries hit a single partition |
| Clustering | `event_type ASC, created_at DESC, event_id ASC` |
| Flows | 2: Cursor Ingestion (REST API → Kafka) + Stream Storage (Kafka → Cassandra) |
| Pipelines | 1 per flow = 2 total |
| Flow 1 transformer chain | `transformer_81` (CityValidator) → `transformer_82` (AmountTypeCaster) → `transformer_83` (PayloadSerialiser) |
| Flow 2 transformer chain | `transformer_88` (TimestampParser) → `transformer_89` (EventIDParser) → `transformer_90` (RunIDStamper) |
| Fault types | A: silent drop (empty city) · B: storage backlog (invalid timestamp) · C: ingestion backlog (fault-inject payload) |
| AuxDB tables | 4: `zepto_ingestion_cursors`, `zepto_ingestion_backlog`, `zepto_storage_offsets`, `zepto_storage_backlog` |
| Control plane | Per-flow checkpointing (cursor for Flow 1, per-partition offsets for Flow 2) + separate backlogs per flow |
| Seeder | Go HTTP mock server on `:11334`, deterministic event pool, cursor-stable responses |
| Implementation steps | 41 steps across 9 phases |
| New patterns vs Case 3 | Cursor API (not offset), Kafka as durable buffer, Cassandra time-series destination, two-backlog architecture, TWCS + per-row TTL |
