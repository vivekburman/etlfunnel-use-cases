# Zomato Platform Order Intelligence — ETL Pipeline Case Study Plan
### PostgreSQL (Multi-Source, WAL + Direct) → Redis Stream → Elasticsearch | Streamcraft Execution Framework

---

## Overview

This case study models a real-world order intelligence platform for a large food-tech and quick-commerce conglomerate — modelled after **Zomato** and its sub-brands. Four independent product lines — **Zomato Food, Blinkit, Hyperpure, and District** — each maintain their own PostgreSQL databases with divergent order schemas, status state machines, and fulfilment models.

The pipeline is built on the Streamcraft Execution framework and covers two distinct ingestion paths:

- **Cold Flow** — Direct paginated SELECT from each source Postgres DB → Elasticsearch. Covers one year of historical order data that predates WAL enablement.
- **Hot Flow** — Postgres logical replication (WAL) → Redis Streams → Elasticsearch. Covers all new order events from the moment WAL is enabled forward.

The destination is a single Elasticsearch index (`platform_orders`) that powers cross-brand analytics, ops dashboards, and business intelligence — with records bucketed by sub-brand, city zone, SLA status, time-of-day, order value band, and fulfilment type.

The architectural centrepiece of this case study is the **WAL enablement problem**: WAL was not active historically, so historical data must be backfilled via direct DB reads while the live stream is simultaneously active. The overlap window (records that appear in both flows) is resolved via Elasticsearch upsert using a composite document ID of `{sub_brand}_{order_id}`.

---

## Part 1 — Source Architecture (PostgreSQL)

### 1.1 Source Databases

Four independent PostgreSQL instances, one per sub-brand:

| Sub-brand | DB Name | Port | Business Domain |
|---|---|---|---|
| Zomato Food | `zomato_food_db` | 5441 | Restaurant food delivery |
| Blinkit | `blinkit_db` | 5442 | Quick commerce — 10-minute grocery |
| Hyperpure | `hyperpure_db` | 5443 | B2B restaurant supply chain |
| District | `district_db` | 5444 | Live events and experiences ticketing |

Each DB has four core tables with a shared logical purpose but divergent column names, data types, status enumerations, and domain-specific fields.

### 1.2 Core Source Tables (per sub-brand)

| Table | Purpose |
|---|---|
| `orders` | Master order record — placed, value, status, timestamps, brand-specific fulfilment fields |
| `order_items` | Line items belonging to each order — quantity, price, brand-specific product identifiers |
| `order_status_events` | Append-only event log of all status transitions for each order |
| `delivery_assignments` | Fulfilment agent assignment — rider, truck, or venue depending on sub-brand |

### 1.3 Per-Brand Schema Divergence

This divergence — analogous to Case 1's per-carrier column naming differences — is the primary transformer design challenge.

**`orders` table — brand-specific columns:**

| Column concept | Zomato Food | Blinkit | Hyperpure | District |
|---|---|---|---|---|
| Fulfilment source | `restaurant_id` | `dark_store_id` | `supplier_id` | `venue_id` |
| Catalogue reference | `cuisine_type` | `slot_type` | `invoice_number` | `event_id` |
| Prep / promise time | `prep_time_secs` | `promise_minutes` | `delivery_window_days` | `event_date` |
| Special flag | `meal_type` | `is_scheduled` | `bulk_order_flag` | `seat_category` |
| Item count proxy | *(from order_items)* | *(from order_items)* | *(from order_items)* | `ticket_count` |

**`delivery_assignments` table — fulfilment agent divergence:**

| Field | Zomato Food | Blinkit | Hyperpure | District |
|---|---|---|---|---|
| Agent identifier | `rider_id` | `rider_id` | `truck_id` | *(none — venue attendance)* |
| Assignment type | human rider | human rider | vehicle dispatch | gate scan |
| SLA anchor | `delivered_at` | `delivered_at` | `received_at` | `event_date` |

> **District is structurally unique**: it has no delivery agent. The `delivery_assignments` table for District tracks gate scan events (`scanned_at`, `gate_id`) rather than rider dispatch. This forces the `SLACalculator` and `FulfilmentTypeMapper` transformers to branch on sub-brand.

### 1.4 Order Status State Machines

Each sub-brand uses a different status vocabulary — a common pattern in micro-service architectures where teams own their own enumerations.

| Brand | Status progression |
|---|---|
| Zomato Food | `PLACED` → `ACCEPTED` → `PREPARING` → `PICKED_UP` → `DELIVERED` / `CANCELLED` |
| Blinkit | `PLACED` → `PICKING` → `PACKED` → `OUT_FOR_DELIVERY` → `DELIVERED` / `CANCELLED` |
| Hyperpure | `PLACED` → `CONFIRMED` → `DISPATCHED` → `IN_TRANSIT` → `RECEIVED` / `REJECTED` |
| District | `BOOKED` → `PAYMENT_CONFIRMED` → `TICKET_ISSUED` → `ATTENDED` / `REFUNDED` / `NO_SHOW` |

The `OrderStatusNormaliser` transformer maps all brand-specific statuses to a canonical set used in Elasticsearch: `pending`, `in_progress`, `completed`, `cancelled`.

### 1.5 The WAL Enablement Problem

When the pipeline is first deployed, WAL (logical replication) is **not yet active** on any source DB. The following sequence is required:

1. **Enable WAL**: Set `wal_level = logical` on all four Postgres instances. Requires a restart.
2. **Create replication slots**: One logical replication slot per brand (`zomato_food_slot`, `blinkit_slot`, `hyperpure_slot`, `district_slot`).
3. **Create publications**: `CREATE PUBLICATION {brand}_pub FOR TABLE orders, order_items, order_status_events, delivery_assignments`
4. **Save LSN bookmark**: Before the cold backfill starts, record the current WAL LSN for each brand to AuxDB `wal_positions` table. The hot flow will replay only from this LSN forward.
5. **Start cold backfill**: Cold flow reads historical data via direct SELECT, paginated by `order_id`.
6. **Start hot flow**: Simultaneously, the WAL consumer begins streaming from the saved LSN. Redis stream producers publish events; consumer group indexes to Elasticsearch.
7. **Overlap resolution**: Records written today appear in both flows. Elasticsearch upsert with `_id = {sub_brand}_{order_id}` ensures idempotent writes — whichever flow lands first wins, the second is a no-op update.

The `cmd/wal_enabler` program automates steps 2–4 and persists the LSN bookmark before returning.

### 1.6 Partitioning Strategy — City-Based Sharding

Unlike Case 1's geographic zone+state sharding, Case 2 partitions by **city**, which is the natural operational unit for a delivery platform. Each brand's orders table is partitioned by city at source.

**Cities covered (10 metro + tier-2 markets):**

| Zone | Cities |
|---|---|
| North | Delhi, Jaipur, Lucknow |
| South | Bengaluru, Chennai, Hyderabad |
| West | Mumbai, Pune, Ahmedabad |
| East | Kolkata |

Each brand's source tables are partitioned as:
```
orders_delhi_1, orders_delhi_2 ...    (1M-row splits, same as Case 1 split convention)
orders_mumbai_1, orders_mumbai_2 ...
```

The `orchestrator_1` (cold flow) discovers all city splits for a given brand entity via `information_schema.tables` — identical to Case 1's dynamic table discovery pattern.

The `orchestrator_2` (hot flow) discovers the replication slot and Redis stream for a given brand and registers consumers for each city's event stream.

---

## Part 2 — ETL Pipeline Components

### 2.1 Transformer Chain

Two transformer chains exist — one for each flow. The cold chain has 10 transformers; the hot chain adds one additional transformer at the end.

**Shared transformers (cold and hot):**

| Transformer | ID | Responsibility |
|---|---|---|
| `SubBrandTagger` | transformer_1 | Stamps `sub_brand` field on every record (zomato_food / blinkit / hyperpure / district) |
| `UnifiedSchemaMapper_ZomatoFood` | transformer_2 | Maps Zomato Food-specific columns to the unified order model |
| `UnifiedSchemaMapper_Blinkit` | transformer_3 | Maps Blinkit-specific columns to the unified order model |
| `UnifiedSchemaMapper_Hyperpure` | transformer_4 | Maps Hyperpure-specific columns to the unified order model |
| `UnifiedSchemaMapper_District` | transformer_5 | Maps District-specific columns to the unified order model (no rider, event_date as SLA anchor) |
| `OrderStatusNormaliser` | transformer_6 | Maps brand-specific status strings → canonical status (`pending` / `in_progress` / `completed` / `cancelled`) |
| `SLACalculator` | transformer_7 | Computes `sla_status` (met / breached / na) per brand SLA rules; District uses event_date vs attended_at |
| `TimeBucketer` | transformer_8 | Maps `placed_at` hour → `meal_period` (breakfast / lunch / snack / dinner / late_night) for food brands; maps `event_category` for District |
| `CityZoneMapper` | transformer_9 | Maps `city_id` → `city_name`, `zone_label` (metro / tier2), `state`; routes unmapped city_ids to backlog |
| `PIIMasker` | transformer_10 | SHA-256 hashes `customer_id`; strips `phone`, `email` fields before Elasticsearch indexing |
| `OrderValueBander` | transformer_11 | Buckets `total_amount` → `order_value_band` (0–100 / 100–500 / 500–2000 / 2000+) |
| `CancellationStageClassifier` | transformer_12 | For cancelled/rejected/refunded orders: determines at which stage cancellation occurred |
| `TypeCaster_PGtoElastic` | transformer_13 | Postgres types → Elasticsearch-compatible types (TIMESTAMPTZ → ISO8601 string, NUMERIC → float64, BOOLEAN → bool) |

**Hot-flow-only transformer:**

| Transformer | ID | Responsibility |
|---|---|---|
| `WALEventUnwrapper` | transformer_14 | Unwraps the WAL change event envelope (INSERT / UPDATE / DELETE operation type, before/after record) before feeding into the shared chain |

Transformers are applied in sequence. A failed transformer sends the record to Backlog with `FailureStageTransform`. Only `WALEventUnwrapper` (transformer_14) runs in the hot flow before the shared chain; cold flow skips it entirely.

**Cold flow transformer order:**
`SubBrandTagger(1)` → `UnifiedSchemaMapper_X(2-5)` → `OrderStatusNormaliser(6)` → `SLACalculator(7)` → `TimeBucketer(8)` → `CityZoneMapper(9)` → `PIIMasker(10)` → `OrderValueBander(11)` → `CancellationStageClassifier(12)` → `TypeCaster_PGtoElastic(13)`

**Hot flow transformer order:**
`WALEventUnwrapper(14)` → *(then same 10 as cold)*

### 2.2 Checkpoints

Checkpoints fire after every committed Elasticsearch bulk-index batch.

**Cold flow checkpoint state:**

| Field | Value |
|---|---|
| `sub_brand` | zomato_food / blinkit / hyperpure / district |
| `city` | delhi / mumbai / bengaluru etc. |
| `entity` | orders / order_items / order_status_events / delivery_assignments |
| `table_split_index` | which numbered city split (1, 2, 3…) |
| `last_processed_pk` | last order_id successfully indexed |
| `batch_id` | batch sequence within this city-split run |
| `flow_type` | cold |
| `phase` | Extract / Transform / Load |
| `timestamp` | wall clock of checkpoint |

**Hot flow checkpoint state:**

| Field | Value |
|---|---|
| `sub_brand` | brand identifier |
| `city` | city shard |
| `entity` | which table's events are being consumed |
| `redis_stream_id` | last Redis stream entry ID acknowledged (`>` cursor for XACK) |
| `wal_lsn` | last confirmed WAL LSN processed |
| `flow_type` | hot |
| `phase` | Stream / Transform / Load |

All checkpoint records write to AuxDB `pipeline_checkpoints`. On resume:
- **Cold flow**: source connector reads `last_processed_pk` and begins SELECT from `WHERE order_id > last_processed_pk`
- **Hot flow**: Redis consumer group resumes from last unacknowledged stream entry ID via `XREADGROUP`

### 2.3 Incident Backlog

Records that cannot be processed are routed to AuxDB `backlog_records` instead of halting the pipeline.

**Backlog triggers:**

- Unmapped `city_id` in `CityZoneMapper` — city not in reference table
- Null or malformed `order_id` — cannot construct Elasticsearch `_id`
- Unresolvable `status` string in `OrderStatusNormaliser` — brand added a new status not yet in the mapping
- Impossible timestamp — `created_at > delivered_at`
- District-specific: `event_date` in the past at booking time (stale ticket seeded for testing)
- Elasticsearch write failure — index unavailable, mapping conflict, bulk reject
- WAL DELETE event for an order not yet indexed (out-of-order replay edge case)

**Backlog record metadata:**

- `sub_brand`, `city`, `entity`, `table_split_index`, `flow_type`
- `batch_id`, `failure_stage` — Transform / Destination / WALUnwrap
- `error_code`, `error_message`
- `raw_record` — full original record JSON (pre-transform, for replay)
- `retry_count`, `status` — PENDING / IN_RETRY / RESOLVED / ABANDONED
- `created_at`, `last_attempted_at`

### 2.4 TerminateRule

Evaluated on a configurable tick interval. Thresholds stored in AuxDB `terminate_rules`.

| Rule | Condition | Action |
|---|---|---|
| `ERROR_RATE_BREACH` | Backlog rate > 10% of batch records | Stop pipeline, preserve checkpoint |
| `SOURCE_UNREACHABLE` | Source Postgres connection failure after N retries | Graceful stop |
| `DESTINATION_SATURATION` | Elasticsearch bulk-index latency > threshold (ms) | Stop, trigger slowify |
| `INTEGRITY_VIOLATION` | Null `order_id` rate > 5% in batch | Stop, flag city-split for source review |
| `REDIS_STREAM_LAG` | Consumer group lag > N entries for > M seconds | Stop hot flow, alert |
| `WAL_SLOT_INACTIVE` | Replication slot inactive / disconnected | Stop hot flow, attempt reconnect |
| `IDLE_TIMEOUT` | No records received for > N seconds | Stop, assume source exhausted |
| `MANUAL_KILL` | Operator sets `force_stop = true` in AuxDB | Graceful stop at next checkpoint |
| `MAX_RECORDS_REACHED` | Total records processed >= configured cap | Clean stop (useful for incremental testing) |

Two new rules (`REDIS_STREAM_LAG` and `WAL_SLOT_INACTIVE`) are specific to Case 2's hot flow and have no equivalent in Case 1.

### 2.5 DestinationWriteTune

Dynamically adjusts Elasticsearch bulk-index batch size based on pipeline health. Config read from AuxDB `write_tune_config` on each ticker tick.

**Speedify mode (off-peak / backfill):**
- Bulk batch size: 5,000 documents per request
- Refresh interval: set to `-1` (disable live refresh during bulk load)
- Concurrent bulk workers: 4
- No inter-batch sleep

**Slowify mode (peak hours / Elastic under load):**
- Bulk batch size: 50 documents per request
- Refresh interval: `30s`
- Concurrent bulk workers: 1
- Inter-batch sleep: configurable ms

**Time-of-day schedule:** Automatic speedify 22:00–09:00 IST; slowify 09:00–22:00 IST (same pattern as Case 1).

For the **hot flow**, slowify also throttles the Redis `XREADGROUP` COUNT parameter — fewer messages fetched per poll — to reduce downstream Elastic pressure while maintaining stream consumption continuity.

### 2.6 Redis Stream Architecture (Hot Flow)

One Redis Stream per sub-brand, fed by a WAL consumer process per brand:

| Stream | Producer | Consumer Group |
|---|---|---|
| `zomato:orders:stream` | `wal_consumer_zomato_food` | `elastic_writer_group` |
| `blinkit:orders:stream` | `wal_consumer_blinkit` | `elastic_writer_group` |
| `hyperpure:orders:stream` | `wal_consumer_hyperpure` | `elastic_writer_group` |
| `district:orders:stream` | `wal_consumer_district` | `elastic_writer_group` |

Each WAL consumer:
1. Reads from the brand's logical replication slot via `pgoutput` protocol
2. Decodes INSERT / UPDATE / DELETE change events
3. Publishes to the brand's Redis stream as a JSON-encoded message
4. Acknowledges WAL consumption only after successful Redis publish

The consumer group `elastic_writer_group` has multiple consumers that can be scaled horizontally. Each consumer uses `XREADGROUP` with `XACK` after successful Elasticsearch upsert. Pending entries (unacknowledged) are recovered via `XAUTOCLAIM` on consumer restart.

**Stream message format:**
```json
{
  "op": "INSERT",
  "table": "orders",
  "city": "delhi",
  "lsn": "0/1A2B3C4D",
  "ts": "2026-05-11T10:32:00Z",
  "after": { ...full order row as JSON... }
}
```

### 2.7 Auxiliary DB

AuxDB is a dedicated Postgres instance (port 5445) serving as the operational backbone for both flows.

**Tables in AuxDB:**

| Table | Purpose |
|---|---|
| `pipeline_checkpoints` | Per-shard checkpoint state for both cold and hot flows |
| `backlog_records` | Failed/deferred records with full metadata |
| `wal_positions` | LSN bookmark per brand — saved before cold backfill starts; used by hot flow to know where to begin |
| `backfill_progress` | Cold flow progress per brand / entity / city — tracks whether backfill is complete for each combination |
| `city_mapping` | Reference table: city_id → city_name, zone_label, state, tier (metro/tier2) |
| `brand_sla_rules` | SLA thresholds per brand — promise_minutes, sla_breach_definition; read by `SLACalculator` at transform time |
| `terminate_rules` | Configurable termination thresholds per pipeline name |
| `write_tune_config` | Runtime-tunable batch sizes, concurrency limits, refresh intervals |
| `es_write_log` | Per-batch Elasticsearch bulk index results — success count, failure count, retry count |
| `backfill_completion_log` | Timestamp when each brand/entity/city cold backfill completed — used by ops to confirm handoff to hot flow |

---

## Part 3 — Destination Architecture (Elasticsearch)

### 3.1 Index Design

Single index: **`platform_orders`**

Document ID: `{sub_brand}_{order_id}` — globally unique across all four brands, enables idempotent upserts from both cold and hot flows.

### 3.2 Index Mappings

```json
{
  "mappings": {
    "properties": {
      "order_id":            { "type": "keyword" },
      "sub_brand":           { "type": "keyword" },
      "city":                { "type": "keyword" },
      "zone_label":          { "type": "keyword" },
      "state":               { "type": "keyword" },
      "order_status":        { "type": "keyword" },
      "canonical_status":    { "type": "keyword" },
      "fulfilment_type":     { "type": "keyword" },
      "sla_status":          { "type": "keyword" },
      "meal_period":         { "type": "keyword" },
      "event_category":      { "type": "keyword" },
      "order_value_band":    { "type": "keyword" },
      "cancellation_stage":  { "type": "keyword" },
      "total_amount":        { "type": "float" },
      "item_count":          { "type": "integer" },
      "placed_at":           { "type": "date" },
      "completed_at":        { "type": "date" },
      "promised_minutes":    { "type": "integer" },
      "actual_minutes":      { "type": "integer" },
      "customer_id_hash":    { "type": "keyword" },
      "flow_type":           { "type": "keyword" },
      "indexed_at":          { "type": "date" }
    }
  },
  "settings": {
    "number_of_shards": 5,
    "number_of_replicas": 1,
    "refresh_interval": "30s"
  }
}
```

### 3.3 Analytics Bucketing

The index is designed for Elasticsearch aggregations across these dimensions:

| Bucket | Field | Values |
|---|---|---|
| Sub-brand | `sub_brand` | zomato_food / blinkit / hyperpure / district |
| City zone | `zone_label` | metro / tier2 |
| City | `city` | delhi / mumbai / bengaluru / chennai / hyderabad / pune / jaipur / lucknow / ahmedabad / kolkata |
| Status | `canonical_status` | pending / in_progress / completed / cancelled |
| SLA | `sla_status` | met / breached / na |
| Time of day | `meal_period` | breakfast / lunch / snack / dinner / late_night |
| District category | `event_category` | concert / comedy / sport / dining_experience / other |
| Fulfilment | `fulfilment_type` | food_delivery / quick_commerce / b2b_supply / live_event |
| Value band | `order_value_band` | 0–100 / 100–500 / 500–2000 / 2000+ |
| Cancellation | `cancellation_stage` | pre_accept / post_accept / post_pickup / pre_event / post_event |

### 3.4 Write Safety

- All writes use Elasticsearch `_bulk` API with `index` action — creates or fully replaces document at `_id`
- Idempotent by construction: re-running any pipeline segment produces the same document
- Cold and hot flow may upsert the same `_id` — last writer wins, which is acceptable since both carry the same source truth
- `indexed_at` field records when the document was last written (useful for diagnosing cold vs hot flow coverage)

---

## Part 4 — Orchestration Design (Streamcraft Framework Mapping)

### 4.1 Hierarchy

```
Collection  = Zomato Platform Order Intelligence Job
  │
  ├── Cold Flow Group (flows 1–4)
  │     ├── flow_1  = zomato_food COLD
  │     │     ├── pipeline_orders
  │     │     ├── pipeline_order_items
  │     │     ├── pipeline_order_status_events
  │     │     └── pipeline_delivery_assignments
  │     ├── flow_2 = blinkit COLD           (same 4 pipelines)
  │     ├── flow_3 = hyperpure COLD         (same 4 pipelines)
  │     └── flow_4 = district COLD          (same 4 pipelines)
  │
  └── Hot Flow Group (flows 5–8)
        ├── flow_5 = zomato_food HOT
        │     ├── pipeline_orders
        │     ├── pipeline_order_items
        │     ├── pipeline_order_status_events
        │     └── pipeline_delivery_assignments
        ├── flow_6 = blinkit HOT            (same 4 pipelines)
        ├── flow_7 = hyperpure HOT          (same 4 pipelines)
        └── flow_8 = district HOT           (same 4 pipelines)
```

Total pipelines: **8 flows × 4 entity pipelines = 32 pipelines**

### 4.2 Connectors

| Connector ID | System | Type | Interface |
|---|---|---|---|
| connector_1 | `zomato_food_db` | Postgres source | `IClientDBPostgresSource` |
| connector_2 | `blinkit_db` | Postgres source | `IClientDBPostgresSource` |
| connector_3 | `hyperpure_db` | Postgres source | `IClientDBPostgresSource` |
| connector_4 | `district_db` | Postgres source | `IClientDBPostgresSource` |
| connector_5 | Elasticsearch | Destination | `IClientElasticsearchDestination` |

**Cold flow iso-entities (connector_1 example — zomato_food):**

| iso_entity ID | Entity | Source table pattern |
|---|---|---|
| iso_entity_1 | orders | `orders_{city}_{n}` |
| iso_entity_2 | order_items | `order_items_{city}_{n}` |
| iso_entity_3 | order_status_events | `order_status_events_{city}_{n}` |
| iso_entity_4 | delivery_assignments | `delivery_assignments_{city}_{n}` |

Connectors 2, 3, 4 follow the same pattern with iso_entity IDs 5–16.

**Hot flow iso-entities (connector_1 hot — zomato_food):**

| iso_entity ID | Entity | Redis stream |
|---|---|---|
| iso_entity_17 | orders (hot) | `zomato:orders:stream` |
| iso_entity_18 | order_items (hot) | `zomato:orders:stream` (filtered by table field) |
| iso_entity_19 | order_status_events (hot) | `zomato:orders:stream` |
| iso_entity_20 | delivery_assignments (hot) | `zomato:orders:stream` |

Hot iso-entities 21–32 cover blinkit, hyperpure, district hot flows.

**Destination iso-entities (connector_5 — Elasticsearch):**

| iso_entity ID | Target |
|---|---|
| iso_entity_33 | `platform_orders` index (cold write) |
| iso_entity_34 | `platform_orders` index (hot write) |

### 4.3 Orchestrators

**orchestrator_1 (Cold Flow — PK range discovery):**
- Queries `information_schema.tables` on each source Postgres for all city-split tables matching `^{entity}_{city}_[0-9]+$`
- Returns one `PipelineOrchestratorTune` per discovered table split
- Same pattern as Case 1's cold flow orchestrator

**orchestrator_2 (Hot Flow — Redis stream & consumer registration):**
- Verifies Redis stream exists and consumer group `elastic_writer_group` is registered (`XGROUP CREATE ... MKSTREAM`)
- Returns one `PipelineOrchestratorTune` per brand stream
- Reads last acknowledged stream ID from AuxDB `pipeline_checkpoints` for resume

### 4.4 Peak Parallelism Model

```
Cold: 4 brands × 4 entities × up to 3 city splits in-flight = up to 48 concurrent cold workers
Hot:  4 brands × 4 streams × 1 consumer each               = 16 concurrent hot consumers
Aux:  1 checkpoint lane + 1 backlog lane                   = 2 auxiliary lanes
──────────────────────────────────────────────────────────────────────────────────
Peak: ~66 concurrent goroutine lanes
```

In practice, `DestinationWriteTune` slowify caps concurrency during peak hours to protect Elasticsearch.

---

## Part 5 — Step-by-Step Implementation Tasks

### Phase 1 — Infrastructure Setup

- [ ] **STEP-01** — Write `docker-compose.yml` with 7 services: 4 Postgres source DBs (ports 5441–5444), 1 AuxDB Postgres (port 5445), 1 Redis (port 6379), 1 Elasticsearch single-node (port 9200). Set `wal_level=logical` and `max_replication_slots=10` in Postgres configs via `command` override.
- [ ] **STEP-02** — Implement `cmd/pg_schema/main.go` — creates source schemas on all 4 Postgres DBs. Each brand gets 4 tables (orders, order_items, order_status_events, delivery_assignments) partitioned by city (`orders_delhi_1`, `orders_mumbai_1` etc.). Creates split `_1` only; subsequent splits are created dynamically by the seeder.
- [ ] **STEP-03** — Implement `cmd/es_schema/main.go` — creates the `platform_orders` Elasticsearch index with full field mappings (§3.2) and settings. Idempotent: uses `PUT /{index}` with `if_not_exists` check.
- [ ] **STEP-04** — Implement `cmd/auxdb_setup/main.go` — creates all 10 AuxDB tables (§2.7) and seeds reference data: `city_mapping` (10 cities), `brand_sla_rules` (one row per brand), initial `terminate_rules` (one row per pipeline), and default `write_tune_config`.
- [ ] **STEP-05** — Implement `cmd/wal_enabler/main.go` — for each brand's Postgres DB: creates logical replication slot (`pg_create_logical_replication_slot`), creates publication (`CREATE PUBLICATION`), reads current LSN (`pg_current_wal_lsn()`), writes LSN bookmark to AuxDB `wal_positions`. Must run BEFORE cold backfill starts.
- [ ] **STEP-06** — Implement `internal/brands/brands.go` — defines the 4 brand structs (name, db name, port, redis stream, replication slot name), the 10 cities with zone labels, and the `SplitRowCap` constant. Replaces Case 1's `internal/sharding/shards.go`.
- [ ] **STEP-07** — Implement `internal/config/config.go` — DSN builders for Postgres (source + AuxDB), Redis address, Elasticsearch address. Consistent with Case 1's config package structure.

### Phase 2 — Data Seeder

- [ ] **STEP-08** — Implement `cmd/seeder/main.go` — generates 1 year of synthetic order data across all 4 brands and 10 cities. Target: ~50K orders/city/brand at default scale. Parallelises by brand × city with configurable worker count. Creates dynamic city split tables when `SplitRowCap` (1,000,000) is reached.
- [ ] **STEP-09** — Implement per-brand data generators within the seeder — each brand generates orders with its own column vocabulary, status values, and domain-specific fields. Zomato Food generates restaurant_id + cuisine_type + meal_type; Blinkit generates dark_store_id + slot_type + promise_minutes; Hyperpure generates supplier_id + invoice_number + bulk_order_flag; District generates venue_id + event_id + event_date + seat_category.
- [ ] **STEP-10** — Inject intentional data quality issues into the seeder:
  - ~3% orders with null `customer_id` (triggers PIIMasker backlog)
  - ~5% orders with duplicate `order_id` within the same city (tests Elasticsearch upsert idempotency)
  - ~2% orders with unmapped `city_id` values (triggers CityZoneMapper backlog)
  - ~1% orders with `created_at > delivered_at` (impossible timestamp, triggers TypeCaster backlog)
  - District only: ~4% bookings with `event_date` in the past (stale tickets for backlog testing)
  - ~2% Blinkit orders with `promise_minutes = 0` (invalid SLA anchor, triggers SLACalculator backlog)

### Phase 3 — Cold Flow Connectors

- [ ] **STEP-11** — Implement source connectors for cold flow: `connector_1` through `connector_4` — one per brand. Each connector implements `IClientDBPostgresSource` with `GenerateQuery()` that reads `table` from `ReplicaProps` (set by `orchestrator_1`) and returns `SELECT * FROM {table} WHERE order_id > {last_pk} ORDER BY order_id` for resumable extraction. `GenerateBinLog()` panics (not used in cold flow).
- [ ] **STEP-12** — Implement cold flow source iso-entities (`iso_entity_1` through `iso_entity_16`) — 4 entities × 4 brands = 16 packages. Each is a thin stub that satisfies the connector interface for its specific table and brand. The `GenerateQuery()` method is inherited from the connector; the iso-entity provides the `entityBaseName` (e.g., `orders`) for orchestrator table discovery.
- [ ] **STEP-13** — Implement destination connector for Elasticsearch: `connector_5` with iso-entities `iso_entity_33` (cold) and `iso_entity_34` (hot). Uses Elasticsearch `_bulk` API. Document ID constructed as `{sub_brand}_{order_id}`. Implements `BulkIndex()` and `FlushBuffer()` methods.

### Phase 4 — Hot Flow Connectors

- [ ] **STEP-14** — Implement hot flow source iso-entities (`iso_entity_17` through `iso_entity_32`) — 4 entities × 4 brands = 16 packages. Each reads from its brand's Redis stream via `XREADGROUP`. Returns decoded JSON messages as `map[string]any`. Filters by the `table` field in the stream message to isolate entity-specific events.
- [ ] **STEP-15** — Implement Redis producer processes (one per brand) as part of `cmd/hot_flow/main.go`. Each producer: connects to the brand's Postgres logical replication slot, decodes `pgoutput` change events, publishes to the brand's Redis stream with the message format defined in §2.6. Handles reconnection on slot disconnect.
- [ ] **STEP-16** — Implement `orchestrator_2` — registers the consumer group in Redis (`XGROUP CREATE ... MKSTREAM`), reads the last stream ID from AuxDB checkpoint, and returns `PipelineOrchestratorTune` for each brand stream.

### Phase 5 — Transformer Chain

- [ ] **STEP-17** — Implement `transformer_1` (`SubBrandTagger`) — reads `sub_brand` from `TransformerProps.State` (set by pipeline context) and stamps it on the record. Simple pass-through enrichment.
- [ ] **STEP-18** — Implement `transformer_2` through `transformer_5` (`UnifiedSchemaMapper_*`) — one per brand. Each maps brand-specific column names to the unified model. e.g., for Zomato Food: `restaurant_id` → `fulfilment_source_id`, `cuisine_type` → `catalogue_label`, `meal_type` → `order_subtype`. District mapper sets `fulfilment_type = live_event` and maps `event_date` → `sla_anchor`. All four mappers produce identical output field names.
- [ ] **STEP-19** — Implement `transformer_6` (`OrderStatusNormaliser`) — reads a hardcoded brand → status → canonical_status mapping table (loaded from AuxDB `brand_sla_rules` at startup). Unmapped status strings route to backlog with error code `UNKNOWN_STATUS`.
- [ ] **STEP-20** — Implement `transformer_7` (`SLACalculator`) — branches on `sub_brand`:
  - Zomato Food / Blinkit: `actual_minutes = (delivered_at - placed_at).Minutes(); sla_status = met if actual_minutes <= promise_minutes`
  - Hyperpure: `sla_status = met if received_at within delivery_window_days of placed_at`
  - District: `sla_status = met if attended_at is not null (event was attended); na if refunded before event; breached if no_show`
- [ ] **STEP-21** — Implement `transformer_8` (`TimeBucketer`) — for food/blinkit/hyperpure: maps `placed_at` hour → `meal_period` (00:00–10:00 = breakfast, 10:00–14:00 = lunch, 14:00–18:00 = snack, 18:00–23:00 = dinner, 23:00–00:00 = late_night). For District: copies `event_category` from source (concert / comedy / sport / dining_experience / other).
- [ ] **STEP-22** — Implement `transformer_9` (`CityZoneMapper`) — reads `city_mapping` reference table from AuxDB at startup (cached). Maps `city_id` integer → `city`, `zone_label`, `state`, `tier`. Routes records with unmapped `city_id` to backlog with error code `UNKNOWN_CITY`.
- [ ] **STEP-23** — Implement `transformer_10` (`PIIMasker`) — SHA-256 hashes `customer_id` field (stored as `customer_id_hash`); removes `phone`, `email`, `address` fields entirely. Idempotent: if `customer_id` is already hashed (hot flow update of a cold-indexed record), detect the hash prefix and skip re-hashing.
- [ ] **STEP-24** — Implement `transformer_11` (`OrderValueBander`) — buckets `total_amount` into `order_value_band`: `<100` → "0–100", `100–500` → "100–500", `500–2000` → "500–2000", `>=2000` → "2000+". Sets `order_value_band = na` for District (event ticket pricing is in `ticket_count × face_value` which is surfaced separately).
- [ ] **STEP-25** — Implement `transformer_12` (`CancellationStageClassifier`) — only activates if `canonical_status = cancelled`. Determines stage from `order_status_events` history embedded in record (or looked up from AuxDB if not embedded): `pre_accept`, `post_accept`, `post_pickup` for food brands; `pre_event`, `post_event` for District.
- [ ] **STEP-26** — Implement `transformer_13` (`TypeCaster_PGtoElastic`) — converts Postgres output types to Elasticsearch-compatible Go types: `time.Time` → ISO8601 string, `pgtype.Numeric` → `float64`, `[]byte` → base64 string, `nil` → omitted field.
- [ ] **STEP-27** — Implement `transformer_14` (`WALEventUnwrapper`) — hot flow only. Parses the Redis stream message JSON, extracts `op` (INSERT/UPDATE/DELETE), `table`, and `after` (the new row). For DELETE events, constructs a tombstone record and routes to backlog with `failure_stage = WALDelete` (Elasticsearch does not support soft-delete via upsert without explicit handling). Returns the `after` record for downstream transformers.
- [ ] **STEP-28** — Wire transformer chains in all 32 pipeline bridge files. Cold bridges: transformers 1 → 2/3/4/5 → 6 → 7 → 8 → 9 → 10 → 11 → 12 → 13. Hot bridges: 14 → then same chain.

### Phase 6 — Pipeline Control Plane

- [ ] **STEP-29** — Implement `checkpoint_1` — writes state to AuxDB `pipeline_checkpoints` with the extended schema (§2.2). Cold flow records `last_processed_pk`; hot flow records `redis_stream_id` and `wal_lsn`. Resume logic: cold connectors query `last_processed_pk` before first SELECT; hot connectors query `redis_stream_id` before first `XREADGROUP`.
- [ ] **STEP-30** — Implement `backlog_1` — writes failed records to AuxDB `backlog_records`. Extended schema includes `flow_type` (cold/hot) and `redis_stream_id` (for hot flow replay). Always returns `ActionContinue` unless AuxDB connection itself fails. Also writes to `es_write_log` on Elasticsearch bulk-index partial failures (some docs in a batch rejected).
- [ ] **STEP-31** — Implement `terminate_1` — extends Case 1's 8 rules with 2 new hot-flow rules: `REDIS_STREAM_LAG` (consumer group lag check via `XINFO GROUPS`) and `WAL_SLOT_INACTIVE` (checks `pg_replication_slots` view for `active = false`). All 9 rules read thresholds from AuxDB `terminate_rules` on each tick.
- [ ] **STEP-32** — Implement `destinationwrite_1` — extends Case 1's speedify/slowify with Elasticsearch-specific tuning: controls `refresh_interval` via Elasticsearch Index Settings API (set to `-1` in speedify, `30s` in slowify). Also throttles Redis `XREADGROUP COUNT` for hot flow during slowify.

### Phase 7 — Backfill Completion Handoff

- [ ] **STEP-33** — Implement backfill completion detection: after each cold flow pipeline finishes a brand/entity/city combination, write a row to AuxDB `backfill_completion_log`. A separate goroutine monitors this table; when all 4 brands × 4 entities × 10 cities = 160 combinations are complete, it marks the overall cold backfill as done.
- [ ] **STEP-34** — On cold backfill completion, run a final Elasticsearch `_refresh` call to make all cold-indexed documents immediately searchable. Log the refresh timestamp to AuxDB `backfill_completion_log` with `is_es_refreshed = true`.
- [ ] **STEP-35** — Verify no checkpoint gaps between cold and hot flow: compare the `last_processed_pk` from the final cold checkpoint for each brand/city against the `wal_lsn` at WAL enablement time (stored in `wal_positions`). Records created between WAL LSN bookmark and the first cold checkpoint batch start are the overlap window — Elasticsearch upsert handles them. Log this gap analysis to AuxDB.

### Phase 8 — Observability

- [ ] **STEP-36** — Implement `cmd/metrics_watcher/main.go` — polls AuxDB and Elasticsearch every N seconds. Displays live dashboard with 8 sections (see §6.2). Uses `pgx/v5` for AuxDB queries and Elasticsearch `_cat/count` + `_stats` APIs for destination metrics. Adapts Case 1's metrics_watcher pattern for dual-flow architecture.
- [ ] **STEP-37** — Add structured logging at key lifecycle events: WAL slot connected/disconnected, Redis producer published N events, consumer group lag delta, checkpoint written, backlog routed, TerminateRule fired, backfill phase complete, Elasticsearch refresh triggered.

### Phase 9 — End-to-End Test Run

- [ ] **STEP-38** — Run `make setup` (full bootstrap: Docker up → pg_schema → es_schema → auxdb_setup → wal_enabler → seed).
- [ ] **STEP-39** — Run cold flow smoke test: single brand (zomato_food), single city (delhi), single entity (orders) only. Verify checkpoint written, records indexed in Elasticsearch, backlog count is within expected threshold.
- [ ] **STEP-40** — Verify intentional bad records from seeder: null customer_id, unmapped city_id, impossible timestamps — all should appear in AuxDB `backlog_records` with correct `error_code`.
- [ ] **STEP-41** — Run hot flow smoke test: run `wal_enabler` again to confirm slot is active, then insert 100 test orders directly into `zomato_food_db`, verify they appear in Elasticsearch within 5 seconds.
- [ ] **STEP-42** — Test overlap: insert 10 orders into `zomato_food_db` that have `order_id` values already indexed by the cold flow. Verify Elasticsearch document count does NOT increase (upsert, not insert) and `indexed_at` is updated.
- [ ] **STEP-43** — Test TerminateRule: artificially set `force_stop = true` in AuxDB `terminate_rules` for one pipeline. Verify graceful stop and checkpoint preservation. Re-run and verify resume from last PK.
- [ ] **STEP-44** — Test DestinationWriteTune: manually set `throttle_schedule_start = now()` in AuxDB. Verify bulk batch size drops from turbo (5000) to throttle (50) within one tick interval.
- [ ] **STEP-45** — Run full parallel cold backfill: all 4 brands × 10 cities × 4 entities = 160 pipelines. Monitor via `make watch`. Verify backfill_completion_log fills up and final Elasticsearch `_refresh` fires.
- [ ] **STEP-46** — Run hot flow for all 4 brands simultaneously. Monitor Redis stream lag via `make watch`. Verify consumer group lag stays under 1000 entries at default seeder throughput.
- [ ] **STEP-47** — Final validation: query Elasticsearch for order count per sub-brand and compare against source Postgres counts (allowing for backlog and intentional bad records). Verify counts match within ±1%.

---

## Part 6 — Live Metrics Monitor

### 6.1 Purpose

Continuous visibility into both cold and hot flow progress simultaneously. The `metrics_watcher` is a standalone Go program that polls AuxDB and Elasticsearch on a configurable interval and prints a live terminal dashboard until Ctrl+C.

### 6.2 What It Monitors

| Section | Data Source | What It Tells You |
|---|---|---|
| **Cold checkpoint progress** | `auxdb.pipeline_checkpoints (flow_type=cold)` | Per brand/city/entity: last committed PK, records processed, delta since last tick |
| **Hot checkpoint progress** | `auxdb.pipeline_checkpoints (flow_type=hot)` | Per brand/city/entity: last Redis stream ID, WAL LSN, events consumed per tick |
| **Redis stream lag** | Redis `XINFO GROUPS` | Consumer group pending count per stream — key hot flow health signal |
| **Elasticsearch doc counts** | `GET /platform_orders/_count` | Total indexed documents; breakdown by sub_brand via terms aggregation |
| **Backlog summary** | `auxdb.backlog_records` | Count by flow_type × failure_stage × status |
| **Backfill completion** | `auxdb.backfill_completion_log` | How many of 160 brand/entity/city combos have completed cold backfill |
| **Write tune config** | `auxdb.write_tune_config` | Current batch sizes, active mode (speedify/slowify) |
| **ES write log** | `auxdb.es_write_log` | Last N bulk index results — success rate, rejection count per batch |

### 6.3 Running It

```bash
go run ./cmd/metrics_watcher \
  --auxdb  "host=localhost port=5445 dbname=auxdb user=etl_user password=etl_pass sslmode=disable" \
  --es     "http://localhost:9200" \
  --redis  "localhost:6379" \
  --interval 5s
```

| Flag | Default | Description |
|---|---|---|
| `--auxdb` | localhost:5445/auxdb | AuxDB Postgres connection string |
| `--es` | http://localhost:9200 | Elasticsearch base URL |
| `--redis` | localhost:6379 | Redis address |
| `--interval` | `5s` | Poll interval |

### 6.4 Sample Output

```
══════════════════════════════════════════════════════════════════════════════════════
  ZOMATO PLATFORM ORDER INTELLIGENCE  —  2026-05-11 18:45:02  (tick #24)
══════════════════════════════════════════════════════════════════════════════════════

  COLD FLOW — CHECKPOINT PROGRESS
  Brand          City         Entity                   Split   Last PK    Rows (+/tick)
  ────────────────────────────────────────────────────────────────────────────────────
  zomato_food    delhi        orders                       1   823,410    823,410 (+8,420)
  zomato_food    mumbai       orders                       1   744,100    744,100 (+7,820)
  blinkit        bengaluru    orders                       1   512,340    512,340 (+5,230)
  hyperpure      delhi        orders                       1   102,100    102,100 (+1,020)
  district       mumbai       orders                       1    48,230     48,230 (+  480)
  ...

  COLD BACKFILL COMPLETION:  38 / 160 brand×entity×city combos complete

  HOT FLOW — STREAM LAG
  Stream                      Pending   Delivered  Consumers
  ────────────────────────────────────────────────────────────
  zomato:orders:stream              0   1,204,830          2
  blinkit:orders:stream            12     843,100          2
  hyperpure:orders:stream           0      98,200          1
  district:orders:stream            0      44,500          1

  ELASTICSEARCH DOC COUNTS
  platform_orders (total):    4,201,840
  ├── zomato_food:             1,823,410
  ├── blinkit:                 1,344,100
  ├── hyperpure:                 986,200
  └── district:                   48,130

  BACKLOG SUMMARY
  Flow   Stage          Status        Count
  ──────────────────────────────────────────
  cold   Transform      PENDING         312
  cold   Destination    PENDING          18
  hot    WALUnwrap      PENDING           4
  cold   Transform      RESOLVED         89

  Total records processed: 4,201,840  |  Total backlog: 423  |  Backlog rate: 0.010%

  WRITE TUNE CONFIG  —  Normal: 1,000  |  Turbo: 5,000  |  Throttle: 50  |  Mode: SPEEDIFY
```

### 6.5 What to Watch For

| Signal | Threshold | Likely Cause |
|---|---|---|
| Redis stream lag climbing | > 1,000 pending | Elasticsearch slowify active, or Elastic unavailable |
| Cold checkpoint delta = 0 | Any shard idle 3+ ticks | TerminateRule triggered, source Postgres connection lost |
| Backlog rate climbing | Approaching 10% | CityZoneMapper failures (check city_mapping reference), status normaliser gap (new brand status added) |
| Backfill completion stalled | Not incrementing | Cold flow terminate triggered, check backlog for that brand/city |
| ES doc count < expected | Any | Partial bulk-index failures — check es_write_log rejection counts |
| WAL slot inactive | Hot flow lag = 0 and stream not growing | Replication slot dropped, re-run wal_enabler |

---

## Part 7 — New Patterns vs Case 1

This section documents what Case 2 introduces that Case 1 did not have — the architectural teaching moments.

| Pattern | Case 1 | Case 2 |
|---|---|---|
| Source DB type | MySQL | PostgreSQL |
| Source read method | Full table scan (shards) | Direct SELECT (cold) + WAL CDC (hot) |
| Streaming buffer | None | Redis Streams with consumer groups |
| Destination type | PostgreSQL (`raw` schema) | Elasticsearch (single index) |
| Dual-flow coordination | None | Cold + hot overlap resolved by ES upsert |
| WAL enablement | Not required | Explicit LSN bookmark before backfill |
| Schema divergence axis | Carrier column naming | Sub-brand column vocabulary + status enums |
| Partitioning dimension | Geographic zone + state | City-based splits |
| SLA computation | Not applicable | Per-brand SLA rules in AuxDB |
| District fulfilment | N/A | Rideless model — venue attendance, event_date anchor |
| Control plane new rules | 8 terminate rules | 9 terminate rules (+REDIS_STREAM_LAG, +WAL_SLOT_INACTIVE) |
| Destination write API | Postgres upsert | Elasticsearch `_bulk` index |
| Idempotency key | Destination PK constraint | `_id = {sub_brand}_{order_id}` |
| Observability additions | AuxDB + Dest DB | AuxDB + Elasticsearch + Redis stream lag |

---

## Summary Reference

| Dimension | Detail |
|---|---|
| Source DBs | 4 PostgreSQL instances (Zomato Food, Blinkit, Hyperpure, District) |
| Source read method | Cold: direct SELECT paginated by order_id. Hot: WAL logical replication → Redis Streams |
| City partitioning | 10 cities across 4 zones. Splits at 1M rows cap (same convention as Case 1) |
| Total Flows | 8 (4 brands × cold + 4 brands × hot) |
| Total Pipelines | 32 (8 flows × 4 entity pipelines each) |
| Peak Concurrency | Up to 48 cold workers + 16 hot consumers = 64 concurrent lanes |
| Transformers | 14 transformers — 13 shared cold/hot + 1 hot-only WAL unwrapper |
| Control Plane | TerminateRule (9 rules) + DestinationWriteTune (speedify/slowify with ES refresh control) |
| Checkpoints | Per brand/entity/city/split — cold tracks last PK; hot tracks Redis stream ID + WAL LSN |
| Backlog | AuxDB-backed, includes flow_type and redis_stream_id for hot-flow replay |
| Redis | 4 streams (one per brand) + 1 consumer group (`elastic_writer_group`) |
| Destination | Elasticsearch `platform_orders` index — bulk upsert, `_id = {sub_brand}_{order_id}` |
| AuxDB Tables | 10 operational tables (adds wal_positions, backfill_progress, city_mapping, brand_sla_rules, es_write_log, backfill_completion_log over Case 1's set) |
| Implementation Steps | 47 steps across 9 phases |
| New patterns vs Case 1 | WAL CDC, Redis Streams, Elasticsearch destination, dual-flow overlap resolution, District rideless model |
