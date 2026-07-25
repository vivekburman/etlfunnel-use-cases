# Fulfillment Center Inventory Ops Pipeline — ETL Case Study Plan
### Parquet + Avro Directory-of-Parts File Landing Zones → MongoDB | Streamcraft Execution Framework

---

## Overview

This case study models a real-world batch inventory ingestion pipeline for a large Indian logistics/fulfillment operator — modelled after **Delhivery** — that operates a network of fulfillment centers (FCs) storing inventory on behalf of marketplace sellers. Unlike Cases 1–5, neither upstream system exposes a live API, database, or message stream. Both sources are **files landing on disk**, produced by systems that were never built to be queried directly:

- A modern Spark data platform computes a nightly **inventory snapshot** per FC and writes it as Spark/Hive-style numbered Parquet part files (`part-00000.parquet`, `part-00001.parquet`, ...) into a fresh dated directory each night.
- A legacy on-prem **Warehouse Management System (WMS)** — inherited from an FC acquisition years ago and never decommissioned — exports **stock movement events** (putaway, pick, return, damage, adjustment) throughout the day as Avro object-container-format (OCF) part files, appended continuously into a single flat directory that never rotates.

Both feeds need to land in **MongoDB** so the new "FC Inventory Ops" dashboard can show current stock levels and full movement history without ever touching the Spark lake or the legacy WMS export share directly.

The pipeline is built on the Streamcraft Execution framework and covers **two independent pipeline flows**:

- **Flow 1 — Parquet Snapshot Ingestion Flow**: Full-scans the Parquet part-file directory for a given day's snapshot, validates and casts each row, and upserts it into MongoDB's `inventory_snapshots` collection. Checkpoints the last delivered `(part, row)` position in AuxDB, keyed by directory, so a restart resumes mid-directory without re-reading or skipping rows.
- **Flow 2 — Avro Movement Ingestion Flow**: Full-scans the (continuously growing) Avro part-file directory, validates each movement record, and upserts it into MongoDB's `stock_movements` collection. Checkpoints the same way, but because WMS keeps dropping new part files into the same directory all day, this flow's checkpoint never "finishes" the way Flow 1's does — it just keeps advancing across an ever-longer part sequence.

The flows share no bus and have no start-order dependency — each reads its own directory and writes its own MongoDB collection. This is a deliberate contrast with Case 4's Kafka-mediated flows and Case 5's shared-topic fan-in: file-based ingestion has no natural fan-in point, so consolidation happens at the MongoDB layer (dashboard-time joins by `fc_id` + `sku_id`), not in the pipeline.

The central engineering challenges of this case are:

1. **The connector no longer discovers files — the client does, and resume walks an index into that list, not a cursor or offset.** Every file scan tune (`ParquetSourceScanTune`, `AvroSourceScanTune`, ...) carries an explicit `Files []string` field; `Connect` just stores config, and `ReadByFullScan` reads exactly the files it's handed, in the order it's handed them — it never scans `Directory` on its own. `StartAfterPart` is a **0-based index into that `Files` slice**, not a parsed Spark part-sequence number. This pushes directory discovery into `GenerateScan` (client code) and makes the client responsible for returning the *same ordering* on every call — a persisted index is only meaningful if the list it indexes into is reproduced identically each run.
2. **One fixed `Directory` per connector, but Flow 1's data rotates to a new sub-directory every day.** `DBParquetConfig`/`DBAvroConfig` each carry a single `Directory` string, resolved once at connect time — it cannot be re-pointed at a new `dt=<date>/` sub-folder on its own each day. So Flow 1's `Directory` (in `collection.json`'s `source.connectionParams`) is fixed at the **stable base path** (`FC_SNAPSHOT_DIR`), and `GenerateScan` builds each `Files` entry as `dt=<date>/part-00000.parquet` — relative to that stable base, with the day's sub-folder baked into the entry itself — rather than assuming `Directory` already points at the dated folder. Flow 2's `Directory` has no such problem: WMS never rotates its export folder, so `Directory` and the AuxDB checkpoint key are the same fixed path for the life of the pipeline, and `Files` entries are bare filenames.
3. **No watch/poll mode in the framework, but also no live connection to keep open.** `ReadByFullScan` reads exactly the `Files` list `GenerateScan` resolved at the start of a run, then closes its own channel once every file/row has been delivered — there's no filesystem watcher, but also nothing analogous to a REST/Kafka connector's persistent connection to hold open while waiting for more data. Both flows terminate the same way: the channel closes on its own, and `PerformOperations` treats "channel closed, no failure, no terminate-rule stop" as a normal finish. The real difference between the flows is **invocation cadence, not termination logic** — Flow 1 is invoked once per day against a fresh directory and reaches a genuine "this directory is done" state (`fc_snapshot_progress.status='complete'`), while Flow 2 is meant to be re-invoked periodically (cron/manual trigger) against the same growing directory, and every invocation just processes whatever new part files exist at that moment before finishing the same way. `MaxPipelineTime` on both flows' terminate rules is a safety net for a wedged run, not the mechanism that ends a normal one.
4. **Format-native typing, business-rule faults instead of parse faults.** Parquet and Avro both carry their own schema (columnar types for Parquet, an OCF header schema for Avro) — there's no delimiter, encoding, or malformed-JSON class of failure to defend against. Every fault in this case is a *valid, well-typed* value that is *semantically* wrong (negative quantity, unknown enum member), caught by the transformer chain rather than the file reader.
5. **Idempotent upsert as the correctness mechanism**, since files can be legitimately re-scanned (checkpoint reset, backfill) without a natural "already seen" guard the way a Kafka consumer group offset gives you. MongoDB's `UPDATE_ONE` + `Upsert: true` on a deterministic key makes re-delivery safe.
6. **`DBMongoConfig.Collection` is fixed at connect time**, so one Mongo destination connector can only ever write one collection — `inventory_snapshots` and `stock_movements` each need their own connector/connection-profile pair, not a single shared connector branching on which pipeline is calling.

---

## Part 1 — Source Architecture (File Landing Zones)

### 1.1 Directory Layout & Part-File Convention

Both sources follow the Spark/Hive-style numbered part-file convention (`<prefix>-00000.<ext>`, default prefix `"part"`). The framework's `helper` package (`helper.PartPath`, `helper.NextPartSeq`, `helper.ListParts`, `helper.ClearDirectory`) formats and discovers this naming on the **write** side (destination connectors use it to find where to keep appending) — but on the **read** side, the file connector itself no longer scans a directory at all (see §1.5). The seeders in this case study still write with the default `"part"` prefix specifically so this case's `GenerateScan` implementations can reuse `helper.ListParts` unmodified to build their `Files` list, rather than inventing bespoke discovery logic:

```
part-<5-digit-zero-padded-sequence>.<ext>
  part-00000.parquet
  part-00001.parquet
  ...
```

| Feed | Configured `Directory` (stable, connect-time) | Actual file location | Rotation |
|---|---|---|---|
| Parquet snapshot | `/data/fc_inventory_snapshots` (`FC_SNAPSHOT_DIR`) | `<Directory>/dt=<YYYY-MM-DD>/part-00000.parquet`, ... | **New sub-directory every night** — one full snapshot of every FC's stock, written by the Spark job. `Directory` itself never changes; the day's `dt=` folder is baked into each `Files` entry (see §1.5). |
| Avro movement | `/data/wms_stock_movements` (`FC_MOVEMENT_DIR`) | `<Directory>/part-000NN.avro` | **Flat, never rotates** — the legacy WMS export job appends new part files as it batches movements throughout the day. Existing parts are never rewritten. `Directory` and the on-disk location are the same path. |

`helper.ListParts` sorts parts ascending by parsed numeric sequence — not filename string — so `part-100000` correctly sorts after `part-99999`. Because this case's part sequences are always contiguous from `0` (the seeders never skip a number), the index a client's `GenerateScan` assigns to each entry in the returned `Files` list happens to equal that file's parsed sequence number — see §1.5 for why that equivalence is a property of *this* data, not a guarantee the framework makes.

### 1.2 Parquet Snapshot Schema

One row per `(fc_id, sku_id)` per day, columnar-typed natively by the Parquet file itself — no delimiter/encoding config, unlike CSV/Excel:

| Field | Type | Description |
|---|---|---|
| `fc_id` | `string` | Fulfillment center code, e.g. `FC-BLR-01` |
| `seller_id` | `string` | Marketplace seller who owns the SKU, e.g. `SEL-000123` |
| `sku_id` | `string` | SKU identifier, e.g. `SKU-00045231` |
| `sku_name` | `string` | Human-readable product name |
| `category` | `string` | One of: Grocery, Electronics, Apparel, Home, Beauty, Personal Care |
| `warehouse_zone` | `string` | Physical zone within the FC: `A`, `B`, `C`, `D` |
| `quantity_on_hand` | `int64` | Units physically present in the zone |
| `quantity_reserved` | `int64` | Units allocated to open orders, not yet picked |
| `unit_cost` | `float64` | Per-unit landed cost, INR |
| `snapshot_date` | `string` (`YYYY-MM-DD`) | The Spark job's as-of date — matches the directory's `dt=` partition |

### 1.3 Avro Movement Schema

One record per stock movement event, schema travels in the OCF file header:

| Field | Type | Description |
|---|---|---|
| `movement_id` | `string` | Globally unique event id (hex) |
| `fc_id` | `string` | Fulfillment center code |
| `sku_id` | `string` | SKU identifier |
| `movement_type` | `string` (enum) | `PUTAWAY`, `PICK`, `RETURN`, `DAMAGE`, `ADJUSTMENT` |
| `quantity_delta` | `int` | Signed stock change (see §1.3.1) |
| `reason_code` | `string`, nullable | Populated only for `DAMAGE`/`ADJUSTMENT`, e.g. `DMG-WATER`, `ADJ-CYCLE-COUNT` |
| `operator_id` | `string` | WMS operator who recorded the movement |
| `recorded_at` | `string` (RFC3339) | Event timestamp per the legacy WMS clock |

#### 1.3.1 Movement Type → Delta Sign Convention

| `movement_type` | Sign | Meaning |
|---|---|---|
| `PUTAWAY` | `+` | Inbound receiving into the zone |
| `PICK` | `-` | Removed from the zone for an order |
| `RETURN` | `+` | Customer return restocked |
| `DAMAGE` | `-` | Written off as unsellable |
| `ADJUSTMENT` | `+` or `-` | Cycle-count correction, either direction |

### 1.4 Fulfillment Centers

6 FCs across 6 cities, each stocking SKUs from many sellers:

| `fc_id` | City |
|---|---|
| `FC-BLR-01` | Bangalore |
| `FC-DEL-01` | Delhi |
| `FC-MUM-01` | Mumbai |
| `FC-HYD-01` | Hyderabad |
| `FC-PNE-01` | Pune |
| `FC-KOL-01` | Kolkata |

~250 SKUs are distributed across the 6 FCs, each carrying a `warehouse_zone` and `seller_id`.

### 1.5 Resume Semantics — Client-Supplied `Files` + `StartAfterPart` / `StartAfterRow`

Both `ParquetSourceScanTune` and `AvroSourceScanTune` carry the same four fields:

```go
type ParquetSourceScanTune struct {
    Files          []string
    RowLimit       int
    StartAfterPart int
    StartAfterRow  int
}
```

- **`Files`** — an explicit, caller-supplied list of file names, resolved relative to the connector's configured `Directory`. **The connector does not scan or discover files on its own** — `Connect` only stores the config; whichever files are named in `Files` are the only files a given `ReadByFullScan` call will ever open. This is what lets the same Parquet/Avro connectors ingest *any* file naming scheme, not just Spark/Hive part files — a case with an oddly-named legacy export could list those names directly instead of relying on `helper.ListParts`.
- **`StartAfterPart`** — a **0-based index into `Files`** (not a parsed part-sequence number). Every entry in `Files` at an index below this value is skipped **entirely** — never opened, never read a single row from. Because it indexes into a list the client itself produced, `GenerateScan` must rebuild that same list, in the same order, on every call — an index persisted from a run where `Files` was `[a, b, c]` is meaningless if a later run passes `[b, c]` or `[c, a, b]`.
- **`StartAfterRow`** — within the resume file itself, this many rows/records are skipped before delivery resumes.
- **`RowLimit`** — caps how many *newly delivered* rows this scan call returns; `0` means read to EOF of the last file in `Files`.

In this case, `GenerateScan` builds `Files` by calling `helper.ListParts(directory, ext)` fresh on every invocation — it returns the directory's `part-*` files sorted ascending by parsed numeric sequence, so the ordering (and therefore what `StartAfterPart` means) is stable across runs, and — because the seeders only ever append contiguous sequence numbers, never skip one — the index into that list happens to equal the file's own `part-NNNNN` sequence number. That equivalence is convenient for this case's checkpoint math, not something the framework guarantees in general.

For Flow 2 (flat, non-rotating directory), `directory` passed to `helper.ListParts` is the same path as the connector's configured `Directory`, so the returned entries are used as bare filenames (`part-00000.avro`) — exactly what `resolvePartPaths` expects. For Flow 1, `directory` is the **dated** path (`<FC_SNAPSHOT_DIR>/dt=<date>`) used only to run `helper.ListParts` against disk — the `Files` entries returned to the framework are rewritten as `dt=<date>/part-00000.parquet`, relative to the connector's actual configured `Directory` (the stable `FC_SNAPSHOT_DIR`), since that's what `resolvePartPaths` will join them against. Both the dated `directory` and the bare `dateSubdir` string come from the pipeline orchestrator's `ReplicaProps` (see §6).

Every delivered row/record carries its position separately from its data — `models.Record` splits the two:

```go
type Record struct {
    Data map[string]any // user-facing data that goes through transformations
    Meta map[string]any // internal metadata preserved throughout the pipeline
}
```

Position is stamped into `Meta`, keyed by two named constants rather than raw string literals:

```go
const (
    MetaFilePart = "_file_part" // 0-based index into the source's Files list
    MetaFileRow  = "_file_row"  // 0-based row index within that file, resets per file
)
```

`TransformerProps.Record`, `CheckpointProps.Records`, and `BacklogProps.Records` all carry the full `*models.Record` — `Data` and `Meta` together — through the transformer chain and into the checkpoint/backlog writers, not `Data` alone. A transformer reads/transforms `param.Record.Data` and returns a `*models.Record`, threading `Meta` forward unchanged unless it deliberately overwrites it; nothing in the chain forces `Meta` to be read-only, it's just convention that transformers don't rewrite engine-stamped position keys. `Meta` still never lands in a MongoDB document by default — the destination stage (`MongoDestQuery`) stays `Data`-only, so nothing changes on the write side. The checkpoint writer reads `record.Meta[models.MetaFilePart]`/`record.Meta[models.MetaFileRow]` directly off the last record in the batch to persist `(part, row)` after every successful destination write, exactly the way Case 4 persists Kafka offsets — just addressing a position in a static, client-enumerated file list rather than a live partition/offset pair or a REST cursor.

Because resume walks `Files` strictly in ascending order, **a single flow cannot parallelize reads across files without losing the ability to resume deterministically** — this is a real constraint versus Case 4's 3-way parallel Kafka partition consumption, and is reflected in §6.3's concurrency model.

### 1.6 Fault Injection

Each flow gets its own two fault classes — one silent drop, one backlog route — cycled at the configured `FAULT_RATE`:

| Flow | Fault | Mutation | Pipeline Effect |
|---|---|---|---|
| Flow 1 (Parquet) | **A** | `sku_id = ""` | `transformer_1` returns `nil` — silently dropped, `records-dropped` metric incremented (an empty SKU can't form the MongoDB upsert key) |
| Flow 1 (Parquet) | **B** | `quantity_on_hand = -1` | `transformer_2` returns an error — routed to `fc_snapshot_backlog` |
| Flow 2 (Avro) | **A** | `fc_id = ""` | `transformer_4` returns `nil` — silently dropped (a movement can't be attributed to any FC) |
| Flow 2 (Avro) | **B** | `movement_type = "UNKNOWN"` | `transformer_5` returns an error — routed to `fc_movement_backlog` |

All four faults are **structurally valid** rows/records from the file reader's point of view — Parquet and Avro both enforce their own schema at the file level, so there is no "malformed file" fault class here, only business-rule violations caught downstream.

---

## Part 2 — Destination Architecture (MongoDB)

### 2.1 Database & Collections

| Database | Collection | Written by | Destination connector |
|---|---|---|---|
| `fc_inventory` | `inventory_snapshots` | Flow 1 | connector_2 / iso_entity_2 |
| `fc_inventory` | `stock_movements` | Flow 2 | connector_4 / iso_entity_4 |

`DBMongoConfig` fixes `Collection` at connect time (alongside `Host`/`Database`), so one connector can only ever target one collection — there is no such thing as a single Mongo destination connector that branches between `inventory_snapshots` and `stock_movements` at write time. Each flow gets its own connector and its own `destination` connection profile in `collection.json`.

### 2.2 `inventory_snapshots`

```js
{
  _id: "FC-BLR-01|SKU-00045231|2026-07-10",   // set by MongoDB on upsert, from the query filter below
  fc_id: "FC-BLR-01",
  seller_id: "SEL-000123",
  sku_id: "SKU-00045231",
  sku_name: "Stainless Steel Water Bottle 1L",
  category: "Home",
  warehouse_zone: "B",
  quantity_on_hand: 214,
  quantity_reserved: 12,
  unit_cost: 187.50,
  snapshot_date: "2026-07-10"
}
```

`transformer_3` (`MongoKeyStamper`) stamps `_mongo_id = "fc_id|sku_id|snapshot_date"` onto the record's `Data`, not `_id` directly — `_mongo_id` is what connector_2's `GenerateQuery` reads to build the `{_id: mongoID}` upsert filter, and MongoDB assigns that filter value as the new document's `_id` on insert. `_mongo_id` doesn't appear in the document: connector_2 strips every underscore-prefixed key from the payload before writing, the same convention Cases 4–5 use for `_kafka_*`/`_es_*` routing keys. File position (`_file_part`/`_file_row`) was never a candidate for stripping in the first place — checkpoint_1 reads it straight off `record.Meta`, so it never touches `Data` and never reaches the destination stage at all.

Unique index: `{ fc_id: 1, sku_id: 1, snapshot_date: 1 }` — matches the stamped key, so a re-scan of the same day's directory upserts in place rather than duplicating.

### 2.3 `stock_movements`

```js
{
  _id: "8f3a1c9e2b4d47a1",                     // set by MongoDB on upsert, from movement_id
  movement_id: "8f3a1c9e2b4d47a1",
  fc_id: "FC-BLR-01",
  sku_id: "SKU-00045231",
  movement_type: "PICK",
  quantity_delta: -2,
  reason_code: null,
  operator_id: "OPR-0042",
  recorded_at: "2026-07-10T09:14:22Z"
}
```

`transformer_6` stamps `_mongo_id = movement_id` (already globally unique, so no composite key needed). Unique index: `{ movement_id: 1 }`. As with `inventory_snapshots`, `_mongo_id` stays off the document — stripped by connector_4's `GenerateQuery` — and file position never reaches `Data` in the first place, so there's nothing else to strip.

### 2.4 Why MongoDB

Parquet snapshot rows and Avro movement records have genuinely different shapes and update patterns — snapshots are replaced wholesale each day per `(fc, sku)`, movements are strictly append-only events. A document store lets each collection's schema evolve independently (Spark can add a snapshot column; WMS can add a movement field) without a migration, and gives the dashboard a single low-latency store for both current-state and event-history queries without a join engine. This is a deliberate contrast with Cases 1–4's relational/columnar/wide-column destinations — file-shaped, semi-structured batch data maps naturally onto documents.

### 2.5 Write Operation Mapping

The framework's Mongo destination bridge interface:

```go
type IClientDBMongoDest interface {
    GenerateQuery(param *models.MongoDestQuery) ([]*models.MongoDestQueryTune, error)
}

type MongoDestQueryTune struct {
    Options   MongoDBWriteOptions
    Query     any
    Payload   any
    Operation MongoWriteOperationType
}
```

Both connectors build the same shape of tune, one per record, differing only in which stamped key they read and which fields they strip:

```go
// connector_2/iso_entity_2 (inventory_snapshots) and connector_4/iso_entity_4
// (stock_movements) both read "_mongo_id" — transformer_3/transformer_6 stamp it
// with different values (a composite key vs. movement_id directly), but the
// connector-side shape is identical:
MongoDestQueryTune{
    Query:     bson.M{"_id": r["_mongo_id"]},
    Payload:   bson.M{"$set": doc}, // doc = r with every "_"-prefixed key stripped
    Operation: models.MongoWriteUpdateOne,
    Options:   models.MongoDBWriteOptions{Upsert: true},
}
```

`MongoWriteUpdateOne` + `Upsert: true` is what makes re-scanning a directory (checkpoint reset, manual backfill, retry after a crash) safe — the same `(part, row)` being delivered twice lands as the same document, not a duplicate.

---

## Part 3 — Control Plane (AuxDB)

AuxDB is a dedicated PostgreSQL instance (port `5446`) holding checkpoint state and backlog records for both flows, same role as in Cases 1–5.

### 3.1 `fc_snapshot_progress` — Flow 1 checkpoints, keyed by directory

```sql
CREATE TABLE IF NOT EXISTS fc_snapshot_progress (
    directory   TEXT PRIMARY KEY,
    last_part   INT NOT NULL DEFAULT 0,  -- index into the Files list GenerateScan built for `directory`, read from record.Meta[MetaFilePart]
    last_row    INT NOT NULL DEFAULT 0,  -- row index within that file, from record.Meta[MetaFileRow]
    status      TEXT NOT NULL DEFAULT 'in_progress',   -- 'in_progress' | 'complete'
    updated_at  TIMESTAMPTZ NOT NULL
);
```

One row per **day's directory** (`/data/fc_inventory_snapshots/dt=2026-07-10/`). A new day gets a new row with `last_part=0, last_row=0` — yesterday's completed directory keeps its own row at `status='complete'` untouched.

### 3.2 `fc_snapshot_backlog` — Flow 1 failed rows

```sql
CREATE TABLE IF NOT EXISTS fc_snapshot_backlog (
    id              BIGSERIAL PRIMARY KEY,
    fc_id           TEXT,
    sku_id          TEXT,
    file_part       INT,           -- from record.Meta[MetaFilePart] — index into that scan's Files list
    file_row        INT,           -- from record.Meta[MetaFileRow]
    failure_stage   TEXT,          -- 'transform' | 'destination'
    error_message   TEXT,
    record_payload  JSONB,
    pipeline_run_id TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### 3.3 `fc_movement_progress` — Flow 2 checkpoint, keyed by directory (never changes)

```sql
CREATE TABLE IF NOT EXISTS fc_movement_progress (
    directory   TEXT PRIMARY KEY,
    last_part   INT NOT NULL DEFAULT 0,  -- index into the Files list GenerateScan built for `directory`
    last_row    INT NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL
);
```

Unlike `fc_snapshot_progress`, this table only ever has **one row** — `/data/wms_stock_movements/` — whose `last_part`/`last_row` keeps climbing as WMS drops new part files in.

### 3.4 `fc_movement_backlog` — Flow 2 failed records

```sql
CREATE TABLE IF NOT EXISTS fc_movement_backlog (
    id              BIGSERIAL PRIMARY KEY,
    movement_id     TEXT,
    fc_id           TEXT,
    file_part       INT,           -- from record.Meta[MetaFilePart]
    file_row        INT,           -- from record.Meta[MetaFileRow]
    failure_stage   TEXT,
    error_message   TEXT,
    record_payload  JSONB,
    pipeline_run_id TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

---

## Part 4 — Pipeline Collections

### 4.1 Flow 1 — Parquet Snapshot Ingestion Flow

**Purpose**: Full-scan a day's Parquet snapshot directory, validate and upsert every row into MongoDB.

**Source connector**: `IParquetNoSQLSource` (connector_1 / iso_entity_1) — `ReadByFullScan`
**Destination**: MongoDB `fc_inventory.inventory_snapshots` (connector_2 / iso_entity_2)
**Checkpoint**: `fc_snapshot_progress`, keyed by directory (checkpoint_1)
**Pipeline orchestrator**: orchestrator_1

**Execution model**:
1. orchestrator_1 resolves `directory` (`<FC_SNAPSHOT_DIR>/dt=<date>`) and `dateSubdir` (`dt=<date>`) and stashes both in `ReplicaProps`.
2. `GenerateScan` reads `(last_part, last_row)` from `fc_snapshot_progress` for that `directory` (`0, 0` if the row doesn't exist yet), lists the directory via `helper.ListParts`, and returns `Files` entries as `dateSubdir/part-NNNNN.parquet`.
3. The engine's `ReadByFullScan` streams every row from those files in order, starting after `(StartAfterPart, StartAfterRow)`.
4. flow_1's bridge runs the transformer chain directly on the full `*models.Record` — `Data` and `Meta` travel together, so there's no separate copy step to make position visible downstream.
5. Upsert passing rows into `inventory_snapshots`.
6. After each successfully written batch, upsert `fc_snapshot_progress` with the highest `(part, row)` read off `record.Meta[MetaFilePart]`/`record.Meta[MetaFileRow]` in that batch.
7. When the source channel closes on its own (every listed file/row delivered) with no failure and no terminate-rule stop, `checkpoint_1.MarkComplete` sets `status='complete'` for that directory — this is a plain "the scan finished normally" signal, not a distinct rule the terminate check polls for.
8. On restart before completion: `GenerateScan` re-reads `(last_part, last_row)` and resumes from step 3.

**Termination**: `terminate_1` registers only a `MaxPipelineTime` safety net (30 min) — a normal run never hits it; it just finishes when its bounded `Files` list is exhausted and the channel closes.

**Flow 1 transformer chain**:

| Transformer | ID | Responsibility |
|---|---|---|
| `SKUValidator` | transformer_1 | `sku_id == ""` → `nil` (silent drop, Fault A). Otherwise passthrough. |
| `QuantityGuard` | transformer_2 | `quantity_on_hand < 0` → error (Fault B, routes to `fc_snapshot_backlog`). Otherwise passthrough. |
| `MongoKeyStamper` | transformer_3 | Stamps `_mongo_id = fc_id + "|" + sku_id + "|" + snapshot_date` for idempotent upsert. |

### 4.2 Flow 2 — Avro Movement Ingestion Flow

**Purpose**: Full-scan the (ever-growing) Avro movement directory, validate and upsert every record into MongoDB.

**Source connector**: `IAvroNoSQLSource` (connector_3 / iso_entity_3) — `ReadByFullScan`
**Destination**: MongoDB `fc_inventory.stock_movements` (connector_4 / iso_entity_4)
**Checkpoint**: `fc_movement_progress` (single row, directory never changes; checkpoint_2)
**Pipeline orchestrator**: orchestrator_2

**Execution model**:
1. orchestrator_2 resolves the fixed `directory` (`FC_MOVEMENT_DIR`) into `ReplicaProps` — no date logic.
2. `GenerateScan` reads `(last_part, last_row)` from the single `fc_movement_progress` row, lists the directory fresh via `helper.ListParts` (so any part files appended since the last invocation are included), and returns bare `part-NNNNN.avro` filenames.
3. The engine's `ReadByFullScan` streams every record after `(StartAfterPart, StartAfterRow)`.
4. flow_2's bridge runs the transformer chain directly on the full `*models.Record`, same as flow_1 — no copy step needed.
5. Upsert passing records into `stock_movements`.
6. After each successfully written batch, upsert `fc_movement_progress` with the highest `(part, row)` read off `record.Meta[MetaFilePart]`/`record.Meta[MetaFileRow]`.
7. The source channel closes once every file `GenerateScan` listed at step 2 has been read — this run ends normally. There is no "mark complete" step, since the directory itself is never done: a later invocation, re-run after `make seed-movement` appends more parts, picks up exactly where this one left off.

**Termination**: `terminate_2` registers only a `MaxPipelineTime` safety net (15 min), same reasoning as Flow 1 — a normal run finishes when the channel closes, not via an active idle/exhaustion check.

**Flow 2 transformer chain**:

| Transformer | ID | Responsibility |
|---|---|---|
| `FCIDValidator` | transformer_4 | `fc_id == ""` → `nil` (silent drop, Fault A). Otherwise passthrough. |
| `MovementTypeValidator` | transformer_5 | `movement_type` not one of `PUTAWAY/PICK/RETURN/DAMAGE/ADJUSTMENT` → error (Fault B, routes to `fc_movement_backlog`). Otherwise passthrough. |
| `MongoKeyStamper` | transformer_6 | Stamps `_mongo_id = movement_id` (already globally unique — no composite key needed). |

---

## Part 5 — Connector Implementation Mapping

### 5.1 `IClientDBParquetSource` — Flow 1 (connector_1 / iso_entity_1)

```go
type IParquetNoSQLSource interface {
    models.IDatabaseEngine
    source.IFileSource[*parquet.File, any, any, models.ParquetSourceScanTune]
}

type IClientDBParquetSource interface {
    GenerateScan(param *models.ParquetSourceScan) (*models.ParquetSourceScanTune, error)
    FetchRecords(param *models.ParquetSourceFetch) <-chan *models.Record
}
```

| Method | Flow 1 Usage |
|---|---|
| `GenerateScan` | Reads `directory`/`dateSubdir` from `param.State.GetReplicaProps()` (stamped by orchestrator_1); reads `(last_part, last_row)` from `fc_snapshot_progress` for that `directory`; calls `helper.ListParts(directory, ".parquet")` to enumerate the dated sub-directory itself (the connector won't); builds each `Files` entry as `filepath.Join(dateSubdir, filepath.Base(p))` so it resolves correctly against the connector's *stable* configured `Directory`; returns `&models.ParquetSourceScanTune{Files: files, RowLimit: 0, StartAfterPart: last_part, StartAfterRow: last_row}` |
| `FetchRecords` | Not used — Flow 1 runs exclusively in `ReadByFullScan` mode (`captureMethod=1`). Stubbed with a closed empty channel. |

### 5.2 `IClientDBAvroSource` — Flow 2 (connector_3 / iso_entity_3)

```go
type IAvroNoSQLSource interface {
    models.IDatabaseEngine
    source.IFileSource[*ocf.Decoder, any, any, models.AvroSourceScanTune]
}

type IClientDBAvroSource interface {
    GenerateScan(param *models.AvroSourceScan) (*models.AvroSourceScanTune, error)
    FetchRecords(param *models.AvroSourceFetch) <-chan *models.Record
}
```

| Method | Flow 2 Usage |
|---|---|
| `GenerateScan` | Reads `directory` from `ReplicaProps` (stamped by orchestrator_2, fixed); reads `(last_part, last_row)` from the single `fc_movement_progress` row; calls `helper.ListParts(directory, ".avro")` fresh on every invocation so newly-appended parts are picked up; returns `&models.AvroSourceScanTune{Files: files, RowLimit: 0, StartAfterPart: last_part, StartAfterRow: last_row}` (bare filenames — `directory` here already equals the connector's configured `Directory`, no sub-folder rewrite needed) |
| `FetchRecords` | Not used — same reasoning as Flow 1. Stubbed similarly. |

### 5.3 `IClientDBMongoDest` — two separate connectors, one per collection

```go
type IClientDBMongoDest interface {
    GenerateQuery(param *models.MongoDestQuery) ([]*models.MongoDestQueryTune, error)
}
```

`DBMongoConfig.Collection` is fixed at connect time alongside `Host`/`Database`, so there is no way for one Mongo destination connector to write to two different collections depending on which pipeline called it. Each flow gets its own connector and connection profile:

| Connector | Collection | Used by | Key read from `Data` |
|---|---|---|---|
| connector_2 / iso_entity_2 | `inventory_snapshots` | Flow 1 | `_mongo_id` (stamped by transformer_3) |
| connector_4 / iso_entity_4 | `stock_movements` | Flow 2 | `_mongo_id` (stamped by transformer_6) |

Both implementations are otherwise identical: build one `MongoDestQueryTune{Query: bson.M{"_id": rec["_mongo_id"]}, Payload: bson.M{"$set": doc}, Operation: models.MongoWriteUpdateOne, Options: models.MongoDBWriteOptions{Upsert: true}}` per record, where `doc` is `rec` with every underscore-prefixed key stripped.

---

## Part 6 — Orchestration Design (Streamcraft Framework Mapping)

### 6.1 Hierarchy

```
Collection  = FC Inventory Ops Pipeline
  │
  ├── Flow 1 — Parquet Snapshot Ingestion (pid=1)   [Parquet dir → MongoDB]
  │     pipeline orchestrator: orchestrator_1 (PID 1)
  │     └── Pipeline 1
  │           source:      connector_1 / iso_entity_1 — Parquet directory (dt=<date>) full scan
  │           transformers: transformer_1 → transformer_2 → transformer_3
  │           destination: connector_2 / iso_entity_2 — MongoDB fc_inventory.inventory_snapshots
  │           checkpoint:  checkpoint_1 → fc_snapshot_progress (AuxDB, keyed by directory)
  │           backlog:     backlog_1 → fc_snapshot_backlog (AuxDB)
  │           terminate:   terminate_1 (MaxPipelineTime safety net only)
  │           destinationWrite: destinationwrite_1
  │
  └── Flow 2 — Avro Movement Ingestion (pid=2)      [Avro dir → MongoDB]
        pipeline orchestrator: orchestrator_2 (PID 2)
        └── Pipeline 1
              source:      connector_3 / iso_entity_3 — Avro directory full scan (flat, never rotates)
              transformers: transformer_4 → transformer_5 → transformer_6
              destination: connector_4 / iso_entity_4 — MongoDB fc_inventory.stock_movements
              checkpoint:  checkpoint_2 → fc_movement_progress (AuxDB, single row)
              backlog:     backlog_2 → fc_movement_backlog (AuxDB)
              terminate:   terminate_2 (MaxPipelineTime safety net only)
              destinationWrite: destinationwrite_2
```

> **Start order**: none. Unlike Case 4 (Kafka consumer must start before producer) and Case 5 (fan-in ordering across sources), Flows 36 and 37 here share no bus and no destination collection — either can start, stop, or be re-run independently with no coordination required.

### 6.2 Connectors

| Connector | System | Interface | Used by |
|---|---|---|---|
| connector_1 / iso_entity_1 | Parquet directory | `IParquetNoSQLSource` / `IClientDBParquetSource` | Flow 1 source |
| connector_2 / iso_entity_2 | MongoDB `fc_inventory.inventory_snapshots` | `IMongoDBDestination` / `IClientDBMongoDest` | Flow 1 destination |
| connector_3 / iso_entity_3 | Avro directory | `IAvroNoSQLSource` / `IClientDBAvroSource` | Flow 2 source |
| connector_4 / iso_entity_4 | MongoDB `fc_inventory.stock_movements` | `IMongoDBDestination` / `IClientDBMongoDest` | Flow 2 destination |

### 6.3 Concurrency Model

```
Flow 1:  1 sequential part-file reader × 1 Mongo writer = 1 active goroutine lane
Flow 2:  1 sequential part-file reader × 1 Mongo writer = 1 active goroutine lane
Aux:      1 AuxDB checkpoint writer (shared by both flows)
─────────────────────────────────────────────────────────
Peak: 3 concurrent goroutine lanes
```

Each flow reads its own directory strictly in ascending part order — resume correctness depends on it, since `StartAfterPart`/`StartAfterRow` addresses a single linear position, not a set of independently-trackable partitions the way Kafka's 3-partition consumer in Case 4 does. This case intentionally has less read parallelism than Case 4; the tradeoff is a much simpler, file-native resume model.

---

## Part 7 — Seeder Design

There is no live API or database to mock here — the "seeder" is a pair of Go CLI tools that **write real Parquet/Avro part files to disk**, using the same `parquet-go` and `hamba/avro/v2/ocf` libraries the framework's own connectors use internally.

### 7.1 Seeder Architecture

```
cmd/snapshot_seeder/
  main.go                      -- writes one day's Parquet directory
  generators/snapshot_rows.go  -- generates deterministic (fc, sku) snapshot rows

cmd/movement_seeder/
  main.go                      -- appends new Avro part files to the flat movement directory
  generators/movement_rows.go  -- generates deterministic movement records

cmd/backlog_seeder/
  main.go                      -- inserts synthetic rows directly into both AuxDB backlog tables
```

### 7.2 Snapshot Seeder — Startup Sequence

1. Reads `SNAPSHOT_DATE` (default: today, `YYYY-MM-DD`), `ROWS_PER_PART` (default `1000`), `TOTAL_ROWS` (default `1500` — 6 FCs × 250 SKUs), and `FAULT_RATE` (default `5`) from environment.
2. Calls `generators.GenerateMixed(TOTAL_ROWS, FAULT_RATE, SNAPSHOT_DATE)` — deterministic modular arithmetic, no `math/rand`, so re-running with the same parameters against the same date produces byte-identical rows.
3. Writes `part-00000.parquet`, `part-00001.parquet`, ... into `<FC_SNAPSHOT_DIR>/dt=<SNAPSHOT_DATE>/`, `ROWS_PER_PART` rows per part.
4. Running the snapshot seeder again for a **new** `SNAPSHOT_DATE` creates a fresh directory — Flow 1 sees it as an entirely new, independent scan.

### 7.3 Movement Seeder — Startup Sequence

1. Reads `NEW_MOVEMENTS` (default `2000`), `ROWS_PER_PART` (default `500`), and `FAULT_RATE` (default `5`) from environment.
2. Lists existing parts in `<FC_MOVEMENT_DIR>/` to find the next free sequence number (mirrors `helper.NextPartSeq`).
3. Calls `generators.GenerateMixed(NEW_MOVEMENTS, FAULT_RATE)`.
4. Appends new `part-000NN.avro` files starting from that sequence number — **existing part files are never touched**, so re-running the movement seeder simulates "WMS exported another batch," and Flow 2's checkpoint naturally picks up only the new parts on its next invocation.

### 7.4 Synthetic Data Parameters

| Parameter | Default | Description |
|---|---|---|
| Fulfillment centers | 6 | See §1.4 |
| SKUs | ~250 | Distributed across FCs and sellers |
| Snapshot rows/run | 1,500 | 6 FCs × 250 SKUs |
| Snapshot rows/part | 1,000 | Parquet part-file size |
| Movements/seeder run | 2,000 | New Avro records appended per invocation |
| Movement rows/part | 500 | Avro part-file size |
| Fault rate | 5% | Split evenly across the flow's two fault types |
| Movement types | 5 | PUTAWAY, PICK, RETURN, DAMAGE, ADJUSTMENT |

### 7.5 Backlog Seeder

`cmd/backlog_seeder` inserts synthetic failure rows directly into `fc_snapshot_backlog` and `fc_movement_backlog`, bypassing the pipeline — same purpose as Case 4's backlog seeder: the metrics dashboard shows non-zero backlog counts immediately after `make setup`, without needing a full fault-injected pipeline run first.

```bash
make seed-backlog          # inserts 20 rows into each backlog table
make seed-backlog N=50     # inserts 50 rows into each backlog table
```

---

## Part 8 — Metrics Dashboard

`cmd/metrics_watcher` polls AuxDB and MongoDB on an interval and displays the state of both flows.

### 8.1 Dashboard Sections

| Section | Source | What It Shows |
|---|---|---|
| **Flow 1 snapshot progress** | `fc_snapshot_progress` | `directory`, `last_part`, `last_row`, `status`, `updated_at` — one row per day processed |
| **Flow 2 movement progress** | `fc_movement_progress` | `last_part`, `last_row`, `updated_at` — confirms Flow 2 is still advancing across the growing directory |
| **MongoDB counts** | `fc_inventory.inventory_snapshots`, `fc_inventory.stock_movements` | `countDocuments()` per collection |
| **Backlog counts** | `fc_snapshot_backlog`, `fc_movement_backlog` | Row counts; recent entries with `failure_stage` and `error_message` |

### 8.2 Running It

```bash
go run ./cmd/metrics_watcher             # defaults: 5s interval, local AuxDB + MongoDB
make watch                               # same
make watch INTERVAL=10s                  # 10-second poll
```

### 8.3 Sample Output

```
=== FC Inventory Ops Pipeline — Live Metrics [09:41:12] ===

── Flow 1: Parquet → MongoDB (snapshot progress) ────────────────────────────
  directory=dt=2026-07-10   part=1   row=412   status=in_progress   updated=09:41:10

── Flow 2: Avro → MongoDB (movement progress) ───────────────────────────────
  directory=/data/wms_stock_movements   part=6   row=213   updated=09:41:09

── MongoDB collection counts ────────────────────────────────────────────────
  fc_inventory.inventory_snapshots : 1,138
  fc_inventory.stock_movements     : 3,204

── Backlogs ──────────────────────────────────────────────────────────────────
  fc_snapshot_backlog (Flow 1 failed rows)    : 4
  fc_movement_backlog (Flow 2 failed records) : 6

(refreshes every few seconds — Ctrl+C to exit)
```

### 8.4 Signals to Watch

| Signal | Threshold | Likely Cause |
|---|---|---|
| Snapshot progress stuck mid-directory | 3+ consecutive ticks | Flow 1 stalled — MongoDB unreachable, or a bad row wedged in a fixed-size batch |
| Movement progress not advancing | No change since the last `make seed-movement` | Normal — Flow 2 already finished the parts that existed at its last invocation; investigate only if new part files exist on disk and a re-run still doesn't advance the checkpoint |
| Snapshot backlog growing | Rate > 5% of rows | Fault rate high, or `transformer_2` (QuantityGuard) threshold misconfigured |
| Movement backlog growing | Rate > 5% of records | Unexpected `movement_type` values — check whether WMS introduced a new enum member upstream (`transformer_5`) |

---

## Part 9 — Makefile Targets

```makefile
make up             # Start AuxDB + MongoDB containers and wait for healthy
make deps           # Download Go module dependencies
make auxdb          # Create AuxDB tables (run once after 'up')
make mongo-init     # Create MongoDB unique indexes on both collections
make seed-snapshot  # Write today's Parquet snapshot directory
make seed-movement  # Append a new batch of Avro movement part files
make seed-backlog   # Insert synthetic backlog rows into AuxDB (N=20 per table by default)
make setup          # Full bootstrap: up → auxdb → mongo-init
make watch          # Live metrics dashboard (polls AuxDB + MongoDB, Ctrl+C to exit)
make logs           # Tail all container logs
make status         # Show container health
make down           # Stop containers (keep volumes)
make reset          # Stop containers AND destroy volumes (DESTRUCTIVE)
make lint           # Run golangci-lint
```

**Seed overrides:**
```makefile
make seed-snapshot SNAPSHOT_DATE=2026-07-11 FAULT_RATE=10
make seed-movement NEW_MOVEMENTS=5000 FAULT_RATE=10
```

**Pipeline environment variables** (set before running streamcraftexecution; also read directly by orchestrator_1/2):
```bash
FC_SNAPSHOT_DIR=/data/fc_inventory_snapshots   # Flow 1 orchestrator resolves <this>/dt=<date>/ per run
SNAPSHOT_DATE=2026-07-10                       # optional override; defaults to today (UTC)
FC_MOVEMENT_DIR=/data/wms_stock_movements      # Flow 2 — fixed, never changes
MONGO_URI=mongodb://localhost:27017
MONGO_DATABASE=fc_inventory
AUXDB_DSN=postgresql://etl_user:etl_pass@localhost:5446/auxdb?sslmode=disable
```

---

## Part 10 — Step-by-Step Implementation Tasks

### Phase 1 — Infrastructure Setup

- [ ] **STEP-01** — Write `docker-compose.yml` with 2 services: `auxdb` (Postgres 15, port `5446`) and `mongo` (`mongo:7`, port `27017`). Add Docker healthchecks: `pg_isready` for AuxDB, `mongosh --eval "db.adminCommand('ping')"` for MongoDB. Mount a host directory into both a `snapshot-seeder` and the pipeline process so `/data/fc_inventory_snapshots` and `/data/wms_stock_movements` are visible to both.
- [ ] **STEP-02** — Implement `cmd/auxdb_setup/main.go` — connects via `-dsn` flag, creates `fc_snapshot_progress`, `fc_snapshot_backlog`, `fc_movement_progress`, `fc_movement_backlog` (see §3). Idempotent `CREATE TABLE IF NOT EXISTS`.
- [ ] **STEP-03** — Implement `cmd/mongo_init/main.go` — connects via `MONGO_URI`, creates the `fc_inventory` database implicitly, and creates the two unique indexes from §2.2/§2.3 with `CreateIndexes`.
- [ ] **STEP-04** — Implement `internal/config/config.go` — loads `FC_SNAPSHOT_DIR`, `FC_MOVEMENT_DIR`, `MONGO_URI`, `MONGO_DATABASE`, `AUXDB_DSN` from environment with local-dev defaults matching §9.

### Phase 2 — Data Seeders

- [ ] **STEP-05** — Implement `cmd/snapshot_seeder/generators/snapshot_rows.go` — generates `n` deterministic `(fc, sku)` snapshot rows via modular arithmetic (no `math/rand`). Covers every field in §1.2, cycling through the 6 FCs and ~250 SKUs.
- [ ] **STEP-06** — Extend with `GenerateMixed(n, faultPercent, snapshotDate)` — injects Fault A (empty `sku_id`) and Fault B (`quantity_on_hand = -1`) alternately at the configured rate. `Generate(n, snapshotDate)` is a thin wrapper calling `GenerateMixed(n, 0, snapshotDate)`.
- [ ] **STEP-07** — Implement `cmd/snapshot_seeder/main.go` — reads `SNAPSHOT_DATE`, `TOTAL_ROWS`, `ROWS_PER_PART`, `FAULT_RATE`. Writes `part-00000.parquet`, `part-00001.parquet`, ... into `<FC_SNAPSHOT_DIR>/dt=<SNAPSHOT_DATE>/` using `parquet.WriteFile[T]` from `github.com/parquet-go/parquet-go`. Logs total rows and parts written.
- [ ] **STEP-08** — Implement `cmd/movement_seeder/generators/movement_rows.go` — generates `n` deterministic movement records covering §1.3, cycling through the 5 movement types with the sign convention from §1.3.1.
- [ ] **STEP-09** — Extend with `GenerateMixed(n, faultPercent)` — injects Fault A (empty `fc_id`) and Fault B (`movement_type = "UNKNOWN"`) alternately. `Generate(n)` wraps `GenerateMixed(n, 0)`.
- [ ] **STEP-10** — Implement `cmd/movement_seeder/main.go` — reads `NEW_MOVEMENTS`, `ROWS_PER_PART`, `FAULT_RATE`. Lists existing parts in `FC_MOVEMENT_DIR` to find the next free sequence (a local regexp-based equivalent of `helper.NextPartSeq`, since this seeder is its own Go module and doesn't import the framework), then writes new `part-000NN.avro` files using `ocf.NewEncoder` from `github.com/hamba/avro/v2/ocf`, **never touching existing parts**. Logs how many new parts were appended and the new highest sequence number.
- [ ] **STEP-11** — Implement `cmd/backlog_seeder/main.go` — `-n` flag, inserts synthetic rows into `fc_snapshot_backlog` (cycling transform/destination failure messages for Fault B and a MongoDB write-timeout scenario) and `fc_movement_backlog` (cycling transform/destination messages for Fault B and a MongoDB connection-pool-exhausted scenario).

### Phase 3 — Parquet Source Connector

- [ ] **STEP-12** — Scaffold `client/connectors/connector_1/iso_entity_1` implementing `IClientDBParquetSource` as a stateless `IUseConnector{}`, exposed via a package-level `Instance` (matching the framework's convention for source bridges even though nothing here needs cross-call state).
- [ ] **STEP-13** — Implement `GenerateScan` — reads `directory`/`dateSubdir` from `param.State.GetReplicaProps()` (stamped by orchestrator_1); calls `helper.ListParts(directory, ".parquet")` to enumerate the dated sub-directory's `part-*.parquet` files ascending (the connector itself never does this); builds each `Files` entry as `filepath.Join(dateSubdir, filepath.Base(p))` so it resolves against the connector's *stable* configured `Directory`; queries `fc_snapshot_progress` for the row matching `directory` (via `ulib.GetAuxPostgresConn`), if absent treats as `(0, 0)`. Returns `&models.ParquetSourceScanTune{Files: files, RowLimit: 0, StartAfterPart: lastPart, StartAfterRow: lastRow}`.
- [ ] **STEP-14** — Implement `FetchRecords` as a closed, empty channel — this pipeline runs exclusively in `ReadByFullScan` mode (`captureMethod=1`), never `ReadByUserDefinedFunc`.

### Phase 4 — Avro Source Connector

- [ ] **STEP-15** — Scaffold `client/connectors/connector_3/iso_entity_3` implementing `IClientDBAvroSource`, same stateless `IUseConnector{}` + `Instance` shape as STEP-12.
- [ ] **STEP-16** — Implement `GenerateScan` — reads the fixed `directory` from `ReplicaProps` (stamped by orchestrator_2); calls `helper.ListParts(directory, ".avro")` fresh every invocation, so parts appended by a later `movement_seeder` run are included automatically; returns bare filenames (no sub-folder rewrite needed, unlike STEP-13); queries the single `fc_movement_progress` row, if absent treats as `(0, 0)`. Returns `&models.AvroSourceScanTune{Files: files, RowLimit: 0, StartAfterPart: lastPart, StartAfterRow: lastRow}`.
- [ ] **STEP-17** — Implement `FetchRecords` with the same reasoning as STEP-14.

### Phase 5 — MongoDB Destination Connectors

- [ ] **STEP-18** — Scaffold **two** connector packages — `client/connectors/connector_2/iso_entity_2` and `client/connectors/connector_4/iso_entity_4` — both implementing `IClientDBMongoDest` as a stateless `IUseConnector{}`. Two packages, not one, because `DBMongoConfig.Collection` is fixed at connect time; there is no branch-on-caller design that would let a single connector serve both collections.
- [ ] **STEP-19** — Implement `GenerateQuery` for connector_2 (`inventory_snapshots`): builds one `MongoDestQueryTune` per record with `Query = bson.M{"_id": rec["_mongo_id"]}`, `Payload = bson.M{"$set": doc}` (`doc` = `rec` with every `_`-prefixed key stripped), `Operation: models.MongoWriteUpdateOne`, `Options: models.MongoDBWriteOptions{Upsert: true}`. Errors if `_mongo_id` is missing.
- [ ] **STEP-20** — Implement `GenerateQuery` for connector_4 (`stock_movements`): identical shape to STEP-19 — both connectors read the same `_mongo_id` key; only the stamped value's shape differs (composite key vs. `movement_id` directly), and that's decided by the transformer, not the connector.

### Phase 6 — Transformer Chains

- [ ] **STEP-21** — Implement `transformer_1` (`SKUValidator`) for Flow 1: `sku_id == ""` → `nil` (silent drop). Otherwise passthrough.
- [ ] **STEP-22** — Implement `transformer_2` (`QuantityGuard`) for Flow 1: `quantity_on_hand < 0` → error `"transformer_2: negative quantity_on_hand"`. Otherwise passthrough.
- [ ] **STEP-23** — Implement `transformer_3` (`MongoKeyStamper`) for Flow 1: stamps `record["_mongo_id"] = fc_id + "|" + sku_id + "|" + snapshot_date`.
- [ ] **STEP-24** — Implement `transformer_4` (`FCIDValidator`) for Flow 2: `fc_id == ""` → `nil` (silent drop). Otherwise passthrough.
- [ ] **STEP-25** — Implement `transformer_5` (`MovementTypeValidator`) for Flow 2: `movement_type` not in `{PUTAWAY, PICK, RETURN, DAMAGE, ADJUSTMENT}` → error `"transformer_5: unknown movement_type <value>"`. Otherwise passthrough.
- [ ] **STEP-26** — Implement `transformer_6` (`MongoKeyStamper`) for Flow 2: stamps `record["_mongo_id"] = movement_id`.

### Phase 7 — Pipeline Control Plane

- [ ] **STEP-27** — Implement `checkpoint_1` (Flow 1): after each successfully written batch, reads `_file_part`/`_file_row` straight off the last record's `Meta` (`record.Meta[models.MetaFilePart]`/`record.Meta[models.MetaFileRow]`) — `CheckpointProps.Records` is `[]*models.Record`, so `Meta` reaches this package directly, no copy onto `Data` required — and upserts `fc_snapshot_progress` with `directory` (read from `param.State.GetReplicaProps()`), that `(last_part, last_row)`, `status='in_progress'`. Add an exported `MarkComplete(directory, auxMap)` that the flow's bridge calls once the source channel closes with no failure and no terminate-rule stop, setting `status='complete'`.
- [ ] **STEP-28** — Implement `checkpoint_2` (Flow 2): same mechanics against the single `fc_movement_progress` row — no `status` column and no `MarkComplete`, since this flow never "completes."
- [ ] **STEP-29** — Implement `backlog_1` (Flow 1): on any transformer or MongoDB write error, insert into `fc_snapshot_backlog` with `fc_id`/`sku_id` (from `record.Data`), `file_part`/`file_row` (read the same way as STEP-27, off `record.Meta`), `failure_stage`, `error_message`, `record_payload` (JSONB of `record.Data` only — position lives in the dedicated `file_part`/`file_row` columns, not baked into the payload), `pipeline_run_id`.
- [ ] **STEP-30** — Implement `backlog_2` (Flow 2): same shape into `fc_movement_backlog` with `movement_id` in place of `sku_id`.
- [ ] **STEP-31** — Implement `terminate_1`/`terminate_2`: register **only** the built-in `MaxPipelineTime` field on `models.TerminateRuleTune` (30 min for Flow 1, 15 min for Flow 2) — no `UserDefinedCheckFunc`. This is a deliberate departure from Cases 4–5's terminate rules, which always poll connector-side "exhausted" state via a custom check func: `ReadByFullScan` here is a bounded, one-shot scan with no live connection to keep alive, so its channel just closes on its own once every listed file/row is delivered, and the engine's built-in `MaxPipelineTime` check (in `contexts.CheckTerminationConditions`) is purely a safety net for a wedged run, not the thing that ends a normal one.

### Phase 8 — Observability

- [ ] **STEP-32** — Implement `cmd/metrics_watcher/main.go` — polls AuxDB (`pgx/v5`) and MongoDB (`countDocuments`) on a configurable `--interval` (default 5s). Sections per §8.1. Clears screen each tick (`\033[H\033[2J`). Exits cleanly on `SIGINT`/`SIGTERM`.
- [ ] **STEP-33** — Add structured logging at key events: directory scan started (directory, resolved `Files` count, `StartAfterPart`, `StartAfterRow`), batch delivered (count, highest `Meta[MetaFilePart]`/`Meta[MetaFileRow]`), MongoDB upsert batch committed (count, collection, latency), progress checkpoint saved, backlog record inserted (stage, error), `TerminateRule` fired (rule name, flow).

### Phase 9 — End-to-End Test Run

- [ ] **STEP-34** — Run `make setup` (Docker up → wait healthy → `make auxdb` → `make mongo-init`). Verify all 4 AuxDB tables exist via `psql`. Verify both MongoDB indexes exist via `mongosh --eval "db.inventory_snapshots.getIndexes()"`.
- [ ] **STEP-35** — Run `make seed-snapshot` with defaults. Verify `<FC_SNAPSHOT_DIR>/dt=<today>/` contains `part-00000.parquet` and `part-00001.parquet`.
- [ ] **STEP-36** — Run `make seed-movement` with defaults. Verify `<FC_MOVEMENT_DIR>/` contains `part-00000.avro` through `part-00003.avro`.
- [ ] **STEP-37** — Run `make seed-backlog`. Verify both AuxDB backlog tables contain 20 rows each. Run `make watch` and confirm non-zero backlog counts.
- [ ] **STEP-38** — Start Flow 1 (pid=1). Verify it processes both Parquet parts and its source channel closes on its own — no terminate-rule stop. Confirm `fc_snapshot_progress.status = 'complete'` and `inventory_snapshots` has ~1,425 documents (1,500 total minus ~37 Fault A silent drops at 5% split evenly across two fault types, ~37 each).
- [ ] **STEP-39** — Start Flow 2 (pid=2). Verify it processes all 4 Avro parts, then its channel closes on its own once they're exhausted (this run is finished, not the directory). Confirm `stock_movements` has ~1,900 documents (2,000 total minus ~100 Fault A silent drops).
- [ ] **STEP-40** — Verify fault handling: confirm `fc_snapshot_backlog` has ~37 rows with `failure_stage=transform` and `transformer_2` error text; confirm `fc_movement_backlog` has ~50 rows with `transformer_5` unknown-movement_type errors.
- [ ] **STEP-41** — Test checkpoint/resume for Flow 1: kill it mid-scan. Note `(last_part, last_row)` in `fc_snapshot_progress`. Restart. Confirm it resumes from that exact position — `inventory_snapshots` document count does not change for rows already upserted (idempotent by `_id`).
- [ ] **STEP-42** — Test the "new directory each day" model: run `make seed-snapshot SNAPSHOT_DATE=<tomorrow>`, then start Flow 1 again (its orchestrator re-resolves `directory` from `SNAPSHOT_DATE`/today). Confirm a **second, independent** row appears in `fc_snapshot_progress` for the new directory, starting from `(0, 0)`, while yesterday's row remains untouched at `status='complete'`.
- [ ] **STEP-43** — Test the "flat, growing directory" model: after STEP-39 finishes, run `make seed-movement NEW_MOVEMENTS=500` to append two more parts. Restart Flow 2. Confirm — via `record.Meta[MetaFilePart]` on delivered records, or the `GenerateScan` log line showing the resolved `Files` count and `StartAfterPart` — that files before the checkpoint index are **skipped entirely, not re-opened**, and only the newly appended parts are read.
- [ ] **STEP-44** — Full pipeline run: `make reset` → `make setup` → `make seed-snapshot` → `make seed-movement` → start Flow 1 → start Flow 2 → monitor via `make watch` until both channels close on their own. Verify final MongoDB counts and both backlog tables match the expected fault-rate math from STEP-38/39/40.

---

## Summary Reference

| Dimension | Detail |
|---|---|
| Flows | 36 (Parquet Snapshot Ingestion), 37 (Avro Movement Ingestion) |
| Pipeline orchestrators | orchestrator_1 (Flow 1), orchestrator_2 (Flow 2) |
| Source 1 | Parquet directory-of-parts (connector_1 / iso_entity_1), Spark/Hive naming (`part-00000.parquet`), one fresh `dt=<date>` sub-directory per day under a stable configured `Directory` |
| Source 2 | Avro (OCF) directory-of-parts (connector_3 / iso_entity_3), same naming, single flat directory that never rotates — `Directory` and the on-disk path are the same |
| FCs / SKUs | 6 fulfillment centers, ~250 SKUs |
| Resume mechanism | Client-supplied `Files []string` (the connector never discovers files itself) + `StartAfterPart` as a 0-based **index into that list** + `StartAfterRow` — skip-ahead over a client-enumerated file set, not a cursor or offset against a live source |
| Position metadata | `record.Meta[MetaFilePart]`/`record.Meta[MetaFileRow]` — kept off `record.Data` and out of the MongoDB document, but read directly by the transformer chain, `checkpoint_1`/`2`, and `backlog_1`/`2` since `TransformerProps.Record`/`CheckpointProps.Records`/`BacklogProps.Records` all carry the full `*models.Record`; no copy onto `Data` is needed |
| Destination | MongoDB 7, database `fc_inventory`, collections `inventory_snapshots` (connector_2 / iso_entity_2) and `stock_movements` (connector_4 / iso_entity_4) — two separate connectors, since `DBMongoConfig.Collection` is fixed at connect time |
| Write semantics | `MongoWriteUpdateOne` + `Upsert: true` on a deterministic `_id`, sourced from a transformer-stamped `_mongo_id` field — idempotent re-delivery |
| Flows | 2, independent, no shared bus, no start-order dependency |
| Pipelines | 1 per flow = 2 total |
| Flow 1 transformer chain | `transformer_1` (SKUValidator) → `transformer_2` (QuantityGuard) → `transformer_3` (MongoKeyStamper) |
| Flow 2 transformer chain | `transformer_4` (FCIDValidator) → `transformer_5` (MovementTypeValidator) → `transformer_6` (MongoKeyStamper) |
| Checkpoints / Backlogs / Terminates | checkpoint_1/2, backlog_1/2, terminate_1/2 |
| Destination writes | destinationwrite_1 (Flow 1), destinationwrite_2 (Flow 2) |
| Fault types | 4 total (2 per flow): silent drop (empty key field) + backlog route (business-rule violation) |
| AuxDB tables | 4: `fc_snapshot_progress`, `fc_snapshot_backlog`, `fc_movement_progress`, `fc_movement_backlog` |
| Termination | Both flows just let `ReadByFullScan`'s channel close on its own once their `GenerateScan`-bounded `Files` list is exhausted; `terminate_1`/`terminate_2` register only a `MaxPipelineTime` safety net (30 min / 15 min), no custom exhausted-polling check func. Flow 1 additionally marks `fc_snapshot_progress.status='complete'` on a clean finish; Flow 2 has no such terminal state — it's simply re-invoked periodically over the same growing directory. |
| Seeder | Two Go CLI tools writing real Parquet/Avro part files to disk (not an HTTP mock) — `parquet-go` (`parquet.WriteFile`) and `hamba/avro/v2/ocf` |
| Implementation steps | 44 steps across 9 phases |
| New patterns vs Cases 1–5 | File-based sources with no live API/DB/stream to poll; client-side file discovery (`GenerateScan` builds `Files` itself via `helper.ListParts` — the connector never scans a directory) with resume as an index into that list rather than cursor/offset/WAL; `Record.Meta`/`Record.Data` separation, with the full `*models.Record` (not just `Data`) threaded through the transformer chain and into `CheckpointProps`/`BacklogProps`, so file position is read straight off `Meta` with no copy step — `Meta` still stops at the destination boundary and never reaches the MongoDB document; a fixed connector `Directory` combined with a client-computed sub-folder to handle a rotating (daily) source directory, vs. a non-rotating (flat) one; a destination connector type (`DBMongoConfig`) whose config fixes the target collection, forcing one connector per collection rather than one shared connector across flows; termination reduced to a `MaxPipelineTime` safety net with no custom check func, since a bounded one-shot file scan — unlike a REST/Kafka connection — closes its own channel when done; MongoDB as a document destination for two structurally distinct feeds; no inter-flow ordering or shared bus |
