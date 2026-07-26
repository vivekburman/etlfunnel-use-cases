# Telecom Customer Merger — ETL Pipeline Case Study Plan
### MySQL (Multi-Source) → PostgreSQL Raw Stage | Streamcraft Execution Framework

---

## Overview

This case study models a real-world telecom consolidation where four acquired companies — **Vodafone, Idea, Tata Docomo, Aircel** — each with their own MySQL databases, are migrated into a unified destination Postgres database (Airtel / Jio or both). The pipeline is built on top of the Streamcraft Execution framework and covers the full raw-stage ingestion: extract from all four MySQL sources, transform, and land into the `raw` schema on Postgres.

---

## Part 1 — Source Architecture (MySQL)

### 1.1 Source Databases
- 4 independent MySQL instances, one per company:
  - `vodafone_db`
  - `idea_db`
  - `tata_docomo_db`
  - `aircel_db`
- Each DB has its own schema, column naming conventions, data types, and quirks.

### 1.2 Sharding Strategy — Two-Tier Geographic Distribution

**Tier 1 — Zone-based sharding (5 zones):**
- North, South, East, West, Central

**Tier 2 — State-based sharding within each zone:**
- North: UP, Bihar, Jharkhand, HP, Uttarakhand, Punjab, Haryana, J&K, Delhi
- South: Tamil Nadu, Kerala, Karnataka, Andhra Pradesh, Telangana
- East: West Bengal, Odisha, Assam, Meghalaya, Tripura, Sikkim
- West: Maharashtra, Gujarat, Rajasthan, Goa
- Central: MP, Chhattisgarh

**Tier 3 — Table-level splitting within a shard:**
- Each shard table is capped at **1 million rows** (`SplitRowCap`)
- When a table grows beyond 1M rows, the seeder creates the next numbered split on the fly:
  - `customers_north_up_1`, `customers_north_up_2`, `customers_north_up_3` …
- `mysql_schema` only creates split `_1` at setup time; splits `_2`, `_3` … are created dynamically at seeding time (and by the source connector at ingest time in production)
- The pipeline connector handles querying across all splits for a given shard

### 1.3 Per-Company Sharding Strategy

Each acquired company used a different geographic organisation for its data before the merger.
Not all companies sharded by both zone and state — this divergence is intentional and exercises
the `GeoTagger` transformer which injects the missing geo dimension from pipeline context.

| Company | Sharding Type | Table name pattern | Geo columns present |
|---|---|---|---|
| Vodafone | Zone + State | `customers_north_up_1` | `zone`, `state` |
| Idea | Zone only | `customers_north_1` | `zone` only |
| Tata Docomo | State only | `customers_up_1` | `state` only |
| Aircel | Zone + State | `customers_north_up_1` | `zone`, `state` |

> **Why this matters for the ETL:** The `SchemaMapper` normalises column names; the `GeoTagger`
> fills in the missing geo dimension (e.g. injects `state` for Idea records, `zone` for Tata
> Docomo records) using the pipeline's shard context so the destination always has both fields.

### 1.4 Core Source Tables (per company, per shard)

Table name patterns vary by company sharding type (see §1.3). The `{geo}` placeholder represents
the zone, state, or zone+state key depending on the company.

- `customers_{geo}_{n}` — customer identity, PII, contact
- `subscriptions_{geo}_{n}` — active/historical plan enrollments
- `billing_accounts_{geo}_{n}` — billing cycle, dues, payment history
- `sim_inventory_{geo}_{n}` — SIM serial, IMSI, activation date
- `port_history_{geo}_{n}` — MNP in/out events

---

## Part 2 — ETL Pipeline Components

### 2.1 Transformations

Each pipeline applies a chain of transformers (matching the framework's `applyTransformations` function):

| Transformer | Responsibility |
|---|---|
| `SchemaMapper` | Normalize column names across sources → unified schema (e.g., `mob_no` / `contact` / `phone_number` → `msisdn`) |
| `TypeCaster` | MySQL `TINYINT` → Postgres `BOOLEAN`, `DATETIME` → `TIMESTAMPTZ`, `TEXT` → `VARCHAR(255)` etc. |
| `NullHandler` | Fill missing state codes, default null plan info, flag null DOBs for backlog |
| `PIIMasker` | Hash Aadhaar and PAN numbers using SHA-256 before write |
| `PlanMapper` | Look up source plan code → destination plan code via AuxDB `plan_mapping` table |
| `GeoTagger` | Inject `zone` and `state` as derived columns for destination partition routing |
| `DedupChecker` | Check `dedup_registry` in AuxDB for cross-source identity conflicts (same MSISDN across companies); route conflicts to backlog |
| `UnitNormalizer` | Standardize data charges (MB → GB), currency formatting |

Transformers are composable and chainable. A failed transformer sends the record to Backlog with `FailureStageTransform`.

### 2.2 Checkpoints

Checkpoints fire after every committed destination batch. State saved per checkpoint:

- `source_company` — Vodafone / Idea / Tata Docomo / Aircel
- `zone` — North / South / East / West / Central
- `state` — e.g., UP, Tamil Nadu
- `table_split_index` — which numbered split table (1, 2, 3…)
- `last_processed_pk` — last primary key successfully written
- `batch_id` — batch sequence within this shard run
- `phase` — Extract / Transform / Load
- `timestamp` — wall clock of checkpoint

Checkpoint records are written to the **AuxDB** `pipeline_checkpoints` table. On resume, the pipeline connector reads the last checkpoint and begins extraction from `last_processed_pk + 1`.

**Shard-level independence:** Each zone+state pipeline has its own checkpoint row. North-UP can resume independently of South-TN without conflict.

**Table-split checkpoints:** If a state shard has multiple table splits, the checkpoint tracks which split is currently in-flight so resumption starts at the correct split.

### 2.3 Incident Backlog

Records that cannot be processed are routed to the **AuxDB** `backlog_records` table instead of failing the pipeline.

**Backlog triggers:**
- Hard transform failure — invalid MSISDN format, unresolvable plan code, unparseable date
- Destination write failure — constraint violation, duplicate key on destination
- Dedup conflict — same MSISDN found in two or more source companies, conflict resolution failed → both raw records go to backlog for manual review
- Schema mismatch — unexpected column from source, missing required field
- Null critical field — e.g., MSISDN is null (cannot route without it)

**Backlog record metadata:**
- `source_company`, `zone`, `state`, `table_split_index`
- `batch_id`, `checkpoint_id`
- `failure_stage` — Transform / Destination (the framework's `FailureStage` enum only has `None`/`Transform`/`Destination` — `models/common_model.go`; dedup conflicts are recorded under Destination and distinguished via `error_code`, not a separate framework-native Dedup stage)
- `error_code`, `error_message`
- `raw_record` — full original record JSON
- `retry_count`, `created_at`, `last_attempted_at`
- `status` — PENDING / IN_RETRY / RESOLVED / ABANDONED

**Reprocessing:** Backlog records with `status = PENDING` can be re-injected into the pipeline as a dedicated reprocessing flow (separate Flow definition in `collection.json`) after root cause is fixed.

### 2.4 TerminateRule

The TerminateRule is registered as a control plane ticker that fires at a configurable interval (`TerminateRuleTune.CheckInterval`, mirrored in AuxDB `terminate_rules` table for operator tuning). Only three of the conditions below are framework-native built-ins, set via dedicated `TerminateRuleTune` fields (`MaxRecords *uint64`, `IdleTimeout *time.Duration`, `MaxPipelineTime *time.Duration` — `models/exit_model.go`); the remaining conditions are hand-rolled inside this case study's single `UserDefinedCheckFunc` rather than separate framework-level rule types:

| Rule | Condition | Action | Framework-native? |
|---|---|---|---|
| `ERROR_RATE_BREACH` | Backlog rate > 10% of batch records | Stop pipeline, preserve checkpoint | Custom — via `UserDefinedCheckFunc` |
| `SOURCE_UNREACHABLE` | Source MySQL connection error after N retries | Graceful stop, trigger Collection-level retry | Custom — via `UserDefinedCheckFunc` |
| `DESTINATION_SATURATION` | Destination write latency > threshold (ms) | Stop, alert, DestinationWriteTune slowify kicks in | Custom — via `UserDefinedCheckFunc` |
| `INTEGRITY_VIOLATION` | Critical field (MSISDN) null rate > 5% in batch | Stop, flag shard for source data review | Custom — via `UserDefinedCheckFunc` |
| `DUPLICATE_STORM` | Dedup conflict rate > 80% of batch | Stop, escalate to manual review | Custom — via `UserDefinedCheckFunc` |
| `IDLE_TIMEOUT` | No records received for > N seconds | Stop, assume source exhausted | Built-in (`TerminateRuleTune.IdleTimeout`) |
| `MANUAL_KILL` | Operator sets `force_stop = true` in AuxDB config | Graceful stop at next checkpoint boundary | Custom — via `UserDefinedCheckFunc` |
| `MAX_RECORDS` | Total records processed >= configured cap | Stop cleanly (useful for incremental runs) | Built-in (`TerminateRuleTune.MaxRecords`) |

Rules are stored in AuxDB and tunable at runtime without code changes; the AuxDB row values feed the two built-in fields directly and are read by the `UserDefinedCheckFunc` for everything else.

### 2.5 DestinationWriteTune

The DestinationWriteTune is registered as a separate control plane ticker. It dynamically adjusts the destination write batch size based on current pipeline health metrics.

**Speedify mode (bulk load / off-peak):**
- Increase batch size (e.g., 5000 records/commit)
- Disable non-critical destination indexes temporarily
- Defer FK constraint checks to end of transaction
- Increase parallel write concurrency

**Slowify mode (production hours / destination under load):**
- Reduce batch size (e.g., 100 records/commit)
- Introduce deliberate inter-batch sleep (configurable ms)
- Reduce parallel write concurrency
- Activate connection pooling limits

**Config stored in AuxDB `write_tune_config`:**
- `batch_size_normal`, `batch_size_turbo`, `batch_size_throttle`
- `check_interval_seconds`
- `throttle_schedule` — time-of-day windows for automatic slowify (e.g., 09:00–22:00 IST)
- `destination_latency_threshold_ms`
- `concurrency_limit`

The `UserDefinedCheckFunc` in the `DestinationWriteTune` reads these config values from AuxDB on each tick and returns a `*DestinationWriteActionTune{NewBatchSize *int}` accordingly (nil = no change); the engine applies that new batch size to the running pipeline (there is no `SetDestinationWriteBatchSize` call made by client code itself).

### 2.6 Auxiliary DB

A dedicated Postgres instance (or isolated database on the destination cluster) serves as the operational backbone of the ETL pipeline.

**Tables in AuxDB:**

| Table | Purpose |
|---|---|
| `pipeline_checkpoints` | Checkpoint state per shard per phase |
| `backlog_records` | Failed/deferred records with full metadata |
| `dedup_registry` | Cross-source MSISDN → canonical customer ID map |
| `plan_mapping` | Source plan code → destination plan code lookup |
| `terminate_rules` | Configurable termination thresholds per pipeline |
| `write_tune_config` | Runtime-tunable batch size and concurrency settings |
| `reconciliation_log` | Post-ingestion row counts and checksums per shard |

---

## Part 3 — Destination Architecture (PostgreSQL)

### 3.1 Raw Schema

The destination writes land in the `raw` schema — a fast-write landing zone with no constraints, one table per entity type. Records are written as-is after transformation; no dedup or constraint enforcement happens at this layer.

### 3.2 Destination Partitioning

Postgres declarative partitioning mirrors source sharding:

```
customers (partitioned by zone)
  ├── customers_north (partitioned by state)
  │     ├── customers_north_up
  │     ├── customers_north_bihar
  │     └── ...
  ├── customers_south
  ├── customers_east
  ├── customers_west
  └── customers_central
```

Each state partition uses range partitioning on `batch_sequence_id` to mirror the 1M row split concept from source. This ensures parallel writers from different shards land in non-overlapping partitions with **zero write contention**.

### 3.3 Core Destination Tables (in `raw` schema)

| Table | Description |
|---|---|
| `customers` | Customer identity — msisdn, name, DOB, Aadhaar hash, PAN hash, zone, state, source_company |
| `subscriptions` | Current and historical plan enrollments |
| `billing_accounts` | Billing cycles, outstanding dues, payment history |
| `sim_inventory` | SIM serial, IMSI, activation/deactivation dates |
| `port_history` | MNP in/out events — critical for merger continuity |

### 3.4 Write Safety Net

- All destination writes use `INSERT ... ON CONFLICT DO UPDATE` (upsert) — pipeline runs are fully idempotent
- Indexes built per partition AFTER bulk load, not during
- FK constraints deferred to end of transaction during bulk load

---

## Part 4 — Orchestration Design (Streamcraft Framework Mapping)

### 4.1 Hierarchy

```
Collection  = The full Telecom Migration Job
  │
  ├── Flow  = {company}_{zone}   (4 companies × 5 zones = 20 Flows)
  │     e.g., vodafone_north, idea_south, tata_docomo_east ...
  │
  └── Pipeline = {state}         (6–9 pipelines per Flow)
        e.g., vodafone_north → up, bihar, jharkhand, hp, uttarakhand, punjab, haryana
```

Total pipelines at peak: **~20 flows × ~7 states avg = ~140 pipelines**

### 4.2 Execution Queue Design

The framework's `ExecutionQueueManager` manages:
- **FlowQueue** — all 20 flows enqueued at collection start
- **PipelineQueue** — flows are materialized into state pipelines on demand
- **RetryQueue** — failed pipelines re-enter here with exponential backoff or immediate retry per `RetryPolicy` config

Priority order: Retry → Pipeline → Materialize next Flow

### 4.3 Flow Orchestrator (to be implemented — PID: new)

Materializes the 20 company×zone Flow replicas by returning one `FlowOrchestratorTune` per replica — the real shape (`models/orchestrator_model.go`) carries per-replica naming/context, not concurrency knobs:

```go
// FlowOrchestratorTune — one per materialized Flow replica
type FlowOrchestratorTune struct {
    ReplicaProps map[string]any // e.g. {"company": "vodafone", "zone": "north"}
    ParentName   string         // template flow name
    ReplicaName  string         // materialized name, e.g. "vodafone_north"
}
```

Actual concurrency limiting (how many Flows run at once, to avoid overwhelming the 4 MySQL source servers) is enforced by the `ExecutionQueueManager`'s bounded `FlowQueue` (§4.2), not by fields on this tune.

### 4.4 Pipeline Orchestrator (PID: 2 — to be implemented)

Materializes each Flow's state-level Pipeline replicas by returning one `PipelineOrchestratorTune` per replica — same real shape as `FlowOrchestratorTune` (`models/orchestrator_model.go`):

```go
// PipelineOrchestratorTune — one per materialized state-level Pipeline replica
type PipelineOrchestratorTune struct {
    ReplicaProps map[string]any // e.g. {"state": "up", "zone": "north"}
    ParentName   string         // template pipeline name, e.g. "state"
    ReplicaName  string         // materialized name, e.g. "up_north"
}
```

Actual concurrency limiting (how many Pipelines run at once within a Flow) is enforced by the `ExecutionQueueManager`'s `PipelineQueue` (§4.2), not by fields on this tune. Initial destination write batch size is instead sourced from AuxDB `write_tune_config` at orchestration time and applied via `DestinationWriteTune`/`DestinationWriteHandle` (§2.5).

### 4.5 Peak Parallelism Model

At full throttle:
```
8 concurrent Flows × 3 concurrent Pipelines per Flow = 24 active pipeline workers
+ 1 Aux write lane (checkpoints, backlogs)
+ 1 Reconciliation job lane (post-shard raw validation)
= ~26 concurrent goroutine lanes
```

Each lane writes to a non-overlapping Postgres partition — zero contention.

### 4.6 collection.json Structure (for this case study)

```json
{
  "flowOrchestratorDefinition": { "Name": "telecom_flow_orchestrator", "PID": 1 },
  "flowDefinition": [
    {
      "flow": { "pid": 1, "name": "vodafone_north" },
      "pipelineOrchestratorDefintion": { "Name": "telecom_pipeline_orchestrator", "PID": 2 },
      "source": { "type": "mysql", "connectionParams": "..." },
      "destination": { "type": "postgres", "connectionParams": "..." },
      "pipelines": [
        {
          "name": "up",
          "sourceIsolationEntity": { "name": "customers_north_up", "pid": 1 },
          "destinatioIsolationEntity": { "name": "raw.customers", "pid": 1 },
          "transformers": [ { "pid": 1 }, { "pid": 2 }, { "pid": 3 }, { "pid": 4 }, { "pid": 5 }, { "pid": 6 }, { "pid": 7 } ],
          "checkpoint": { "pid": 1 },
          "backlog": { "pid": 1 },
          "terminate": { "pid": 1 },
          "auxiliaryHubs": [
            { "name": "auxdb", "type": "postgres", "connectionParams": "..." }
          ]
        }
        // ... more state pipelines
      ]
    }
    // ... 19 more flows
  ]
}
```

---

## Part 5 — Step-by-Step Implementation Tasks

### Phase 1 — Infrastructure Setup

- [ ] **STEP-01** — Provision 4 MySQL source instances (one per company: Vodafone, Idea, Tata Docomo, Aircel)
- [ ] **STEP-02** — Design and create source schema for each company with per-company sharding strategy (zone+state / zone-only / state-only) and split naming convention (`customers_{geo}_1`). `mysql_schema` creates only split `_1`; subsequent splits are created dynamically when the 1M-row cap is hit.
- [ ] **STEP-03** — Seed source databases with realistic synthetic telecom data (customers, subscriptions, billing, SIM inventory, port history) — target 3–5M records per company. Use `--records-per-shard` flag; splits `_2`, `_3` … are auto-created by the seeder when `SplitRowCap` (1,000,000) is reached. Child tables (subscriptions, billing, sim, port) are seeded into the same split as their parent customer rows.
- [ ] **STEP-04** — Provision destination Postgres instance (Airtel / Jio)
- [ ] **STEP-05** — Create `raw` schema on destination Postgres
- [ ] **STEP-06** — Create destination tables in `raw` schema with declarative partitioning (zone → state → batch range)
- [ ] **STEP-07** — Provision AuxDB Postgres instance and create all AuxDB tables (checkpoints, backlog, dedup_registry, plan_mapping, terminate_rules, write_tune_config, reconciliation_log)
- [ ] **STEP-08** — Seed AuxDB lookup tables: `plan_mapping` (all 4 source companies' plan codes → destination plan codes) and initial `terminate_rules` and `write_tune_config` rows

### Phase 2 — Connector Implementation

- [ ] **STEP-09** — Implement `IUseConnector` for each source company's MySQL (4 connectors) with support for querying split tables (`customers_{zone}_{state}_1`, `_2`, `_3` …) in sequence
- [ ] **STEP-10** — Implement state-level source isolation entities (one per state per zone — these are the pipeline's `sourceIsolationEntity`) that read from the correct shard table set
- [ ] **STEP-11** — Implement destination `IUseConnector` for Postgres targeting the `raw` schema with upsert (`INSERT ... ON CONFLICT DO UPDATE`)
- [ ] **STEP-12** — Implement destination isolation entity for each partitioned table (routes records to the correct zone/state partition)

### Phase 3 — Transformer Chain

- [ ] **STEP-13** — Implement `SchemaMapper` transformer — normalize all source column names to unified destination schema per company
- [ ] **STEP-14** — Implement `TypeCaster` transformer — handle all MySQL → Postgres type conversions
- [ ] **STEP-15** — Implement `NullHandler` transformer — define rules for each nullable field (default, skip, backlog)
- [ ] **STEP-16** — Implement `PIIMasker` transformer — SHA-256 hash Aadhaar and PAN fields
- [ ] **STEP-17** — Implement `PlanMapper` transformer — look up `plan_mapping` table in AuxDB to resolve source plan codes
- [ ] **STEP-18** — Implement `GeoTagger` transformer — inject `zone` and `state` columns derived from the pipeline's shard context (passed via `TransformerProps`)
- [ ] **STEP-19** — Implement `DedupChecker` transformer — check AuxDB `dedup_registry` for MSISDN conflicts across companies; route ambiguous conflicts to backlog
- [ ] **STEP-20** — Implement `UnitNormalizer` transformer — standardize data units and currency formats
- [ ] **STEP-21** — Wire transformer chain into pipeline `applyTransformations` (ordered: SchemaMapper → TypeCaster → NullHandler → PIIMasker → PlanMapper → GeoTagger → DedupChecker → UnitNormalizer)

### Phase 4 — Pipeline Control Plane

- [ ] **STEP-22** — Implement `Checkpoint` function — write shard state (company, zone, state, split index, last PK, batch ID, phase, timestamp) to AuxDB `pipeline_checkpoints`; implement resume logic in source connector to read last checkpoint and start from `last_processed_pk + 1`
- [ ] **STEP-23** — Implement `Backlog` function — write failed records with full metadata (source, stage, error code, raw record JSON, retry count) to AuxDB `backlog_records`; define `BacklogTune.Action` for each failure type (Continue / Stop)
- [ ] **STEP-24** — Implement `TerminateRule` — wire the 2 framework built-ins (`IDLE_TIMEOUT`, `MAX_RECORDS` via `TerminateRuleTune.IdleTimeout`/`MaxRecords`) plus the remaining 6 conditions (error rate, source unreachable, destination saturation, integrity violation, duplicate storm, manual kill) hand-rolled in a single `UserDefinedCheckFunc`; read thresholds from AuxDB `terminate_rules` on each tick
- [ ] **STEP-25** — Implement `DestinationWriteTune` — `UserDefinedCheckFunc` reads `write_tune_config` from AuxDB on each tick; implements speedify (high batch size, index disable, bulk insert) and slowify (low batch size, inter-batch sleep, concurrency cap) modes with time-of-day scheduling

### Phase 5 — Orchestration

- [ ] **STEP-26** — Implement `FlowOrchestrator` (new PID) — materializes the 20 company×zone Flow replicas, returning a `FlowOrchestratorTune` (`ParentName`/`ReplicaName`/`ReplicaProps`) per replica; actual max-concurrent-flows limiting is the `ExecutionQueueManager`'s bounded `FlowQueue`, configured via job/collection settings, not a field on this tune
- [ ] **STEP-27** — Implement `PipelineOrchestrator` (PID: 2) — materializes each Flow's state-level Pipeline replicas, returning a `PipelineOrchestratorTune` (`ParentName`/`ReplicaName`/`ReplicaProps`) array; reads initial destination write batch size from AuxDB `write_tune_config` and applies it via `DestinationWriteTune`; actual max-concurrent-pipelines limiting is the `ExecutionQueueManager`'s `PipelineQueue`, not a field on this tune
- [ ] **STEP-28** — Build `collection.json` for the full telecom migration: 20 flows (4 companies × 5 zones), each with 6–9 state pipelines, correct connector PIDs, transformer PIDs, checkpoint/backlog/terminate PIDs, and AuxDB hub connections

### Phase 6 — Backlog Reprocessing Flow

- [ ] **STEP-29** — Design and implement a dedicated reprocessing `Flow` definition that reads from AuxDB `backlog_records` (status = PENDING), re-runs through the transformer chain, and attempts destination write again
- [ ] **STEP-30** — Implement retry count enforcement and ABANDONED status for records exceeding `max_retry` threshold
- [ ] **STEP-31** — Add backlog reprocessing flow to `collection.json` as a separate flow that can be triggered independently

### Phase 7 — Reconciliation & Validation

- [ ] **STEP-32** — Implement post-shard reconciliation: after each pipeline completes, count source records vs destination `raw` records for that shard; write result to AuxDB `reconciliation_log`
- [ ] **STEP-33** — Implement checksum validation: compare hash of key fields (msisdn, plan_code, state) between source and `raw` destination for a sample set per shard

### Phase 8 — Observability

- [ ] **STEP-34** — Add structured logging (already in framework via `zap`) at all key events: shard start/end, checkpoint written, backlog routed, termination triggered, tune adjusted, flow completed
- [ ] **STEP-35** — Expose pipeline metrics (total records, backlog rate, checkpoint lag, destination write latency) — suitable for OpenTelemetry integration (framework has KAN-16 OTEL branch)

### Phase 9 — End-to-End Test Run

- [ ] **STEP-36** — Run a single pipeline in isolation (Vodafone → North → UP) as smoke test — validate checkpoint, backlog, transform, and destination write all work correctly
- [ ] **STEP-37** — Introduce intentional bad records (null MSISDN, invalid plan codes, duplicate MSISDNs across two companies) and verify backlog routing and dedup handling
- [ ] **STEP-38** — Test TerminateRule triggers — artificially breach error rate threshold and verify graceful stop + checkpoint preservation
- [ ] **STEP-39** — Test DestinationWriteTune — simulate off-peak (speedify) and peak hours (slowify) and verify batch size changes dynamically
- [ ] **STEP-40** — Run full parallel execution: all 20 flows, all ~140 pipelines — monitor queue throughput, destination partition health, AuxDB backlog volume
- [ ] **STEP-41** — Simulate mid-run failure: kill one pipeline mid-shard, verify checkpoint allows clean resume from last committed PK
- [ ] **STEP-42** — Run backlog reprocessing flow on all PENDING backlog records — verify resolution rate and ABANDONED count
- [ ] **STEP-43** — Run full reconciliation suite (STEP-32 to STEP-33) — verify source vs raw destination counts match within acceptable tolerance
- [ ] **STEP-44** — Final sign-off: all raw records validated, all checkpoints marked complete, reconciliation log clean

---

## Summary Reference

| Dimension | Detail |
|---|---|
| Source DBs | 4 MySQL instances (Vodafone, Idea, Tata Docomo, Aircel) |
| Sharding | Per-company: Zone+State (Vodafone, Aircel) / Zone-only (Idea) / State-only (Tata Docomo). Splits at 1M rows cap, created dynamically. |
| Total Flows | 20 (4 companies × 5 zones) |
| Total Pipelines | ~140 (avg 7 states per zone) |
| Peak Concurrency | 8 flows × 3 pipelines = 24 active workers |
| Transformers | 8 chained transformers per pipeline |
| Control Plane | TerminateRule + DestinationWriteTune (independent tickers) |
| Checkpoints | Per shard, per phase, per table split — resume from last PK |
| Backlog | AuxDB-backed, retryable, with failure stage metadata |
| Destination | Postgres `raw` schema — fast-write landing zone, no constraints |
| Partitioning | Zone → State → Batch range (declarative, zero write contention) |
| AuxDB Tables | 7 operational tables (checkpoints, backlog, dedup, plan map, rules, config, reconciliation) |
| Implementation Steps | 44 steps across 9 phases |
