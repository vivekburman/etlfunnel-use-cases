# Payments Reconciliation & Fraud Intelligence Pipeline — ETL Case Study Plan
### PostgreSQL → Snowflake (raw + in-warehouse transform) → Elasticsearch | Streamcraft Execution Framework

**Status: design plan, not yet implemented.** This document exists to fix scope *before* any Snowflake credits or build time are spent — see [Part 5](#part-5--the-scale-problem-and-how-this-case-works-around-it).

---

## Overview

Cases 1–6 all treat the data warehouse as a pure destination. Case 7 is the first to treat **Snowflake as both a destination and a source in the same case** — data lands in Snowflake, gets transformed *inside* Snowflake (SQL, not Go), and only the transformed result leaves again, either toward a fast query layer (Elasticsearch) or by staying in Snowflake behind a serving-optimized table. This models a **payments reconciliation and fraud-signal pipeline** for a fictional payments aggregator ("SettleRight") that authorizes and settles transactions across many merchants:

- **PostgreSQL** is the OLTP system of record for raw transaction events (`transactions`, `settlements`) — high write volume, no analytical indexes, not meant for ad-hoc aggregation.
- **Snowflake** is where transactions get joined against merchant risk profiles, aggregated into rolling per-merchant fraud-signal windows, and reconciled against settlement batches — the kind of multi-way join and window aggregation Postgres is the wrong tool for at analytical scale.
- **Elasticsearch** is the support/ops query layer — a fraud analyst or support agent searching "show me this merchant's flagged transactions this week" needs sub-second response, not a warehouse query.

Three flows, chained by Snowflake sitting in the middle:

- **Flow 1 — Cold + Incremental Load (Postgres → Snowflake)**: time-window polls `transactions`/`settlements` in Postgres and lands them in Snowflake `RAW_TRANSACTIONS` / `RAW_SETTLEMENTS` tables via batched multi-row `INSERT`s (not one `INSERT` per record — see §5.2).
- **Flow 2 — In-Warehouse Transform (Snowflake → Snowflake)**: a Snowflake **Stream** on `RAW_TRANSACTIONS` is consumed on a schedule; each run joins the newly streamed rows against `MERCHANT_RISK_PROFILE`, computes a rolling fraud score and reconciliation status, and `MERGE`s the result into a curated `MERCHANT_TXN_SUMMARY` table. Source and destination connector are both `Snowflake` in this flow — the "transform" is SQL executed by the warehouse, the pipeline's job is just scheduling, checkpointing the stream consumption, and fault routing.
- **Flow 3 — Downstream to Elasticsearch (Snowflake → Elasticsearch)**: time-window polls `MERCHANT_TXN_SUMMARY` on `updated_at` and bulk-indexes into an Elasticsearch index (`merchant_txn_summary`), upserted by transaction id — this is what the support/fraud-ops UI actually queries.

This mirrors the two options floated for this case (keep the fast-query layer *in* Snowflake vs. push it to Elasticsearch): **both are built**, because Flow 3 is cheap to add once Flow 2 exists, and it lets the case demonstrate the "Snowflake as an OLAP core, ES as the serving cache" pattern explicitly rather than picking one and hand-waving the other.

---

## Part 1 — Why Snowflake in the middle (not just Postgres → Snowflake)

Every prior case ends its chain at the destination. This case's Flow 2 is a **Snowflake-to-Snowflake flow** — both `source.connectionParams` and `destination.connectionParams` in its `collection.json` point at the same account/database, different schemas (`RAW` → `CURATED`). The framework supports this natively: `core/source/snowflake` and `core/destination/snowflake` are independent connector implementations that happen to share a driver, and nothing in the pipeline model requires source and destination to be different systems.

Two source-read strategies exist for Flow 2's input side, and the case deliberately uses the **stream** one to show it off (`enum.UseDBSnowflakeCaptureStreamMethod`):

- **Stream consumption semantics are unusual and worth designing around carefully.** Per the framework's own connector notes: querying a Snowflake stream does **not** advance its offset — only a DML statement that reads the stream, committed in the *same transaction*, does. `ReadByStream` reads with `SELECT * FROM <stream>`, and once every row has been confirmed delivered to the destination (via `CommitHook`), it runs `INSERT INTO <ConsumeTable> SELECT COUNT(*) FROM <stream>` in that same transaction just before `COMMIT` — this is what actually advances the stream. If the destination never fully confirms (e.g. rows permanently backlogged), `Cleanup` rolls back instead, leaving the stream untouched so those rows are re-read next run rather than silently dropped.
- `SnowflakeSourceStreamOptions.ConsumeTable` is a **required field the pipeline author must supply** — the connector never creates or picks this table itself. Flow 2's design needs one dummy table (`STREAM_CONSUME_MARKER`) purely to give the stream-advancing `INSERT` somewhere to write.
- A stopped-mid-read pipeline can leave the reader's transaction open and block the *next* reader against the same stream. The framework begins the stream's transaction with a context that ignores cancellation for the `COMMIT`/`ROLLBACK` call specifically so a cancelled run still frees the stream — this case's Flow 2 should be designed assuming that behavior, not routed around it.

---

## Part 2 — Data Model

### 2.1 PostgreSQL (source of record)

| Table | Key fields |
|---|---|
| `transactions` | `txn_id` (PK), `merchant_id`, `amount`, `currency`, `card_bin`, `status` (`AUTHORIZED`/`CAPTURED`/`DECLINED`/`REFUNDED`), `created_at` |
| `settlements` | `settlement_id` (PK), `txn_id`, `settled_amount`, `settlement_batch_id`, `settled_at` |

~6–8 synthetic merchants, a handful of card BINs reused across merchants (so cross-merchant fraud patterns are visible), a small deliberate settlement/transaction mismatch rate to exercise reconciliation.

### 2.2 Snowflake

| Table/object | Schema | Purpose |
|---|---|---|
| `RAW_TRANSACTIONS`, `RAW_SETTLEMENTS` | `RAW` | Flow 1 landing tables, append-only |
| `MERCHANT_RISK_PROFILE` | `RAW` | Small static/slow-changing reference table, seeded once (merchant risk tier, historical chargeback rate) |
| `TXN_STREAM` (Snowflake Stream on `RAW_TRANSACTIONS`) | `RAW` | Feeds Flow 2 |
| `STREAM_CONSUME_MARKER` | `RAW` | Dummy sink for the stream-advancing `INSERT` (§1) |
| `MERCHANT_TXN_SUMMARY` | `CURATED` | Flow 2's output: per-transaction fraud score + reconciliation status, `updated_at` for Flow 3's time-window read |

### 2.3 Elasticsearch

Index `merchant_txn_summary`, upserted by `txn_id`: merchant, amount, fraud_score, fraud_reason, reconciliation_status, timestamps. This is what the support-agent search UI queries.

---

## Part 3 — AuxDB Checkpointing

| Flow | Checkpoint key | What's tracked |
|---|---|---|
| 1a (transactions) | `pg_txn_progress` | last `created_at` watermark synced to Snowflake |
| 1b (settlements) | `pg_settlement_progress` | last `settled_at` watermark |
| 2 (stream transform) | none needed beyond Snowflake's own stream offset — the stream *is* the checkpoint. AuxDB only needs a run-log table for observability (last run time, rows processed) |
| 3 (Snowflake → ES) | `sf_es_sync_progress` | last `updated_at` watermark read from `MERCHANT_TXN_SUMMARY` |

Each flow gets its own backlog table for fault routing (bad currency codes, negative amounts, orphaned settlements with no matching `txn_id` — the last one is a natural fit for this domain and easy to seed deliberately).

---

## Part 4 — Novel Concepts vs. Cases 1–6

1. **Snowflake as both source and destination in one flow** — nothing prior does this; every other case's warehouse/search destination is a terminal node.
2. **Snowflake Streams as the source connector's capture method** — Cases 1–6 use WAL (Case 2), binlog-style CDC is discussed but not built, and REST cursors (Cases 3–5); this is the first pipeline built on a warehouse-native change-data object, with the "querying doesn't advance the offset, only a same-transaction DML commit does" mechanic as the central gotcha to document, matching Case 6's approach of centering the write-up on one non-obvious framework mechanic.
3. **A transformation flow with no new business data arriving from outside** — Flow 2's source and destination are the same account; the "transform" is entirely the `MERGE`/join SQL in `GenerateQuery`, not new I/O. Every prior case's transform logic reshapes data in transit between two different systems.
4. **Warehouse-side compute cost as a first-class pipeline design constraint** — see Part 5. No prior case has to think about its source/destination *charging by the second the connection is open*.

---

## Part 5 — The Scale Problem, and How This Case Works Around It

This is the reason the case is being planned before being built. Snowflake is fundamentally different from every connector used in Cases 1–6 in three ways that all point the same direction: **build small, document the ceiling, don't pretend this runs at production volume.**

### 5.1 No local emulator, no docker-compose entry, a ticking trial clock

Every other case's infrastructure is `docker compose up` and free forever. Snowflake has no open-source emulator — Redshift's suite stands in on plain Postgres, BigQuery has a real emulator, Snowflake has neither. The only way to run this case for real is a live trial account: 30 days, $400 credit, suspended (not billed) at expiry as long as no card is added. That means:

- Snowflake connection details (`account`, `warehouse`, `database`, `role`, key-pair auth) come from a `.env` the user fills in against their own trial account — the same setup already documented for this framework's own live connector tests (key-pair generation, `GRANT`s, warehouse sizing). Case 7's Makefile should follow that same runbook rather than inventing a new one.
- The case is written and reviewed assuming the account may not exist yet at read time — like Cases 5 and 6, it's fair to ship this as a **design-first case**: docker-compose covers Postgres, AuxDB, and Elasticsearch (all free, all local); Snowflake is BYO-trial, documented, not automated by `make up`.
- Every `make seed-*` target defaults to small volumes (hundreds to low thousands of rows) — enough to see every code path (fraud flag, reconciliation mismatch, backlog routing) at least a few times, not enough to threaten either the 30-day window or the credit balance.

### 5.2 Destination writes are one round trip per call, not a bulk loader

The framework's Snowflake destination executes whatever SQL `GenerateQuery` builds inside a single transaction, one `ExecContext` per payload — there is no COPY-from-stage bulk path wired into the standard write flow (a `PUT`+`COPY INTO` sequence is possible since it's just SQL in the same transaction, but it's not what `ExecuteQuery` does by default, and it wasn't built or tested as a supported pipeline pattern). Two consequences for this case's design:

- **Flow 1 batches multi-row `INSERT ... VALUES (...), (...), ...` per payload**, rather than one `INSERT` per record — cuts round trips roughly by the batch size, which matters because every open connection/running warehouse is billed per-second.
- **The warehouse should be `XS`, with a short auto-suspend (60s) and auto-resume on.** At this case's data volumes, the warehouse should barely ever be warm for more than a few seconds per flow invocation.

### 5.3 A cancelled or runaway query keeps burning credits until it actually stops server-side

Cancelling a Snowflake query from the client only stops chunk downloads if the query already finished executing before cancellation reached it — a genuinely long-running query keeps the warehouse (and the credit meter) alive server-side until Snowflake itself sees the cancel. This case's queries are all small joins over small tables, so it's a non-issue in practice, but it's the reason `MaxPipelineTime` terminate rules on all three flows are a hard requirement here, not just a safety net copied from other cases — an unbounded query against a misconfigured `WHERE` clause is the one mistake in this case that costs real money, not just time.

### 5.4 What "runs at scale" would actually require (documented, not built)

So the write-up is honest about the gap rather than silently capping volume and calling it done — if this were a production pipeline instead of a demo case:

- Flow 1 would batch by row-count *and* byte-size, and likely use `PUT`+`COPY INTO` from a staged file instead of `INSERT` batches (the SF-D5 pattern) — several orders of magnitude cheaper than bound `INSERT`s per Snowflake's own load guidance.
- Flow 2 would run on a real schedule (Snowflake `TASK` or an external scheduler) rather than a manually-invoked `make` target, and the warehouse would be sized against actual join cardinality, not `XS`.
- The 30-day/$400 trial ceiling is specific to *this case's* budget for building and demoing it — it says nothing about what the pattern costs to run for real, and the write-up should say so explicitly rather than implying the trial limits are a property of the architecture.

---

## Part 6 — Execution Plan (once this design is approved)

1. Scaffold `cases/case_7/` following the Case 6 layout: `docker-compose.yml` (Postgres + AuxDB + Elasticsearch only), `Makefile`, `cmd/auxdb_setup`, `cmd/pg_schema` (or reuse a seeder pattern from Case 2/3), `cmd/seeder`, `cmd/es_schema`.
2. Write `collection.json` for the three flows/pipelines, including Flow 2's Snowflake→Snowflake wiring and the `ConsumeTable` requirement.
3. `.env.example` with the Snowflake key-pair/account fields, pointing at the same runbook this framework's own live connector tests use — not duplicating setup instructions.
4. Seed volumes fixed small by default (`TOTAL_ROWS` etc., same convention as Cases 1–6), with a comment at the top of the Makefile stating the credit/time ceiling explicitly, the way Case 6's Makefile documents its own OS-specific path constraints.
5. Update the root `README.md` cases table with a **Case 7** entry, marked "(design)" until it's actually been run end-to-end against a live trial account at least once.

This plan intentionally stops short of writing any Go code or SQL — confirm the flow boundaries (especially whether Flow 3 to Elasticsearch is wanted alongside keeping `MERCHANT_TXN_SUMMARY` queryable in Snowflake directly, or should replace it) before scaffolding begins.
