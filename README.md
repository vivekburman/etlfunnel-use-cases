# ETLFunnel Use Cases

A public library of real-world ETL examples built with **ETLFunnel** — each case demonstrates how to solve a specific data engineering problem using the framework's primitives: connectors, transformers, pipelines, orchestrators, checkpoints, and more.

Browse the cases to understand patterns, copy what you need, or use them as a starting point for your own ETL flows.

---

## What is ETLFunnel?

ETLFunnel is a framework for building reliable, resumable, and observable ETL pipelines. It gives you typed building blocks (connectors, transformers, pipelines, orchestrators) and takes care of the hard parts: checkpointing, backlogging failed records, dynamic throughput tuning, and termination safety.

These use cases show what that looks like in practice.

---

## Cases

<details>
<summary><strong>Case 1 — Telecom Merger: Multi-Source Database Consolidation</strong></summary>

Four MySQL databases (Vodafone, Idea, Tata Docomo, Aircel) consolidated into a single PostgreSQL destination with schema normalisation, deduplication, per-shard checkpointing, and dynamic batch tuning.

**Stack:** Go, MySQL 8.0, PostgreSQL, Docker Compose · [Case study](cases/case_1/)

</details>

<details>
<summary><strong>Case 2 — Zomato Platform Order Intelligence: WAL + Cold Backfill to Elasticsearch</strong></summary>

Four PostgreSQL databases (Zomato Food, Blinkit, Hyperpure, District) unified into a single Elasticsearch index via three independent pipeline collections: a cold backfill flow (paginated SELECT), a WAL ingestion stage (Postgres logical replication → Redis Streams), and a stream indexing stage (Redis Streams → Elasticsearch). Overlap between flows resolved via upsert on `{sub_brand}_{order_id}`.

**Stack:** Go, PostgreSQL (WAL), Redis Streams, Elasticsearch, Docker Compose · [Case study](cases/case_2/)

</details>

<details>
<summary><strong>Case 3 — Myntra Digital Analytics Intelligence: GA4 Multi-Property → MSSQL</strong></summary>

Three Google Analytics 4 properties (Web, Android, iOS) ingested into a Microsoft SQL Server data warehouse via three pipeline flows: a historical backfill flow (730-day chunked daily pagination with quota throttling), an incremental daily flow (T-2 upsert for settled GA4 data), and a realtime pulse flow (60-second cadence via the GA4 Realtime API). Central challenge is GA4 quota exhaustion — per-property, per-hour token budgets require the pipeline to track spend and back off before limits are hit.

**Stack:** Go, REST API (GA4 Data API), Microsoft SQL Server, Docker Compose · [Case study](cases/case_3/)

</details>

<details>
<summary><strong>Case 4 — Zepto Order Events: REST API (cursor) → Kafka → Cassandra</strong></summary>

Zepto order lifecycle events (ORDER_CREATED → ORDER_DELIVERED) ingested from an internal REST API across 7 cities and written into Cassandra for analytics and auditing, via two decoupled pipeline flows: a cursor ingestion flow (paginated GET with cursor checkpointing → Kafka topic `zepto.order.events`) and a stream storage flow (Kafka → Cassandra `zepto_events.order_events`). Each flow maintains its own AuxDB checkpoint table — cursor positions for Flow 1, per-partition Kafka offsets for Flow 2 — so either can resume independently after a crash. Three distinct fault paths are exercised: silent record drops (missing city), parse errors routed to `zepto_storage_backlog` (Flow 2), and publish errors routed to `zepto_ingestion_backlog` (Flow 1). Cassandra table is partitioned by `(city, store_id)` with a 90-day TTL and TimeWindowCompactionStrategy for time-series workloads.

**Stack:** Go, REST API (cursor pagination), Kafka (KRaft), Cassandra 4.1, PostgreSQL (AuxDB), Docker Compose · [Case study](cases/case_4/)

</details>

---

## How to use this repo

Each case lives in `cases/case_N/` and is self-contained. Inside you will find:

- A `docker-compose.yml` to spin up all the required databases locally
- A `Makefile` with targets to bootstrap, seed data, run the pipeline, and clean up
- The full ETLFunnel client code generated for that scenario
- A case study plan document explaining the architecture and design decisions

Start with the case study plan to understand the problem, then follow the `make setup` → `make watch` flow to see it run end to end.

---

## Contributing

New cases are welcome. A good case has a clear real-world problem, a self-contained Docker setup, and enough generated client code to demonstrate the pattern end to end. Open a PR and describe what ETL problem the case solves.
