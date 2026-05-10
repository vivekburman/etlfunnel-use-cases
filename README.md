# ETLFunnel Use Cases

A public library of real-world ETL examples built with **ETLFunnel** — each case demonstrates how to solve a specific data engineering problem using the framework's primitives: connectors, transformers, pipelines, orchestrators, checkpoints, and more.

Browse the cases to understand patterns, copy what you need, or use them as a starting point for your own ETL flows.

---

## What is ETLFunnel?

ETLFunnel is a framework for building reliable, resumable, and observable ETL pipelines. It gives you typed building blocks (connectors, transformers, pipelines, orchestrators) and takes care of the hard parts: checkpointing, backlogging failed records, dynamic throughput tuning, and termination safety.

These use cases show what that looks like in practice.

---

## Cases

### Case 1 — Telecom Merger: Multi-Source Database Consolidation

**Scenario:** Four telecom companies (Vodafone, Idea, Tata Docomo, Aircel) are being merged. Each has its own MySQL customer database, its own sharding strategy, and its own schema conventions. The goal is to consolidate all customer, subscription, billing, SIM, and porting data into a single unified PostgreSQL destination — without data loss, with deduplication, and with the ability to resume if the pipeline is interrupted.

**What it covers:**

- Connecting to 4 independent MySQL sources with different zone/state sharding schemes
- Schema normalization across companies with mismatched column names and types
- Transformations: type casting, PII masking, plan mapping, geo tagging, null handling, dedup checking
- A 4-layer destination model: `raw → staging → curated → audit`
- Idempotent writes with `INSERT ... ON CONFLICT DO UPDATE`
- Per-shard checkpointing so the pipeline can resume exactly where it left off
- Backlog routing for records that fail validation or hit conflicts
- Runtime-tunable termination rules (error rate thresholds, idle timeout, manual kill)
- Dynamic batch size tuning (bulk mode during off-hours, throttled mode during production)
- A live metrics watcher to monitor pipeline progress

**Stack:** Go, MySQL 8.0, PostgreSQL, Docker Compose

[Go to Case 1](cases/case_1/)

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
