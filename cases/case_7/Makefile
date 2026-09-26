# ============================================================
# Payments Reconciliation & Fraud Intelligence Pipeline — Case 7
# Setup Makefile (infrastructure + seed data only)
# ============================================================
#
# ARCHITECTURE:
#   Flow 1: Postgres transactions            -> Snowflake RAW_TRANSACTIONS
#   Flow 2: Snowflake TXN_STREAM (in-warehouse transform/MERGE) -> Snowflake MERCHANT_TXN_SUMMARY
#   Flow 3: Snowflake MERCHANT_TXN_SUMMARY   -> Elasticsearch merchant_txn_summary
#
# Snowflake has no local emulator, so it is NOT in docker-compose.yml -
# docker-compose here only brings up Postgres (source) + AuxDB + Elasticsearch.
# `make snowflake-setup` runs the one-time DDL against your own trial account
# instead (30-day/$400-credit trial, key-pair auth - see
# cases/case_7/case_7_etl_casestudy_plan.md Part 5 for why this case is
# deliberately small-scale).
#
# PREREQUISITES:
#   - Docker Desktop running
#   - A Snowflake trial account + key-pair auth set up (SNOWFLAKE_* env vars below)
#   - The etlfunnel-execution engine's collection.json/client code pointed at
#     the same account (see that repo's case 7 build)
#
# USAGE:
#   make up               — start Postgres + AuxDB + Elasticsearch and wait for healthy
#   make setup             — full local bootstrap: up -> Postgres schema -> AuxDB tables -> ES index
#   make snowflake-setup    — one-time DDL against your live Snowflake trial account
#   make seed               — insert TOTAL transaction rows (FAULT_RATE% deliberately invalid)
#   make seed-loop           — keep inserting a fresh batch every SEED_INTERVAL seconds, until Ctrl+C
#   make down               — stop containers (keep volumes)
#   make reset               — stop containers AND destroy volumes (DESTRUCTIVE)
#   make clean-destination    — empty the merchant_txn_summary ES index (keep the mapping)
#   make clean-auxdb           — empty flow_watermark/flow_backlog (restart watermarks from zero)
#
# EXECUTION ORDER:
#   make setup
#   make snowflake-setup   (once, against your trial account)
#   make seed
#   Start flows: pid=1 (Postgres->Snowflake), pid=2 (Snowflake stream transform), pid=3 (Snowflake->ES)
#   Flow 2 depends on Flow 1 having landed rows for TXN_STREAM to have anything to read;
#   Flow 3 depends on Flow 2 having written MERCHANT_TXN_SUMMARY rows.

ifeq ($(OS),Windows_NT)
SHELL := C:/Program Files/Git/bin/bash.exe
endif
.SHELLFLAGS := -euo pipefail -c

export GOTMPDIR := $(CURDIR)/tmp
$(shell mkdir -p $(CURDIR)/tmp)

COMPOSE_FILE := docker-compose.yml

AUXDB_DSN ?= postgresql://etl_user:etl_pass@localhost:5435/auxdb?sslmode=disable
ES_URL    ?= http://localhost:9200

TOTAL      ?= 1000
FAULT_RATE ?= 5
# Seconds between iterations for seed-loop.
SEED_INTERVAL ?= 20

# Snowflake trial account details - override with your own, or export the
# SNOWFLAKE_* env vars directly (same names cmd/snowflake_setup reads).
SNOWFLAKE_ACCOUNT           ?= $(TEST_SNOWFLAKE_ACCOUNT)
SNOWFLAKE_USER              ?= $(TEST_SNOWFLAKE_USER)
SNOWFLAKE_PRIVATE_KEY_FILE  ?= $(TEST_SNOWFLAKE_PRIVATE_KEY_FILE)
SNOWFLAKE_WAREHOUSE         ?= $(TEST_SNOWFLAKE_WAREHOUSE)
SNOWFLAKE_DATABASE          ?= $(TEST_SNOWFLAKE_DATABASE)
SNOWFLAKE_ROLE              ?= $(TEST_SNOWFLAKE_ROLE)
SNOWFLAKE_SCHEMA            ?= $(or $(TEST_SNOWFLAKE_SCHEMA),PUBLIC)

.PHONY: up down reset setup snowflake-setup seed seed-loop clean-destination clean-auxdb

## Start Postgres + AuxDB + Elasticsearch
up:
	@echo ">>> Starting Docker containers..."
	docker compose -f $(COMPOSE_FILE) up -d
	@echo ">>> Waiting for all services to be healthy..."
	@for svc in case7_payments_db case7_auxdb case7_elasticsearch; do \
		printf "  Waiting for $$svc "; \
		for i in $$(seq 1 60); do \
			STATUS=$$(docker inspect --format='{{.State.Health.Status}}' $$svc 2>/dev/null || echo "missing"); \
			if [ "$$STATUS" = "healthy" ]; then printf " ok\n"; break; fi; \
			printf "."; sleep 3; \
		done; \
	done
	@echo ">>> All containers healthy"

## Stop containers, keep volumes
down:
	docker compose -f $(COMPOSE_FILE) down

## Stop containers and destroy volumes (DESTRUCTIVE)
reset:
	docker compose -f $(COMPOSE_FILE) down -v
	@echo ">>> Volumes destroyed"

## Full local bootstrap: containers -> Postgres schema -> AuxDB tables -> ES index
setup: up
	@echo ">>> Downloading Go dependencies..."
	go mod download
	go mod tidy
	@echo ">>> Creating Postgres transactions table..."
	go run ./cmd/pg_schema
	@echo ">>> Creating AuxDB tables..."
	go run ./cmd/auxdb_setup
	@echo ">>> Creating Elasticsearch index..."
	go run ./cmd/es_schema -es "$(ES_URL)"
	@echo ""
	@echo ">>> Local setup complete. Next steps:"
	@echo "   make snowflake-setup   - one-time DDL against your trial account"
	@echo "   make seed              - seed transaction rows"
	@echo "   Start flows: pid=1, pid=2, pid=3"

## One-time DDL against your live Snowflake trial account
snowflake-setup:
	@echo ">>> Running Snowflake setup..."
	SNOWFLAKE_ACCOUNT="$(SNOWFLAKE_ACCOUNT)" \
	SNOWFLAKE_USER="$(SNOWFLAKE_USER)" \
	SNOWFLAKE_PRIVATE_KEY_FILE="$(SNOWFLAKE_PRIVATE_KEY_FILE)" \
	SNOWFLAKE_WAREHOUSE="$(SNOWFLAKE_WAREHOUSE)" \
	SNOWFLAKE_DATABASE="$(SNOWFLAKE_DATABASE)" \
	SNOWFLAKE_ROLE="$(SNOWFLAKE_ROLE)" \
	SNOWFLAKE_SCHEMA="$(SNOWFLAKE_SCHEMA)" \
	go run ./cmd/snowflake_setup
	@echo ">>> Snowflake setup complete"

## Seed TOTAL transaction rows (FAULT_RATE% deliberately invalid, to exercise backlog_1)
seed:
	@echo ">>> Seeding $(TOTAL) transaction rows (fault rate $(FAULT_RATE)%)..."
	go run ./cmd/seeder -total $(TOTAL) -fault-rate $(FAULT_RATE)
	@echo ">>> Seed complete"

## Keep seeding a new batch every SEED_INTERVAL seconds. Ctrl+C to stop.
seed-loop:
	@echo ">>> Looping seed every $(SEED_INTERVAL)s (Ctrl+C to stop)..."
	@i=0; \
	while true; do \
		echo ">>> [seed-loop] iteration $$i"; \
		go run ./cmd/seeder -total $(TOTAL) -fault-rate $(FAULT_RATE); \
		i=$$((i+1)); \
		sleep $(SEED_INTERVAL); \
	done

## Empty the merchant_txn_summary ES index (keep the mapping)
clean-destination:
	@echo ">>> Deleting all documents from merchant_txn_summary..."
	curl -s -X POST "$(ES_URL)/merchant_txn_summary/_delete_by_query" \
		-H 'Content-Type: application/json' -d '{"query":{"match_all":{}}}' >/dev/null
	@echo ">>> Destination clean"

## Empty flow_watermark/flow_backlog (restart watermarks from zero)
clean-auxdb:
	@echo ">>> Truncating AuxDB runtime tables..."
	docker exec case7_auxdb psql -U etl_user -d auxdb -c "TRUNCATE TABLE flow_watermark, flow_backlog RESTART IDENTITY;"
	@echo ">>> AuxDB runtime state clean"
