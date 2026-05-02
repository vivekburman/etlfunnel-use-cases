# ============================================================
# Telecom ETL — DB Setup Makefile
# ============================================================
#
# USAGE:
#   make up          — start all 6 Docker containers
#   make deps        — download Go module dependencies
#   make mysql       — create MySQL sharded schemas on all 4 source DBs
#   make postgres    — create destination Postgres schemas (raw/staging/curated/audit + partitions)
#   make auxdb       — create AuxDB schemas + seed plan_mapping, terminate_rules, write_tune_config
#   make seed        — insert synthetic telecom data (500 rec/shard, smoke-test volume)
#   make seed-scale  — production-scale seed (1.1M rec/shard, triggers dynamic table splits)
#   make setup       — run ALL of the above in order (full bootstrap)
#   make down        — stop and remove containers (keeps volumes)
#   make reset           — stop containers AND destroy volumes (DESTRUCTIVE)
#   make clean-migration — wipe destination + AuxDB state for a fresh pipeline re-run (keeps source data & config)
#   make logs            — tail all container logs
#   make status      — show container health

SHELL := C:/Program Files/Git/bin/bash.exe
.SHELLFLAGS := -euo pipefail -c

COMPOSE_FILE := docker-compose.yml
RECORDS_PER_SHARD ?= 500
WORKERS ?= 4

.PHONY: up down reset clean-migration deps mysql postgres auxdb seed setup logs status wait-healthy

## Start all 6 containers
up:
	@echo ">>> Starting Docker containers..."
	docker compose -f $(COMPOSE_FILE) up -d
	@echo ">>> Waiting for all services to be healthy..."
	$(MAKE) wait-healthy

## Wait until all 6 containers report healthy
wait-healthy:
	@echo "  Checking health..."
	@for svc in vodafone_db idea_db tata_docomo_db aircel_db destination_db auxdb; do \
		printf "  Waiting for $$svc "; \
		for i in $$(seq 1 30); do \
			STATUS=$$(docker inspect --format='{{.State.Health.Status}}' $$svc 2>/dev/null || echo "missing"); \
			if [ "$$STATUS" = "healthy" ]; then printf " ✓\n"; break; fi; \
			printf "."; sleep 3; \
		done; \
	done
	@echo ">>> All containers healthy"

## Download Go module dependencies
deps:
	@echo ">>> Downloading Go dependencies..."
	cd . && go mod download
	@echo ">>> Dependencies ready"

## Create MySQL sharded schemas on all 4 source DBs
mysql: deps
	@echo ">>> Creating MySQL schemas..."
	go run ./cmd/mysql_schema
	@echo ">>> MySQL schemas done"

## Create destination Postgres schemas
postgres: deps
	@echo ">>> Creating Postgres destination schemas..."
	go run ./cmd/postgres_schema
	@echo ">>> Postgres destination schemas done"

## Create AuxDB schemas + seed
auxdb: deps
	@echo ">>> Creating AuxDB schemas and seeding..."
	go run ./cmd/auxdb_setup
	@echo ">>> AuxDB done"

## Seed synthetic data into MySQL source DBs
seed: deps
	@echo ">>> Seeding synthetic data ($(RECORDS_PER_SHARD) records/shard, $(WORKERS) workers)..."
	go run ./cmd/seeder --records-per-shard=$(RECORDS_PER_SHARD) --workers=$(WORKERS)
	@echo ">>> Seeding done"

## Production-scale seed — 1.1M records/shard guarantees at least one dynamic split per shard.
## Runtime: long (tens of minutes). Ensure Docker containers have sufficient disk + memory.
seed-scale: deps
	@echo ">>> Production-scale seeding (1100000 records/shard, $(WORKERS) workers)..."
	@echo ">>> Each shard will overflow 1M rows and trigger dynamic split _2 creation."
	go run ./cmd/seeder --records-per-shard=1100000 --workers=$(WORKERS)
	@echo ">>> Scale seeding done"

## Full bootstrap: up → mysql → postgres → auxdb → seed
setup: up mysql postgres auxdb seed
	@echo ""
	@echo "============================================"
	@echo "  Telecom ETL infrastructure is READY"
	@echo "============================================"
	@echo ""
	@echo "  MySQL sources:"
	@echo "    vodafone_db    → localhost:3306"
	@echo "    idea_db        → localhost:3307"
	@echo "    tata_docomo_db → localhost:3308"
	@echo "    aircel_db      → localhost:3309"
	@echo ""
	@echo "  Postgres destination → localhost:5434 / destination_db"
	@echo "  AuxDB               → localhost:5435 / auxdb"
	@echo ""
	@echo "  Credentials: user=etl_user  password=etl_pass"
	@echo "============================================"

## Wipe destination + AuxDB state for a fresh migration re-run (source data and config are untouched)
clean-migration:
	@echo ">>> Truncating destination DB tables (raw, staging, curated, audit)..."
	@docker exec -e PGPASSWORD=etl_pass destination_db \
		psql -U etl_user -d destination_db -c \
		"TRUNCATE raw.customers, raw.subscriptions, raw.billing_accounts, raw.sim_inventory, raw.port_history, staging.customers, staging.subscriptions, staging.billing_accounts, staging.sim_inventory, staging.port_history, curated.customers, curated.subscriptions, curated.billing_accounts, curated.sim_inventory, curated.port_history, curated.network_assignments, curated.customer_merge_log, audit.audit_trail RESTART IDENTITY CASCADE;"
	@echo ">>> Clearing AuxDB migration state tables..."
	@docker exec -e PGPASSWORD=etl_pass auxdb \
		psql -U etl_user -d auxdb -c \
		"TRUNCATE pipeline_checkpoints, backlog_records, dedup_registry, reconciliation_log, audit_trail, customer_merge_log RESTART IDENTITY CASCADE;"
	@echo ""
	@echo "============================================"
	@echo "  Migration state cleared — ready to re-run"
	@echo "============================================"
	@echo "  Destination DB : raw/staging/curated/audit tables truncated (schemas intact)"
	@echo "  AuxDB cleared  : checkpoints, backlogs, dedup, reconciliation,"
	@echo "                   audit_trail, customer_merge_log"
	@echo "  AuxDB preserved: plan_mapping, terminate_rules, write_tune_config"
	@echo "  Source DBs     : untouched (MySQL data intact)"
	@echo "============================================"

## Stop containers (keep volumes)
down:
	docker compose -f $(COMPOSE_FILE) down

## Destroy everything including volumes
reset:
	@echo "WARNING: This will destroy all data volumes!"
	docker compose -f $(COMPOSE_FILE) down -v

## Tail logs
logs:
	docker compose -f $(COMPOSE_FILE) logs -f

## Container status
status:
	docker compose -f $(COMPOSE_FILE) ps
