# ============================================================
# Telecom ETL — DB Setup Makefile
# ============================================================
#
# USAGE:
#   make up          — start all 6 Docker containers
#   make setup       — full bootstrap: up + MySQL/Postgres/AuxDB schemas + seed
#   make seed        — insert synthetic telecom data (500 rec/shard, smoke-test volume)
#   make seed-scale  — scale seed (100K rec/shard, split cap 50K → forces dynamic table splits)
#   make down        — stop and remove containers (keeps volumes)
#   make reset       — stop containers AND destroy volumes (DESTRUCTIVE)

ifeq ($(OS),Windows_NT)
SHELL := C:/Program Files/Git/bin/bash.exe
endif
.SHELLFLAGS := -euo pipefail -c

export GOTMPDIR := $(CURDIR)/tmp
$(shell mkdir -p $(CURDIR)/tmp)

COMPOSE_FILE := docker-compose.yml
RECORDS_PER_SHARD ?= 500
WORKERS ?= 4
SPLIT_ROW_CAP ?= 1000000

.PHONY: up down reset seed seed-scale setup

## Start all 6 containers
up:
	@echo ">>> Starting Docker containers..."
	docker compose -f $(COMPOSE_FILE) up -d
	@echo ">>> Waiting for all services to be healthy..."
	@for svc in vodafone_db idea_db tata_docomo_db aircel_db destination_db auxdb; do \
		printf "  Waiting for $$svc "; \
		for i in $$(seq 1 30); do \
			STATUS=$$(docker inspect --format='{{.State.Health.Status}}' $$svc 2>/dev/null || echo "missing"); \
			if [ "$$STATUS" = "healthy" ]; then printf " ok\n"; break; fi; \
			printf "."; sleep 3; \
		done; \
	done
	@echo ">>> All containers healthy"

## Seed synthetic data into MySQL source DBs
seed:
	@echo ">>> Seeding synthetic data ($(RECORDS_PER_SHARD) records/shard, $(WORKERS) workers, split cap $(SPLIT_ROW_CAP))..."
	go run ./cmd/seeder --records-per-shard=$(RECORDS_PER_SHARD) --workers=$(WORKERS) --split-row-cap=$(SPLIT_ROW_CAP)
	@echo ">>> Seeding done"

## Scale seed — seeds RECORDS_PER_SHARD records with a lower split cap to force dynamic table splits.
## Default: 100K records/shard, split cap 50K → guarantees at least one _2 split per shard.
## Override: make seed-scale RECORDS_PER_SHARD=200000 SPLIT_ROW_CAP=100000
seed-scale: RECORDS_PER_SHARD = 100000
seed-scale: SPLIT_ROW_CAP = 50000
seed-scale:
	@echo ">>> Scale seeding ($(RECORDS_PER_SHARD) records/shard, split cap $(SPLIT_ROW_CAP), $(WORKERS) workers)..."
	@echo ">>> Each shard will overflow the split cap and trigger dynamic split _2 creation."
	go run ./cmd/seeder --records-per-shard=$(RECORDS_PER_SHARD) --workers=$(WORKERS) --split-row-cap=$(SPLIT_ROW_CAP)
	@echo ">>> Scale seeding done"

## Full bootstrap: up → MySQL/Postgres/AuxDB schemas → seed
setup: up
	@echo ">>> Downloading Go dependencies..."
	go mod download
	@echo ">>> Creating MySQL schemas..."
	go run ./cmd/mysql_schema
	@echo ">>> Creating Postgres destination schemas..."
	go run ./cmd/postgres_schema
	@echo ">>> Creating AuxDB schemas and seeding reference data..."
	go build -o $(GOTMPDIR)/auxdb_init$(GOEXE) ./cmd/auxdb_setup
	$(GOTMPDIR)/auxdb_init$(GOEXE)
	$(MAKE) seed
	@echo ""
	@echo "============================================"
	@echo "  Telecom ETL infrastructure is READY"
	@echo "============================================"
	@echo ""
	@echo "  MySQL sources:"
	@echo "    vodafone_db    -> localhost:3306"
	@echo "    idea_db        -> localhost:3307"
	@echo "    tata_docomo_db -> localhost:3308"
	@echo "    aircel_db      -> localhost:3309"
	@echo ""
	@echo "  Postgres destination -> localhost:5434 / destination_db"
	@echo "  AuxDB               -> localhost:5435 / auxdb"
	@echo ""
	@echo "  Credentials: user=etl_user  password=etl_pass"
	@echo "============================================"

## Stop containers (keep volumes)
down:
	docker compose -f $(COMPOSE_FILE) down

## Destroy everything including volumes
reset:
	@echo "WARNING: This will destroy all data volumes!"
	docker compose -f $(COMPOSE_FILE) down -v
