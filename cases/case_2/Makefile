# ============================================================
# Zomato Platform Order Intelligence ETL — DB Setup Makefile
# ============================================================
#
# USAGE:
#   make up           — start all 7 Docker containers
#   make setup        — full bootstrap: up + Postgres/ES schemas + AuxDB + WAL enable + seed
#   make seed         — insert synthetic order data (500 rec/city, smoke-test volume)
#   make seed-scale   — scale seed (100K rec/city, split cap 50K → forces dynamic splits)
#   make down         — stop and remove containers (keeps volumes)
#   make reset        — stop containers AND destroy volumes (DESTRUCTIVE)
#   make clean-destination — empty the platform_orders ES index (keep index/mapping)
#
# HOT PIPELINE (live Postgres writer — makes WAL pile up for the ETL to consume):
#   make hot          — start the live order generator (runs until Ctrl+C)
#                       Continuously inserts synthetic orders into all 4 brand DBs
#                       so WAL events accumulate in the replication slots.
#                       The separate ETL pipeline reads those slots → Redis → ES.
#                       Override: make hot BATCH=200 HOT_INTERVAL=5s

ifeq ($(OS),Windows_NT)
SHELL := C:/Program Files/Git/bin/bash.exe
endif
.SHELLFLAGS := -euo pipefail -c

export GOTMPDIR := $(CURDIR)/tmp
$(shell mkdir -p $(CURDIR)/tmp)

COMPOSE_FILE    := docker-compose.yml
RECORDS_PER_CITY ?= 500
WORKERS          ?= 4
SPLIT_ROW_CAP    ?= 1000000

AUXDB_DSN  ?= host=localhost port=5445 dbname=auxdb user=etl_user password=etl_pass sslmode=disable
ES_URL     ?= http://localhost:9200

.PHONY: up down reset seed seed-scale setup hot clean-destination

## Start all 7 containers
up:
	@echo ">>> Starting Docker containers..."
	docker compose -f $(COMPOSE_FILE) up -d
	@echo ">>> Waiting for all services to be healthy..."
	@for svc in zomato_food_db blinkit_db hyperpure_db district_db auxdb redis elasticsearch; do \
		printf "  Waiting for $$svc "; \
		for i in $$(seq 1 40); do \
			STATUS=$$(docker inspect --format='{{.State.Health.Status}}' $$svc 2>/dev/null || echo "missing"); \
			if [ "$$STATUS" = "healthy" ]; then printf " ok\n"; break; fi; \
			printf "."; sleep 3; \
		done; \
	done
	@echo ">>> All containers healthy"

## Seed synthetic order data
seed:
	@echo ">>> Seeding synthetic data ($(RECORDS_PER_CITY) records/city, $(WORKERS) workers, split cap $(SPLIT_ROW_CAP))..."
	go run ./cmd/seeder --records-per-city=$(RECORDS_PER_CITY) --workers=$(WORKERS) --split-row-cap=$(SPLIT_ROW_CAP)
	@echo ">>> Seeding done"

## Scale seed — forces dynamic table splits
seed-scale: RECORDS_PER_CITY = 100000
seed-scale: SPLIT_ROW_CAP = 50000
seed-scale:
	@echo ">>> Scale seeding ($(RECORDS_PER_CITY) records/city, split cap $(SPLIT_ROW_CAP), $(WORKERS) workers)..."
	go run ./cmd/seeder --records-per-city=$(RECORDS_PER_CITY) --workers=$(WORKERS) --split-row-cap=$(SPLIT_ROW_CAP)
	@echo ">>> Scale seeding done"

## Full bootstrap: up → Postgres/ES schemas → AuxDB → WAL enable → seed
setup: up
	@echo ">>> Downloading Go dependencies..."
	go mod download
	@echo ">>> Creating Postgres source schemas..."
	go run ./cmd/pg_schema
	@echo ">>> Creating Elasticsearch index..."
	go run ./cmd/es_schema --es=$(ES_URL)
	@echo ">>> Creating AuxDB schemas and seeding reference data..."
	go build -o $(GOTMPDIR)/auxdb_init$(GOEXE) ./cmd/auxdb_setup
	$(GOTMPDIR)/auxdb_init$(GOEXE)
	@echo ">>> Enabling WAL logical replication..."
	go run ./cmd/wal_enabler --auxdb "$(AUXDB_DSN)"
	$(MAKE) seed
	@echo ""
	@echo "============================================"
	@echo "  Zomato ETL infrastructure is READY"
	@echo "============================================"
	@echo ""
	@echo "  Source Postgres DBs:"
	@echo "    zomato_food_db -> localhost:5441"
	@echo "    blinkit_db     -> localhost:5442"
	@echo "    hyperpure_db   -> localhost:5443"
	@echo "    district_db    -> localhost:5444"
	@echo ""
	@echo "  AuxDB            -> localhost:5445 / auxdb"
	@echo "  Redis            -> localhost:6379"
	@echo "  Elasticsearch    -> localhost:9200"
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

## Empty the platform_orders ES index without dropping the index/mapping
clean-destination:
	@echo ">>> Deleting all documents from platform_orders..."
	docker exec elasticsearch curl -s -u elastic:etl_pass -X POST "localhost:9200/platform_orders/_delete_by_query" \
		-H 'Content-Type: application/json' -d '{"query":{"match_all":{}}}'
	@echo ""
	@echo ">>> Destination clean"

# ── Hot Pipeline ─────────────────────────────────────────────────────────────
# Prereq: make setup must have completed (containers healthy, schemas created,
#         WAL slots established, source data seeded) before running this.

## hot — continuously insert live orders into all 4 brand DBs (WAL accumulates for ETL).
## Runs until Ctrl+C.  Override batch size and interval:
##   make hot BATCH=200 HOT_INTERVAL=5s
BATCH        ?= 50
HOT_INTERVAL ?= 2s
hot:
	@echo ">>> Starting live order generator (batch=$(BATCH) interval=$(HOT_INTERVAL))..."
	go run ./cmd/hot_pipeline \
		--batch    $(BATCH) \
		--interval $(HOT_INTERVAL)
