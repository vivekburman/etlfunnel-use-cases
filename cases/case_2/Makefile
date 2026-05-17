# ============================================================
# Zomato Platform Order Intelligence ETL — DB Setup Makefile
# ============================================================
#
# USAGE:
#   make up           — start all 7 Docker containers
#   make deps         — download Go module dependencies
#   make pg-schema    — create Postgres schemas on all 4 source DBs
#   make es-schema    — create Elasticsearch platform_orders index
#   make auxdb        — create AuxDB schemas + seed reference data
#   make wal-enable   — enable WAL logical replication on all 4 source DBs
#   make seed         — insert synthetic order data (500 rec/city, smoke-test volume)
#   make seed-scale   — scale seed (100K rec/city, split cap 50K → forces dynamic splits)
#   make setup        — run ALL of the above in order (full bootstrap)
#   make down         — stop and remove containers (keeps volumes)
#   make reset        — stop containers AND destroy volumes (DESTRUCTIVE)
#   make clean-migration — wipe AuxDB state + ES documents for a fresh pipeline re-run
#   make logs         — tail all container logs
#   make status       — show container health
#   make watch        — start the live metrics watcher (polls until Ctrl+C)
#                       Override: make watch INTERVAL=10s

SHELL := C:/Program Files/Git/bin/bash.exe
.SHELLFLAGS := -euo pipefail -c

COMPOSE_FILE    := docker-compose.yml
RECORDS_PER_CITY ?= 500
WORKERS          ?= 4
SPLIT_ROW_CAP    ?= 1000000

AUXDB_DSN  ?= host=localhost port=5445 dbname=auxdb user=etl_user password=etl_pass sslmode=disable
ES_URL     ?= http://localhost:9200
REDIS_ADDR ?= localhost:6379
INTERVAL   ?= 5s
WATCH_LOG  ?= metrics.log

.PHONY: up down reset clean-migration deps pg-schema es-schema auxdb wal-enable seed seed-scale setup logs status wait-healthy watch

## Start all 7 containers
up:
	@echo ">>> Starting Docker containers..."
	docker compose -f $(COMPOSE_FILE) up -d
	@echo ">>> Waiting for all services to be healthy..."
	$(MAKE) wait-healthy

## Wait until all 7 containers report healthy
wait-healthy:
	@echo "  Checking health..."
	@for svc in zomato_food_db blinkit_db hyperpure_db district_db auxdb redis elasticsearch; do \
		printf "  Waiting for $$svc "; \
		for i in $$(seq 1 40); do \
			STATUS=$$(docker inspect --format='{{.State.Health.Status}}' $$svc 2>/dev/null || echo "missing"); \
			if [ "$$STATUS" = "healthy" ]; then printf " ok\n"; break; fi; \
			printf "."; sleep 3; \
		done; \
	done
	@echo ">>> All containers healthy"

## Download Go module dependencies
deps:
	@echo ">>> Downloading Go dependencies..."
	cd . && go mod download
	@echo ">>> Dependencies ready"

## Create Postgres schemas on all 4 source DBs
pg-schema: deps
	@echo ">>> Creating Postgres source schemas..."
	go run ./cmd/pg_schema
	@echo ">>> Postgres schemas done"

## Create Elasticsearch platform_orders index
es-schema: deps
	@echo ">>> Creating Elasticsearch index..."
	go run ./cmd/es_schema --es=$(ES_URL)
	@echo ">>> ES schema done"

## Create AuxDB schemas + seed reference data
auxdb: deps
	@echo ">>> Creating AuxDB schemas and seeding..."
	go run ./cmd/auxdb_setup
	@echo ">>> AuxDB done"

## Enable WAL logical replication on all 4 source DBs
wal-enable: deps
	@echo ">>> Enabling WAL logical replication..."
	go run ./cmd/wal_enabler --auxdb "$(AUXDB_DSN)"
	@echo ">>> WAL enabler done"

## Seed synthetic order data
seed: deps
	@echo ">>> Seeding synthetic data ($(RECORDS_PER_CITY) records/city, $(WORKERS) workers, split cap $(SPLIT_ROW_CAP))..."
	go run ./cmd/seeder --records-per-city=$(RECORDS_PER_CITY) --workers=$(WORKERS) --split-row-cap=$(SPLIT_ROW_CAP)
	@echo ">>> Seeding done"

## Scale seed — forces dynamic table splits
seed-scale: RECORDS_PER_CITY = 100000
seed-scale: SPLIT_ROW_CAP = 50000
seed-scale: deps
	@echo ">>> Scale seeding ($(RECORDS_PER_CITY) records/city, split cap $(SPLIT_ROW_CAP), $(WORKERS) workers)..."
	go run ./cmd/seeder --records-per-city=$(RECORDS_PER_CITY) --workers=$(WORKERS) --split-row-cap=$(SPLIT_ROW_CAP)
	@echo ">>> Scale seeding done"

## Full bootstrap: up → pg-schema → es-schema → auxdb → wal-enable → seed
setup: up pg-schema es-schema auxdb wal-enable seed
	@echo ""
	@echo "============================================"
	@echo "  Zomato ETL infrastructure is READY"
	@echo "============================================"
	@echo ""
	@echo "  Source Postgres DBs:"
	@echo "    zomato_food_db → localhost:5441"
	@echo "    blinkit_db     → localhost:5442"
	@echo "    hyperpure_db   → localhost:5443"
	@echo "    district_db    → localhost:5444"
	@echo ""
	@echo "  AuxDB            → localhost:5445 / auxdb"
	@echo "  Redis            → localhost:6379"
	@echo "  Elasticsearch    → localhost:9200"
	@echo ""
	@echo "  Credentials: user=etl_user  password=etl_pass"
	@echo "============================================"

## Wipe AuxDB migration state + ES documents for a fresh pipeline re-run
clean-migration:
	@echo ">>> [1/2] Clearing AuxDB migration state tables..."
	@docker exec -e PGPASSWORD=etl_pass auxdb \
		psql -U etl_user -d auxdb -c \
		"TRUNCATE pipeline_checkpoints, backlog_records, wal_positions, backfill_progress, backfill_completion_log, es_write_log RESTART IDENTITY CASCADE;"
	@echo ">>> [2/2] Deleting all documents from Elasticsearch index 'platform_orders'..."
	@curl -s -o /dev/null -w "    ES delete-by-query: HTTP %{http_code}\n" \
		-u elastic:etl_pass \
		-X POST "$(ES_URL)/platform_orders/_delete_by_query?refresh=true&conflicts=proceed" \
		-H 'Content-Type: application/json' \
		-d '{"query":{"match_all":{}}}' \
	|| echo "    (skipped — ES may be down or index does not exist yet)"
	@echo ""
	@echo "============================================"
	@echo "  Migration state cleared — ready to re-run"
	@echo "============================================"
	@echo "  AuxDB cleared  : pipeline_checkpoints, backlog_records, wal_positions,"
	@echo "                   backfill_progress, backfill_completion_log, es_write_log"
	@echo "  AuxDB preserved: city_mapping, brand_sla_rules, terminate_rules, write_tune_config"
	@echo "  ES cleared     : all documents in platform_orders (index + mapping preserved)"
	@echo "  Source DBs     : untouched (Postgres order data intact)"
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

## watch — live metrics dashboard (polls until Ctrl+C)
watch:
	go run ./cmd/metrics_watcher \
		--auxdb   "$(AUXDB_DSN)" \
		--es      "$(ES_URL)" \
		--redis   "$(REDIS_ADDR)" \
		--interval $(INTERVAL) \
		--log     $(WATCH_LOG)
