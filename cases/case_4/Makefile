# ============================================================
# Zepto Order Events Pipeline — Case 4 Setup Makefile
# ============================================================
#
# ARCHITECTURE:
#   Flow 1: Zepto REST API (cursor) → Kafka topic zepto.order.events
#   Flow 2: Kafka topic              → Cassandra zepto_events.order_events
#
# USAGE:
#   make up           — start AuxDB + Kafka + Cassandra containers
#   make setup        — full bootstrap: up → AuxDB tables → Cassandra keyspace/table
#   make seed         — start Zepto REST API mock server (:11334) with realistic fault mix
#   make seed-stop    — stop the mock seeder
#   make seed-backlog — insert synthetic backlog rows into AuxDB
#   make down         — stop containers (keep volumes)
#   make reset        — stop containers AND destroy volumes (DESTRUCTIVE)
#   make clean-destination — empty zepto_events.order_events (keep keyspace/table)
#
# SEED OVERRIDES:
#   make seed TOTAL_EVENTS=5000 SEEDER_PORT=11334
#
# PIPELINE ENV VARS (set before running streamcraftexecution):
#   ZEPTO_API_BASE_URL=http://localhost:11334
#   ZEPTO_API_TOKEN=dev-token   (seeder accepts any non-empty value)

ifeq ($(OS),Windows_NT)
SHELL := C:/Program Files/Git/bin/bash.exe
endif
.SHELLFLAGS := -euo pipefail -c

export GOTMPDIR := $(CURDIR)/tmp
export GONOSUMCHECK := *

# Ensure Go's build tmp dir exists (required on Windows — Go won't create it)
$(shell mkdir -p $(CURDIR)/tmp)

COMPOSE_FILE := docker-compose.yml

AUXDB_DSN   ?= postgresql://etl_user:etl_pass@localhost:5446/auxdb?sslmode=disable

SEEDER_PORT  ?= 11334
TOTAL_EVENTS ?= 2000
FAULT_RATE   ?= 5

BACKLOG_N    ?= 20

.PHONY: up down reset setup seed seed-stop seed-backlog clean-destination

## Start AuxDB + Kafka + Cassandra containers
up:
	@echo ">>> Starting Docker containers..."
	docker compose -f $(COMPOSE_FILE) up -d
	@echo ">>> Waiting for all services to be healthy..."
	@for svc in zepto_auxdb zepto_kafka zepto_cassandra; do \
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

## Empty zepto_events.order_events without dropping the keyspace/table
clean-destination:
	@echo ">>> Truncating zepto_events.order_events..."
	docker exec zepto_cassandra cqlsh -e "TRUNCATE zepto_events.order_events;"
	@echo ">>> Destination clean"

## Full bootstrap: start containers → AuxDB tables → Cassandra keyspace/table
setup: up
	@echo ">>> Downloading Go dependencies..."
	go mod download
	go mod tidy
	@echo ">>> Setting up AuxDB..."
	go build -o $(GOTMPDIR)/auxdb_init$(GOEXE) ./cmd/auxdb_setup
	$(GOTMPDIR)/auxdb_init$(GOEXE) -dsn "$(AUXDB_DSN)"
	@echo ">>> Setting up Cassandra keyspace and table..."
	docker exec zepto_cassandra cqlsh -e "\
		CREATE KEYSPACE IF NOT EXISTS zepto_events \
		  WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1} \
		  AND durable_writes = true; \
		CREATE TABLE IF NOT EXISTS zepto_events.order_events ( \
		  city        text, \
		  store_id    text, \
		  event_type  text, \
		  created_at  timestamp, \
		  event_id    uuid, \
		  order_id    text, \
		  customer_id text, \
		  status      text, \
		  amount      decimal, \
		  payload     text, \
		  run_id      text, \
		  PRIMARY KEY ((city, store_id), event_type, created_at, event_id) \
		) WITH CLUSTERING ORDER BY (event_type ASC, created_at DESC, event_id ASC) \
		  AND compaction = {'class': 'TimeWindowCompactionStrategy', \
		                    'compaction_window_unit': 'DAYS', \
		                    'compaction_window_size': 1} \
		  AND comment = 'Order lifecycle events. TTL=90d set per-row by pipeline.';"
	@echo ">>> Setup complete."
	@echo ">>> Tip: run 'make seed-backlog' to pre-populate backlog tables."
	@echo ">>> Run 'make seed' to start the Zepto mock API (includes 5% fault records by default)."
	@echo ">>> Then start Flow 2 (pid=29) followed by Flow 1 (pid=28) in streamcraftexecution."

## Start Zepto REST API mock server in the background (stops any existing instance first)
seed: seed-stop
	@echo ">>> Starting Zepto mock seeder on :$(SEEDER_PORT) with $(TOTAL_EVENTS) events (fault_rate=$(FAULT_RATE)%)..."
	SEEDER_PORT=$(SEEDER_PORT) TOTAL_EVENTS=$(TOTAL_EVENTS) FAULT_RATE=$(FAULT_RATE) \
	go run ./cmd/seeder & echo $$! > .seeder.pid
	@echo "  Seeder PID: $$(cat .seeder.pid)"
	@sleep 2
	@echo ">>> Seeder listening on :$(SEEDER_PORT)"
	@echo ">>> API: GET http://localhost:$(SEEDER_PORT)/api/v2/order-events?limit=500"

## Seed both AuxDB backlog tables with synthetic failed records
seed-backlog:
	@echo ">>> Seeding backlog tables with $(BACKLOG_N) synthetic failures each..."
	go run ./cmd/backlog_seeder -n $(BACKLOG_N)
	@echo ">>> Done."

## Stop the mock seeder
seed-stop:
	@if [ -f .seeder.pid ]; then \
		PID=$$(cat .seeder.pid); \
		kill $$PID 2>/dev/null && echo ">>> Seeder (PID $$PID) stopped" || echo ">>> Seeder already stopped"; \
		rm -f .seeder.pid; \
	else \
		echo ">>> No seeder PID file found"; \
	fi
