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
#   make deps         — download Go module dependencies
#   make auxdb        — create AuxDB tables (run once after 'up')
#   make cassandra    — create Cassandra keyspace + table (run once after 'up')
#   make seed         — start Zepto REST API mock server (:11334)
#   make seed-stop    — stop the mock seeder
#   make setup        — full bootstrap: up → auxdb
#   make watch        — live metrics dashboard (polls AuxDB, Ctrl+C to exit)
#                       Override: make watch INTERVAL=10s
#   make logs         — tail all container logs
#   make status       — show container health
#   make down         — stop containers (keep volumes)
#   make reset        — stop containers AND destroy volumes (DESTRUCTIVE)
#   make lint         — run golangci-lint
#
# SEED OVERRIDES:
#   make seed TOTAL_EVENTS=5000 SEEDER_PORT=11334
#
# PIPELINE ENV VARS (set before running streamcraftexecution):
#   ZEPTO_API_BASE_URL=http://localhost:11334
#   ZEPTO_API_TOKEN=dev-token   (seeder accepts any non-empty value)

SHELL := C:/Program Files/Git/bin/bash.exe
.SHELLFLAGS := -euo pipefail -c

export GOTMPDIR := $(CURDIR)/tmp
export GONOSUMCHECK := *

# Ensure Go's build tmp dir exists (required on Windows — Go won't create it)
$(shell mkdir -p $(CURDIR)/tmp)

COMPOSE_FILE := docker-compose.yml

AUXDB_DSN   ?= postgresql://etl_user:etl_pass@localhost:5446/auxdb?sslmode=disable

SEEDER_PORT  ?= 11334
TOTAL_EVENTS ?= 2000

INTERVAL     ?= 5s

.PHONY: up down reset wait-healthy deps auxdb cassandra seed seed-stop setup \
        watch logs status lint

## Start AuxDB + Kafka + Cassandra containers
up:
	@echo ">>> Starting Docker containers..."
	docker compose -f $(COMPOSE_FILE) up -d
	@echo ">>> Waiting for all services to be healthy..."
	$(MAKE) wait-healthy

## Wait until all containers report healthy
wait-healthy:
	@echo "  Checking health..."
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

## Download Go module dependencies
deps:
	@echo ">>> Downloading Go dependencies..."
	go mod download
	go mod tidy
	@echo ">>> Dependencies ready"

## Create AuxDB tables
auxdb:
	@echo ">>> Setting up AuxDB..."
	go build -o $(GOTMPDIR)/auxdb_init$(GOEXE) ./cmd/auxdb_setup
	$(GOTMPDIR)/auxdb_init$(GOEXE) -dsn "$(AUXDB_DSN)"
	@echo ">>> AuxDB ready"

## Create Cassandra keyspace and table
cassandra:
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
	@echo ">>> Cassandra ready"

## Start Zepto REST API mock server in the background
seed:
	@echo ">>> Starting Zepto mock seeder on :$(SEEDER_PORT) with $(TOTAL_EVENTS) events..."
	SEEDER_PORT=$(SEEDER_PORT) TOTAL_EVENTS=$(TOTAL_EVENTS) \
	go run ./cmd/seeder & echo $$! > .seeder.pid
	@echo "  Seeder PID: $$(cat .seeder.pid)"
	@sleep 2
	@echo ">>> Seeder listening on :$(SEEDER_PORT)"
	@echo ">>> API: GET http://localhost:$(SEEDER_PORT)/api/v2/order-events?limit=500"

## Stop the mock seeder
seed-stop:
	@if [ -f .seeder.pid ]; then \
		PID=$$(cat .seeder.pid); \
		kill $$PID 2>/dev/null && echo ">>> Seeder (PID $$PID) stopped" || echo ">>> Seeder already stopped"; \
		rm -f .seeder.pid; \
	else \
		echo ">>> No seeder PID file found"; \
	fi

## Full bootstrap: start containers → auxdb → cassandra
setup: up auxdb cassandra
	@echo ">>> Setup complete."
	@echo ">>> Run 'make seed' to start the Zepto mock API server."
	@echo ">>> Then start Flow 2 (pid=29) followed by Flow 1 (pid=28) in streamcraftexecution."

## Live metrics dashboard
watch:
	AUXDB_DSN="$(AUXDB_DSN)" \
	go run ./cmd/metrics_watcher --interval $(INTERVAL)

## Tail container logs
logs:
	docker compose -f $(COMPOSE_FILE) logs -f

## Show container health
status:
	@docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' \
		--filter name=zepto_auxdb \
		--filter name=zepto_kafka \
		--filter name=zepto_cassandra

## Run golangci-lint
lint:
	@echo ">>> Running linter..."
	golangci-lint run ./...
	@echo ">>> Lint done"
