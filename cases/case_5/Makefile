# ============================================================
# Pepperfry Product Catalog Enrichment Pipeline — Case 5 Setup Makefile
# ============================================================
#
# ARCHITECTURE:
#   Flow 32: Postgres pf_catalog → Kafka pepperfry.catalog.raw  (finite)
#   Flow 33: Oracle PF_ERP      → Kafka pepperfry.catalog.raw  (finite, fan-in)
#   Flow 34: Kafka              → Mac Enrich Service POST /enrich  (infinite)
#   Flow 35: Mac Enrich Service GET /results → Elasticsearch  (finite, idle-timeout)
#
# PREREQUISITES:
#   - Docker Desktop running
#   - Ollama installed: brew install ollama
#   - Model pulled: ollama pull nomic-embed-text
#
# USAGE:
#   make up           — start AuxDB + Postgres source + Kafka + Elasticsearch
#   make deps         — download Go module dependencies
#   make auxdb        — create AuxDB checkpoint/backlog tables (run once after 'up')
#   make es-index     — create Elasticsearch pf_products index with dense_vector (run once)
#   make seed         — insert 200 test products into Postgres source DB
#   make enrich       — start the Mac Enrich Service on :8765 (Ollama must be running)
#   make enrich-stop  — stop the Mac Enrich Service
#   make setup        — full bootstrap: up → auxdb → es-index → seed
#   make logs         — tail all container logs
#   make status       — show container health
#   make down         — stop containers (keep volumes)
#   make reset        — stop containers AND destroy volumes (DESTRUCTIVE)
#
# PIPELINE ENV VARS (set before running streamcraftexecution):
#   $$env:PF_POSTGRES_PASSWORD = "etl_pass"
#   $$env:PF_ORACLE_PASSWORD   = "etl_pass"   (if running Flow 33)
#   $$env:ENRICH_SERVICE_URL   = "http://localhost:8765"
#
# EXECUTION ORDER:
#   1. make setup && make enrich
#   2. Start Flow 34 (pid=34) — Kafka consumer must be up before producers
#   3. Start Flow 35 (pid=35) — polls Mac service for results
#   4. Start Flow 32 (pid=32) — Postgres ingest (finite)
#   5. Start Flow 33 (pid=33) — Oracle ingest (finite, if Oracle is configured)

SHELL := C:/Program Files/Git/bin/bash.exe
.SHELLFLAGS := -euo pipefail -c

export GOTMPDIR := $(CURDIR)/tmp
export GONOSUMCHECK := *

$(shell mkdir -p $(CURDIR)/tmp)

COMPOSE_FILE := docker-compose.yml

AUXDB_DSN       ?= postgresql://etl_user:etl_pass@localhost:5446/auxdb?sslmode=disable
POSTGRES_SRC_DSN ?= postgresql://etl_user:etl_pass@localhost:5433/pf_catalog?sslmode=disable
ES_URL          ?= http://127.0.0.1:9200
ENRICH_PORT     ?= 8765
OLLAMA_URL      ?= http://localhost:11434
SEED_N          ?= 200

.PHONY: up down reset wait-healthy deps auxdb es-index seed enrich enrich-stop \
        setup logs status

## Start all containers
up:
	@echo ">>> Starting Docker containers..."
	docker compose -f $(COMPOSE_FILE) up -d
	@echo ">>> Waiting for all services to be healthy..."
	$(MAKE) wait-healthy

## Wait until all containers report healthy
wait-healthy:
	@echo "  Checking health..."
	@for svc in pf_auxdb pf_postgres_src pf_kafka pf_elasticsearch; do \
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

## Create AuxDB tables (run once after 'up')
auxdb:
	@echo ">>> Setting up AuxDB tables..."
	go build -o $(GOTMPDIR)/auxdb_init$(GOEXE) ./cmd/auxdb_setup
	$(GOTMPDIR)/auxdb_init$(GOEXE) -dsn "$(AUXDB_DSN)"
	@echo ">>> AuxDB ready"

## Create Elasticsearch pf_products index with dense_vector mapping (run once after 'up')
es-index:
	@echo ">>> Creating Elasticsearch pf_products index..."
	go run ./cmd/es_schema --es=$(ES_URL)
	@echo ">>> Elasticsearch index ready"

## Seed Postgres source DB with synthetic product records
seed:
	@echo ">>> Seeding Postgres pf_catalog with $(SEED_N) products..."
	go run ./cmd/seeder -dsn "$(POSTGRES_SRC_DSN)" -n $(SEED_N)
	@echo ">>> Seed complete"

## Start the Mac Enrich Service (Ollama must already be running: ollama serve)
enrich: enrich-stop
	@echo ">>> Starting Mac Enrich Service on :$(ENRICH_PORT)..."
	ENRICH_PORT=$(ENRICH_PORT) OLLAMA_URL=$(OLLAMA_URL) \
	go run ./cmd/mac_enrich_service & echo $$! > .enrich.pid
	@echo "  Enrich Service PID: $$(cat .enrich.pid)"
	@sleep 2
	@curl --ipv4 -sf http://127.0.0.1:$(ENRICH_PORT)/health && echo ">>> Mac Enrich Service healthy" || echo ">>> WARNING: health check failed"

## Stop the Mac Enrich Service
enrich-stop:
	@if [ -f .enrich.pid ]; then \
		PID=$$(cat .enrich.pid); \
		kill $$PID 2>/dev/null && echo ">>> Enrich Service (PID $$PID) stopped" || echo ">>> Enrich Service already stopped"; \
		rm -f .enrich.pid; \
	else \
		echo ">>> No enrich service PID file found"; \
	fi

## Full bootstrap: start containers → create AuxDB tables → create ES index → seed Postgres
setup: up auxdb es-index seed
	@echo ""
	@echo ">>> Setup complete. Next steps:"
	@echo "   1. Start Ollama:              ollama serve"
	@echo "   2. Start Enrich Service:      make enrich"
	@echo "   3. Set env vars:"
	@echo "      \$$env:PF_POSTGRES_PASSWORD = 'etl_pass'"
	@echo "      \$$env:ENRICH_SERVICE_URL   = 'http://localhost:8765'"
	@echo "   4. Start flows in order:      pid=34 → pid=35 → pid=32"
	@echo "   5. Verify ES:                 curl http://localhost:9200/pf_products/_count"

## Tail container logs
logs:
	docker compose -f $(COMPOSE_FILE) logs -f

## Show container health
status:
	@docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' \
		--filter name=pf_auxdb \
		--filter name=pf_postgres_src \
		--filter name=pf_kafka \
		--filter name=pf_elasticsearch
