# ============================================================
# Pepperfry Product Catalog Enrichment Pipeline — Case 5
# Windows Setup Makefile  (infrastructure only)
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
#
# USAGE (run on Windows):
#   make up           — start AuxDB + Postgres source + Kafka + Elasticsearch
#   make deps         — download Go module dependencies
#   make auxdb        — create AuxDB checkpoint/backlog tables (run once after 'up')
#   make es-index     — create Elasticsearch pf_products index with dense_vector (run once)
#   make seed         — insert 200 test products into Postgres source DB
#   make setup        — full bootstrap: up → auxdb → es-index → seed
#   make logs         — tail all container logs
#   make status       — show container health
#   make down         — stop containers (keep volumes)
#   make reset        — stop containers AND destroy volumes (DESTRUCTIVE)
#
# PIPELINE ENV VARS (set in PowerShell before running flows):
#   $$env:PF_POSTGRES_PASSWORD = "etl_pass"
#   $$env:PF_ORACLE_PASSWORD   = "etl_pass"   (if running Flow 33)
#   $$env:ENRICH_SERVICE_URL   = "http://<mac-ip>:8765"
#
# EXECUTION ORDER:
#   [Windows] make setup
#   [Mac]     make -f Makefile.mac enrich
#   [Mac]     Start flows: pid=34 → pid=35 → pid=32

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

.PHONY: up down reset wait-healthy deps auxdb es-index seed setup logs status

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

## Full bootstrap: start containers → create AuxDB tables → create ES index → seed Postgres
setup: up auxdb es-index seed
	@echo ""
	@echo ">>> Setup complete. Next steps (on Mac):"
	@echo "   1. ollama serve"
	@echo "   2. make -f Makefile.mac enrich"
	@echo "   3. export PF_POSTGRES_PASSWORD=etl_pass ENRICH_SERVICE_URL=http://localhost:8765"
	@echo "   4. Start flows: pid=34 → pid=35 → pid=32"
	@echo "   5. Verify ES:   curl http://$(ES_URL)/pf_products/_count"

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
