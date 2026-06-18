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
MAC_IP          ?= 192.168.1.41
OLLAMA_URL      ?= http://$(MAC_IP):11434
ENRICH_URL      ?= http://$(MAC_IP):8765
SEED_N          ?= 200
COLLECTION_JSON ?= C:/Users/vivek/streamcraftexecution/collection.json

.PHONY: up down reset wait-healthy deps auxdb es-index seed setup oracle-pull oracle-run oracle-schema oracle-seed oracle-setup logs status ping-mac patch-collection

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

## ── Oracle (Flow 33 source — manual license required) ──────────────────────
##
## Oracle XE is excluded from docker-compose.yml because the image requires
## accepting Oracle's license at container-registry.oracle.com before pulling.
##
## Run these targets in order (once only):
##   make oracle-pull    — authenticate and pull the Oracle XE image
##   make oracle-run     — start the Oracle XE container on port 1521
##   make oracle-schema  — create etl_user and PRODUCT_ATTRS table
##   make oracle-seed    — insert 50 synthetic ERP product rows
##   make oracle-setup   — runs oracle-run → oracle-schema → oracle-seed
##
## Prerequisites:
##   docker login container-registry.oracle.com
##   (accept the Oracle Database license at https://container-registry.oracle.com)

ORACLE_PWD      ?= etl_pass
ORACLE_IMG      ?= container-registry.oracle.com/database/express:21.3.0-xe

## Pull Oracle XE image (requires docker login to container-registry.oracle.com first)
oracle-pull:
	@echo ">>> Pulling Oracle XE image (requires prior docker login)..."
	docker pull $(ORACLE_IMG)
	@echo "  ✓ Oracle XE image ready"

## Start Oracle XE container on port 1521
oracle-run:
	@echo ">>> Starting Oracle XE container..."
	docker run -d --name pf_oracle \
		-p 1521:1521 \
		-e ORACLE_PWD=$(ORACLE_PWD) \
		$(ORACLE_IMG)
	@echo "  Waiting for Oracle listener (up to 3 min)..."
	@for i in $$(seq 1 36); do \
		STATUS=$$(docker inspect --format='{{.State.Health.Status}}' pf_oracle 2>/dev/null || echo "missing"); \
		if [ "$$STATUS" = "healthy" ]; then echo "  ✓ Oracle ready"; break; fi; \
		printf "."; sleep 5; \
	done
	@echo ""

## Create etl_user and PRODUCT_ATTRS table in Oracle XE
oracle-schema:
	@echo ">>> Creating Oracle schema and PRODUCT_ATTRS table..."
	@printf '%s\n' \
		'ALTER SESSION SET "_ORACLE_SCRIPT"=TRUE;' \
		'CREATE USER etl_user IDENTIFIED BY $(ORACLE_PWD);' \
		'GRANT CREATE SESSION, CREATE TABLE, UNLIMITED TABLESPACE TO etl_user;' \
		'EXIT;' \
		| docker exec -i pf_oracle sqlplus -S SYSTEM/$(ORACLE_PWD)@//localhost:1521/XEPDB1
	@printf '%s\n' \
		'CREATE TABLE PRODUCT_ATTRS (' \
		'  PRODUCT_ID  VARCHAR2(50)  NOT NULL PRIMARY KEY,' \
		'  SKU_CODE    VARCHAR2(100),' \
		'  SUPPLIER_ID VARCHAR2(50),' \
		'  WEIGHT_KG   NUMBER(8,3),' \
		'  CATEGORY    VARCHAR2(100),' \
		'  COMPLIANCE  VARCHAR2(500),' \
		'  UPDATED_AT  TIMESTAMP DEFAULT SYSTIMESTAMP' \
		');' \
		'EXIT;' \
		| docker exec -i pf_oracle sqlplus -S etl_user/$(ORACLE_PWD)@//localhost:1521/XEPDB1
	@echo "  ✓ Oracle schema ready"

## Seed Oracle PRODUCT_ATTRS with synthetic ERP rows
oracle-seed:
	@echo ">>> Seeding Oracle PRODUCT_ATTRS..."
	go run ./cmd/oracle_seeder -host localhost -port 1521 -service XEPDB1 \
		-user etl_user -password $(ORACLE_PWD) -n 50
	@echo "  ✓ Oracle seed complete"

## Full Oracle bootstrap: run container → create schema → seed data
oracle-setup: oracle-run oracle-schema oracle-seed
	@echo ""
	@echo ">>> Oracle setup complete."
	@echo "   Set env var before running flows: $$env:PF_ORACLE_PASSWORD=$(ORACLE_PWD)"
	@echo "   Flow 33 (pid=33) reads from etl_user.PRODUCT_ATTRS on localhost:1521/XEPDB1"

## ────────────────────────────────────────────────────────────────────────────

## Patch collection.json with current MAC_IP and correct Postgres settings (re-run if Mac IP changes)
patch-collection:
	@echo ">>> Patching collection.json for MAC_IP=$(MAC_IP)..."
	@sed -i 's|"baseURL": "http://[0-9.]*:8765"|"baseURL": "http://$(MAC_IP):8765"|g' "$(COLLECTION_JSON)"
	@sed -i 's|"port": 5432, "sslMode": "disable", "database": "pepperfry"|"port": 5433, "sslMode": "disable", "database": "pf_catalog"|g' "$(COLLECTION_JSON)"
	@echo "  ✓ Enrich Service URL → http://$(MAC_IP):8765"
	@echo "  ✓ Postgres           → port=5433 database=pf_catalog"

## Check Mac Enrich Service is reachable from Windows
ping-mac:
	@echo ">>> Pinging Mac Enrich Service at $(ENRICH_URL)..."
	@curl --max-time 5 --connect-timeout 3 -sf "$(ENRICH_URL)/health" \
		&& echo "  ✓ Enrich Service reachable at $(ENRICH_URL)" \
		|| echo "  ✗ Enrich Service NOT reachable at $(ENRICH_URL)"

## Wipe AuxDB checkpoint/cursor/offset state for a clean pipeline re-run
clean-checkpoints:
	@echo ">>> Clearing AuxDB pipeline state..."
	@docker exec pf_auxdb psql -U etl_user -d auxdb -c \
		"TRUNCATE pf_catalog_checkpoints, pf_enrich_collect_cursors, pf_enrich_submit_offsets, \
		          pf_postgres_ingest_backlog, pf_enrich_submit_backlog, pf_es_index_backlog RESTART IDENTITY CASCADE;"
	@echo "  ✓ Checkpoints cleared — pipeline will re-process from scratch"

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
