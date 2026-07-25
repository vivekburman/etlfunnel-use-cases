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
#   make up                — start AuxDB + Postgres source + Kafka + Elasticsearch
#   make setup              — full bootstrap: up → AuxDB tables → ES index → seed
#   make seed              — one-shot: insert 200 products into Postgres (initial bootstrap only)
#   make seed-hot          — continuous: keep inserting new products into Postgres (run alongside ETL)
#   make oracle-seed-hot   — continuous: keep inserting new rows into Oracle PRODUCT_ATTRS (run alongside ETL)
#   make seeds-hot         — continuous: run both Postgres + Oracle seeders in parallel (Ctrl-C stops both)
#   make down              — stop containers (keep volumes)
#   make reset             — stop containers AND destroy volumes (DESTRUCTIVE)
#
# ORACLE (Flow 33 source — manual license required, run once):
#   docker login container-registry.oracle.com
#   (accept the Oracle Database license at https://container-registry.oracle.com)
#   make oracle-setup      — pulls Oracle XE, starts the container, creates schema
#
# PIPELINE ENV VARS (set in PowerShell before running flows):
#   $$env:PF_POSTGRES_PASSWORD = "etl_pass"
#   $$env:PF_ORACLE_PASSWORD   = "etl_pass"   (if running Flow 33)
#   $$env:ENRICH_SERVICE_URL   = "http://<mac-ip>:8765"
#
# EXECUTION ORDER:
#   [Windows] make setup
#   [Mac]     make -f Makefile.mac start
#   [Windows] make seed-hot          (terminal 1 — continuous Postgres feed)
#   [Windows] make oracle-seed-hot   (terminal 2 — continuous Oracle feed)
#   [Windows] Start flows: pid=34 → pid=35 → pid=32

SHELL := C:/Program Files/Git/bin/bash.exe
.SHELLFLAGS := -euo pipefail -c

export GOTMPDIR := $(CURDIR)/tmp
export GONOSUMCHECK := *

$(shell mkdir -p $(CURDIR)/tmp)

COMPOSE_FILE := docker-compose.yml

AUXDB_DSN       ?= postgresql://etl_user:etl_pass@localhost:5446/auxdb?sslmode=disable
POSTGRES_SRC_DSN ?= postgresql://etl_user:etl_pass@localhost:5433/pf_catalog?sslmode=disable
ES_URL          ?= http://127.0.0.1:9200
SEED_N          ?= 200
HOT_BATCH       ?= 20
HOT_INTERVAL    ?= 5s
ORACLE_HOT_BATCH     ?= 10
ORACLE_HOT_INTERVAL  ?= 5s

.PHONY: up down reset setup seed seed-hot seeds-hot oracle-setup oracle-seed-hot

## Start all containers
up:
	@echo ">>> Starting Docker containers..."
	docker compose -f $(COMPOSE_FILE) up -d
	@echo ">>> Waiting for all services to be healthy..."
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

## Full bootstrap: start containers → AuxDB tables → ES index → initial Postgres seed
setup: up
	@echo ">>> Downloading Go dependencies..."
	go mod download
	go mod tidy
	@echo ">>> Setting up AuxDB tables..."
	go build -o $(GOTMPDIR)/auxdb_init$(GOEXE) ./cmd/auxdb_setup
	$(GOTMPDIR)/auxdb_init$(GOEXE) -dsn "$(AUXDB_DSN)"
	@echo ">>> Creating Elasticsearch pf_products index..."
	go run ./cmd/es_schema --es=$(ES_URL)
	$(MAKE) seed
	@echo ""
	@echo ">>> Setup complete. Next steps:"
	@echo "   [Mac]     ollama serve"
	@echo "   [Mac]     make -f Makefile.mac start"
	@echo "   [Windows] make seeds-hot         - continuous Postgres + Oracle feed (keep running)"
	@echo "   [Windows] export PF_POSTGRES_PASSWORD=etl_pass"
	@echo "   [Windows] Start flows: pid=34 -> pid=35 -> pid=32"
	@echo "   [Windows] Verify ES:   curl $(ES_URL)/pf_products/_count"

## Seed Postgres source DB with synthetic product records (one-shot, for initial bootstrap)
seed:
	@echo ">>> Seeding Postgres pf_catalog with $(SEED_N) products..."
	go run ./cmd/seeder -dsn "$(POSTGRES_SRC_DSN)" -n $(SEED_N)
	@echo ">>> Seed complete"

## Continuously insert new products into Postgres (run alongside the ETL pipelines)
seed-hot:
	@echo ">>> Starting continuous Postgres seeder (batch=$(HOT_BATCH), interval=$(HOT_INTERVAL))..."
	@echo ">>> Ctrl-C to stop."
	go run ./cmd/hot_seeder --dsn "$(POSTGRES_SRC_DSN)" --batch $(HOT_BATCH) --interval $(HOT_INTERVAL)

## Run both Postgres and Oracle hot seeders in parallel (Ctrl-C stops both)
seeds-hot:
	@echo ">>> Starting both hot seeders in parallel (Ctrl-C stops both)..."
	@go run ./cmd/hot_seeder --dsn "$(POSTGRES_SRC_DSN)" --batch $(HOT_BATCH) --interval $(HOT_INTERVAL) & \
	go run ./cmd/oracle_seeder -host localhost -port 1521 -service XEPDB1 \
		-user etl_user -password $(ORACLE_PWD) \
		--batch $(ORACLE_HOT_BATCH) --interval $(ORACLE_HOT_INTERVAL) & \
	wait

## ── Oracle (Flow 33 source — manual license required) ──────────────────────
##
## Oracle XE is excluded from docker-compose.yml because the image requires
## accepting Oracle's license at container-registry.oracle.com before pulling.
## Run once: docker login container-registry.oracle.com, then 'make oracle-setup'.

ORACLE_PWD      ?= etl_pass
ORACLE_IMG      ?= container-registry.oracle.com/database/express:21.3.0-xe

## Pull Oracle XE, start the container, create etl_user + PRODUCT_ATTRS table (run once)
oracle-setup:
	@echo ">>> Pulling Oracle XE image (requires prior docker login)..."
	docker pull $(ORACLE_IMG)
	@echo "  Oracle XE image ready"
	@echo ">>> Starting Oracle XE container..."
	docker run -d --name pf_oracle \
		-p 1521:1521 \
		-e ORACLE_PWD=$(ORACLE_PWD) \
		$(ORACLE_IMG)
	@echo "  Waiting for Oracle listener (up to 3 min)..."
	@for i in $$(seq 1 36); do \
		STATUS=$$(docker inspect --format='{{.State.Health.Status}}' pf_oracle 2>/dev/null || echo "missing"); \
		if [ "$$STATUS" = "healthy" ]; then echo "  Oracle ready"; break; fi; \
		printf "."; sleep 5; \
	done
	@echo ""
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
	@echo "  Oracle schema ready"
	@echo ""
	@echo ">>> Oracle setup complete."
	@echo "   Set env var before running flows: $$env:PF_ORACLE_PASSWORD=$(ORACLE_PWD)"
	@echo "   Flow 33 (pid=33) reads from etl_user.PRODUCT_ATTRS on localhost:1521/XEPDB1"
	@echo "   Run 'make oracle-seed-hot' in a separate terminal to continuously feed new rows."

## Continuously insert new rows into Oracle PRODUCT_ATTRS (run alongside the ETL pipelines)
oracle-seed-hot:
	@echo ">>> Starting continuous Oracle seeder (batch=$(ORACLE_HOT_BATCH), interval=$(ORACLE_HOT_INTERVAL))..."
	@echo ">>> Ctrl-C to stop."
	go run ./cmd/oracle_seeder -host localhost -port 1521 -service XEPDB1 \
		-user etl_user -password $(ORACLE_PWD) \
		--batch $(ORACLE_HOT_BATCH) --interval $(ORACLE_HOT_INTERVAL)
