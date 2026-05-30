# ============================================================
# Myntra Digital Analytics Intelligence — DB Setup Makefile
# ============================================================
#
# USAGE:
#   make up           — start SQL Server + AuxDB containers
#   make deps         — download Go module dependencies
#   make auxdb        — create AuxDB schemas + seed reference data
#   make migrate      — create SQL Server schemas, tables, indexes
#   make seed         — start the GA4 mock seeder HTTP server (:9090)
#   make seed-stop    — stop the seeder process
#   make setup        — full bootstrap: up → auxdb → migrate
#   make backfill     — run Historical Backfill Flow (730 days × 3 properties)
#   make daily        — run Incremental Daily Flow (T-2 + T-1 for all 3 properties)
#   make realtime     — run a single Realtime Pulse snapshot (test mode)
#   make verify       — assert row counts and no duplicates in SQL Server
#   make clean        — truncate stage.* and dbo.ga4_sessions / realtime_sessions
#   make down         — stop containers (keep volumes)
#   make reset        — stop containers AND destroy volumes (DESTRUCTIVE)
#   make logs         — tail all container logs
#   make status       — show container health
#   make test         — run Go unit tests
#   make lint         — run golangci-lint
#
# BACKFILL OVERRIDES:
#   make backfill DATE_FROM=2024-06-01 DATE_TO=2024-12-31
#   make seed ROWS_PER_DAY=5000 SEEDER_PORT=9090

SHELL := C:/Program Files/Git/bin/bash.exe
.SHELLFLAGS := -euo pipefail -c

COMPOSE_FILE   := docker-compose.yml

MSSQL_HOST     ?= localhost
MSSQL_PORT     ?= 1433
MSSQL_SA_PASS  ?= Etl_Pass_123!
MSSQL_DB       ?= analytics_warehouse
MSSQL_USER     ?= etl_writer
MSSQL_PASS     ?= Etl_Pass_456!

AUXDB_DSN      ?= host=localhost port=5446 dbname=auxdb user=etl_user password=etl_pass sslmode=disable

SEEDER_PORT    ?= 9090
SEEDER_URL     ?= http://localhost:$(SEEDER_PORT)
ROWS_PER_DAY   ?= 15000

DATE_FROM      ?= 2024-01-01
DATE_TO        ?= 2025-12-31

.PHONY: up down reset wait-healthy deps auxdb migrate seed seed-stop setup \
        backfill daily realtime verify clean logs status test lint

## Start SQL Server + AuxDB containers
up:
	@echo ">>> Starting Docker containers..."
	docker compose -f $(COMPOSE_FILE) up -d
	@echo ">>> Waiting for all services to be healthy..."
	$(MAKE) wait-healthy

## Wait until both containers report healthy
wait-healthy:
	@echo "  Checking health..."
	@for svc in analytics_sqlserver auxdb; do \
		printf "  Waiting for $$svc "; \
		for i in $$(seq 1 40); do \
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

## Create AuxDB schema + seed reference data
auxdb:
	@echo ">>> Setting up AuxDB..."
	go run ./cmd/auxdb_setup \
		-dsn "$(AUXDB_DSN)"
	@echo ">>> AuxDB ready"

## Create SQL Server database, schemas, tables, indexes
migrate:
	@echo ">>> Applying SQL Server schema..."
	go run ./cmd/mssql_schema \
		-host $(MSSQL_HOST) \
		-port $(MSSQL_PORT) \
		-sa-pass "$(MSSQL_SA_PASS)" \
		-db $(MSSQL_DB) \
		-user $(MSSQL_USER) \
		-pass "$(MSSQL_PASS)"
	@echo ">>> SQL Server schema ready"

## Start GA4 mock seeder HTTP server in the background
seed:
	@echo ">>> Starting GA4 seeder on :$(SEEDER_PORT)..."
	DATE_FROM=$(DATE_FROM) DATE_TO=$(DATE_TO) \
	ROWS_PER_DAY=$(ROWS_PER_DAY) SEEDER_PORT=$(SEEDER_PORT) \
	go run ./cmd/seeder &
	echo $$! > .seeder.pid
	@echo "  Seeder PID: $$(cat .seeder.pid)"
	@sleep 2
	@echo ">>> Seeder listening on :$(SEEDER_PORT)"

## Stop the GA4 seeder
seed-stop:
	@if [ -f .seeder.pid ]; then \
		PID=$$(cat .seeder.pid); \
		kill $$PID 2>/dev/null && echo ">>> Seeder (PID $$PID) stopped" || echo ">>> Seeder already stopped"; \
		rm -f .seeder.pid; \
	else \
		echo ">>> No seeder PID file found"; \
	fi

## Full bootstrap: start containers → auxdb → migrate
setup: up auxdb migrate
	@echo ">>> Setup complete. Run 'make seed' then 'make backfill' to ingest data."

## Run Historical Backfill Flow (all 3 properties, chunked by day)
backfill:
	@echo ">>> Starting Historical Backfill: $(DATE_FROM) → $(DATE_TO)"
	SEEDER_URL=$(SEEDER_URL) \
	MSSQL_HOST=$(MSSQL_HOST) MSSQL_PORT=$(MSSQL_PORT) MSSQL_DB=$(MSSQL_DB) \
	MSSQL_USER=$(MSSQL_USER) MSSQL_PASS="$(MSSQL_PASS)" \
	AUXDB_DSN="$(AUXDB_DSN)" \
	DATE_FROM=$(DATE_FROM) DATE_TO=$(DATE_TO) \
	go run ./cmd/backfill

## Run Incremental Daily Flow (T-2 + T-1 for all 3 properties)
daily:
	@echo ">>> Starting Incremental Daily Flow..."
	SEEDER_URL=$(SEEDER_URL) \
	MSSQL_HOST=$(MSSQL_HOST) MSSQL_PORT=$(MSSQL_PORT) MSSQL_DB=$(MSSQL_DB) \
	MSSQL_USER=$(MSSQL_USER) MSSQL_PASS="$(MSSQL_PASS)" \
	AUXDB_DSN="$(AUXDB_DSN)" \
	go run ./cmd/daily

## Run a single Realtime Pulse snapshot (for testing; exits after one poll)
realtime:
	@echo ">>> Starting Realtime Pulse (single snapshot)..."
	SEEDER_URL=$(SEEDER_URL) \
	MSSQL_HOST=$(MSSQL_HOST) MSSQL_PORT=$(MSSQL_PORT) MSSQL_DB=$(MSSQL_DB) \
	MSSQL_USER=$(MSSQL_USER) MSSQL_PASS="$(MSSQL_PASS)" \
	go run ./cmd/realtime --once

## Assert row counts and no duplicates
verify:
	@echo ">>> Running verification queries..."
	/opt/mssql-tools18/bin/sqlcmd -S $(MSSQL_HOST),$(MSSQL_PORT) \
		-U $(MSSQL_USER) -P "$(MSSQL_PASS)" -C \
		-d $(MSSQL_DB) -Q "\
		SELECT 'ga4_sessions'    AS tbl, COUNT(*) AS rows FROM dbo.ga4_sessions; \
		SELECT 'realtime'        AS tbl, COUNT(*) AS rows FROM dbo.realtime_sessions; \
		SELECT 'pipeline_log'    AS tbl, COUNT(*) AS rows FROM dbo.pipeline_run_log; \
		SELECT property_id, surface, COUNT(*) AS rows, MIN(report_date) AS min_date, MAX(report_date) AS max_date \
		FROM dbo.ga4_sessions GROUP BY property_id, surface ORDER BY property_id; \
		SELECT COUNT(*) AS dup_count FROM ( \
		  SELECT property_id, report_date, session_id, COUNT(*) AS n \
		  FROM dbo.ga4_sessions GROUP BY property_id, report_date, session_id HAVING COUNT(*) > 1 \
		) d;"
	@echo ">>> Verification complete"

## Truncate staging and production tables for a clean re-run
clean:
	@echo ">>> Truncating tables..."
	/opt/mssql-tools18/bin/sqlcmd -S $(MSSQL_HOST),$(MSSQL_PORT) \
		-U $(MSSQL_USER) -P "$(MSSQL_PASS)" -C \
		-d $(MSSQL_DB) -Q "\
		TRUNCATE TABLE stage.ga4_sessions; \
		DELETE FROM dbo.ga4_sessions; \
		DELETE FROM dbo.realtime_sessions; \
		DELETE FROM dbo.pipeline_run_log;"
	@echo ">>> Tables cleared"

## Tail container logs
logs:
	docker compose -f $(COMPOSE_FILE) logs -f

## Show container health
status:
	@docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' \
		--filter name=analytics_sqlserver \
		--filter name=auxdb

## Run Go unit tests
test:
	@echo ">>> Running tests..."
	go test ./... -v -count=1
	@echo ">>> Tests done"

## Run golangci-lint
lint:
	@echo ">>> Running linter..."
	golangci-lint run ./...
	@echo ">>> Lint done"
