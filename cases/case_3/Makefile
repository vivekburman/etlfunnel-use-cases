# ============================================================
# Myntra Digital Analytics Intelligence — DB Setup Makefile
# ============================================================
#
# USAGE:
#   make up           — start SQL Server + AuxDB containers
#   make setup        — full bootstrap: up → AuxDB schema → SQL Server schema
#   make seed         — start the GA4 mock seeder HTTP server (:11333)
#   make seed-stop    — stop the seeder process
#   make down         — stop containers (keep volumes)
#   make reset        — stop containers AND destroy volumes (DESTRUCTIVE)
#   make clean-destination — empty analytics_warehouse tables (keep schema/indexes)
#
# SEED OVERRIDES:
#   make seed ROWS_PER_DAY=5000 SEEDER_PORT=11333

ifeq ($(OS),Windows_NT)
SHELL := C:/Program Files/Git/bin/bash.exe
endif
.SHELLFLAGS := -euo pipefail -c

# Redirect Go build cache to a local dir so Windows doesn't block execution from %TEMP%
export GOTMPDIR := $(CURDIR)/tmp
$(shell mkdir -p $(CURDIR)/tmp)

COMPOSE_FILE   := docker-compose.yml

MSSQL_CONTAINER ?= analytics_sqlserver
MSSQL_HOST     ?= localhost
MSSQL_PORT     ?= 1433
MSSQL_SA_PASS  ?= Etl_Pass_123!
MSSQL_DB       ?= analytics_warehouse
MSSQL_USER     ?= etl_writer
MSSQL_PASS     ?= Etl_Pass_456!

AUXDB_DSN      ?= host=localhost port=5446 dbname=auxdb user=etl_user password=etl_pass sslmode=disable

SEEDER_PORT    ?= 11333
ROWS_PER_DAY   ?= 15000

.PHONY: up down reset setup seed seed-stop clean-destination

## Start SQL Server + AuxDB containers
up:
	@echo ">>> Starting Docker containers..."
	docker compose -f $(COMPOSE_FILE) up -d
	@echo ">>> Waiting for all services to be healthy..."
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

## Empty analytics_warehouse tables without dropping schema/indexes
clean-destination:
	@echo ">>> Truncating analytics_warehouse tables..."
	docker exec $(MSSQL_CONTAINER) /opt/mssql-tools18/bin/sqlcmd -S localhost -U $(MSSQL_USER) -P "$(MSSQL_PASS)" -C -d $(MSSQL_DB) \
		-Q "TRUNCATE TABLE dbo.ga4_sessions; TRUNCATE TABLE stage.ga4_sessions; TRUNCATE TABLE dbo.realtime_sessions; TRUNCATE TABLE dbo.pipeline_run_log;"
	@echo ">>> Destination clean"

## Full bootstrap: start containers → AuxDB schema → SQL Server schema
setup: up
	@echo ">>> Downloading Go dependencies..."
	go mod download
	go mod tidy
	@echo ">>> Setting up AuxDB..."
	go build -o $(GOTMPDIR)/auxdb_init$(GOEXE) ./cmd/auxdb_setup
	$(GOTMPDIR)/auxdb_init$(GOEXE) -dsn "$(AUXDB_DSN)"
	@echo ">>> Applying SQL Server schema..."
	go run ./cmd/mssql_schema \
		-host $(MSSQL_HOST) \
		-port $(MSSQL_PORT) \
		-sa-pass "$(MSSQL_SA_PASS)" \
		-db $(MSSQL_DB) \
		-user $(MSSQL_USER) \
		-pass "$(MSSQL_PASS)"
	@echo ">>> Setup complete. Run 'make seed' to start the GA4 mock server."

## Start GA4 mock seeder HTTP server in the background
seed:
	@echo ">>> Starting GA4 seeder on :$(SEEDER_PORT)..."
	ROWS_PER_DAY=$(ROWS_PER_DAY) SEEDER_PORT=$(SEEDER_PORT) \
	go run ./cmd/seeder & echo $$! > .seeder.pid
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
