# ============================================================
# FC Inventory Ops Pipeline — Case 6
# Windows Setup Makefile (infrastructure only)
# ============================================================
#
# ARCHITECTURE:
#   Flow 36: Parquet snapshot directory (dt=<date>, rotates daily) → MongoDB inventory_snapshots
#   Flow 37: Avro movement directory (flat, never rotates)         → MongoDB stock_movements
#
# Neither source has a live API/DB/stream to poll — both are directories of
# Spark/Hive-style numbered part files landing on disk. The seeders here
# write real Parquet/Avro files, not HTTP mock responses.
#
# PREREQUISITES:
#   - Docker Desktop running
#
# WINDOWS NOTE:
#   On Windows, FC_SNAPSHOT_DIR/FC_MOVEMENT_DIR default to
#   /data/fc_inventory_snapshots and /data/wms_stock_movements —
#   deliberately matching the literal defaults baked into
#   streamcraftexecution's orchestrator_21/orchestrator_22 and hardcoded in
#   its collection.json connectionParams. Those can't be pointed anywhere
#   else without editing that repo, so this Makefile must land files at the
#   exact same place the execution side resolves to.
#
#   On native Windows (Go, PowerShell), a rooted path with no drive letter
#   like "/data/..." resolves relative to the current drive, i.e.
#   C:\data\.... Git Bash's MSYS layer normally intercepts that and rewrites
#   it to "<git-install-dir>\data\..." instead (access-denied under
#   C:\Program Files\Git\data\...) before the Go seeder ever sees it. MSYS_NO_PATHCONV=1
#   (exported below) disables that rewrite so bash leaves the path alone and
#   Go's native resolution takes over — landing on C:\data\..., the same
#   place streamcraftexecution's PowerShell-run orchestrators/connectors
#   resolve "/data/..." to. Create C:\data\fc_inventory_snapshots and
#   C:\data\wms_stock_movements once (no admin rights needed); both sides
#   then agree without either repo needing a path override.
#
# MACOS/LINUX NOTE:
#   "/data/..." doesn't work here — on macOS the root volume is sealed and
#   read-only (SIP), and on Linux it's usually root-owned, so mkdir fails
#   either way. There's no drive-letter trick to fall back on like Windows
#   has, so FC_SNAPSHOT_DIR/FC_MOVEMENT_DIR instead default to this repo's
#   own tmp/ scratch dir (user space, no root needed). streamcraftexecution's
#   orchestrators already read these same env vars at runtime and only fall
#   back to the hardcoded /data/... default when unset (see
#   orchestrator_1/orchestrator.go, orchestrator_2/orchestrator.go), so
#   export FC_SNAPSHOT_DIR/FC_MOVEMENT_DIR with these same values when
#   running the flow locally so both sides agree.
#
# USAGE:
#   make up             — start AuxDB + MongoDB containers and wait for healthy
#   make setup          — full bootstrap: up → AuxDB tables → MongoDB indexes
#   make seed-snapshot  — write today's Parquet snapshot directory
#   make seed-movement  — append a new batch of Avro movement part files
#   make seed-backlog   — insert synthetic backlog rows into AuxDB (N=20 per table by default)
#   make down           — stop containers (keep volumes)
#   make reset          — stop containers AND destroy volumes (DESTRUCTIVE)
#
# EXECUTION ORDER:
#   make setup
#   make seed-snapshot
#   make seed-movement
#   Start flows: pid=36 (Parquet snapshot) and pid=37 (Avro movement) — no ordering constraint

ifeq ($(OS),Windows_NT)
SHELL := C:/Program Files/Git/bin/bash.exe
endif
.SHELLFLAGS := -euo pipefail -c

export GOTMPDIR := $(CURDIR)/tmp
export GONOSUMCHECK := *
export MSYS_NO_PATHCONV := 1

$(shell mkdir -p $(CURDIR)/tmp)

COMPOSE_FILE := docker-compose.yml

AUXDB_DSN       ?= postgresql://etl_user:etl_pass@localhost:5446/auxdb?sslmode=disable
# Creds must match collection.json's hardcoded Mongo destination connectionParams.
MONGO_URI       ?= mongodb://etl_user:etl_pass@localhost:27017/?authSource=admin
# See "WINDOWS NOTE" / "MACOS/LINUX NOTE" above for why this default differs by OS.
ifeq ($(OS),Windows_NT)
FC_SNAPSHOT_DIR ?= /data/fc_inventory_snapshots
FC_MOVEMENT_DIR ?= /data/wms_stock_movements
else
FC_SNAPSHOT_DIR ?= $(CURDIR)/tmp/data/fc_inventory_snapshots
FC_MOVEMENT_DIR ?= $(CURDIR)/tmp/data/wms_stock_movements
endif
TOTAL_ROWS      ?= 1500
ROWS_PER_PART   ?= 1000
NEW_MOVEMENTS   ?= 2000
MOVEMENT_ROWS_PER_PART ?= 500
FAULT_RATE      ?= 5
N               ?= 20

.PHONY: up down reset setup seed-snapshot seed-movement seed-backlog

## Start all containers
up:
	@echo ">>> Starting Docker containers..."
	docker compose -f $(COMPOSE_FILE) up -d
	@echo ">>> Waiting for all services to be healthy..."
	@for svc in fc_auxdb fc_mongo; do \
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

## Full bootstrap: start containers → AuxDB tables → MongoDB indexes
setup: up
	@echo ">>> Downloading Go dependencies..."
	go mod download
	go mod tidy
	@echo ">>> Setting up AuxDB tables..."
	go build -o $(GOTMPDIR)/auxdb_init$(GOEXE) ./cmd/auxdb_setup
	$(GOTMPDIR)/auxdb_init$(GOEXE) -dsn "$(AUXDB_DSN)"
	@echo ">>> Creating MongoDB indexes..."
	MONGO_URI="$(MONGO_URI)" go run ./cmd/mongo_init
	@echo ""
	@echo ">>> Setup complete. Next steps:"
	@echo "   make seed-snapshot   - write today's Parquet snapshot directory"
	@echo "   make seed-movement   - append a batch of Avro movement part files"
	@echo "   Start flows: pid=36 (Parquet snapshot) and pid=37 (Avro movement)"

## Write today's Parquet snapshot directory (or SNAPSHOT_DATE=YYYY-MM-DD)
seed-snapshot:
	@echo ">>> Seeding Parquet snapshot directory..."
	FC_SNAPSHOT_DIR="$(FC_SNAPSHOT_DIR)" go run ./cmd/snapshot_seeder \
		-total $(TOTAL_ROWS) -rows-per-part $(ROWS_PER_PART) -fault-rate $(FAULT_RATE) $(if $(SNAPSHOT_DATE),-date $(SNAPSHOT_DATE),)
	@echo ">>> Snapshot seed complete"

## Append a new batch of Avro movement part files
seed-movement:
	@echo ">>> Seeding Avro movement directory..."
	FC_MOVEMENT_DIR="$(FC_MOVEMENT_DIR)" go run ./cmd/movement_seeder \
		-new $(NEW_MOVEMENTS) -rows-per-part $(MOVEMENT_ROWS_PER_PART) -fault-rate $(FAULT_RATE)
	@echo ">>> Movement seed complete"

## Insert synthetic backlog rows into AuxDB (N=20 per table by default)
seed-backlog:
	@echo ">>> Seeding synthetic backlog rows (N=$(N))..."
	AUXDB_DSN="$(AUXDB_DSN)" go run ./cmd/backlog_seeder -n $(N)
	@echo ">>> Backlog seed complete"
