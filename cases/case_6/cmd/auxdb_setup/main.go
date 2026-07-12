package main

// auxdb_setup — creates AuxDB tables for the FC Inventory Ops pipeline.
//
// Tables created:
//   fc_snapshot_progress  — Flow 36 checkpoint, one row per day's directory
//   fc_snapshot_backlog   — Flow 36 failed rows
//   fc_movement_progress  — Flow 37 checkpoint, single row (directory never rotates)
//   fc_movement_backlog   — Flow 37 failed records
//
// USAGE:
//   go run ./cmd/auxdb_setup -dsn "host=localhost port=5446 dbname=auxdb user=etl_user password=etl_pass sslmode=disable"
//   make auxdb

import (
	"context"
	"flag"
	"log"

	"github.com/jackc/pgx/v5"

	"github.com/streamcraft/fc-inventory-etl/case6/internal/config"
)

func main() {
	cfg := config.Load()
	flagDSN := flag.String("dsn", cfg.AuxDBDSN, "AuxDB connection string")
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, *flagDSN)
	if err != nil {
		log.Fatalf("connect to AuxDB: %v", err)
	}
	defer conn.Close(ctx)

	log.Println("=== AuxDB Setup — FC Inventory Ops Pipeline ===")

	for _, stmt := range ddlStatements {
		if _, err := conn.Exec(ctx, stmt.sql); err != nil {
			log.Fatalf("execute %q: %v", stmt.name, err)
		}
		log.Printf("  done: %s", stmt.name)
	}

	log.Println("=== AuxDB setup complete ===")
}

type namedSQL struct {
	name string
	sql  string
}

var ddlStatements = []namedSQL{
	{
		name: "fc_snapshot_progress",
		sql: `CREATE TABLE IF NOT EXISTS fc_snapshot_progress (
			directory   TEXT PRIMARY KEY,
			last_part   INT NOT NULL DEFAULT 0,
			last_row    INT NOT NULL DEFAULT 0,
			status      TEXT NOT NULL DEFAULT 'in_progress',
			updated_at  TIMESTAMPTZ NOT NULL
		)`,
	},
	{
		name: "fc_snapshot_backlog",
		sql: `CREATE TABLE IF NOT EXISTS fc_snapshot_backlog (
			id              BIGSERIAL PRIMARY KEY,
			fc_id           TEXT,
			sku_id          TEXT,
			file_part       INT,
			file_row        INT,
			failure_stage   TEXT,
			error_message   TEXT,
			record_payload  JSONB,
			pipeline_run_id TEXT,
			created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
	},
	{
		name: "fc_movement_progress",
		sql: `CREATE TABLE IF NOT EXISTS fc_movement_progress (
			directory   TEXT PRIMARY KEY,
			last_part   INT NOT NULL DEFAULT 0,
			last_row    INT NOT NULL DEFAULT 0,
			updated_at  TIMESTAMPTZ NOT NULL
		)`,
	},
	{
		name: "fc_movement_backlog",
		sql: `CREATE TABLE IF NOT EXISTS fc_movement_backlog (
			id              BIGSERIAL PRIMARY KEY,
			movement_id     TEXT,
			fc_id           TEXT,
			file_part       INT,
			file_row        INT,
			failure_stage   TEXT,
			error_message   TEXT,
			record_payload  JSONB,
			pipeline_run_id TEXT,
			created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
	},
}
