package main

// auxdb_setup — creates AuxDB tables and seeds reference data.
//
// USAGE:
//   go run ./cmd/auxdb_setup -dsn "host=localhost port=5446 dbname=auxdb user=etl_user password=etl_pass sslmode=disable"
//   make auxdb

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"
)

var flagDSN = flag.String("dsn", "host=localhost port=5446 dbname=auxdb user=etl_user password=etl_pass sslmode=disable", "AuxDB connection string")

func main() {
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, *flagDSN)
	if err != nil {
		log.Fatalf("connect to AuxDB: %v", err)
	}
	defer conn.Close(ctx)

	log.Println("=== AuxDB Setup — Myntra GA4 Analytics ETL ===")

	for _, stmt := range ddlStatements {
		if _, err := conn.Exec(ctx, stmt.sql); err != nil {
			log.Fatalf("execute %q: %v", stmt.name, err)
		}
		log.Printf("  ✓ %s", stmt.name)
	}

	for _, stmt := range seedStatements {
		if _, err := conn.Exec(ctx, stmt.sql); err != nil {
			log.Fatalf("seed %q: %v", stmt.name, err)
		}
		log.Printf("  ✓ seeded %s", stmt.name)
	}

	log.Println("=== AuxDB setup complete ===")
}

type namedSQL struct {
	name string
	sql  string
}

var ddlStatements = []namedSQL{
	{
		name: "pipeline_checkpoints",
		sql: `CREATE TABLE IF NOT EXISTS pipeline_checkpoints (
			property_id      VARCHAR(50)  NOT NULL,
			surface          VARCHAR(20)  NOT NULL,
			last_merged_date DATE,
			pipeline_run_id  VARCHAR(100),
			updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
			PRIMARY KEY (property_id, surface)
		)`,
	},
	{
		name: "pipeline_backlog",
		sql: `CREATE TABLE IF NOT EXISTS pipeline_backlog (
			id               BIGSERIAL    PRIMARY KEY,
			property_id      VARCHAR(50),
			surface          VARCHAR(20),
			failure_stage    VARCHAR(30),
			error_message    TEXT,
			record_payload   JSONB,
			pipeline_run_id  VARCHAR(100),
			created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
		)`,
	},
	{
		name: "terminate_rules",
		sql: `CREATE TABLE IF NOT EXISTS terminate_rules (
			pipeline_name    VARCHAR(200) PRIMARY KEY,
			force_stop       BOOLEAN      NOT NULL DEFAULT FALSE,
			updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
		)`,
	},
	{
		name: "write_tune_config",
		sql: `CREATE TABLE IF NOT EXISTS write_tune_config (
			pipeline_name    VARCHAR(200) PRIMARY KEY,
			batch_size       INT          NOT NULL DEFAULT 1000,
			updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
		)`,
	},
	{
		name: "pipeline_run_log",
		sql: `CREATE TABLE IF NOT EXISTS pipeline_run_log (
			run_id           VARCHAR(100) PRIMARY KEY,
			pipeline_name    VARCHAR(200),
			property_id      VARCHAR(50),
			surface          VARCHAR(20),
			started_at       TIMESTAMPTZ,
			finished_at      TIMESTAMPTZ,
			rows_fetched     BIGINT       DEFAULT 0,
			rows_merged      BIGINT       DEFAULT 0,
			quota_tokens_spent INT        DEFAULT 0,
			status           VARCHAR(20)  DEFAULT 'running',
			error_message    TEXT
		)`,
	},
}

var seedStatements = []namedSQL{
	{
		name: "terminate_rules (backfill)",
		sql:  seedTerminateRule("pipeline_backfill"),
	},
	{
		name: "terminate_rules (daily)",
		sql:  seedTerminateRule("pipeline_daily"),
	},
	{
		name: "terminate_rules (realtime)",
		sql:  seedTerminateRule("pipeline_realtime"),
	},
	{
		name: "write_tune_config (backfill)",
		sql:  seedWriteTune("pipeline_backfill", 1000),
	},
	{
		name: "write_tune_config (daily)",
		sql:  seedWriteTune("pipeline_daily", 1000),
	},
	{
		name: "write_tune_config (realtime)",
		sql:  seedWriteTune("pipeline_realtime", 500),
	},
}

func seedTerminateRule(name string) string {
	return fmt.Sprintf(`INSERT INTO terminate_rules (pipeline_name, force_stop)
		VALUES ('%s', FALSE)
		ON CONFLICT (pipeline_name) DO NOTHING`, name)
}

func seedWriteTune(name string, batchSize int) string {
	return fmt.Sprintf(`INSERT INTO write_tune_config (pipeline_name, batch_size)
		VALUES ('%s', %d)
		ON CONFLICT (pipeline_name) DO NOTHING`, name, batchSize)
}
