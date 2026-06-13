package main

// auxdb_setup — creates AuxDB tables for the Zepto Order Events pipeline.
//
// Tables:
//   zepto_ingestion_cursors  — cursor checkpoint for Flow 1 (REST API → Kafka)
//   zepto_ingestion_backlog  — failed publish records for Flow 1
//   zepto_storage_offsets    — Kafka offset checkpoint for Flow 2 (Kafka → Cassandra)
//   zepto_storage_backlog    — failed write records for Flow 2
//
// USAGE:
//   go run ./cmd/auxdb_setup -dsn "host=localhost port=5446 dbname=auxdb user=etl_user password=etl_pass sslmode=disable"
//   make auxdb

import (
	"context"
	"flag"
	"log"

	"github.com/jackc/pgx/v5"
)

var flagDSN = flag.String("dsn", "postgresql://etl_user:etl_pass@localhost:5446/auxdb?sslmode=disable", "AuxDB connection string")

func main() {
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, *flagDSN)
	if err != nil {
		log.Fatalf("connect to AuxDB: %v", err)
	}
	defer conn.Close(ctx)

	log.Println("=== AuxDB Setup — Zepto Order Events Pipeline ===")

	for _, stmt := range ddlStatements {
		if _, err := conn.Exec(ctx, stmt.sql); err != nil {
			log.Fatalf("execute %q: %v", stmt.name, err)
		}
		log.Printf("  ✓ %s", stmt.name)
	}

	log.Println("=== AuxDB setup complete ===")
}

type namedSQL struct {
	name string
	sql  string
}

var ddlStatements = []namedSQL{
	{
		name: "zepto_ingestion_cursors",
		sql: `CREATE TABLE IF NOT EXISTS zepto_ingestion_cursors (
			pipeline    TEXT PRIMARY KEY,
			last_cursor TEXT NOT NULL,
			updated_at  TIMESTAMPTZ NOT NULL
		)`,
	},
	{
		name: "zepto_ingestion_backlog",
		sql: `CREATE TABLE IF NOT EXISTS zepto_ingestion_backlog (
			id              BIGSERIAL PRIMARY KEY,
			order_id        TEXT,
			event_id        TEXT,
			failure_stage   TEXT,
			error_message   TEXT,
			record_payload  JSONB,
			pipeline_run_id TEXT,
			created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
	},
	{
		name: "zepto_storage_offsets",
		sql: `CREATE TABLE IF NOT EXISTS zepto_storage_offsets (
			topic       TEXT,
			partition   INT,
			last_offset BIGINT NOT NULL,
			updated_at  TIMESTAMPTZ NOT NULL,
			PRIMARY KEY (topic, partition)
		)`,
	},
	{
		name: "zepto_storage_backlog",
		sql: `CREATE TABLE IF NOT EXISTS zepto_storage_backlog (
			id              BIGSERIAL PRIMARY KEY,
			order_id        TEXT,
			event_id        TEXT,
			kafka_topic     TEXT,
			kafka_partition INT,
			kafka_offset    BIGINT,
			failure_stage   TEXT,
			error_message   TEXT,
			record_payload  JSONB,
			pipeline_run_id TEXT,
			created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
	},
}
