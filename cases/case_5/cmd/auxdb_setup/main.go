package main

// auxdb_setup — creates AuxDB tables for the Pepperfry Product Catalog Enrichment pipeline.
//
// Tables created:
//   pf_catalog_checkpoints       — last updated_at per source (postgres / oracle)
//   pf_enrich_collect_cursors    — Mac service results cursor (single row, id=1)
//   pf_enrich_submit_offsets     — Kafka offset checkpoint (per topic/partition)
//   pf_postgres_ingest_backlog   — failed Kafka publishes from Flow 32
//   pf_oracle_ingest_backlog     — failed Kafka publishes from Flow 33
//   pf_enrich_submit_backlog     — failed /enrich POSTs from Flow 34
//   pf_es_index_backlog          — failed ES writes from Flow 35
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

	log.Println("=== AuxDB Setup — Pepperfry Product Catalog Enrichment Pipeline ===")

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
		name: "pf_catalog_checkpoints",
		sql: `CREATE TABLE IF NOT EXISTS pf_catalog_checkpoints (
			source          TEXT PRIMARY KEY,
			last_updated_at TEXT NOT NULL,
			updated_at      TIMESTAMPTZ NOT NULL
		)`,
	},
	{
		name: "pf_enrich_collect_cursors",
		sql: `CREATE TABLE IF NOT EXISTS pf_enrich_collect_cursors (
			id          INT PRIMARY KEY DEFAULT 1,
			last_cursor INT NOT NULL DEFAULT 0,
			updated_at  TIMESTAMPTZ NOT NULL
		)`,
	},
	{
		name: "pf_enrich_submit_offsets",
		sql: `CREATE TABLE IF NOT EXISTS pf_enrich_submit_offsets (
			topic       TEXT,
			partition   INT,
			last_offset BIGINT NOT NULL,
			updated_at  TIMESTAMPTZ NOT NULL,
			PRIMARY KEY (topic, partition)
		)`,
	},
	{
		name: "pf_postgres_ingest_backlog",
		sql: `CREATE TABLE IF NOT EXISTS pf_postgres_ingest_backlog (
			id               BIGSERIAL PRIMARY KEY,
			product_id       TEXT,
			failure_stage    TEXT,
			error_message    TEXT,
			record_payload   JSONB,
			pipeline_run_id  TEXT,
			created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
	},
	{
		name: "pf_oracle_ingest_backlog",
		sql: `CREATE TABLE IF NOT EXISTS pf_oracle_ingest_backlog (
			id               BIGSERIAL PRIMARY KEY,
			product_id       TEXT,
			failure_stage    TEXT,
			error_message    TEXT,
			record_payload   JSONB,
			pipeline_run_id  TEXT,
			created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
	},
	{
		name: "pf_enrich_submit_backlog",
		sql: `CREATE TABLE IF NOT EXISTS pf_enrich_submit_backlog (
			id               BIGSERIAL PRIMARY KEY,
			product_id       TEXT,
			kafka_topic      TEXT,
			kafka_partition  INT,
			kafka_offset     BIGINT,
			failure_stage    TEXT,
			error_message    TEXT,
			record_payload   JSONB,
			pipeline_run_id  TEXT,
			created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
	},
	{
		name: "pf_es_index_backlog",
		sql: `CREATE TABLE IF NOT EXISTS pf_es_index_backlog (
			id               BIGSERIAL PRIMARY KEY,
			product_id       TEXT,
			failure_stage    TEXT,
			error_message    TEXT,
			record_payload   JSONB,
			pipeline_run_id  TEXT,
			created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
	},
}
