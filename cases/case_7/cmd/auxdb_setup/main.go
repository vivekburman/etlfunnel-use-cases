package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/streamcraft/settleright-etl/db_setup/internal/config"
)

func main() {
	log.Println("=== AuxDB Setup ===")

	dsn := config.PostgresDSN(config.DBHost, config.AuxDBPort, config.AuxDB)
	db, err := connectWithRetry(dsn, 15)
	if err != nil {
		log.Fatalf("failed to connect to AuxDB: %v", err)
	}
	defer db.Close()

	// Table/column names here must match client/userlibraries/userlibrary_1.go
	// in etlfunnel-execution exactly - that's what actually reads/writes them.
	steps := []struct {
		name string
		ddl  string
	}{
		{"Create flow_watermark", flowWatermarkTable},
		{"Create flow_backlog", flowBacklogTable},
	}

	for _, step := range steps {
		log.Printf("[auxdb] %s ...", step.name)
		if _, err := db.Exec(step.ddl); err != nil {
			log.Fatalf("  FAILED: %v", err)
		}
		log.Printf("  done")
	}

	log.Println("=== AuxDB setup complete ===")
}

const flowWatermarkTable = `
CREATE TABLE IF NOT EXISTS flow_watermark (
    flow_name    TEXT         PRIMARY KEY,
    watermark_ts TIMESTAMPTZ  NOT NULL DEFAULT 'epoch',
    watermark_id BIGINT       NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);`

const flowBacklogTable = `
CREATE TABLE IF NOT EXISTS flow_backlog (
    id             BIGSERIAL    PRIMARY KEY,
    flow_name      TEXT         NOT NULL,
    failure_stage  TEXT,
    error_message  TEXT,
    raw_record     JSONB,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_flow_backlog_flow ON flow_backlog (flow_name, created_at);`

func connectWithRetry(dsn string, retries int) (*sql.DB, error) {
	var lastErr error
	for i := 0; i < retries; i++ {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			lastErr = err
		} else if pingErr := db.Ping(); pingErr != nil {
			lastErr = pingErr
			db.Close()
		} else {
			return db, nil
		}
		log.Printf("  waiting for AuxDB... (%d/%d): %v", i+1, retries, lastErr)
		time.Sleep(3 * time.Second)
	}
	return nil, fmt.Errorf("could not connect after %d retries: %w", retries, lastErr)
}
