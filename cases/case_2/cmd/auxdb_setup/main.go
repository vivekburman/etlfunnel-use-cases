package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/streamcraft/zomato-etl/db_setup/internal/config"
)

func main() {
	log.Println("=== AuxDB Setup ===")

	dsn := config.PostgresDSN(config.DBHost, config.AuxDBPort, config.AuxDB)
	db, err := connectWithRetry(dsn, 15)
	if err != nil {
		log.Fatalf("Failed to connect to AuxDB: %v", err)
	}
	defer db.Close()

	steps := []struct {
		name string
		fn   func(*sql.DB) error
	}{
		{"Create pipeline_checkpoints", createPipelineCheckpoints},
		{"Create backlog_records", createBacklogRecords},
		{"Create wal_positions", createWalPositions},
		{"Create backfill_progress", createBackfillProgress},
		{"Create city_mapping", createCityMapping},
		{"Create brand_sla_rules", createBrandSLARules},
		{"Create terminate_rules", createTerminateRules},
		{"Create write_tune_config", createWriteTuneConfig},
		{"Create es_write_log", createESWriteLog},
		{"Create backfill_completion_log", createBackfillCompletionLog},
		{"Seed city_mapping", seedCityMapping},
		{"Seed brand_sla_rules", seedBrandSLARules},
		{"Seed terminate_rules", seedTerminateRules},
		{"Seed write_tune_config", seedWriteTuneConfig},
	}

	for _, step := range steps {
		log.Printf("[auxdb] %s ...", step.name)
		if err := step.fn(db); err != nil {
			log.Fatalf("  FAILED: %v", err)
		}
		log.Printf("  done")
	}

	log.Println("=== AuxDB setup complete ===")
}

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

func exec(db *sql.DB, ddl string) error {
	_, err := db.Exec(ddl)
	return err
}

func createPipelineCheckpoints(db *sql.DB) error {
	return exec(db, `
CREATE TABLE IF NOT EXISTS pipeline_checkpoints (
    id                  BIGSERIAL    PRIMARY KEY,
    sub_brand           VARCHAR(30)  NOT NULL,
    city                VARCHAR(30)  NOT NULL,
    entity              VARCHAR(50)  NOT NULL,
    table_split_index   INT          NOT NULL DEFAULT 1,
    last_processed_pk   BIGINT       NOT NULL DEFAULT 0,
    batch_id            BIGINT       NOT NULL DEFAULT 0,
    redis_stream_id     VARCHAR(100),
    wal_lsn             VARCHAR(50),
    flow_type           VARCHAR(10)  NOT NULL DEFAULT 'cold',
    phase               VARCHAR(20)  NOT NULL DEFAULT 'Extract',
    status              VARCHAR(20)  NOT NULL DEFAULT 'IN_PROGRESS',
    records_processed   BIGINT       NOT NULL DEFAULT 0,
    checkpoint_ts       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (sub_brand, city, entity, table_split_index, flow_type)
);
CREATE INDEX IF NOT EXISTS idx_chk_brand_city ON pipeline_checkpoints (sub_brand, city);
CREATE INDEX IF NOT EXISTS idx_chk_status ON pipeline_checkpoints (status);
CREATE INDEX IF NOT EXISTS idx_chk_flow ON pipeline_checkpoints (flow_type);`)
}

func createBacklogRecords(db *sql.DB) error {
	return exec(db, `
CREATE TABLE IF NOT EXISTS backlog_records (
    id                  BIGSERIAL    PRIMARY KEY,
    sub_brand           VARCHAR(30)  NOT NULL,
    city                VARCHAR(30)  NOT NULL,
    entity              VARCHAR(50)  NOT NULL,
    table_split_index   INT          NOT NULL DEFAULT 1,
    batch_id            BIGINT,
    flow_type           VARCHAR(10),
    redis_stream_id     VARCHAR(100),
    failure_stage       VARCHAR(30),
    error_code          VARCHAR(50),
    error_message       TEXT,
    raw_record          JSONB,
    retry_count         INT          NOT NULL DEFAULT 0,
    max_retries         INT          NOT NULL DEFAULT 3,
    status              VARCHAR(20)  NOT NULL DEFAULT 'PENDING',
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_attempted_at   TIMESTAMPTZ,
    resolved_at         TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_backlog_status ON backlog_records (status);
CREATE INDEX IF NOT EXISTS idx_backlog_brand ON backlog_records (sub_brand, city);
CREATE INDEX IF NOT EXISTS idx_backlog_stage ON backlog_records (failure_stage);`)
}

func createWalPositions(db *sql.DB) error {
	return exec(db, `
CREATE TABLE IF NOT EXISTS wal_positions (
    id                  BIGSERIAL    PRIMARY KEY,
    sub_brand           VARCHAR(30)  NOT NULL UNIQUE,
    lsn                 VARCHAR(50),
    recorded_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    publication_name    VARCHAR(100),
    slot_name           VARCHAR(100)
);`)
}

func createBackfillProgress(db *sql.DB) error {
	return exec(db, `
CREATE TABLE IF NOT EXISTS backfill_progress (
    id                  BIGSERIAL    PRIMARY KEY,
    sub_brand           VARCHAR(30)  NOT NULL,
    entity              VARCHAR(50)  NOT NULL,
    city                VARCHAR(30)  NOT NULL,
    is_complete         BOOLEAN      NOT NULL DEFAULT FALSE,
    completed_at        TIMESTAMPTZ,
    total_records       BIGINT       NOT NULL DEFAULT 0,
    UNIQUE (sub_brand, entity, city)
);`)
}

func createCityMapping(db *sql.DB) error {
	return exec(db, `
CREATE TABLE IF NOT EXISTS city_mapping (
    id                  BIGSERIAL    PRIMARY KEY,
    city_id             INT          NOT NULL UNIQUE,
    city_name           VARCHAR(50)  NOT NULL,
    zone_label          VARCHAR(20)  NOT NULL,
    state               VARCHAR(50)  NOT NULL,
    tier                VARCHAR(10)  NOT NULL
);`)
}

func createBrandSLARules(db *sql.DB) error {
	return exec(db, `
CREATE TABLE IF NOT EXISTS brand_sla_rules (
    id                      BIGSERIAL    PRIMARY KEY,
    sub_brand               VARCHAR(30)  NOT NULL UNIQUE,
    promise_minutes         INT,
    sla_breach_definition   TEXT,
    delivery_window_days    INT
);`)
}

func createTerminateRules(db *sql.DB) error {
	return exec(db, `
CREATE TABLE IF NOT EXISTS terminate_rules (
    id                  BIGSERIAL    PRIMARY KEY,
    pipeline_name       VARCHAR(100) NOT NULL DEFAULT 'global',
    rule_name           VARCHAR(50)  NOT NULL,
    enabled             BOOLEAN      NOT NULL DEFAULT TRUE,
    threshold_value     NUMERIC(10,4),
    threshold_unit      VARCHAR(20),
    action              VARCHAR(20)  NOT NULL DEFAULT 'STOP',
    check_interval_sec  INT          NOT NULL DEFAULT 30,
    description         TEXT,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (pipeline_name, rule_name)
);`)
}

func createWriteTuneConfig(db *sql.DB) error {
	return exec(db, `
CREATE TABLE IF NOT EXISTS write_tune_config (
    id                              BIGSERIAL    PRIMARY KEY,
    config_name                     VARCHAR(50)  NOT NULL UNIQUE DEFAULT 'global',
    batch_size_normal               INT          NOT NULL DEFAULT 1000,
    batch_size_turbo                INT          NOT NULL DEFAULT 5000,
    batch_size_throttle             INT          NOT NULL DEFAULT 50,
    throttle_schedule               VARCHAR(50)  NOT NULL DEFAULT '09:00-22:00',
    redis_xread_count_slowify       INT          NOT NULL DEFAULT 10,
    destination_latency_threshold_ms INT         NOT NULL DEFAULT 500,
    concurrency_limit               INT          NOT NULL DEFAULT 8,
    max_concurrent_flows            INT          NOT NULL DEFAULT 8,
    max_concurrent_pipelines        INT          NOT NULL DEFAULT 3,
    force_stop                      BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at                      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at                      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);`)
}

func createESWriteLog(db *sql.DB) error {
	return exec(db, `
CREATE TABLE IF NOT EXISTS es_write_log (
    id              BIGSERIAL    PRIMARY KEY,
    sub_brand       VARCHAR(30)  NOT NULL,
    city            VARCHAR(30)  NOT NULL,
    entity          VARCHAR(50)  NOT NULL,
    flow_type       VARCHAR(10),
    batch_id        BIGINT,
    success_count   INT          NOT NULL DEFAULT 0,
    failure_count   INT          NOT NULL DEFAULT 0,
    retry_count     INT          NOT NULL DEFAULT 0,
    logged_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_es_write_log_brand ON es_write_log (sub_brand, city);
CREATE INDEX IF NOT EXISTS idx_es_write_log_ts ON es_write_log (logged_at DESC);`)
}

func createBackfillCompletionLog(db *sql.DB) error {
	return exec(db, `
CREATE TABLE IF NOT EXISTS backfill_completion_log (
    id              BIGSERIAL    PRIMARY KEY,
    sub_brand       VARCHAR(30)  NOT NULL,
    entity          VARCHAR(50)  NOT NULL,
    city            VARCHAR(30)  NOT NULL,
    completed_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    is_es_refreshed BOOLEAN      NOT NULL DEFAULT FALSE,
    UNIQUE (sub_brand, entity, city)
);`)
}

func seedCityMapping(db *sql.DB) error {
	type cityRow struct {
		id    int
		name  string
		zone  string
		state string
		tier  string
	}

	cities := []cityRow{
		{1, "delhi", "north", "delhi", "metro"},
		{2, "jaipur", "north", "rajasthan", "tier2"},
		{3, "lucknow", "north", "up", "tier2"},
		{4, "bengaluru", "south", "karnataka", "metro"},
		{5, "chennai", "south", "tamilnadu", "metro"},
		{6, "hyderabad", "south", "telangana", "metro"},
		{7, "mumbai", "west", "maharashtra", "metro"},
		{8, "pune", "west", "maharashtra", "tier2"},
		{9, "ahmedabad", "west", "gujarat", "tier2"},
		{10, "kolkata", "east", "westbengal", "metro"},
	}

	stmt, err := db.Prepare(`
INSERT INTO city_mapping (city_id, city_name, zone_label, state, tier)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (city_id) DO NOTHING;`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, c := range cities {
		if _, err := stmt.Exec(c.id, c.name, c.zone, c.state, c.tier); err != nil {
			return fmt.Errorf("city_mapping insert %s: %w", c.name, err)
		}
	}
	log.Printf("  seeded %d city mapping rows", len(cities))
	return nil
}

func seedBrandSLARules(db *sql.DB) error {
	type slaRow struct {
		brand       string
		minutes     *int
		definition  string
		windowDays  *int
	}

	m30 := 30
	m10 := 10
	w3 := 3

	rules := []slaRow{
		{brand: "zomato_food", minutes: &m30, definition: "delivered_at > placed_at + 30min", windowDays: nil},
		{brand: "blinkit", minutes: &m10, definition: "delivered_at > placed_at + 10min", windowDays: nil},
		{brand: "hyperpure", minutes: nil, definition: "received_at > placed_at + 3 days", windowDays: &w3},
		{brand: "district", minutes: nil, definition: "no_show=attended_at IS NULL AND status=NO_SHOW", windowDays: nil},
	}

	stmt, err := db.Prepare(`
INSERT INTO brand_sla_rules (sub_brand, promise_minutes, sla_breach_definition, delivery_window_days)
VALUES ($1, $2, $3, $4)
ON CONFLICT (sub_brand) DO NOTHING;`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, r := range rules {
		if _, err := stmt.Exec(r.brand, r.minutes, r.definition, r.windowDays); err != nil {
			return fmt.Errorf("brand_sla_rules insert %s: %w", r.brand, err)
		}
	}
	log.Printf("  seeded %d brand SLA rules", len(rules))
	return nil
}

func seedTerminateRules(db *sql.DB) error {
	type ruleRow struct {
		name        string
		threshold   float64
		unit        string
		intervalSec int
		desc        string
	}

	rules := []ruleRow{
		{"ERROR_RATE_BREACH", 10.0, "PERCENT", 30, "Stop if backlog rate > 10% of batch records"},
		{"SOURCE_UNREACHABLE", 3, "COUNT", 10, "Stop after N consecutive source connection failures"},
		{"DESTINATION_SATURATION", 500, "MS", 30, "Stop if ES write latency exceeds threshold"},
		{"INTEGRITY_VIOLATION", 5.0, "PERCENT", 30, "Stop if critical field null rate > 5% in batch"},
		{"REDIS_STREAM_LAG", 1000, "COUNT", 30, "Stop if Redis stream pending exceeds threshold"},
		{"WAL_SLOT_INACTIVE", 1, "COUNT", 30, "Stop if WAL replication slot becomes inactive"},
		{"IDLE_TIMEOUT", 120, "SECONDS", 10, "Stop if no records received for N seconds"},
		{"MANUAL_KILL", 1, "COUNT", 5, "Stop when operator sets force_stop=true in write_tune_config"},
		{"MAX_RECORDS_REACHED", 0, "COUNT", 30, "Stop cleanly when total processed records >= cap (0 = disabled)"},
	}

	stmt, err := db.Prepare(`
INSERT INTO terminate_rules (pipeline_name, rule_name, threshold_value, threshold_unit, check_interval_sec, description)
VALUES ('global', $1, $2, $3, $4, $5)
ON CONFLICT (pipeline_name, rule_name) DO NOTHING;`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, r := range rules {
		if _, err := stmt.Exec(r.name, r.threshold, r.unit, r.intervalSec, r.desc); err != nil {
			return fmt.Errorf("terminate_rule insert %s: %w", r.name, err)
		}
	}
	log.Printf("  seeded %d terminate rules", len(rules))
	return nil
}

func seedWriteTuneConfig(db *sql.DB) error {
	_, err := db.Exec(`
INSERT INTO write_tune_config
    (config_name, batch_size_normal, batch_size_turbo, batch_size_throttle,
     throttle_schedule, redis_xread_count_slowify, destination_latency_threshold_ms,
     concurrency_limit, max_concurrent_flows, max_concurrent_pipelines)
VALUES ('global', 1000, 5000, 50, '09:00-22:00', 10, 500, 8, 8, 3)
ON CONFLICT (config_name) DO NOTHING;`)
	return err
}
