// auxdb_setup — creates all 9 AuxDB operational tables and seeds lookup data.
//
// Usage:
//   go run ./cmd/auxdb_setup
//
// Tables created:
//   pipeline_checkpoints, backlog_records, dedup_registry, plan_mapping,
//   terminate_rules, write_tune_config, reconciliation_log, audit_trail,
//   customer_merge_log

package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/streamcraft/telecom-etl/db_setup/internal/config"
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
		{"Enable pgcrypto extension", enableExtensions},
		{"Create pipeline_checkpoints", createPipelineCheckpoints},
		{"Create backlog_records", createBacklogRecords},
		{"Create dedup_registry", createDedupRegistry},
		{"Create plan_mapping", createPlanMapping},
		{"Create terminate_rules", createTerminateRules},
		{"Create write_tune_config", createWriteTuneConfig},
		{"Create reconciliation_log", createReconciliationLog},
		{"Create audit_trail", createAuditTrail},
		{"Create customer_merge_log", createCustomerMergeLog},
		{"Seed plan_mapping (all 4 companies)", seedPlanMapping},
		{"Seed terminate_rules defaults", seedTerminateRules},
		{"Seed write_tune_config defaults", seedWriteTuneConfig},
	}

	for _, step := range steps {
		log.Printf("[auxdb] %s ...", step.name)
		if err := step.fn(db); err != nil {
			log.Fatalf("  FAILED: %v", err)
		}
		log.Printf("  ✓ done")
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

// ---------- EXTENSIONS ----------

func enableExtensions(db *sql.DB) error {
	return exec(db, `CREATE EXTENSION IF NOT EXISTS "pgcrypto";`)
}

// ---------- TABLE CREATION ----------

func createPipelineCheckpoints(db *sql.DB) error {
	return exec(db, `
CREATE TABLE IF NOT EXISTS pipeline_checkpoints (
    id                  BIGSERIAL PRIMARY KEY,
    source_company      VARCHAR(30)  NOT NULL,
    zone                VARCHAR(20)  NOT NULL,
    state               VARCHAR(50)  NOT NULL,
    table_split_index   INT          NOT NULL DEFAULT 1,
    last_processed_pk   BIGINT       NOT NULL DEFAULT 0,
    batch_id            BIGINT       NOT NULL DEFAULT 0,
    phase               VARCHAR(20)  NOT NULL DEFAULT 'Extract',  -- Extract / Transform / Load
    status              VARCHAR(20)  NOT NULL DEFAULT 'IN_PROGRESS',  -- IN_PROGRESS / COMPLETED / FAILED
    records_processed   BIGINT       NOT NULL DEFAULT 0,
    checkpoint_ts       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (source_company, zone, state, table_split_index)
);
CREATE INDEX IF NOT EXISTS idx_chk_company_zone_state ON pipeline_checkpoints (source_company, zone, state);
CREATE INDEX IF NOT EXISTS idx_chk_status ON pipeline_checkpoints (status);`)
}

func createBacklogRecords(db *sql.DB) error {
	return exec(db, `
CREATE TABLE IF NOT EXISTS backlog_records (
    id                  BIGSERIAL PRIMARY KEY,
    source_company      VARCHAR(30)  NOT NULL,
    zone                VARCHAR(20)  NOT NULL,
    state               VARCHAR(50)  NOT NULL,
    table_split_index   INT          NOT NULL DEFAULT 1,
    batch_id            BIGINT,
    checkpoint_id       BIGINT,
    failure_stage       VARCHAR(30)  NOT NULL,  -- Transform / Destination / Dedup / Schema
    error_code          VARCHAR(50)  NOT NULL,
    error_message       TEXT         NOT NULL,
    raw_record          JSONB        NOT NULL,
    retry_count         INT          NOT NULL DEFAULT 0,
    max_retries         INT          NOT NULL DEFAULT 3,
    status              VARCHAR(20)  NOT NULL DEFAULT 'PENDING',  -- PENDING / IN_RETRY / RESOLVED / ABANDONED
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_attempted_at   TIMESTAMPTZ,
    resolved_at         TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_backlog_status ON backlog_records (status);
CREATE INDEX IF NOT EXISTS idx_backlog_company ON backlog_records (source_company, zone, state);
CREATE INDEX IF NOT EXISTS idx_backlog_failure_stage ON backlog_records (failure_stage);
CREATE INDEX IF NOT EXISTS idx_backlog_batch ON backlog_records (batch_id);`)
}

func createDedupRegistry(db *sql.DB) error {
	return exec(db, `
CREATE TABLE IF NOT EXISTS dedup_registry (
    id                  BIGSERIAL PRIMARY KEY,
    msisdn              VARCHAR(15)  NOT NULL UNIQUE,
    canonical_id        UUID         NOT NULL DEFAULT gen_random_uuid(),
    winning_company     VARCHAR(30)  NOT NULL,
    all_sources         TEXT[]       NOT NULL,  -- array of all companies that had this MSISDN
    conflict_detected   BOOLEAN      NOT NULL DEFAULT FALSE,
    conflict_resolved   BOOLEAN      NOT NULL DEFAULT FALSE,
    resolution_method   VARCHAR(50),  -- FIRST_SEEN / MANUAL / RULE_BASED
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_dedup_msisdn ON dedup_registry (msisdn);
CREATE INDEX IF NOT EXISTS idx_dedup_conflict ON dedup_registry (conflict_detected, conflict_resolved);`)
}

func createPlanMapping(db *sql.DB) error {
	return exec(db, `
CREATE TABLE IF NOT EXISTS plan_mapping (
    id                  BIGSERIAL PRIMARY KEY,
    source_company      VARCHAR(30)  NOT NULL,
    source_plan_code    VARCHAR(50)  NOT NULL,
    dest_plan_code      VARCHAR(50)  NOT NULL,
    plan_name           VARCHAR(100),
    data_gb             NUMERIC(6,2),
    voice_minutes       INT,
    sms_count           INT,
    price_inr           NUMERIC(8,2),
    validity_days       INT,
    active              BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (source_company, source_plan_code)
);
CREATE INDEX IF NOT EXISTS idx_plan_map_source ON plan_mapping (source_company, source_plan_code);
CREATE INDEX IF NOT EXISTS idx_plan_map_dest ON plan_mapping (dest_plan_code);`)
}

func createTerminateRules(db *sql.DB) error {
	return exec(db, `
CREATE TABLE IF NOT EXISTS terminate_rules (
    id                  BIGSERIAL PRIMARY KEY,
    pipeline_name       VARCHAR(100) NOT NULL DEFAULT 'global',  -- 'global' applies to all
    rule_name           VARCHAR(50)  NOT NULL,
    enabled             BOOLEAN      NOT NULL DEFAULT TRUE,
    threshold_value     NUMERIC(10,4),
    threshold_unit      VARCHAR(20),  -- PERCENT / MS / COUNT / SECONDS
    action              VARCHAR(20)  NOT NULL DEFAULT 'STOP',  -- STOP / ALERT / LOG
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
    id                      BIGSERIAL PRIMARY KEY,
    config_name             VARCHAR(50)  NOT NULL UNIQUE DEFAULT 'global',
    batch_size_normal       INT          NOT NULL DEFAULT 1000,
    batch_size_turbo        INT          NOT NULL DEFAULT 5000,
    batch_size_throttle     INT          NOT NULL DEFAULT 100,
    check_interval_seconds  INT          NOT NULL DEFAULT 60,
    throttle_schedule       VARCHAR(50)  NOT NULL DEFAULT '09:00-22:00',  -- IST window for slowify
    destination_latency_threshold_ms INT NOT NULL DEFAULT 500,
    concurrency_limit       INT          NOT NULL DEFAULT 8,
    max_concurrent_flows    INT          NOT NULL DEFAULT 8,
    max_concurrent_pipelines INT         NOT NULL DEFAULT 3,
    force_stop              BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);`)
}

func createReconciliationLog(db *sql.DB) error {
	return exec(db, `
CREATE TABLE IF NOT EXISTS reconciliation_log (
    id                  BIGSERIAL PRIMARY KEY,
    source_company      VARCHAR(30)  NOT NULL,
    zone                VARCHAR(20)  NOT NULL,
    state               VARCHAR(50)  NOT NULL,
    table_split_index   INT          NOT NULL DEFAULT 1,
    source_count        BIGINT       NOT NULL DEFAULT 0,
    destination_raw_count    BIGINT  NOT NULL DEFAULT 0,
    destination_curated_count BIGINT NOT NULL DEFAULT 0,
    discrepancy         BIGINT       NOT NULL DEFAULT 0,
    checksum_match      BOOLEAN,
    reconciled_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    pipeline_run_id     VARCHAR(50),
    notes               TEXT
);
CREATE INDEX IF NOT EXISTS idx_recon_company ON reconciliation_log (source_company, zone, state);
CREATE INDEX IF NOT EXISTS idx_recon_run ON reconciliation_log (pipeline_run_id);`)
}

func createAuditTrail(db *sql.DB) error {
	return exec(db, `
CREATE TABLE IF NOT EXISTS audit_trail (
    id                  BIGSERIAL PRIMARY KEY,
    record_id           BIGINT,
    msisdn              VARCHAR(15),
    source_company      VARCHAR(30),
    zone                VARCHAR(20),
    state               VARCHAR(50),
    transformer_name    VARCHAR(50),
    before_state        JSONB,
    after_state         JSONB,
    transformation_desc TEXT,
    batch_id            BIGINT,
    pipeline_run_id     VARCHAR(50),
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_aux_audit_msisdn ON audit_trail (msisdn);
CREATE INDEX IF NOT EXISTS idx_aux_audit_transformer ON audit_trail (transformer_name);
CREATE INDEX IF NOT EXISTS idx_aux_audit_batch ON audit_trail (batch_id);`)
}

func createCustomerMergeLog(db *sql.DB) error {
	return exec(db, `
CREATE TABLE IF NOT EXISTS customer_merge_log (
    id                  BIGSERIAL PRIMARY KEY,
    msisdn              VARCHAR(15)  NOT NULL,
    canonical_id        UUID,
    winning_company     VARCHAR(30)  NOT NULL,
    losing_company      VARCHAR(30)  NOT NULL,
    winning_record      JSONB,
    losing_record       JSONB,
    merge_reason        VARCHAR(100),
    batch_id            BIGINT,
    pipeline_run_id     VARCHAR(50),
    merged_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_merge_log_msisdn ON customer_merge_log (msisdn);
CREATE INDEX IF NOT EXISTS idx_merge_log_run ON customer_merge_log (pipeline_run_id);`)
}

// ---------- SEED DATA ----------

func seedPlanMapping(db *sql.DB) error {
	// Realistic Indian telecom plan codes → unified destination plan codes
	type planRow struct {
		company, srcCode, dstCode, name string
		dataGB                           float64
		voice, sms, price, validity      int
	}

	plans := []planRow{
		// Vodafone
		{"vodafone", "VF_49", "DEST_BASIC_49", "Basic Daily 49", 1.0, 100, 100, 49, 28},
		{"vodafone", "VF_149", "DEST_STD_149", "Standard 149", 2.0, 300, 300, 149, 28},
		{"vodafone", "VF_299", "DEST_PRO_299", "Pro 299", 3.0, 0, 100, 299, 28},
		{"vodafone", "VF_599", "DEST_ELITE_599", "Elite 599", 6.0, 0, 100, 599, 84},
		{"vodafone", "VF_999", "DEST_MAX_999", "Max Annual 999", 2.0, 0, 100, 999, 365},
		{"vodafone", "VF_PREPAID_10", "DEST_TALK_10", "Talk 10", 0, 30, 0, 10, 1},
		// Idea
		{"idea", "ID_49", "DEST_BASIC_49", "Idea Basic 49", 1.0, 100, 100, 49, 28},
		{"idea", "ID_199", "DEST_STD_199", "Idea Standard 199", 2.5, 300, 300, 199, 28},
		{"idea", "ID_349", "DEST_PRO_349", "Idea Pro 349", 4.0, 0, 100, 349, 28},
		{"idea", "ID_449", "DEST_PRO_449", "Idea Pro Plus 449", 5.0, 0, 100, 449, 56},
		{"idea", "ID_595", "DEST_ELITE_595", "Idea Elite 595", 6.0, 0, 100, 595, 84},
		{"idea", "ID_1199", "DEST_MAX_1199", "Idea Annual 1199", 2.5, 0, 100, 1199, 365},
		// Tata Docomo
		{"tata_docomo", "TD_52", "DEST_BASIC_52", "Tata Basic 52", 0.5, 100, 100, 52, 28},
		{"tata_docomo", "TD_155", "DEST_STD_155", "Tata Standard 155", 2.0, 200, 300, 155, 28},
		{"tata_docomo", "TD_255", "DEST_STD_255", "Tata Standard Plus 255", 3.0, 0, 100, 255, 28},
		{"tata_docomo", "TD_455", "DEST_PRO_455", "Tata Pro 455", 5.0, 0, 100, 455, 56},
		{"tata_docomo", "TD_755", "DEST_ELITE_755", "Tata Elite 755", 7.5, 0, 100, 755, 84},
		{"tata_docomo", "TD_ANNUAL", "DEST_MAX_999", "Tata Annual", 2.0, 0, 100, 999, 365},
		// Aircel
		{"aircel", "AC_44", "DEST_BASIC_44", "Aircel Basic 44", 0.5, 50, 50, 44, 28},
		{"aircel", "AC_98", "DEST_BASIC_98", "Aircel 98", 1.5, 100, 100, 98, 28},
		{"aircel", "AC_198", "DEST_STD_198", "Aircel 198", 2.0, 300, 300, 198, 28},
		{"aircel", "AC_398", "DEST_PRO_398", "Aircel Pro 398", 4.0, 0, 100, 398, 56},
		{"aircel", "AC_598", "DEST_ELITE_598", "Aircel Elite 598", 6.0, 0, 100, 598, 84},
		{"aircel", "AC_1098", "DEST_MAX_1099", "Aircel Annual 1098", 2.0, 0, 100, 1098, 365},
	}

	stmt, err := db.Prepare(`
INSERT INTO plan_mapping
    (source_company, source_plan_code, dest_plan_code, plan_name, data_gb, voice_minutes, sms_count, price_inr, validity_days)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (source_company, source_plan_code) DO NOTHING;`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, p := range plans {
		if _, err := stmt.Exec(p.company, p.srcCode, p.dstCode, p.name, p.dataGB, p.voice, p.sms, p.price, p.validity); err != nil {
			return fmt.Errorf("plan_mapping insert %s/%s: %w", p.company, p.srcCode, err)
		}
	}
	log.Printf("  seeded %d plan mapping rows", len(plans))
	return nil
}

func seedTerminateRules(db *sql.DB) error {
	type ruleRow struct {
		name, unit, desc string
		threshold        float64
		intervalSec      int
	}

	rules := []ruleRow{
		{"ERROR_RATE_BREACH", "PERCENT", "Stop if backlog rate > 10% of batch records", 10.0, 30},
		{"SOURCE_UNREACHABLE", "COUNT", "Stop after N consecutive source connection failures", 3, 10},
		{"DESTINATION_SATURATION", "MS", "Stop if destination write latency exceeds threshold", 500, 30},
		{"INTEGRITY_VIOLATION", "PERCENT", "Stop if critical field null rate > 5% in batch", 5.0, 30},
		{"DUPLICATE_STORM", "PERCENT", "Stop if dedup conflict rate > 80% of batch", 80.0, 30},
		{"IDLE_TIMEOUT", "SECONDS", "Stop if no records received for N seconds", 120, 10},
		{"MANUAL_KILL", "COUNT", "Stop when operator sets force_stop=true in write_tune_config", 1, 5},
		{"MAX_RECORDS_REACHED", "COUNT", "Stop cleanly when total processed records >= cap (0 = disabled)", 0, 30},
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
     check_interval_seconds, throttle_schedule, destination_latency_threshold_ms,
     concurrency_limit, max_concurrent_flows, max_concurrent_pipelines)
VALUES ('global', 1000, 5000, 100, 60, '09:00-22:00', 500, 8, 8, 3)
ON CONFLICT (config_name) DO NOTHING;`)
	return err
}
