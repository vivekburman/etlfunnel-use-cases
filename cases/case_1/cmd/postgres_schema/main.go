// postgres_schema — creates raw/staging/curated/audit schemas on the destination Postgres,
// with declarative zone → state partitioning on all core tables.
//
// Usage:
//   go run ./cmd/postgres_schema

package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"

	"github.com/streamcraft/telecom-etl/db_setup/internal/config"
	"github.com/streamcraft/telecom-etl/db_setup/internal/sharding"
)

func main() {
	log.Println("=== Postgres Destination Schema Creator ===")

	dsn := config.PostgresDSN(config.DBHost, config.DestinationPort, config.DestinationDB)
	db, err := connectWithRetry(dsn, 15)
	if err != nil {
		log.Fatalf("Failed to connect to destination DB: %v", err)
	}
	defer db.Close()

	steps := []struct {
		name string
		fn   func(*sql.DB) error
	}{
		{"Create schemas (raw, staging, curated, audit)", createSchemas},
		{"Create raw schema tables", createRawTables},
		{"Create staging schema tables", createStagingTables},
		{"Create curated schema tables (partitioned)", createCuratedTables},
		{"Create curated partitions (zone → state)", createCuratedPartitions},
		{"Create audit schema tables", createAuditTables},
		{"Create indexes", createIndexes},
	}

	for _, step := range steps {
		log.Printf("[destination_db] %s ...", step.name)
		if err := step.fn(db); err != nil {
			log.Fatalf("  FAILED: %v", err)
		}
		log.Printf("  ✓ done")
	}

	log.Println("=== Destination Postgres schema complete ===")
}

func connectWithRetry(dsn string, retries int) (*sql.DB, error) {
	for i := 0; i < retries; i++ {
		db, err := sql.Open("postgres", dsn)
		if err == nil {
			if pingErr := db.Ping(); pingErr == nil {
				return db, nil
			}
		}
		log.Printf("  waiting for Postgres... (%d/%d)", i+1, retries)
		time.Sleep(3 * time.Second)
	}
	return nil, fmt.Errorf("could not connect after %d retries", retries)
}

func exec(db *sql.DB, ddl string) error {
	_, err := db.Exec(ddl)
	return err
}

// ---------- SCHEMAS ----------

func createSchemas(db *sql.DB) error {
	for _, schema := range []string{"raw", "staging", "curated", "audit"} {
		if err := exec(db, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", schema)); err != nil {
			return fmt.Errorf("schema %s: %w", schema, err)
		}
	}
	return nil
}

// ---------- RAW SCHEMA ----------
// Landing zone — fast writes, no constraints, one table per source company.

func createRawTables(db *sql.DB) error {
	ddls := []string{
		rawCustomers(),
		rawSubscriptions(),
		rawBillingAccounts(),
		rawSimInventory(),
		rawPortHistory(),
	}
	for _, ddl := range ddls {
		if err := exec(db, ddl); err != nil {
			return err
		}
	}
	return nil
}

func rawCustomers() string {
	return `
CREATE TABLE IF NOT EXISTS raw.customers (
    id                  BIGSERIAL PRIMARY KEY,
    source_company      VARCHAR(30)  NOT NULL,
    msisdn              VARCHAR(15)  NOT NULL,
    name                VARCHAR(100),
    dob                 DATE,
    aadhaar_hash        VARCHAR(64)  COMMENT_PLACEHOLDER,
    pan_hash            VARCHAR(64),
    email               VARCHAR(150),
    address             TEXT,
    activation_date     TIMESTAMPTZ,
    zone                VARCHAR(20)  NOT NULL,
    state               VARCHAR(50)  NOT NULL,
    table_split         SMALLINT     NOT NULL DEFAULT 1,
    batch_id            BIGINT,
    raw_record          JSONB,
    ingested_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);`
}

func rawSubscriptions() string {
	return `
CREATE TABLE IF NOT EXISTS raw.subscriptions (
    id                  BIGSERIAL PRIMARY KEY,
    source_company      VARCHAR(30)  NOT NULL,
    msisdn              VARCHAR(15)  NOT NULL,
    plan_code           VARCHAR(50)  NOT NULL,
    plan_start          DATE,
    plan_end            DATE,
    status              VARCHAR(20)  NOT NULL DEFAULT 'ACTIVE',
    zone                VARCHAR(20)  NOT NULL,
    state               VARCHAR(50)  NOT NULL,
    table_split         SMALLINT     NOT NULL DEFAULT 1,
    batch_id            BIGINT,
    raw_record          JSONB,
    ingested_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);`
}

func rawBillingAccounts() string {
	return `
CREATE TABLE IF NOT EXISTS raw.billing_accounts (
    id                  BIGSERIAL PRIMARY KEY,
    source_company      VARCHAR(30)  NOT NULL,
    msisdn              VARCHAR(15)  NOT NULL,
    amount_due          NUMERIC(12,2) NOT NULL DEFAULT 0.00,
    amount_paid         NUMERIC(12,2) NOT NULL DEFAULT 0.00,
    cycle_start         DATE,
    cycle_end           DATE,
    bill_status         VARCHAR(20)  NOT NULL DEFAULT 'UNPAID',
    zone                VARCHAR(20)  NOT NULL,
    state               VARCHAR(50)  NOT NULL,
    table_split         SMALLINT     NOT NULL DEFAULT 1,
    batch_id            BIGINT,
    raw_record          JSONB,
    ingested_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);`
}

func rawSimInventory() string {
	return `
CREATE TABLE IF NOT EXISTS raw.sim_inventory (
    id                  BIGSERIAL PRIMARY KEY,
    source_company      VARCHAR(30)  NOT NULL,
    msisdn              VARCHAR(15)  NOT NULL,
    sim_serial          VARCHAR(22)  NOT NULL,
    imsi                VARCHAR(15)  NOT NULL,
    sim_status          VARCHAR(20)  NOT NULL DEFAULT 'ACTIVE',
    activated_date      DATE,
    deactivated_date    DATE,
    zone                VARCHAR(20)  NOT NULL,
    state               VARCHAR(50)  NOT NULL,
    table_split         SMALLINT     NOT NULL DEFAULT 1,
    batch_id            BIGINT,
    raw_record          JSONB,
    ingested_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);`
}

func rawPortHistory() string {
	return `
CREATE TABLE IF NOT EXISTS raw.port_history (
    id                  BIGSERIAL PRIMARY KEY,
    source_company      VARCHAR(30)  NOT NULL,
    msisdn              VARCHAR(15)  NOT NULL,
    port_direction      VARCHAR(10)  NOT NULL,
    from_carrier        VARCHAR(50),
    to_carrier          VARCHAR(50),
    port_date           DATE         NOT NULL,
    porting_ref         VARCHAR(30),
    status              VARCHAR(20)  NOT NULL DEFAULT 'COMPLETED',
    zone                VARCHAR(20)  NOT NULL,
    state               VARCHAR(50)  NOT NULL,
    table_split         SMALLINT     NOT NULL DEFAULT 1,
    batch_id            BIGINT,
    raw_record          JSONB,
    ingested_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);`
}

// ---------- STAGING SCHEMA ----------
// Deduped, normalized, constraints applied. Conflict resolution happens here.

func createStagingTables(db *sql.DB) error {
	ddls := []string{
		stagingCustomers(),
		stagingSubscriptions(),
		stagingBillingAccounts(),
		stagingSimInventory(),
		stagingPortHistory(),
	}
	for _, ddl := range ddls {
		if err := exec(db, ddl); err != nil {
			return err
		}
	}
	return nil
}

func stagingCustomers() string {
	return `
CREATE TABLE IF NOT EXISTS staging.customers (
    id                  BIGSERIAL PRIMARY KEY,
    canonical_id        UUID         NOT NULL DEFAULT gen_random_uuid(),
    source_company      VARCHAR(30)  NOT NULL,
    msisdn              VARCHAR(15)  NOT NULL,
    name                VARCHAR(100) NOT NULL,
    dob                 DATE,
    aadhaar_hash        VARCHAR(64),
    pan_hash            VARCHAR(64),
    email               VARCHAR(150),
    address             TEXT,
    activation_date     TIMESTAMPTZ,
    zone                VARCHAR(20)  NOT NULL,
    state               VARCHAR(50)  NOT NULL,
    dedup_status        VARCHAR(20)  NOT NULL DEFAULT 'CLEAN',
    promoted_at         TIMESTAMPTZ,
    UNIQUE (msisdn, source_company)
);`
}

func stagingSubscriptions() string {
	return `
CREATE TABLE IF NOT EXISTS staging.subscriptions (
    id                  BIGSERIAL PRIMARY KEY,
    source_company      VARCHAR(30)  NOT NULL,
    msisdn              VARCHAR(15)  NOT NULL,
    plan_code           VARCHAR(50)  NOT NULL,
    plan_start          DATE,
    plan_end            DATE,
    status              VARCHAR(20)  NOT NULL DEFAULT 'ACTIVE',
    zone                VARCHAR(20)  NOT NULL,
    state               VARCHAR(50)  NOT NULL,
    promoted_at         TIMESTAMPTZ
);`
}

func stagingBillingAccounts() string {
	return `
CREATE TABLE IF NOT EXISTS staging.billing_accounts (
    id                  BIGSERIAL PRIMARY KEY,
    source_company      VARCHAR(30)  NOT NULL,
    msisdn              VARCHAR(15)  NOT NULL,
    amount_due          NUMERIC(12,2) NOT NULL DEFAULT 0.00,
    amount_paid         NUMERIC(12,2) NOT NULL DEFAULT 0.00,
    cycle_start         DATE,
    cycle_end           DATE,
    bill_status         VARCHAR(20)  NOT NULL DEFAULT 'UNPAID',
    zone                VARCHAR(20)  NOT NULL,
    state               VARCHAR(50)  NOT NULL,
    promoted_at         TIMESTAMPTZ
);`
}

func stagingSimInventory() string {
	return `
CREATE TABLE IF NOT EXISTS staging.sim_inventory (
    id                  BIGSERIAL PRIMARY KEY,
    source_company      VARCHAR(30)  NOT NULL,
    msisdn              VARCHAR(15)  NOT NULL,
    sim_serial          VARCHAR(22)  NOT NULL,
    imsi                VARCHAR(15)  NOT NULL,
    sim_status          VARCHAR(20)  NOT NULL DEFAULT 'ACTIVE',
    activated_date      DATE,
    deactivated_date    DATE,
    zone                VARCHAR(20)  NOT NULL,
    state               VARCHAR(50)  NOT NULL,
    promoted_at         TIMESTAMPTZ,
    UNIQUE (sim_serial)
);`
}

func stagingPortHistory() string {
	return `
CREATE TABLE IF NOT EXISTS staging.port_history (
    id                  BIGSERIAL PRIMARY KEY,
    source_company      VARCHAR(30)  NOT NULL,
    msisdn              VARCHAR(15)  NOT NULL,
    port_direction      VARCHAR(10)  NOT NULL,
    from_carrier        VARCHAR(50),
    to_carrier          VARCHAR(50),
    port_date           DATE         NOT NULL,
    porting_ref         VARCHAR(30),
    status              VARCHAR(20)  NOT NULL DEFAULT 'COMPLETED',
    zone                VARCHAR(20)  NOT NULL,
    state               VARCHAR(50)  NOT NULL,
    promoted_at         TIMESTAMPTZ
);`
}

// ---------- CURATED SCHEMA (PARTITIONED) ----------
// Golden records partitioned by zone → state using Postgres declarative partitioning.

func createCuratedTables(db *sql.DB) error {
	// Create the top-level partitioned parent tables first
	ddls := []string{
		`CREATE TABLE IF NOT EXISTS curated.customers (
    id                  BIGSERIAL,
    canonical_id        UUID         NOT NULL DEFAULT gen_random_uuid(),
    source_company      VARCHAR(30)  NOT NULL,
    msisdn              VARCHAR(15)  NOT NULL,
    name                VARCHAR(100) NOT NULL,
    dob                 DATE,
    aadhaar_hash        VARCHAR(64),
    pan_hash            VARCHAR(64),
    email               VARCHAR(150),
    address             TEXT,
    activation_date     TIMESTAMPTZ,
    zone                VARCHAR(20)  NOT NULL,
    state               VARCHAR(50)  NOT NULL,
    batch_sequence_id   BIGINT       NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
) PARTITION BY LIST (zone);`,

		`CREATE TABLE IF NOT EXISTS curated.subscriptions (
    id                  BIGSERIAL,
    canonical_id        UUID         NOT NULL DEFAULT gen_random_uuid(),
    source_company      VARCHAR(30)  NOT NULL,
    msisdn              VARCHAR(15)  NOT NULL,
    plan_code           VARCHAR(50)  NOT NULL,
    plan_start          DATE,
    plan_end            DATE,
    status              VARCHAR(20)  NOT NULL DEFAULT 'ACTIVE',
    zone                VARCHAR(20)  NOT NULL,
    state               VARCHAR(50)  NOT NULL,
    batch_sequence_id   BIGINT       NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
) PARTITION BY LIST (zone);`,

		`CREATE TABLE IF NOT EXISTS curated.billing_accounts (
    id                  BIGSERIAL,
    source_company      VARCHAR(30)  NOT NULL,
    msisdn              VARCHAR(15)  NOT NULL,
    amount_due          NUMERIC(12,2) NOT NULL DEFAULT 0.00,
    amount_paid         NUMERIC(12,2) NOT NULL DEFAULT 0.00,
    cycle_start         DATE,
    cycle_end           DATE,
    bill_status         VARCHAR(20)  NOT NULL DEFAULT 'UNPAID',
    zone                VARCHAR(20)  NOT NULL,
    state               VARCHAR(50)  NOT NULL,
    batch_sequence_id   BIGINT       NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
) PARTITION BY LIST (zone);`,

		`CREATE TABLE IF NOT EXISTS curated.sim_inventory (
    id                  BIGSERIAL,
    source_company      VARCHAR(30)  NOT NULL,
    msisdn              VARCHAR(15)  NOT NULL,
    sim_serial          VARCHAR(22)  NOT NULL,
    imsi                VARCHAR(15)  NOT NULL,
    sim_status          VARCHAR(20)  NOT NULL DEFAULT 'ACTIVE',
    activated_date      DATE,
    deactivated_date    DATE,
    zone                VARCHAR(20)  NOT NULL,
    state               VARCHAR(50)  NOT NULL,
    batch_sequence_id   BIGINT       NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
) PARTITION BY LIST (zone);`,

		`CREATE TABLE IF NOT EXISTS curated.port_history (
    id                  BIGSERIAL,
    source_company      VARCHAR(30)  NOT NULL,
    msisdn              VARCHAR(15)  NOT NULL,
    port_direction      VARCHAR(10)  NOT NULL,
    from_carrier        VARCHAR(50),
    to_carrier          VARCHAR(50),
    port_date           DATE         NOT NULL,
    porting_ref         VARCHAR(30),
    status              VARCHAR(20)  NOT NULL DEFAULT 'COMPLETED',
    zone                VARCHAR(20)  NOT NULL,
    state               VARCHAR(50)  NOT NULL,
    batch_sequence_id   BIGINT       NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
) PARTITION BY LIST (zone);`,

		`CREATE TABLE IF NOT EXISTS curated.network_assignments (
    id                  BIGSERIAL,
    msisdn              VARCHAR(15)  NOT NULL,
    circle              VARCHAR(50),
    tower_id            VARCHAR(30),
    assigned_at         TIMESTAMPTZ,
    zone                VARCHAR(20)  NOT NULL,
    state               VARCHAR(50)  NOT NULL,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
) PARTITION BY LIST (zone);`,

		`CREATE TABLE IF NOT EXISTS curated.customer_merge_log (
    id                  BIGSERIAL PRIMARY KEY,
    msisdn              VARCHAR(15)  NOT NULL,
    winning_company     VARCHAR(30)  NOT NULL,
    losing_companies    TEXT[],
    merge_reason        TEXT,
    canonical_id        UUID,
    merged_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);`,
	}

	for _, ddl := range ddls {
		if err := exec(db, ddl); err != nil {
			return fmt.Errorf("curated table: %w", err)
		}
	}
	return nil
}

// createCuratedPartitions creates zone-level and state-level sub-partitions for each curated table.
func createCuratedPartitions(db *sql.DB) error {
	tables := []string{"customers", "subscriptions", "billing_accounts", "sim_inventory", "port_history", "network_assignments"}

	for _, zone := range sharding.Zones {
		for _, table := range tables {
			// Zone-level partition
			zonePartition := fmt.Sprintf(
				`CREATE TABLE IF NOT EXISTS curated.%s_%s PARTITION OF curated.%s FOR VALUES IN ('%s') PARTITION BY LIST (state);`,
				table, zone.Name, table, zone.Name,
			)
			if err := exec(db, zonePartition); err != nil {
				return fmt.Errorf("zone partition %s_%s: %w", table, zone.Name, err)
			}

			// State-level partitions within each zone partition
			for _, state := range zone.States {
				statePartition := fmt.Sprintf(
					`CREATE TABLE IF NOT EXISTS curated.%s_%s_%s PARTITION OF curated.%s_%s FOR VALUES IN ('%s');`,
					table, zone.Name, state, table, zone.Name, state,
				)
				if err := exec(db, statePartition); err != nil {
					return fmt.Errorf("state partition %s_%s_%s: %w", table, zone.Name, state, err)
				}
			}
		}
		log.Printf("  zone [%s] partitions created for all tables", zone.Name)
	}
	return nil
}

// ---------- AUDIT SCHEMA ----------

func createAuditTables(db *sql.DB) error {
	return exec(db, `
CREATE TABLE IF NOT EXISTS audit.audit_trail (
    id                  BIGSERIAL PRIMARY KEY,
    table_name          VARCHAR(100) NOT NULL,
    operation           VARCHAR(10)  NOT NULL,  -- INSERT, UPDATE, DELETE
    record_id           BIGINT,
    msisdn              VARCHAR(15),
    source_company      VARCHAR(30),
    zone                VARCHAR(20),
    state               VARCHAR(50),
    before_state        JSONB,
    after_state         JSONB,
    changed_by          VARCHAR(50)  NOT NULL DEFAULT 'etl_pipeline',
    changed_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    batch_id            BIGINT,
    pipeline_run_id     VARCHAR(50)
);`)
}

// ---------- INDEXES ----------

func createIndexes(db *sql.DB) error {
	indexes := []string{
		// raw schema
		`CREATE INDEX IF NOT EXISTS idx_raw_customers_msisdn ON raw.customers (msisdn);`,
		`CREATE INDEX IF NOT EXISTS idx_raw_customers_source ON raw.customers (source_company, zone, state);`,
		`CREATE INDEX IF NOT EXISTS idx_raw_subscriptions_msisdn ON raw.subscriptions (msisdn);`,
		`CREATE INDEX IF NOT EXISTS idx_raw_billing_msisdn ON raw.billing_accounts (msisdn);`,
		`CREATE INDEX IF NOT EXISTS idx_raw_sim_msisdn ON raw.sim_inventory (msisdn);`,
		`CREATE INDEX IF NOT EXISTS idx_raw_port_msisdn ON raw.port_history (msisdn);`,
		// staging schema
		`CREATE INDEX IF NOT EXISTS idx_staging_customers_msisdn ON staging.customers (msisdn);`,
		`CREATE INDEX IF NOT EXISTS idx_staging_customers_canonical ON staging.customers (canonical_id);`,
		// audit
		`CREATE INDEX IF NOT EXISTS idx_audit_msisdn ON audit.audit_trail (msisdn);`,
		`CREATE INDEX IF NOT EXISTS idx_audit_changed_at ON audit.audit_trail (changed_at);`,
		`CREATE INDEX IF NOT EXISTS idx_audit_batch ON audit.audit_trail (batch_id);`,
		// curated merge log
		`CREATE INDEX IF NOT EXISTS idx_merge_log_msisdn ON curated.customer_merge_log (msisdn);`,
	}
	for _, idx := range indexes {
		if err := exec(db, idx); err != nil {
			return fmt.Errorf("index: %w", err)
		}
	}
	return nil
}
