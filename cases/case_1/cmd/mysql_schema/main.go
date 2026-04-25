// mysql_schema — creates the initial sharded source tables on the 4 MySQL Docker instances.
//
// Usage:
//   go run ./cmd/mysql_schema
//
// Only split _1 is created here. The seeder dynamically creates _2, _3 … when a
// table grows past sharding.SplitRowCap (1 million rows).
//
// Sharding strategy differs per company (simulating real-world divergence):
//   vodafone  — zone + state  → customers_north_up_1
//   idea      — zone only     → customers_north_1
//   tata_docomo — state only  → customers_up_1
//   aircel    — zone + state  → customers_north_up_1
//
// Tables for zone-only companies omit the `state` column; state-only companies
// omit the `zone` column. This exercises the SchemaMapper / GeoTagger transformers.

package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/streamcraft/telecom-etl/db_setup/internal/config"
	"github.com/streamcraft/telecom-etl/db_setup/internal/sharding"
)

// companySchema defines how each company names its columns (intentional divergence).
type companySchema struct {
	msisdnCol   string
	nameCol     string
	dobCol      string
	aadhaarCol  string
	panCol      string
	emailCol    string
	addressCol  string
	activatedAt string

	planCodeCol string
	startCol    string
	endCol      string
	statusCol   string

	dueAmountCol  string
	paidAmountCol string
	cycleStartCol string
	cycleEndCol   string

	simSerialCol string
	imsiCol      string
	simStatusCol string

	portTypeCol string
	fromCarrier string
	toCarrier   string
}

var companySchemas = map[string]companySchema{
	"vodafone": {
		msisdnCol: "mob_no", nameCol: "cust_name", dobCol: "date_of_birth",
		aadhaarCol: "aadhaar_num", panCol: "pan_num", emailCol: "email_id",
		addressCol: "address", activatedAt: "activation_date",
		planCodeCol: "plan_code", startCol: "plan_start", endCol: "plan_end", statusCol: "sub_status",
		dueAmountCol: "amount_due", paidAmountCol: "amount_paid", cycleStartCol: "cycle_from", cycleEndCol: "cycle_to",
		simSerialCol: "sim_serial", imsiCol: "imsi_no", simStatusCol: "sim_status",
		portTypeCol: "port_direction", fromCarrier: "from_operator", toCarrier: "to_operator",
	},
	"idea": {
		msisdnCol: "contact", nameCol: "full_name", dobCol: "dob",
		aadhaarCol: "aadhaar", panCol: "pan", emailCol: "email",
		addressCol: "addr", activatedAt: "activated_on",
		planCodeCol: "tariff_code", startCol: "validity_start", endCol: "validity_end", statusCol: "status",
		dueAmountCol: "outstanding", paidAmountCol: "paid", cycleStartCol: "bill_from", cycleEndCol: "bill_to",
		simSerialCol: "serial_no", imsiCol: "imsi", simStatusCol: "status",
		portTypeCol: "mnp_type", fromCarrier: "source_operator", toCarrier: "dest_operator",
	},
	"tata_docomo": {
		msisdnCol: "phone_number", nameCol: "subscriber_name", dobCol: "birth_date",
		aadhaarCol: "id_aadhaar", panCol: "id_pan", emailCol: "subscriber_email",
		addressCol: "residential_address", activatedAt: "sim_activation_dt",
		planCodeCol: "pack_code", startCol: "pack_start_dt", endCol: "pack_end_dt", statusCol: "pack_status",
		dueAmountCol: "dues", paidAmountCol: "payments_received", cycleStartCol: "period_start", cycleEndCol: "period_end",
		simSerialCol: "iccid", imsiCol: "imsi_code", simStatusCol: "sim_active",
		portTypeCol: "port_in_out", fromCarrier: "prev_carrier", toCarrier: "new_carrier",
	},
	"aircel": {
		msisdnCol: "msisdn", nameCol: "name", dobCol: "dob",
		aadhaarCol: "aadhaar_id", panCol: "pan_id", emailCol: "email_address",
		addressCol: "full_address", activatedAt: "created_at",
		planCodeCol: "product_code", startCol: "start_date", endCol: "end_date", statusCol: "subscription_state",
		dueAmountCol: "balance_due", paidAmountCol: "balance_paid", cycleStartCol: "cycle_open", cycleEndCol: "cycle_close",
		simSerialCol: "sim_id", imsiCol: "imsi_number", simStatusCol: "active_flag",
		portTypeCol: "port_event", fromCarrier: "from_network", toCarrier: "to_network",
	},
}

func main() {
	log.Println("=== MySQL Schema Creator ===")

	for _, company := range sharding.Companies {
		dsn := config.MySQLDSN(config.DBHost, company.Port, company.DBName)
		log.Printf("[%s] connecting on port %d (sharding: %s) ...", company.Name, company.Port, company.ShardingType)

		db, err := connectWithRetry(dsn, 15)
		if err != nil {
			log.Fatalf("[%s] failed to connect: %v", company.Name, err)
		}
		defer db.Close()

		s := companySchemas[company.Name]
		n, err := createInitialSplits(db, company.Name, company.ShardingType, s)
		if err != nil {
			log.Fatalf("[%s] schema creation failed: %v", company.Name, err)
		}
		log.Printf("[%s] ✓ %d tables created (split _1 only; seeder creates _2+ dynamically)\n", company.Name, n)
	}

	log.Println("=== MySQL schema setup complete ===")
}

// createInitialSplits creates only the _1 split for each shard.
// Subsequent splits are created on the fly by the seeder when SplitRowCap is reached.
func createInitialSplits(db *sql.DB, company string, st sharding.ShardingType, s companySchema) (int, error) {
	var suffixes []suffixMeta

	switch st {
	case sharding.ZoneState:
		for _, z := range sharding.Zones {
			for _, state := range z.States {
				suffixes = append(suffixes, suffixMeta{
					suffix:   fmt.Sprintf("%s_%s_1", z.Name, state),
					hasZone:  true,
					hasState: true,
				})
			}
		}
	case sharding.ZoneOnly:
		for _, z := range sharding.Zones {
			suffixes = append(suffixes, suffixMeta{
				suffix:   fmt.Sprintf("%s_1", z.Name),
				hasZone:  true,
				hasState: false,
			})
		}
	case sharding.StateOnly:
		for _, state := range sharding.AllStates() {
			suffixes = append(suffixes, suffixMeta{
				suffix:   fmt.Sprintf("%s_1", state),
				hasZone:  false,
				hasState: true,
			})
		}
	}

	count := 0
	for _, sm := range suffixes {
		ddls := buildDDLs(sm.suffix, s, sm.hasZone, sm.hasState)
		for _, ddl := range ddls {
			if _, err := db.Exec(ddl); err != nil {
				return count, fmt.Errorf("suffix=%s: %w", sm.suffix, err)
			}
			count++
		}
	}
	return count, nil
}

type suffixMeta struct {
	suffix   string
	hasZone  bool
	hasState bool
}

func buildDDLs(suffix string, s companySchema, hasZone, hasState bool) []string {
	return []string{
		customersTable(suffix, s, hasZone, hasState),
		subscriptionsTable(suffix, s, hasZone, hasState),
		billingAccountsTable(suffix, s, hasZone, hasState),
		simInventoryTable(suffix, s, hasZone, hasState),
		portHistoryTable(suffix, s, hasZone, hasState),
	}
}

// geoColsDDL returns the zone/state column definitions appropriate for this company's sharding type.
func geoColsDDL(hasZone, hasState bool) string {
	switch {
	case hasZone && hasState:
		return "zone            VARCHAR(20)  NOT NULL,\n    state           VARCHAR(50)  NOT NULL,"
	case hasZone:
		return "zone            VARCHAR(20)  NOT NULL,"
	case hasState:
		return "state           VARCHAR(50)  NOT NULL,"
	default:
		return ""
	}
}

func geoIdxDDL(hasZone, hasState bool) string {
	switch {
	case hasZone && hasState:
		return "INDEX idx_zone_state (zone, state)"
	case hasZone:
		return "INDEX idx_zone (zone)"
	case hasState:
		return "INDEX idx_state (state)"
	default:
		return ""
	}
}

func customersTable(suffix string, s companySchema, hasZone, hasState bool) string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS customers_%s (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    %s              VARCHAR(15)  COMMENT 'msisdn',
    %s              VARCHAR(100) NOT NULL COMMENT 'name',
    %s              DATE         COMMENT 'dob',
    %s              VARCHAR(20)  COMMENT 'aadhaar (raw, pre-mask)',
    %s              VARCHAR(15)  COMMENT 'pan (raw, pre-mask)',
    %s              VARCHAR(150) COMMENT 'email',
    %s              TEXT         COMMENT 'address',
    %s              DATETIME     COMMENT 'activation_date',
    %s
    table_split     TINYINT UNSIGNED NOT NULL DEFAULT 1,
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_msisdn (%s),
    %s
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		suffix,
		s.msisdnCol, s.nameCol, s.dobCol, s.aadhaarCol, s.panCol, s.emailCol, s.addressCol, s.activatedAt,
		geoColsDDL(hasZone, hasState),
		s.msisdnCol,
		geoIdxDDL(hasZone, hasState),
	)
}

func subscriptionsTable(suffix string, s companySchema, hasZone, hasState bool) string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS subscriptions_%s (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    customer_id     BIGINT UNSIGNED NOT NULL COMMENT 'FK → customers.id',
    %s              VARCHAR(15)  NOT NULL COMMENT 'msisdn',
    %s              VARCHAR(50)  NOT NULL COMMENT 'plan_code',
    %s              DATE         COMMENT 'plan_start',
    %s              DATE         COMMENT 'plan_end',
    %s              VARCHAR(20)  NOT NULL DEFAULT 'ACTIVE' COMMENT 'status',
    %s
    table_split     TINYINT UNSIGNED NOT NULL DEFAULT 1,
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_msisdn (%s),
    INDEX idx_plan (%s)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		suffix,
		s.msisdnCol, s.planCodeCol, s.startCol, s.endCol, s.statusCol,
		geoColsDDL(hasZone, hasState),
		s.msisdnCol, s.planCodeCol,
	)
}

func billingAccountsTable(suffix string, s companySchema, hasZone, hasState bool) string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS billing_accounts_%s (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    customer_id     BIGINT UNSIGNED NOT NULL COMMENT 'FK → customers.id',
    %s              VARCHAR(15)  NOT NULL COMMENT 'msisdn',
    %s              DECIMAL(10,2) NOT NULL DEFAULT 0.00 COMMENT 'amount_due',
    %s              DECIMAL(10,2) NOT NULL DEFAULT 0.00 COMMENT 'amount_paid',
    %s              DATE         COMMENT 'cycle_start',
    %s              DATE         COMMENT 'cycle_end',
    bill_status     VARCHAR(20)  NOT NULL DEFAULT 'UNPAID',
    %s
    table_split     TINYINT UNSIGNED NOT NULL DEFAULT 1,
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_msisdn (%s),
    INDEX idx_cycle (%s, %s)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		suffix,
		s.msisdnCol, s.dueAmountCol, s.paidAmountCol, s.cycleStartCol, s.cycleEndCol,
		geoColsDDL(hasZone, hasState),
		s.msisdnCol, s.cycleStartCol, s.cycleEndCol,
	)
}

func simInventoryTable(suffix string, s companySchema, hasZone, hasState bool) string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS sim_inventory_%s (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    customer_id     BIGINT UNSIGNED NOT NULL COMMENT 'FK → customers.id',
    %s              VARCHAR(15)  NOT NULL COMMENT 'msisdn',
    %s              VARCHAR(22)  NOT NULL COMMENT 'sim_serial / ICCID',
    %s              VARCHAR(15)  NOT NULL COMMENT 'imsi',
    %s              VARCHAR(20)  NOT NULL DEFAULT 'ACTIVE' COMMENT 'sim_status',
    activated_date  DATE         COMMENT 'sim activation date',
    deactivated_date DATE        COMMENT 'sim deactivation date (nullable)',
    %s
    table_split     TINYINT UNSIGNED NOT NULL DEFAULT 1,
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_msisdn (%s),
    UNIQUE idx_iccid (%s)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		suffix,
		s.msisdnCol, s.simSerialCol, s.imsiCol, s.simStatusCol,
		geoColsDDL(hasZone, hasState),
		s.msisdnCol, s.simSerialCol,
	)
}

func portHistoryTable(suffix string, s companySchema, hasZone, hasState bool) string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS port_history_%s (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    customer_id     BIGINT UNSIGNED NOT NULL COMMENT 'FK → customers.id',
    %s              VARCHAR(15)  NOT NULL COMMENT 'msisdn',
    %s              VARCHAR(10)  NOT NULL COMMENT 'port direction: IN or OUT',
    %s              VARCHAR(50)  COMMENT 'from_carrier',
    %s              VARCHAR(50)  COMMENT 'to_carrier',
    port_date       DATE         NOT NULL,
    porting_ref     VARCHAR(30)  COMMENT 'UPC / porting reference number',
    status          VARCHAR(20)  NOT NULL DEFAULT 'COMPLETED',
    %s
    table_split     TINYINT UNSIGNED NOT NULL DEFAULT 1,
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_msisdn (%s),
    INDEX idx_port_date (port_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		suffix,
		s.msisdnCol, s.portTypeCol, s.fromCarrier, s.toCarrier,
		geoColsDDL(hasZone, hasState),
		s.msisdnCol,
	)
}

func connectWithRetry(dsn string, retries int) (*sql.DB, error) {
	for i := 0; i < retries; i++ {
		db, err := sql.Open("mysql", dsn)
		if err == nil {
			if pingErr := db.Ping(); pingErr == nil {
				return db, nil
			}
		}
		log.Printf("  waiting for DB... (%d/%d)", i+1, retries)
		time.Sleep(3 * time.Second)
	}
	return nil, fmt.Errorf("could not connect after %d retries", retries)
}
