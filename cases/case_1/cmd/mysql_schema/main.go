// mysql_schema — creates all sharded source tables on the 4 MySQL Docker instances.
//
// Usage:
//   go run ./cmd/mysql_schema
//
// Connects to each MySQL container (ports 3306-3309) and creates:
//   customers_{zone}_{state}_{n}
//   subscriptions_{zone}_{state}_{n}
//   billing_accounts_{zone}_{state}_{n}
//   sim_inventory_{zone}_{state}_{n}
//   port_history_{zone}_{state}_{n}
//
// Each company uses deliberately different column naming to simulate real-world
// schema divergence (the SchemaMapper transformer handles normalization).

package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/streamcraft/telecom-etl/db_setup/internal/config"
	"github.com/streamcraft/telecom-etl/db_setup/internal/sharding"
)

// companySchema defines how each company names its columns (intentional divergence).
type companySchema struct {
	// customers table column variants
	msisdnCol   string // phone number column name
	nameCol     string
	dobCol      string
	aadhaarCol  string
	panCol      string
	emailCol    string
	addressCol  string
	activatedAt string

	// subscriptions
	planCodeCol string
	startCol    string
	endCol      string
	statusCol   string

	// billing
	dueAmountCol  string
	paidAmountCol string
	cycleStartCol string
	cycleEndCol   string

	// sim_inventory
	simSerialCol string
	imsiCol      string
	simStatusCol string

	// port_history
	portTypeCol string  // IN or OUT
	fromCarrier string
	toCarrier   string
}

// companySchemas maps company name → its column naming quirks.
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
	log.Printf("Creating sharded tables across %d companies\n", len(sharding.Companies))

	for _, company := range sharding.Companies {
		dsn := config.MySQLDSN(config.DBHost, company.Port, company.DBName)
		log.Printf("[%s] Connecting on port %d ...", company.Name, company.Port)

		db, err := connectWithRetry(dsn, 15)
		if err != nil {
			log.Fatalf("[%s] Failed to connect: %v", company.Name, err)
		}
		defer db.Close()

		schema := companySchemas[company.Name]
		if err := createAllShardedTables(db, company.Name, schema); err != nil {
			log.Fatalf("[%s] Schema creation failed: %v", company.Name, err)
		}
		log.Printf("[%s] ✓ All tables created\n", company.Name)
	}

	log.Println("=== MySQL schema setup complete ===")
}

// connectWithRetry retries the MySQL connection (containers may take time to be ready).
func connectWithRetry(dsn string, retries int) (*sql.DB, error) {
	var db *sql.DB
	var err error
	for i := 0; i < retries; i++ {
		db, err = sql.Open("mysql", dsn)
		if err == nil {
			if pingErr := db.Ping(); pingErr == nil {
				return db, nil
			}
		}
		log.Printf("  waiting for DB... (%d/%d)", i+1, retries)
		time.Sleep(3 * time.Second)
	}
	return nil, fmt.Errorf("could not connect after %d retries: %v", retries, err)
}

// createAllShardedTables iterates every zone → state → split and creates all 5 table types.
func createAllShardedTables(db *sql.DB, company string, s companySchema) error {
	totalTables := 0
	for _, zone := range sharding.Zones {
		for _, state := range zone.States {
			for split := 1; split <= sharding.DefaultSplits; split++ {
				suffix := fmt.Sprintf("%s_%s_%d", zone.Name, state, split)

				ddls := []string{
					customersTable(suffix, s),
					subscriptionsTable(suffix, s),
					billingAccountsTable(suffix, s),
					simInventoryTable(suffix, s),
					portHistoryTable(suffix, s),
				}

				for _, ddl := range ddls {
					if _, err := db.Exec(ddl); err != nil {
						return fmt.Errorf("zone=%s state=%s split=%d: %w", zone.Name, state, split, err)
					}
					totalTables++
				}
			}
		}
	}
	log.Printf("  created %d tables", totalTables)
	return nil
}

// ---------- DDL builders ----------

func customersTable(suffix string, s companySchema) string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS customers_%s (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    %s              VARCHAR(15)  NOT NULL COMMENT 'msisdn',
    %s              VARCHAR(100) NOT NULL COMMENT 'name',
    %s              DATE         COMMENT 'dob',
    %s              VARCHAR(20)  COMMENT 'aadhaar (raw, pre-mask)',
    %s              VARCHAR(15)  COMMENT 'pan (raw, pre-mask)',
    %s              VARCHAR(150) COMMENT 'email',
    %s              TEXT         COMMENT 'address',
    %s              DATETIME     COMMENT 'activation_date',
    zone            VARCHAR(20)  NOT NULL,
    state           VARCHAR(50)  NOT NULL,
    table_split     TINYINT UNSIGNED NOT NULL DEFAULT 1,
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_msisdn (%s),
    INDEX idx_zone_state (zone, state)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		suffix,
		s.msisdnCol, s.nameCol, s.dobCol, s.aadhaarCol, s.panCol, s.emailCol, s.addressCol, s.activatedAt,
		s.msisdnCol,
	)
}

func subscriptionsTable(suffix string, s companySchema) string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS subscriptions_%s (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    customer_id     BIGINT UNSIGNED NOT NULL COMMENT 'FK → customers.id',
    %s              VARCHAR(15)  NOT NULL COMMENT 'msisdn',
    %s              VARCHAR(50)  NOT NULL COMMENT 'plan_code',
    %s              DATE         COMMENT 'plan_start',
    %s              DATE         COMMENT 'plan_end',
    %s              VARCHAR(20)  NOT NULL DEFAULT 'ACTIVE' COMMENT 'status',
    zone            VARCHAR(20)  NOT NULL,
    state           VARCHAR(50)  NOT NULL,
    table_split     TINYINT UNSIGNED NOT NULL DEFAULT 1,
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_msisdn (%s),
    INDEX idx_plan (%s)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		suffix,
		s.msisdnCol, s.planCodeCol, s.startCol, s.endCol, s.statusCol,
		s.msisdnCol, s.planCodeCol,
	)
}

func billingAccountsTable(suffix string, s companySchema) string {
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
    zone            VARCHAR(20)  NOT NULL,
    state           VARCHAR(50)  NOT NULL,
    table_split     TINYINT UNSIGNED NOT NULL DEFAULT 1,
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_msisdn (%s),
    INDEX idx_cycle (%s, %s)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		suffix,
		s.msisdnCol, s.dueAmountCol, s.paidAmountCol, s.cycleStartCol, s.cycleEndCol,
		s.msisdnCol, s.cycleStartCol, s.cycleEndCol,
	)
}

func simInventoryTable(suffix string, s companySchema) string {
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
    zone            VARCHAR(20)  NOT NULL,
    state           VARCHAR(50)  NOT NULL,
    table_split     TINYINT UNSIGNED NOT NULL DEFAULT 1,
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_msisdn (%s),
    UNIQUE idx_iccid (%s)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		suffix,
		s.msisdnCol, s.simSerialCol, s.imsiCol, s.simStatusCol,
		s.msisdnCol, s.simSerialCol,
	)
}

func portHistoryTable(suffix string, s companySchema) string {
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
    zone            VARCHAR(20)  NOT NULL,
    state           VARCHAR(50)  NOT NULL,
    table_split     TINYINT UNSIGNED NOT NULL DEFAULT 1,
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_msisdn (%s),
    INDEX idx_port_date (port_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
		suffix,
		s.msisdnCol, s.portTypeCol, s.fromCarrier, s.toCarrier,
		s.msisdnCol,
	)
}

// splitName returns a formatted table name like "customers_north_up_1"
func splitName(tableType, zone, state string, split int) string {
	return fmt.Sprintf("%s_%s_%s_%d", tableType, zone, state, split)
}

// ensure splitName is referenced to avoid unused import lint
var _ = strings.ToUpper
var _ = splitName
