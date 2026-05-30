package main

// mssql_schema — creates the analytics_warehouse database, dbo/stage schemas,
// all tables, indexes, and the etl_writer service account on SQL Server.
//
// USAGE:
//   go run ./cmd/mssql_schema \
//     -host localhost -port 1433 \
//     -sa-pass "Etl_Pass_123!" \
//     -db analytics_warehouse \
//     -user etl_writer -pass Etl_Pass_456!
//   make migrate

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"

	_ "github.com/microsoft/go-mssqldb"
)

var (
	flagHost   = flag.String("host", "localhost", "SQL Server host")
	flagPort   = flag.String("port", "1433", "SQL Server port")
	flagSAPass = flag.String("sa-pass", "Etl_Pass_123!", "SA password")
	flagDB     = flag.String("db", "analytics_warehouse", "Target database name")
	flagUser   = flag.String("user", "etl_writer", "ETL service account name")
	flagPass   = flag.String("pass", "Etl_Pass_456!", "ETL service account password")
)

func main() {
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	ctx := context.Background()

	// ── Phase 1: connect as SA to master, create DB and login ────────────────
	masterDSN := fmt.Sprintf("sqlserver://SA:%s@%s:%s?database=master&encrypt=disable",
		*flagSAPass, *flagHost, *flagPort)

	masterDB, err := sql.Open("sqlserver", masterDSN)
	if err != nil {
		log.Fatalf("open master: %v", err)
	}
	if err := masterDB.PingContext(ctx); err != nil {
		log.Fatalf("ping master: %v", err)
	}
	log.Println("=== SQL Server Schema Setup — Myntra GA4 Analytics ETL ===")

	for _, stmt := range masterDDL(*flagDB, *flagUser, *flagPass) {
		if _, err := masterDB.ExecContext(ctx, stmt.sql); err != nil {
			log.Printf("  warn: %s: %v (may already exist)", stmt.name, err)
		} else {
			log.Printf("  ✓ %s", stmt.name)
		}
	}
	masterDB.Close()

	// ── Phase 2: connect to the target DB, create schemas + tables ───────────
	dbDSN := fmt.Sprintf("sqlserver://SA:%s@%s:%s?database=%s&encrypt=disable",
		*flagSAPass, *flagHost, *flagPort, *flagDB)

	db, err := sql.Open("sqlserver", dbDSN)
	if err != nil {
		log.Fatalf("open %s: %v", *flagDB, err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("ping %s: %v", *flagDB, err)
	}

	for _, stmt := range schemaDDL() {
		if _, err := db.ExecContext(ctx, stmt.sql); err != nil {
			log.Printf("  warn: %s: %v", stmt.name, err)
		} else {
			log.Printf("  ✓ %s", stmt.name)
		}
	}

	log.Println("=== SQL Server schema setup complete ===")
}

type namedSQL struct {
	name string
	sql  string
}

func masterDDL(dbName, user, pass string) []namedSQL {
	return []namedSQL{
		{
			name: fmt.Sprintf("database %s", dbName),
			sql:  fmt.Sprintf("IF DB_ID('%s') IS NULL CREATE DATABASE [%s]", dbName, dbName),
		},
		{
			name: fmt.Sprintf("login %s", user),
			sql: fmt.Sprintf(`IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = '%s')
				CREATE LOGIN [%s] WITH PASSWORD = '%s'`, user, user, pass),
		},
		{
			name: fmt.Sprintf("db user %s", user),
			sql: fmt.Sprintf(`USE [%s];
				IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = '%s')
				CREATE USER [%s] FOR LOGIN [%s]`, dbName, user, user, user),
		},
		{
			name: fmt.Sprintf("grant %s db_owner", user),
			sql:  fmt.Sprintf("USE [%s]; ALTER ROLE db_owner ADD MEMBER [%s]", dbName, user),
		},
	}
}

func schemaDDL() []namedSQL {
	return []namedSQL{
		{name: "schema dbo", sql: "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='dbo') EXEC('CREATE SCHEMA dbo')"},
		{name: "schema stage", sql: "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='stage') EXEC('CREATE SCHEMA stage')"},

		{name: "dbo.ga4_sessions", sql: createGA4SessionsTable("dbo")},
		{name: "stage.ga4_sessions", sql: createGA4SessionsTable("stage")},
		{name: "dbo.realtime_sessions", sql: createRealtimeTable()},
		{name: "dbo.pipeline_run_log", sql: createPipelineRunLog()},

		{name: "idx_ga4_date_property", sql: `IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='idx_ga4_date_property')
			CREATE CLUSTERED INDEX idx_ga4_date_property ON dbo.ga4_sessions (property_id, report_date)`},
		{name: "idx_ga4_attribution", sql: `IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='idx_ga4_attribution')
			CREATE NONCLUSTERED INDEX idx_ga4_attribution ON dbo.ga4_sessions (source, medium, campaign)`},
		{name: "idx_ga4_columnstore", sql: `IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='idx_ga4_columnstore')
			CREATE NONCLUSTERED COLUMNSTORE INDEX idx_ga4_columnstore
			ON dbo.ga4_sessions (report_date, property_id, conversions, purchase_revenue_inr)`},
		{name: "idx_realtime_snapshot", sql: `IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='idx_realtime_snapshot')
			CREATE NONCLUSTERED INDEX idx_realtime_snapshot ON dbo.realtime_sessions (snapshot_at, property_id)`},
	}
}

func createGA4SessionsTable(schema string) string {
	pk := ""
	heap := ""
	if schema == "dbo" {
		pk = ",\n\tCONSTRAINT pk_ga4_sessions PRIMARY KEY NONCLUSTERED (property_id, report_date, session_id)"
	} else {
		heap = " -- HEAP: no clustered index for fast bulk insert"
	}
	return fmt.Sprintf(`IF OBJECT_ID('%s.ga4_sessions', 'U') IS NULL
	CREATE TABLE %s.ga4_sessions (%s
		property_id             VARCHAR(50)     NOT NULL,
		surface                 VARCHAR(20)     NOT NULL,
		report_date             DATE            NOT NULL,
		session_id              VARCHAR(100)    NOT NULL,
		user_pseudo_id          VARCHAR(100),
		device_category         VARCHAR(30),
		city                    VARCHAR(100),
		country                 CHAR(2),
		source                  VARCHAR(200),
		medium                  VARCHAR(100),
		campaign                VARCHAR(500),
		product_category        VARCHAR(200),
		payment_method          VARCHAR(100),
		wishlisted              VARCHAR(10),
		app_version             VARCHAR(50),
		os_version              VARCHAR(50),
		sessions                INT,
		engaged_sessions        INT,
		total_users             INT,
		new_users               INT,
		bounce_rate             DECIMAL(6,4),
		avg_session_duration_secs DECIMAL(10,2),
		conversions             INT,
		purchase_revenue_inr    DECIMAL(18,2),
		event_count             INT,
		screen_page_views       INT,
		ingested_at             DATETIME2,
		pipeline_run_id         VARCHAR(100)%s
	)%s`, schema, schema, "", pk, heap)
}

func createRealtimeTable() string {
	return `IF OBJECT_ID('dbo.realtime_sessions', 'U') IS NULL
	CREATE TABLE dbo.realtime_sessions (
		snapshot_at     DATETIME2    NOT NULL,
		property_id     VARCHAR(50)  NOT NULL,
		surface         VARCHAR(20)  NOT NULL,
		active_users    INT,
		city            VARCHAR(100),
		device_category VARCHAR(30),
		page_path       VARCHAR(1000),
		event_name      VARCHAR(200)
	)`
}

func createPipelineRunLog() string {
	return `IF OBJECT_ID('dbo.pipeline_run_log', 'U') IS NULL
	CREATE TABLE dbo.pipeline_run_log (
		run_id              VARCHAR(100) PRIMARY KEY,
		pipeline_name       VARCHAR(200),
		property_id         VARCHAR(50),
		surface             VARCHAR(20),
		started_at          DATETIME2,
		finished_at         DATETIME2,
		rows_fetched        BIGINT       DEFAULT 0,
		rows_merged         BIGINT       DEFAULT 0,
		quota_tokens_spent  INT          DEFAULT 0,
		status              VARCHAR(20)  DEFAULT 'running',
		error_message       NVARCHAR(MAX)
	)`
}
