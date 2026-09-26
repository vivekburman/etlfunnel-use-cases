package main

import (
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/pem"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/snowflakedb/gosnowflake"

	"github.com/streamcraft/settleright-etl/db_setup/internal/merchants"
)

// One-time DDL against a live Snowflake trial account (no local emulator
// exists, so this can't be a docker-compose service - see the case study
// plan's Part 5.1). Table/column names and the stream/consume-table pair
// here must match what client/connectors/connector_2..5 in
// etlfunnel-execution expect exactly.

var (
	account        = flag.String("account", envOr("SNOWFLAKE_ACCOUNT", ""), "Snowflake account identifier")
	user           = flag.String("user", envOr("SNOWFLAKE_USER", ""), "Snowflake username")
	privateKeyFile = flag.String("private-key-file", envOr("SNOWFLAKE_PRIVATE_KEY_FILE", ""), "path to an unencrypted PKCS8 private key .p8 file")
	warehouse      = flag.String("warehouse", envOr("SNOWFLAKE_WAREHOUSE", ""), "Snowflake warehouse")
	database       = flag.String("database", envOr("SNOWFLAKE_DATABASE", ""), "Snowflake database")
	role           = flag.String("role", envOr("SNOWFLAKE_ROLE", ""), "Snowflake role")
	schema         = flag.String("schema", envOr("SNOWFLAKE_SCHEMA", "PUBLIC"), "Snowflake schema")
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	flag.Parse()
	log.Println("=== Snowflake Setup (RAW_TRANSACTIONS, MERCHANT_RISK_PROFILE, TXN_STREAM, STREAM_CONSUME_MARKER, MERCHANT_TXN_SUMMARY) ===")

	if *account == "" || *user == "" || *privateKeyFile == "" || *warehouse == "" || *database == "" || *role == "" {
		log.Fatal("account, user, private-key-file, warehouse, database and role are all required (flags or SNOWFLAKE_* env vars)")
	}

	privateKey, err := loadPrivateKey(*privateKeyFile)
	if err != nil {
		log.Fatalf("load private key: %v", err)
	}

	dsn, err := gosnowflake.DSN(&gosnowflake.Config{
		Account:       *account,
		User:          *user,
		Authenticator: gosnowflake.AuthTypeJwt,
		PrivateKey:    privateKey,
		Warehouse:     *warehouse,
		Database:      *database,
		Schema:        *schema,
		Role:          *role,
	})
	if err != nil {
		log.Fatalf("build dsn: %v", err)
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		log.Fatalf("ping: %v", err)
	}

	steps := []struct {
		name string
		ddl  string
	}{
		{"Create RAW_TRANSACTIONS", rawTransactionsDDL},
		{"Enable change tracking on RAW_TRANSACTIONS", changeTrackingDDL},
		{"Create MERCHANT_RISK_PROFILE", merchantRiskProfileDDL},
		{"Create TXN_STREAM", txnStreamDDL},
		{"Create STREAM_CONSUME_MARKER", streamConsumeMarkerDDL},
		{"Create MERCHANT_TXN_SUMMARY", merchantTxnSummaryDDL},
	}
	for _, step := range steps {
		log.Printf("[snowflake] %s ...", step.name)
		if _, err := db.Exec(step.ddl); err != nil {
			log.Fatalf("  FAILED: %v", err)
		}
		log.Printf("  done")
	}

	log.Printf("[snowflake] seeding MERCHANT_RISK_PROFILE (%d merchants) ...", len(merchants.All))
	if err := seedMerchantRiskProfile(db); err != nil {
		log.Fatalf("  FAILED: %v", err)
	}
	log.Printf("  done")

	log.Println("=== Snowflake setup complete ===")
}

func loadPrivateKey(path string) (*rsa.PrivateKey, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, fmt.Errorf("%s is not a PEM file", path)
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse PKCS8 key: %w", err)
	}
	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("%s is not an RSA private key", path)
	}
	return rsaKey, nil
}

const rawTransactionsDDL = `
CREATE TABLE IF NOT EXISTS RAW_TRANSACTIONS (
	TXN_ID     NUMBER,
	MERCHANT_ID VARCHAR,
	AMOUNT     FLOAT,
	CURRENCY   VARCHAR,
	CARD_BIN   VARCHAR,
	STATUS     VARCHAR,
	CREATED_AT TIMESTAMP_NTZ,
	LOADED_AT  TIMESTAMP_NTZ
)`

const changeTrackingDDL = `ALTER TABLE RAW_TRANSACTIONS SET CHANGE_TRACKING = TRUE`

const merchantRiskProfileDDL = `
CREATE TABLE IF NOT EXISTS MERCHANT_RISK_PROFILE (
	MERCHANT_ID     VARCHAR PRIMARY KEY,
	RISK_TIER       VARCHAR,
	CHARGEBACK_RATE FLOAT
)`

const txnStreamDDL = `CREATE STREAM IF NOT EXISTS TXN_STREAM ON TABLE RAW_TRANSACTIONS`

const streamConsumeMarkerDDL = `
CREATE TABLE IF NOT EXISTS STREAM_CONSUME_MARKER (
	CONSUMED_COUNT NUMBER
)`

const merchantTxnSummaryDDL = `
CREATE TABLE IF NOT EXISTS MERCHANT_TXN_SUMMARY (
	TXN_ID                NUMBER PRIMARY KEY,
	MERCHANT_ID            VARCHAR,
	AMOUNT                 FLOAT,
	RISK_TIER              VARCHAR,
	FRAUD_SCORE            FLOAT,
	FRAUD_REASON           VARCHAR,
	RECONCILIATION_STATUS  VARCHAR,
	UPDATED_AT             TIMESTAMP_NTZ
)`

func seedMerchantRiskProfile(db *sql.DB) error {
	for _, m := range merchants.All {
		_, err := db.Exec(`
			MERGE INTO MERCHANT_RISK_PROFILE t
			USING (SELECT ? AS MERCHANT_ID, ? AS RISK_TIER, ? AS CHARGEBACK_RATE) s
			ON t.MERCHANT_ID = s.MERCHANT_ID
			WHEN MATCHED THEN UPDATE SET RISK_TIER = s.RISK_TIER, CHARGEBACK_RATE = s.CHARGEBACK_RATE
			WHEN NOT MATCHED THEN INSERT (MERCHANT_ID, RISK_TIER, CHARGEBACK_RATE) VALUES (s.MERCHANT_ID, s.RISK_TIER, s.CHARGEBACK_RATE)`,
			m.ID, m.RiskTier, m.ChargebackRate)
		if err != nil {
			return fmt.Errorf("merchant %s: %w", m.ID, err)
		}
	}
	return nil
}
