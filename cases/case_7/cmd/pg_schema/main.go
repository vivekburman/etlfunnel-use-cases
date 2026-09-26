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
	log.Println("=== Payments Postgres Schema Creator ===")

	dsn := config.PostgresDSN(config.DBHost, config.PGPort, config.PGDatabase)
	db, err := connectWithRetry(dsn, 15)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(transactionsTable); err != nil {
		log.Fatalf("create transactions: %v", err)
	}
	log.Println("[pg] transactions table ready")
	log.Println("=== Postgres schema setup complete ===")
}

const transactionsTable = `
CREATE TABLE IF NOT EXISTS transactions (
    id          BIGSERIAL    PRIMARY KEY,
    merchant_id VARCHAR(50)  NOT NULL,
    amount      NUMERIC(12,2),
    currency    VARCHAR(3),
    card_bin    VARCHAR(6),
    status      VARCHAR(20)  NOT NULL DEFAULT 'AUTHORIZED',
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions (created_at, id);
CREATE INDEX IF NOT EXISTS idx_transactions_merchant ON transactions (merchant_id);`

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
		log.Printf("  waiting for DB... (%d/%d): %v", i+1, retries, lastErr)
		time.Sleep(3 * time.Second)
	}
	return nil, fmt.Errorf("could not connect after %d retries: %w", retries, lastErr)
}
