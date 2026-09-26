package main

import (
	"database/sql"
	"flag"
	"log"
	"math/rand"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/streamcraft/settleright-etl/db_setup/internal/config"
	"github.com/streamcraft/settleright-etl/db_setup/internal/merchants"
)

var (
	total     = flag.Int("total", 1000, "number of transaction rows to insert")
	faultRate = flag.Int("fault-rate", 5, "percent of rows seeded with a deliberately invalid amount/currency/merchant_id, to exercise transformer_1's backlog routing")
)

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	log.Println("=== Payments Transaction Seeder ===")
	log.Printf("total=%d fault-rate=%d%%", *total, *faultRate)

	dsn := config.PostgresDSN(config.DBHost, config.PGPort, config.PGDatabase)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("begin: %v", err)
	}
	stmt, err := tx.Prepare(`
		INSERT INTO transactions (merchant_id, amount, currency, card_bin, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)`)
	if err != nil {
		log.Fatalf("prepare: %v", err)
	}
	defer stmt.Close()

	now := time.Now().UTC()
	faulted := 0
	for i := 0; i < *total; i++ {
		m := merchants.All[rand.Intn(len(merchants.All))]
		amount := 50 + rand.Float64()*4950
		currency := merchants.Currencies[rand.Intn(len(merchants.Currencies))]
		merchantID := m.ID
		createdAt := now.Add(-time.Duration(rand.Intn(30*24*60)) * time.Minute)

		if rand.Intn(100) < *faultRate {
			faulted++
			switch rand.Intn(3) {
			case 0:
				amount = -amount // transformer_1 rejects amount <= 0
			case 1:
				currency = "" // transformer_1 rejects missing currency
			case 2:
				merchantID = "" // transformer_1 rejects missing merchant_id
			}
		}

		cardBIN := merchants.CardBINs[rand.Intn(len(merchants.CardBINs))]
		status := merchants.Statuses[rand.Intn(len(merchants.Statuses))]

		if _, err := stmt.Exec(merchantID, amount, currency, cardBIN, status, createdAt); err != nil {
			log.Fatalf("insert row %d: %v", i, err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatalf("commit: %v", err)
	}

	log.Printf("inserted %d rows (%d deliberately faulted)", *total, faulted)
	log.Println("=== Seeding complete ===")
}
