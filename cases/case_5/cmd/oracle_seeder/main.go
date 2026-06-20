package main

// oracle_seeder — continuous ERP product attribute generator for Oracle XE.
//
// Queries the current max PRODUCT_ID on startup to find the offset, then
// inserts a fresh batch of new PRODUCT_ATTRS rows every tick so Flow 33
// (Oracle → Kafka) has a steady stream of incremental rows to process.
//
// USAGE:
//   go run ./cmd/oracle_seeder
//   go run ./cmd/oracle_seeder --batch 10 --interval 5s
//   make oracle-seed-hot
//
// FLAGS:
//   --batch      rows inserted per tick  (default 10)
//   --interval   pause between ticks    (default 5s)
//   --host, --port, --service, --user, --password

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	go_ora "github.com/sijms/go-ora/v2"
)

var (
	flagHost     = flag.String("host", "localhost", "Oracle host")
	flagPort     = flag.Int("port", 1521, "Oracle port")
	flagService  = flag.String("service", "XEPDB1", "Oracle service name")
	flagUser     = flag.String("user", "etl_user", "Oracle username")
	flagPassword = flag.String("password", "etl_pass", "Oracle password")
	flagBatch    = flag.Int("batch", 10, "rows inserted per tick")
	flagInterval = flag.Duration("interval", 5*time.Second, "pause between ticks")
)

var (
	categories  = []string{"sofa", "bed", "dining-table", "wardrobe", "bookshelf", "coffee-table", "chair", "desk"}
	suppliers   = []string{"SUP-001", "SUP-002", "SUP-003", "SUP-004", "SUP-005"}
	compliances = []string{"BIS:IS1", "BIS:IS2", "FSC-certified", "ISO9001", "RoHS"}
)

func main() {
	flag.Parse()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	connStr := go_ora.BuildUrl(*flagHost, *flagPort, *flagService, *flagUser, *flagPassword, nil)
	db, err := sql.Open("oracle", connStr)
	if err != nil {
		log.Fatalf("open oracle: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("ping oracle: %v", err)
	}

	offset, err := currentMaxOffset(db)
	if err != nil {
		log.Fatalf("read max offset: %v", err)
	}

	log.Println("=== Oracle Hot Seeder — continuous ERP product generator ===")
	log.Printf("  target:   %s@%s:%d/%s", *flagUser, *flagHost, *flagPort, *flagService)
	log.Printf("  batch:    %d rows/tick", *flagBatch)
	log.Printf("  interval: %s", *flagInterval)
	log.Printf("  offset:   starting from PF-%06d", offset+1)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	ticker := time.NewTicker(*flagInterval)
	defer ticker.Stop()

	tick := 0
	totalInserted := 0

	for {
		select {
		case <-stop:
			log.Printf("=== Oracle Hot Seeder stopped — %d rows inserted across %d ticks ===",
				totalInserted, tick)
			return
		case <-ticker.C:
			tick++
			n, err := insertBatch(db, rng, &offset, *flagBatch)
			if err != nil {
				log.Printf("tick #%d insert error: %v", tick, err)
				continue
			}
			totalInserted += n
			log.Printf("tick #%d — inserted %d rows (total: %d, next: PF-%06d)",
				tick, n, totalInserted, offset+1)
		}
	}
}

func currentMaxOffset(db *sql.DB) (int, error) {
	var max int
	err := db.QueryRow(
		`SELECT NVL(MAX(TO_NUMBER(SUBSTR(PRODUCT_ID, 4))), 99999) FROM PRODUCT_ATTRS`,
	).Scan(&max)
	return max, err
}

func insertBatch(db *sql.DB, rng *rand.Rand, offset *int, n int) (int, error) {
	inserted := 0
	for i := 0; i < n; i++ {
		*offset++
		productID := fmt.Sprintf("PF-%06d", *offset)
		skuCode := fmt.Sprintf("SKU-%s-%04d", suppliers[rng.Intn(len(suppliers))], rng.Intn(9999))
		supplierID := suppliers[rng.Intn(len(suppliers))]
		weightKg := 5.0 + rng.Float64()*95.0
		category := categories[rng.Intn(len(categories))]
		compliance := compliances[rng.Intn(len(compliances))]

		_, err := db.Exec(`
			MERGE INTO PRODUCT_ATTRS dst
			USING (SELECT :1 AS PRODUCT_ID FROM DUAL) src
			ON (dst.PRODUCT_ID = src.PRODUCT_ID)
			WHEN MATCHED THEN UPDATE SET
				SKU_CODE    = :2,
				SUPPLIER_ID = :3,
				WEIGHT_KG   = :4,
				CATEGORY    = :5,
				COMPLIANCE  = :6,
				UPDATED_AT  = SYSTIMESTAMP
			WHEN NOT MATCHED THEN INSERT
				(PRODUCT_ID, SKU_CODE, SUPPLIER_ID, WEIGHT_KG, CATEGORY, COMPLIANCE)
			VALUES (:1, :2, :3, :4, :5, :6)`,
			productID, skuCode, supplierID, weightKg, category, compliance)
		if err != nil {
			return inserted, fmt.Errorf("row %d (%s): %w", i, productID, err)
		}
		inserted++
	}
	return inserted, nil
}
