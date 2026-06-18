package main

// Oracle PRODUCT_ATTRS seeder — inserts synthetic ERP product rows into
// the Oracle XE source database so Flow 33 has data to process.
//
// USAGE:
//   go run ./cmd/oracle_seeder -host localhost -port 1521 -service XEPDB1 \
//     -user etl_user -password etl_pass -n 50
//   make oracle-seed

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"

	go_ora "github.com/sijms/go-ora/v2"
)

var (
	flagHost     = flag.String("host", "localhost", "Oracle host")
	flagPort     = flag.Int("port", 1521, "Oracle port")
	flagService  = flag.String("service", "XEPDB1", "Oracle service name")
	flagUser     = flag.String("user", "etl_user", "Oracle username")
	flagPassword = flag.String("password", "etl_pass", "Oracle password")
	flagN        = flag.Int("n", 50, "number of rows to insert")
)

var (
	categories  = []string{"sofa", "bed", "dining-table", "wardrobe", "bookshelf", "coffee-table", "chair", "desk"}
	suppliers   = []string{"SUP-001", "SUP-002", "SUP-003", "SUP-004", "SUP-005"}
	compliances = []string{"BIS:IS1", "BIS:IS2", "FSC-certified", "ISO9001", "RoHS"}
)

func main() {
	flag.Parse()
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

	log.Println("=== Oracle PRODUCT_ATTRS Seeder ===")
	log.Printf("  target:   %s@%s:%d/%s", *flagUser, *flagHost, *flagPort, *flagService)
	log.Printf("  rows:     %d", *flagN)

	inserted, err := insertRows(db, *flagN)
	if err != nil {
		log.Fatalf("insert: %v", err)
	}
	log.Printf("=== Seeder complete — %d rows inserted ===", inserted)
}

func insertRows(db *sql.DB, n int) (int, error) {
	rng := rand.New(rand.NewSource(99))
	inserted := 0

	for i := 0; i < n; i++ {
		productID := fmt.Sprintf("PF-%06d", 100000+i)
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
	log.Printf("  ✓ %d rows upserted into PRODUCT_ATTRS", inserted)
	return inserted, nil
}
