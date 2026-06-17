package main

// Pepperfry Catalog Seeder — inserts synthetic product records into the Postgres
// source database (pf_catalog) so Flow 32 has data to process.
//
// Creates the products table if it does not exist, then inserts N product rows
// spread across several furniture/home-décor categories.
//
// Oracle seeding is not automated here; see the README / Makefile for Oracle XE
// Docker instructions if you want to exercise Flow 33. The Oracle table DDL is:
//
//   CREATE TABLE PRODUCT_ATTRS (
//     PRODUCT_ID  VARCHAR2(50) NOT NULL PRIMARY KEY,
//     SKU_CODE    VARCHAR2(100),
//     SUPPLIER_ID VARCHAR2(50),
//     WEIGHT_KG   NUMBER(8,3),
//     CATEGORY    VARCHAR2(100),
//     COMPLIANCE  VARCHAR2(500),
//     UPDATED_AT  TIMESTAMP DEFAULT SYSTIMESTAMP
//   );
//
// USAGE:
//   go run ./cmd/seeder -dsn "host=localhost port=5433 dbname=pf_catalog user=etl_user password=etl_pass sslmode=disable"
//   go run ./cmd/seeder -n 500
//   make seed

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	flagDSN = flag.String("dsn", "postgresql://etl_user:etl_pass@localhost:5433/pf_catalog?sslmode=disable", "Postgres source DB connection string")
	flagN   = flag.Int("n", 200, "number of product rows to insert")
)

func main() {
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, *flagDSN)
	if err != nil {
		log.Fatalf("connect to Postgres: %v", err)
	}
	defer conn.Close(ctx)

	log.Println("=== Pepperfry Catalog Seeder ===")
	log.Printf("  target:   %s", *flagDSN)
	log.Printf("  products: %d", *flagN)

	if err := ensureTable(ctx, conn); err != nil {
		log.Fatalf("create table: %v", err)
	}

	inserted, err := insertProducts(ctx, conn, *flagN)
	if err != nil {
		log.Fatalf("insert products: %v", err)
	}

	log.Printf("=== Seeder complete — %d products inserted ===", inserted)
	log.Printf("Now start Flow 34 (pid=34), then Flow 35 (pid=35), then Flow 32 (pid=32) in streamcraftexecution.")
}

func ensureTable(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS products (
			product_id   VARCHAR(50) PRIMARY KEY,
			title        TEXT        NOT NULL,
			description  TEXT,
			category     VARCHAR(100),
			price        NUMERIC(10,2),
			material     VARCHAR(100),
			dimensions   JSONB,
			updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`)
	if err != nil {
		return fmt.Errorf("create products table: %w", err)
	}
	log.Println("  ✓ products table ready")
	return nil
}

var (
	categories = []string{"sofa", "bed", "dining-table", "wardrobe", "bookshelf", "coffee-table", "chair", "desk"}
	materials  = []string{"teak wood", "sheesham wood", "fabric", "leather", "metal", "bamboo", "MDF"}
	adjectives = []string{"Modern", "Classic", "Elegant", "Rustic", "Minimalist", "Royal", "Contemporary", "Traditional"}
	nouns      = []string{"Sofa", "Sectional", "Daybed", "Ottoman", "Chaise Lounge", "Loveseat", "Recliner", "Bench"}
)

func insertProducts(ctx context.Context, conn *pgx.Conn, n int) (int, error) {
	rng := rand.New(rand.NewSource(42))
	batch := &pgx.Batch{}
	now := time.Now().UTC()

	for i := 0; i < n; i++ {
		productID := fmt.Sprintf("PF-%06d", 100000+i)
		adj := adjectives[rng.Intn(len(adjectives))]
		noun := nouns[rng.Intn(len(nouns))]
		category := categories[rng.Intn(len(categories))]
		material := materials[rng.Intn(len(materials))]
		title := fmt.Sprintf("%s %s", adj, noun)
		description := fmt.Sprintf("%s %s crafted from premium %s. Ideal for living rooms and modern homes.", adj, noun, material)
		price := 4999.0 + float64(rng.Intn(50000))
		dimensions := fmt.Sprintf(`{"width_cm": %d, "height_cm": %d, "depth_cm": %d}`,
			60+rng.Intn(140), 60+rng.Intn(80), 40+rng.Intn(80))

		// Spread updated_at across last 30 days so incremental queries work sensibly
		updatedAt := now.Add(-time.Duration(rng.Intn(30*24)) * time.Hour)

		batch.Queue(`
			INSERT INTO products (product_id, title, description, category, price, material, dimensions, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
			ON CONFLICT (product_id) DO UPDATE SET
				title       = EXCLUDED.title,
				description = EXCLUDED.description,
				updated_at  = EXCLUDED.updated_at`,
			productID, title, description, category, price, material, dimensions, updatedAt)
	}

	results := conn.SendBatch(ctx, batch)
	defer results.Close()

	inserted := 0
	for i := 0; i < n; i++ {
		if _, err := results.Exec(); err != nil {
			return inserted, fmt.Errorf("batch exec row %d: %w", i, err)
		}
		inserted++
	}

	log.Printf("  ✓ %d products upserted into pf_catalog.products", inserted)
	return inserted, nil
}
