package main

// hot_seeder — continuous product record generator for pf_catalog (Postgres).
//
// Queries the current max product_id on startup to find the offset, then
// inserts a fresh batch of new products every tick so Flow 32
// (Postgres → Kafka) has a steady stream of incremental rows to process.
//
// USAGE:
//   go run ./cmd/hot_seeder
//   go run ./cmd/hot_seeder --batch 20 --interval 5s
//   make seed-hot
//
// FLAGS:
//   --batch      products inserted per tick (default 20)
//   --interval   pause between ticks       (default 5s)
//   --dsn        Postgres connection string

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	flagDSN      = flag.String("dsn", "postgresql://etl_user:etl_pass@localhost:5433/pf_catalog?sslmode=disable", "Postgres source DB connection string")
	flagBatch    = flag.Int("batch", 20, "products inserted per tick")
	flagInterval = flag.Duration("interval", 5*time.Second, "pause between ticks")
)

var (
	categories = []string{"sofa", "bed", "dining-table", "wardrobe", "bookshelf", "coffee-table", "chair", "desk"}
	materials  = []string{"teak wood", "sheesham wood", "fabric", "leather", "metal", "bamboo", "MDF"}
	adjectives = []string{"Modern", "Classic", "Elegant", "Rustic", "Minimalist", "Royal", "Contemporary", "Traditional"}
	nouns      = []string{"Sofa", "Sectional", "Daybed", "Ottoman", "Chaise Lounge", "Loveseat", "Recliner", "Bench"}
)

func main() {
	flag.Parse()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := pgx.Connect(ctx, *flagDSN)
	if err != nil {
		log.Fatalf("connect to Postgres: %v", err)
	}
	defer conn.Close(ctx)

	offset, err := currentMaxOffset(ctx, conn)
	if err != nil {
		log.Fatalf("read max offset: %v", err)
	}

	log.Println("=== Pepperfry Hot Seeder — continuous product generator ===")
	log.Printf("  dsn:      %s", *flagDSN)
	log.Printf("  batch:    %d products/tick", *flagBatch)
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
			log.Printf("=== Hot Seeder stopped — %d products inserted across %d ticks ===",
				totalInserted, tick)
			return
		case <-ticker.C:
			tick++
			n, err := insertBatch(ctx, conn, rng, &offset, *flagBatch)
			if err != nil {
				log.Printf("tick #%d insert error: %v", tick, err)
				continue
			}
			totalInserted += n
			log.Printf("tick #%d — inserted %d products (total: %d, next: PF-%06d)",
				tick, n, totalInserted, offset+1)
		}
	}
}

func currentMaxOffset(ctx context.Context, conn *pgx.Conn) (int, error) {
	var max int
	err := conn.QueryRow(ctx,
		`SELECT COALESCE(MAX(CAST(SUBSTRING(product_id FROM 4) AS INTEGER)), 99999)
		 FROM pf_catalog.products`,
	).Scan(&max)
	return max, err
}

func insertBatch(ctx context.Context, conn *pgx.Conn, rng *rand.Rand, offset *int, n int) (int, error) {
	batch := &pgx.Batch{}
	now := time.Now().UTC()

	for i := 0; i < n; i++ {
		*offset++
		productID := fmt.Sprintf("PF-%06d", *offset)
		adj := adjectives[rng.Intn(len(adjectives))]
		noun := nouns[rng.Intn(len(nouns))]
		category := categories[rng.Intn(len(categories))]
		material := materials[rng.Intn(len(materials))]
		title := fmt.Sprintf("%s %s", adj, noun)
		description := fmt.Sprintf("%s %s crafted from premium %s. Ideal for living rooms and modern homes.",
			adj, noun, material)
		price := 4999.0 + float64(rng.Intn(50000))
		dimensions := fmt.Sprintf(`{"width_cm": %d, "height_cm": %d, "depth_cm": %d}`,
			60+rng.Intn(140), 60+rng.Intn(80), 40+rng.Intn(80))

		batch.Queue(`
			INSERT INTO pf_catalog.products
				(product_id, title, description, category, price, material, dimensions, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
			ON CONFLICT (product_id) DO UPDATE SET
				title       = EXCLUDED.title,
				description = EXCLUDED.description,
				updated_at  = EXCLUDED.updated_at`,
			productID, title, description, category, price, material, dimensions, now)
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
	return inserted, nil
}
