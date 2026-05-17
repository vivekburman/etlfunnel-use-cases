package main

// hot_pipeline — continuous live-order generator
//
// Keeps inserting synthetic order records into all 4 brand Postgres DBs so
// that WAL events pile up in the logical replication slots.  The downstream
// ETL pipeline reads those slots (WAL → Redis streams → Elasticsearch).
// This program has NO Redis or WAL interaction — it is purely a Postgres writer.
//
// USAGE:
//   go run ./cmd/hot_pipeline                  # defaults: 50 orders/city every 2s
//   go run ./cmd/hot_pipeline --batch 200 --interval 5s
//   go run ./cmd/hot_pipeline --brands zomato_food,blinkit   # subset of brands
//   make hot
//
// FLAGS:
//   --batch      orders inserted per city per tick (default 50)
//   --interval   pause between ticks            (default 2s)
//   --workers    parallel city goroutines per brand (default 4)
//   --brands     comma-separated brand subset; omit for all 4

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/streamcraft/zomato-etl/db_setup/internal/brands"
	"github.com/streamcraft/zomato-etl/db_setup/internal/config"
)

// ── flags ─────────────────────────────────────────────────────────────────

var (
	flagBatch    = flag.Int("batch", 50, "orders inserted per city per tick")
	flagInterval = flag.Duration("interval", 2*time.Second, "pause between insert ticks")
	flagWorkers  = flag.Int("workers", 4, "parallel city goroutines per brand")
	flagBrands   = flag.String("brands", "", "comma-separated brand names to run (default: all 4)")
)

// ── static reference data (mirrors seeder) ────────────────────────────────

var cityIDMap = map[string]int{
	"delhi": 1, "jaipur": 2, "lucknow": 3, "bengaluru": 4, "chennai": 5,
	"hyderabad": 6, "mumbai": 7, "pune": 8, "ahmedabad": 9, "kolkata": 10,
}

var brandStatuses = map[string][]string{
	"zomato_food": {"PLACED", "ACCEPTED", "PREPARING", "PICKED_UP", "DELIVERED", "CANCELLED"},
	"blinkit":     {"PLACED", "PICKING", "PACKED", "OUT_FOR_DELIVERY", "DELIVERED", "CANCELLED"},
	"hyperpure":   {"PLACED", "CONFIRMED", "DISPATCHED", "IN_TRANSIT", "RECEIVED", "REJECTED"},
	"district":    {"BOOKED", "PAYMENT_CONFIRMED", "TICKET_ISSUED", "ATTENDED", "REFUNDED", "NO_SHOW"},
}

var productNames = []string{
	"Butter Chicken", "Paneer Tikka", "Biryani", "Dal Makhani", "Naan",
	"Veg Sandwich", "Samosa", "Pav Bhaji", "Dosa", "Idli",
	"Milk 1L", "Bread", "Eggs 12pk", "Tomatoes 500g", "Onions 1kg",
	"Cooking Oil 1L", "Basmati Rice 5kg", "Wheat Flour 2kg", "Sugar 1kg", "Salt 1kg",
	"Concert Ticket", "Movie Ticket", "Comedy Show", "Sports Match", "Exhibition Pass",
}

// ── entry point ───────────────────────────────────────────────────────────

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	activeBrands := selectBrands(*flagBrands)
	log.Println("=== Hot Pipeline — live order generator started ===")
	log.Printf("  brands:   %s", brandNames(activeBrands))
	log.Printf("  batch:    %d orders/city/tick", *flagBatch)
	log.Printf("  interval: %s", *flagInterval)
	log.Printf("  workers:  %d city goroutines/brand", *flagWorkers)

	// Open one DB connection pool per brand, kept alive for the whole run.
	pools := make(map[string]*sql.DB, len(activeBrands))
	for _, b := range activeBrands {
		dsn := config.PostgresDSN(config.DBHost, b.Port, b.DBName)
		db, err := connectWithRetry(dsn, 15, b.Name)
		if err != nil {
			log.Fatalf("[%s] cannot connect: %v", b.Name, err)
		}
		db.SetMaxOpenConns(*flagWorkers * 2)
		db.SetMaxIdleConns(*flagWorkers)
		pools[b.Name] = db
		log.Printf("[%s] connected on port %d", b.Name, b.Port)
	}
	defer func() {
		for _, db := range pools {
			db.Close()
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	ticker := time.NewTicker(*flagInterval)
	defer ticker.Stop()

	tick := 0
	totalInserted := 0

	for {
		select {
		case <-stop:
			log.Printf("=== Hot Pipeline stopped — %d total records inserted across %d ticks ===",
				totalInserted, tick)
			return

		case <-ticker.C:
			tick++
			n := runTick(tick, activeBrands, pools)
			totalInserted += n
			log.Printf("tick #%d — inserted %d records  (total: %d)", tick, n, totalInserted)
		}
	}
}

// ── tick: one round of inserts across all brands × cities ─────────────────

// runTick inserts *flagBatch orders per city for every active brand, in
// parallel.  Returns the total number of orders actually inserted.
func runTick(tick int, activeBrands []brands.Brand, pools map[string]*sql.DB) int {
	var mu sync.Mutex
	var total int
	var wg sync.WaitGroup

	for _, b := range activeBrands {
		db := pools[b.Name]

		// Fan out city inserts across worker goroutines.
		cityCh := make(chan brands.City, len(brands.Cities))
		for _, c := range brands.Cities {
			cityCh <- c
		}
		close(cityCh)

		for w := 0; w < *flagWorkers; w++ {
			wg.Add(1)
			go func(brand brands.Brand, pool *sql.DB) {
				defer wg.Done()
				for city := range cityCh {
					n, err := insertLiveOrders(pool, brand, city, *flagBatch)
					if err != nil {
						log.Printf("  [%s][%s] tick #%d insert error: %v",
							brand.Name, city.Name, tick, err)
						continue
					}
					mu.Lock()
					total += n
					mu.Unlock()
				}
			}(b, db)
		}
	}

	wg.Wait()
	return total
}

// ── core insert helper ────────────────────────────────────────────────────

// insertLiveOrders inserts `count` synthetic orders (+ items, status events,
// delivery assignments) for one brand/city combination.
// It always appends to the highest-numbered existing split table; if that
// split has reached brands.SplitRowCap it creates the next one first.
func insertLiveOrders(db *sql.DB, brand brands.Brand, city brands.City, count int) (int, error) {
	// Find the current active split.
	splitNum, err := activeSplit(db, brand.Name, city.Name)
	if err != nil {
		return 0, fmt.Errorf("activeSplit: %w", err)
	}

	// Check if the current split is full; if so, create the next one.
	rowCount, err := countRows(db, fmt.Sprintf("orders_%s_%d", city.Name, splitNum))
	if err != nil {
		rowCount = 0
	}
	if rowCount >= brands.SplitRowCap {
		splitNum++
		if err := createNextSplitTables(db, brand.Name, city.Name, splitNum); err != nil {
			return 0, fmt.Errorf("createNextSplitTables: %w", err)
		}
		log.Printf("  [%s][%s] auto-created split _%d for live inserts", brand.Name, city.Name, splitNum)
	}

	statuses := brandStatuses[brand.Name]

	// ── Build order rows ──────────────────────────────────────────────────
	type orderRow struct {
		customerID  interface{}
		cityID      int
		totalAmount float64
		placedAt    string
		status      string
		brandCols   []interface{}
	}

	rows := make([]orderRow, 0, count)
	for i := 0; i < count; i++ {
		var customerID interface{}
		if rand.Float64() >= 0.03 {
			customerID = rand.Int63n(1e6) + 1
		}

		cityID := cityIDMap[city.Name]
		if rand.Float64() < 0.02 {
			cityID = 999
		}

		totalAmount := float64(rand.Intn(2000)+50) + rand.Float64()
		placedAt := randomTimestamp(2023, 2026)
		status := statuses[rand.Intn(len(statuses))]

		var brandCols []interface{}
		switch brand.Name {
		case "zomato_food":
			cuisines := []string{"North Indian", "South Indian", "Chinese", "Italian", "Fast Food"}
			meals := []string{"breakfast", "lunch", "dinner", "snack"}
			brandCols = []interface{}{
				rand.Int63n(5000) + 1,
				cuisines[rand.Intn(len(cuisines))],
				rand.Intn(60) + 10,
				meals[rand.Intn(len(meals))],
			}
		case "blinkit":
			slots := []string{"express", "scheduled", "standard"}
			brandCols = []interface{}{
				rand.Int63n(200) + 1,
				slots[rand.Intn(len(slots))],
				rand.Intn(30) + 8,
				rand.Float64() < 0.3,
			}
		case "hyperpure":
			brandCols = []interface{}{
				rand.Int63n(1000) + 1,
				fmt.Sprintf("INV-%08d", rand.Intn(1e8)),
				rand.Intn(5) + 1,
				rand.Float64() < 0.2,
			}
		case "district":
			cats := []string{"VIP", "General", "Premium", "Economy"}
			brandCols = []interface{}{
				rand.Int63n(500) + 1,
				rand.Int63n(10000) + 1,
				randomDate(2024, 2027),
				cats[rand.Intn(len(cats))],
				rand.Intn(4) + 1,
			}
		}

		rows = append(rows, orderRow{
			customerID:  customerID,
			cityID:      cityID,
			totalAmount: totalAmount,
			placedAt:    placedAt,
			status:      status,
			brandCols:   brandCols,
		})
	}

	// ── Bulk-insert orders, collect returned order_ids ────────────────────
	colNames := []string{"customer_id", "city_id", "total_amount", "placed_at", "city_split", "order_status"}
	switch brand.Name {
	case "zomato_food":
		colNames = append(colNames, "restaurant_id", "cuisine_type", "prep_time_secs", "meal_type")
	case "blinkit":
		colNames = append(colNames, "dark_store_id", "slot_type", "promise_minutes", "is_scheduled")
	case "hyperpure":
		colNames = append(colNames, "supplier_id", "invoice_number", "delivery_window_days", "bulk_order_flag")
	case "district":
		colNames = append(colNames, "venue_id", "event_id", "event_date", "seat_category", "ticket_count")
	}

	const chunkSize = 500
	var orderIDs []int64
	inserted := 0

	for start := 0; start < len(rows); start += chunkSize {
		end := start + chunkSize
		if end > len(rows) {
			end = len(rows)
		}
		chunk := rows[start:end]

		var phs []string
		var args []interface{}
		argIdx := 1

		for _, r := range chunk {
			rowArgs := []interface{}{r.customerID, r.cityID, r.totalAmount, r.placedAt, splitNum, r.status}
			rowArgs = append(rowArgs, r.brandCols...)
			var rowPHs []string
			for range rowArgs {
				rowPHs = append(rowPHs, fmt.Sprintf("$%d", argIdx))
				argIdx++
			}
			phs = append(phs, "("+strings.Join(rowPHs, ", ")+")")
			args = append(args, rowArgs...)
		}

		query := fmt.Sprintf(
			"INSERT INTO orders_%s_%d (%s) VALUES %s ON CONFLICT DO NOTHING RETURNING order_id",
			city.Name, splitNum,
			strings.Join(colNames, ", "),
			strings.Join(phs, ", "),
		)

		qrows, err := db.Query(query, args...)
		if err != nil {
			return inserted, fmt.Errorf("insert orders chunk: %w", err)
		}
		for qrows.Next() {
			var id int64
			if scanErr := qrows.Scan(&id); scanErr == nil {
				orderIDs = append(orderIDs, id)
				inserted++
			}
		}
		qrows.Close()
	}

	if len(orderIDs) == 0 {
		return 0, nil
	}

	// ── Cascade: order_items, status events, delivery assignments ─────────
	terminalStatus := statuses[len(statuses)-2]
	if err := seedOrderItems(db, city.Name, splitNum, orderIDs); err != nil {
		log.Printf("  [%s][%s] order_items warn: %v", brand.Name, city.Name, err)
	}
	if err := seedOrderStatusEvents(db, city.Name, splitNum, orderIDs, terminalStatus); err != nil {
		log.Printf("  [%s][%s] status_events warn: %v", brand.Name, city.Name, err)
	}
	if err := seedDeliveryAssignments(db, brand.Name, city.Name, splitNum, orderIDs); err != nil {
		log.Printf("  [%s][%s] delivery_assignments warn: %v", brand.Name, city.Name, err)
	}

	return inserted, nil
}

// ── cascade insert helpers ────────────────────────────────────────────────

func seedOrderItems(db *sql.DB, city string, split int, orderIDs []int64) error {
	var args []interface{}
	for _, id := range orderIDs {
		for j := 0; j < rand.Intn(4)+1; j++ {
			args = append(args,
				id,
				rand.Int63n(10000)+1,
				productNames[rand.Intn(len(productNames))],
				rand.Intn(5)+1,
				float64(rand.Intn(500)+20)+rand.Float64(),
				split,
			)
		}
	}
	return batchInsert(db,
		fmt.Sprintf("INSERT INTO order_items_%s_%d (order_id,product_id,product_name,quantity,unit_price,city_split) VALUES", city, split),
		6, args)
}

func seedOrderStatusEvents(db *sql.DB, city string, split int, orderIDs []int64, terminalStatus string) error {
	var args []interface{}
	for _, id := range orderIDs {
		args = append(args, id, terminalStatus, split)
	}
	return batchInsert(db,
		fmt.Sprintf("INSERT INTO order_status_events_%s_%d (order_id,status,city_split) VALUES", city, split),
		3, args)
}

func seedDeliveryAssignments(db *sql.DB, brandName, city string, split int, orderIDs []int64) error {
	var args []interface{}
	for _, id := range orderIDs {
		if rand.Float64() < 0.10 {
			continue
		}
		var agentID int64
		switch brandName {
		case "zomato_food", "blinkit":
			agentID = rand.Int63n(5000) + 1
		case "hyperpure":
			agentID = rand.Int63n(500) + 1
		case "district":
			agentID = rand.Int63n(100) + 1
		}
		args = append(args, id, agentID, randomTimestamp(2023, 2026), randomTimestamp(2023, 2026), split)
	}
	if len(args) == 0 {
		return nil
	}
	var cols string
	switch brandName {
	case "zomato_food", "blinkit":
		cols = "order_id,rider_id,assigned_at,delivered_at,city_split"
	case "hyperpure":
		cols = "order_id,truck_id,assigned_at,received_at,city_split"
	case "district":
		cols = "order_id,gate_id,scanned_at,attended_at,city_split"
	}
	return batchInsert(db,
		fmt.Sprintf("INSERT INTO delivery_assignments_%s_%d (%s) VALUES", city, split, cols),
		5, args)
}

// ── split management ──────────────────────────────────────────────────────

// activeSplit returns the highest-numbered split table that exists for
// orders_<city>_N by probing N = 1, 2, 3, … until the next table is missing.
func activeSplit(db *sql.DB, brandName, city string) (int, error) {
	split := 1
	for {
		var exists bool
		err := db.QueryRow(
			`SELECT EXISTS (
				SELECT 1 FROM information_schema.tables
				WHERE table_name = $1
			)`,
			fmt.Sprintf("orders_%s_%d", city, split+1),
		).Scan(&exists)
		if err != nil || !exists {
			return split, nil
		}
		split++
	}
}

func createNextSplitTables(db *sql.DB, brandName, city string, splitNum int) error {
	ddls := []string{
		ordersTableDDL(brandName, city, splitNum),
		orderItemsTableDDL(city, splitNum),
		orderStatusEventsTableDDL(city, splitNum),
		deliveryAssignmentsTableDDL(brandName, city, splitNum),
	}
	for _, ddl := range ddls {
		if _, err := db.Exec(ddl); err != nil {
			return err
		}
	}
	return nil
}

func ordersTableDDL(brandName, city string, split int) string {
	var brandCols, brandStatus string
	switch brandName {
	case "zomato_food":
		brandCols = "\n    restaurant_id BIGINT, cuisine_type VARCHAR(50), prep_time_secs INT, meal_type VARCHAR(30),"
		brandStatus = "order_status VARCHAR(30) NOT NULL DEFAULT 'PLACED',"
	case "blinkit":
		brandCols = "\n    dark_store_id BIGINT, slot_type VARCHAR(30), promise_minutes INT, is_scheduled BOOLEAN NOT NULL DEFAULT FALSE,"
		brandStatus = "order_status VARCHAR(30) NOT NULL DEFAULT 'PLACED',"
	case "hyperpure":
		brandCols = "\n    supplier_id BIGINT, invoice_number VARCHAR(50), delivery_window_days INT, bulk_order_flag BOOLEAN NOT NULL DEFAULT FALSE,"
		brandStatus = "order_status VARCHAR(30) NOT NULL DEFAULT 'PLACED',"
	case "district":
		brandCols = "\n    venue_id BIGINT, event_id BIGINT, event_date DATE, seat_category VARCHAR(30), ticket_count INT NOT NULL DEFAULT 1,"
		brandStatus = "order_status VARCHAR(30) NOT NULL DEFAULT 'BOOKED',"
	}
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS orders_%s_%d (
    order_id BIGSERIAL PRIMARY KEY, customer_id BIGINT,%s %s
    city_id INT, total_amount NUMERIC(10,2), placed_at TIMESTAMPTZ,
    city_split SMALLINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);`, city, split, brandCols, brandStatus)
}

func orderItemsTableDDL(city string, split int) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS order_items_%s_%d (
    item_id BIGSERIAL PRIMARY KEY, order_id BIGINT, product_id BIGINT,
    product_name VARCHAR(100), quantity INT, unit_price NUMERIC(10,2),
    city_split SMALLINT NOT NULL DEFAULT 1, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);`, city, split)
}

func orderStatusEventsTableDDL(city string, split int) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS order_status_events_%s_%d (
    event_id BIGSERIAL PRIMARY KEY, order_id BIGINT, status VARCHAR(30),
    event_time TIMESTAMPTZ NOT NULL DEFAULT NOW(), city_split SMALLINT NOT NULL DEFAULT 1
);`, city, split)
}

func deliveryAssignmentsTableDDL(brandName, city string, split int) string {
	var cols string
	switch brandName {
	case "zomato_food", "blinkit":
		cols = "rider_id BIGINT, assigned_at TIMESTAMPTZ, delivered_at TIMESTAMPTZ,"
	case "hyperpure":
		cols = "truck_id BIGINT, assigned_at TIMESTAMPTZ, received_at TIMESTAMPTZ,"
	case "district":
		cols = "gate_id BIGINT, scanned_at TIMESTAMPTZ, attended_at TIMESTAMPTZ,"
	}
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS delivery_assignments_%s_%d (
    assignment_id BIGSERIAL PRIMARY KEY, order_id BIGINT, %s
    city_split SMALLINT NOT NULL DEFAULT 1
);`, city, split, cols)
}

// ── generic batch insert ──────────────────────────────────────────────────

const insertChunkSize = 500

func batchInsert(db *sql.DB, prefix string, argsPerRow int, allArgs []interface{}) error {
	if len(allArgs) == 0 {
		return nil
	}
	totalRows := len(allArgs) / argsPerRow
	for start := 0; start < totalRows; start += insertChunkSize {
		end := start + insertChunkSize
		if end > totalRows {
			end = totalRows
		}
		var phs []string
		argIdx := 1
		for r := 0; r < end-start; r++ {
			var cols []string
			for p := 0; p < argsPerRow; p++ {
				cols = append(cols, fmt.Sprintf("$%d", argIdx))
				argIdx++
			}
			phs = append(phs, "("+strings.Join(cols, ", ")+")")
		}
		chunk := allArgs[start*argsPerRow : end*argsPerRow]
		if _, err := db.Exec(prefix+" "+strings.Join(phs, ", ")+" ON CONFLICT DO NOTHING", chunk...); err != nil {
			return err
		}
	}
	return nil
}

// ── utilities ─────────────────────────────────────────────────────────────

func countRows(db *sql.DB, table string) (int, error) {
	var n int
	err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&n)
	return n, err
}

func selectBrands(filter string) []brands.Brand {
	if filter == "" {
		return brands.Brands
	}
	names := make(map[string]bool)
	for _, n := range strings.Split(filter, ",") {
		names[strings.TrimSpace(n)] = true
	}
	var out []brands.Brand
	for _, b := range brands.Brands {
		if names[b.Name] {
			out = append(out, b)
		}
	}
	if len(out) == 0 {
		log.Fatalf("--brands %q matched no known brands; valid: zomato_food,blinkit,hyperpure,district", filter)
	}
	return out
}

func brandNames(bs []brands.Brand) string {
	var names []string
	for _, b := range bs {
		names = append(names, b.Name)
	}
	return strings.Join(names, ", ")
}

func connectWithRetry(dsn string, retries int, label string) (*sql.DB, error) {
	var lastErr error
	for i := 0; i < retries; i++ {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			lastErr = err
		} else if ping := db.Ping(); ping != nil {
			db.Close()
			lastErr = ping
		} else {
			return db, nil
		}
		log.Printf("  [%s] waiting for DB... (%d/%d): %v", label, i+1, retries, lastErr)
		time.Sleep(3 * time.Second)
	}
	return nil, fmt.Errorf("could not connect after %d retries: %w", retries, lastErr)
}

func randomTimestamp(fromYear, toYear int) string {
	year := fromYear + rand.Intn(toYear-fromYear)
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d+05:30",
		year, rand.Intn(12)+1, rand.Intn(28)+1,
		rand.Intn(24), rand.Intn(60), rand.Intn(60))
}

func randomDate(fromYear, toYear int) string {
	year := fromYear + rand.Intn(toYear-fromYear)
	return fmt.Sprintf("%04d-%02d-%02d", year, rand.Intn(12)+1, rand.Intn(28)+1)
}
