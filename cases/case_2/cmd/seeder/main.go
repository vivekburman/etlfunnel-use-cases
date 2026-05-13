package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/streamcraft/zomato-etl/db_setup/internal/brands"
	"github.com/streamcraft/zomato-etl/db_setup/internal/config"
)

const insertBatchSize = 1000

var (
	recordsPerCity = flag.Int("records-per-city", 500, "number of order records to insert per city")
	workers        = flag.Int("workers", 4, "parallel worker goroutines")
	splitRowCap    = flag.Int("split-row-cap", brands.SplitRowCap, "rows per table before a new split is created")
)

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

type orderRecord struct {
	id    int64
	split int
}

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	log.Println("=== Zomato Synthetic Data Seeder ===")
	log.Printf("Records per city: %d | Workers: %d | Split cap: %d",
		*recordsPerCity, *workers, *splitRowCap)

	sharedOrderIDs := generateSharedOrderIDPool(int(float64(*recordsPerCity) * 0.05))

	var wg sync.WaitGroup
	results := make(chan string, len(brands.Brands))

	for _, brand := range brands.Brands {
		wg.Add(1)
		go func(b brands.Brand) {
			defer wg.Done()
			if err := seedBrand(b, sharedOrderIDs); err != nil {
				results <- fmt.Sprintf("[%s] FAILED: %v", b.Name, err)
			} else {
				results <- fmt.Sprintf("[%s] seeding complete", b.Name)
			}
		}(brand)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for msg := range results {
		log.Println(msg)
	}
	log.Println("=== Seeding complete ===")
}

func generateSharedOrderIDPool(n int) []int64 {
	pool := make([]int64, n)
	for i := range pool {
		pool[i] = rand.Int63n(1e9) + 1
	}
	return pool
}

func seedBrand(brand brands.Brand, sharedOrderIDs []int64) error {
	dsn := config.PostgresDSN(config.DBHost, brand.Port, brand.DBName)
	db, err := connectWithRetry(dsn, 15, brand.Name)
	if err != nil {
		return err
	}
	defer db.Close()

	db.SetMaxOpenConns(*workers * 2)
	db.SetMaxIdleConns(*workers)

	taskCh := make(chan brands.City, len(brands.Cities))
	for _, city := range brands.Cities {
		taskCh <- city
	}
	close(taskCh)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var ok, failed int

	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for city := range taskCh {
				if err := seedCity(db, brand, city, sharedOrderIDs); err != nil {
					mu.Lock()
					failed++
					mu.Unlock()
					log.Printf("[%s][%s] SKIPPED: %v", brand.Name, city.Name, err)
				} else {
					mu.Lock()
					ok++
					mu.Unlock()
				}
			}
		}()
	}

	wg.Wait()
	log.Printf("[%s] cities seeded: %d ok, %d failed", brand.Name, ok, failed)
	return nil
}

func seedCity(db *sql.DB, brand brands.Brand, city brands.City, sharedOrderIDs []int64) error {
	splitNum := 1
	splitRowCount, err := countRows(db, fmt.Sprintf("orders_%s_%d", city.Name, splitNum))
	if err != nil {
		splitRowCount = 0
	}

	toInsert := *recordsPerCity - splitRowCount
	if toInsert <= 0 {
		log.Printf("  [%s][%s] already at target, skipping", brand.Name, city.Name)
		return nil
	}

	statuses := brandStatuses[brand.Name]
	terminalStatus := statuses[len(statuses)-2] // e.g. DELIVERED, RECEIVED, ATTENDED

	var orderIDs []int64

	type orderRow struct {
		customerID  interface{}
		cityID      int
		totalAmount float64
		placedAt    string
		split       int
		brandCols   []interface{}
		status      string
	}

	var pending []orderRow

	flushOrders := func() error {
		if len(pending) == 0 {
			return nil
		}

		var colNames []string
		var placeholders []string
		var args []interface{}

		colNames = append(colNames, "customer_id", "city_id", "total_amount", "placed_at", "city_split", "order_status")

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

		argIdx := 1
		for _, row := range pending {
			var phs []string
			args = append(args, row.customerID, row.cityID, row.totalAmount, row.placedAt, row.split, row.status)
			for range colNames[:6] {
				phs = append(phs, fmt.Sprintf("$%d", argIdx))
				argIdx++
			}
			for _, bc := range row.brandCols {
				args = append(args, bc)
				phs = append(phs, fmt.Sprintf("$%d", argIdx))
				argIdx++
			}
			placeholders = append(placeholders, "("+strings.Join(phs, ", ")+")")
		}

		query := fmt.Sprintf("INSERT INTO orders_%s_%d (%s) VALUES %s ON CONFLICT DO NOTHING",
			city.Name, splitNum,
			strings.Join(colNames, ", "),
			strings.Join(placeholders, ", "),
		)

		rows, err := db.Query(query+" RETURNING order_id", args...)
		if err != nil {
			_, execErr := db.Exec(strings.TrimSuffix(query, " ON CONFLICT DO NOTHING")+" ON CONFLICT DO NOTHING", args...)
			if execErr != nil {
				return execErr
			}
			splitRowCount += len(pending)
			pending = pending[:0]
			return nil
		}
		defer rows.Close()
		for rows.Next() {
			var id int64
			if scanErr := rows.Scan(&id); scanErr == nil {
				orderIDs = append(orderIDs, id)
			}
		}
		splitRowCount += len(pending)
		pending = pending[:0]
		return nil
	}

	for i := 0; i < toInsert; i++ {
		if splitRowCount >= *splitRowCap {
			if err := flushOrders(); err != nil {
				return fmt.Errorf("flush before split: %w", err)
			}
			splitNum++
			splitRowCount = 0
			if err := createNextSplitTables(db, brand.Name, city.Name, splitNum); err != nil {
				return fmt.Errorf("create split %d: %w", splitNum, err)
			}
			log.Printf("  [%s][%s] created split _%d", brand.Name, city.Name, splitNum)
		}

		var customerID interface{}
		switch {
		case rand.Float64() < 0.03:
			customerID = nil
		default:
			customerID = rand.Int63n(1e6) + 1
		}

		cityID := cityIDMap[city.Name]
		if rand.Float64() < 0.02 {
			cityID = 999
		}

		totalAmount := float64(rand.Intn(2000)+50) + rand.Float64()

		placedAt := randomTimestamp(2023, 2025)
		if rand.Float64() < 0.01 {
			// impossible timestamp: created_at > delivered_at injection marker
			placedAt = randomTimestamp(2025, 2027)
		}

		status := statuses[rand.Intn(len(statuses))]

		var brandCols []interface{}
		switch brand.Name {
		case "zomato_food":
			cuisines := []string{"North Indian", "South Indian", "Chinese", "Italian", "Fast Food"}
			meals := []string{"breakfast", "lunch", "dinner", "snack"}
			brandCols = []interface{}{rand.Int63n(5000) + 1, cuisines[rand.Intn(len(cuisines))], rand.Intn(60) + 10, meals[rand.Intn(len(meals))]}
		case "blinkit":
			slots := []string{"express", "scheduled", "standard"}
			promMins := rand.Intn(30) + 8
			if rand.Float64() < 0.02 {
				promMins = 0
			}
			brandCols = []interface{}{rand.Int63n(200) + 1, slots[rand.Intn(len(slots))], promMins, rand.Float64() < 0.3}
		case "hyperpure":
			invoice := fmt.Sprintf("INV-%08d", rand.Intn(1e8))
			bulk := rand.Float64() < 0.2
			brandCols = []interface{}{rand.Int63n(1000) + 1, invoice, rand.Intn(5) + 1, bulk}
		case "district":
			eventDate := randomDate(2024, 2026)
			if rand.Float64() < 0.04 {
				eventDate = randomDate(2020, 2024)
			}
			cats := []string{"VIP", "General", "Premium", "Economy"}
			brandCols = []interface{}{rand.Int63n(500) + 1, rand.Int63n(10000) + 1, eventDate, cats[rand.Intn(len(cats))], rand.Intn(4) + 1}
		}

		var orderIDOverride interface{}
		if rand.Float64() < 0.05 && len(sharedOrderIDs) > 0 {
			_ = sharedOrderIDs[rand.Intn(len(sharedOrderIDs))]
		}
		_ = orderIDOverride

		pending = append(pending, orderRow{
			customerID:  customerID,
			cityID:      cityID,
			totalAmount: totalAmount,
			placedAt:    placedAt,
			split:       splitNum,
			brandCols:   brandCols,
			status:      status,
		})

		if len(pending) >= insertBatchSize {
			if err := flushOrders(); err != nil {
				return fmt.Errorf("flush orders: %w", err)
			}
		}
	}
	if err := flushOrders(); err != nil {
		return fmt.Errorf("final flush orders: %w", err)
	}

	if len(orderIDs) == 0 {
		// fallback: query inserted order IDs
		rows, err := db.Query(fmt.Sprintf("SELECT order_id FROM orders_%s_%d LIMIT $1", city.Name, splitNum), *recordsPerCity)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var id int64
				if scanErr := rows.Scan(&id); scanErr == nil {
					orderIDs = append(orderIDs, id)
				}
			}
		}
	}

	if err := seedOrderItems(db, brand.Name, city.Name, splitNum, orderIDs); err != nil {
		log.Printf("  [%s][%s] order_items warning: %v", brand.Name, city.Name, err)
	}
	if err := seedOrderStatusEvents(db, brand.Name, city.Name, splitNum, orderIDs, terminalStatus); err != nil {
		log.Printf("  [%s][%s] status_events warning: %v", brand.Name, city.Name, err)
	}
	if err := seedDeliveryAssignments(db, brand.Name, city.Name, splitNum, orderIDs); err != nil {
		log.Printf("  [%s][%s] delivery_assignments warning: %v", brand.Name, city.Name, err)
	}

	return nil
}

func seedOrderItems(db *sql.DB, brandName, city string, split int, orderIDs []int64) error {
	if len(orderIDs) == 0 {
		return nil
	}

	var rows []string
	var args []interface{}
	argIdx := 1

	for _, orderID := range orderIDs {
		itemCount := rand.Intn(5) + 1
		for j := 0; j < itemCount; j++ {
			productID := rand.Int63n(10000) + 1
			productName := productNames[rand.Intn(len(productNames))]
			qty := rand.Intn(5) + 1
			price := float64(rand.Intn(500)+20) + rand.Float64()

			rows = append(rows, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, %d)",
				argIdx, argIdx+1, argIdx+2, argIdx+3, argIdx+4, split))
			args = append(args, orderID, productID, productName, qty, price)
			argIdx += 5
		}
	}

	return batchInsertRaw(db,
		fmt.Sprintf("INSERT INTO order_items_%s_%d (order_id, product_id, product_name, quantity, unit_price, city_split) VALUES ", city, split),
		rows, args)
}

func seedOrderStatusEvents(db *sql.DB, brandName, city string, split int, orderIDs []int64, terminalStatus string) error {
	if len(orderIDs) == 0 {
		return nil
	}

	var rows []string
	var args []interface{}
	argIdx := 1

	for _, orderID := range orderIDs {
		rows = append(rows, fmt.Sprintf("($%d, $%d, NOW(), %d)", argIdx, argIdx+1, split))
		args = append(args, orderID, terminalStatus)
		argIdx += 2
	}

	return batchInsertRaw(db,
		fmt.Sprintf("INSERT INTO order_status_events_%s_%d (order_id, status, event_time, city_split) VALUES ", city, split),
		rows, args)
}

func seedDeliveryAssignments(db *sql.DB, brandName, city string, split int, orderIDs []int64) error {
	if len(orderIDs) == 0 {
		return nil
	}

	var rows []string
	var args []interface{}
	argIdx := 1

	for _, orderID := range orderIDs {
		if rand.Float64() < 0.10 {
			continue
		}

		assignedAt := randomTimestamp(2023, 2025)
		completedAt := randomTimestamp(2023, 2025)

		switch brandName {
		case "zomato_food", "blinkit":
			rows = append(rows, fmt.Sprintf("($%d, $%d, $%d, $%d, %d)", argIdx, argIdx+1, argIdx+2, argIdx+3, split))
			args = append(args, orderID, rand.Int63n(5000)+1, assignedAt, completedAt)
			argIdx += 4
		case "hyperpure":
			rows = append(rows, fmt.Sprintf("($%d, $%d, $%d, $%d, %d)", argIdx, argIdx+1, argIdx+2, argIdx+3, split))
			args = append(args, orderID, rand.Int63n(500)+1, assignedAt, completedAt)
			argIdx += 4
		case "district":
			rows = append(rows, fmt.Sprintf("($%d, $%d, $%d, $%d, %d)", argIdx, argIdx+1, argIdx+2, argIdx+3, split))
			args = append(args, orderID, rand.Int63n(100)+1, assignedAt, completedAt)
			argIdx += 4
		}
	}

	if len(rows) == 0 {
		return nil
	}

	var colsDef string
	switch brandName {
	case "zomato_food", "blinkit":
		colsDef = "order_id, rider_id, assigned_at, delivered_at, city_split"
	case "hyperpure":
		colsDef = "order_id, truck_id, assigned_at, received_at, city_split"
	case "district":
		colsDef = "order_id, gate_id, scanned_at, attended_at, city_split"
	}

	return batchInsertRaw(db,
		fmt.Sprintf("INSERT INTO delivery_assignments_%s_%d (%s) VALUES ", city, split, colsDef),
		rows, args)
}

func createNextSplitTables(db *sql.DB, brandName, city string, splitNum int) error {
	ddls := []string{
		buildOrdersTableDDL(brandName, city, splitNum),
		buildOrderItemsTableDDL(city, splitNum),
		buildOrderStatusEventsTableDDL(city, splitNum),
		buildDeliveryAssignmentsTableDDL(brandName, city, splitNum),
	}
	for _, ddl := range ddls {
		if _, err := db.Exec(ddl); err != nil {
			return err
		}
	}
	return nil
}

func buildOrdersTableDDL(brandName, city string, split int) string {
	var brandCols string
	var brandStatus string
	switch brandName {
	case "zomato_food":
		brandCols = `
    restaurant_id   BIGINT,
    cuisine_type    VARCHAR(50),
    prep_time_secs  INT,
    meal_type       VARCHAR(30),`
		brandStatus = `order_status    VARCHAR(30)  NOT NULL DEFAULT 'PLACED',`
	case "blinkit":
		brandCols = `
    dark_store_id   BIGINT,
    slot_type       VARCHAR(30),
    promise_minutes INT,
    is_scheduled    BOOLEAN      NOT NULL DEFAULT FALSE,`
		brandStatus = `order_status    VARCHAR(30)  NOT NULL DEFAULT 'PLACED',`
	case "hyperpure":
		brandCols = `
    supplier_id          BIGINT,
    invoice_number       VARCHAR(50),
    delivery_window_days INT,
    bulk_order_flag      BOOLEAN      NOT NULL DEFAULT FALSE,`
		brandStatus = `order_status    VARCHAR(30)  NOT NULL DEFAULT 'PLACED',`
	case "district":
		brandCols = `
    venue_id        BIGINT,
    event_id        BIGINT,
    event_date      DATE,
    seat_category   VARCHAR(30),
    ticket_count    INT          NOT NULL DEFAULT 1,`
		brandStatus = `order_status    VARCHAR(30)  NOT NULL DEFAULT 'BOOKED',`
	}

	tbl := fmt.Sprintf("orders_%s_%d", city, split)
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    order_id        BIGSERIAL    PRIMARY KEY,
    customer_id     BIGINT,%s
    %s
    city_id         INT,
    total_amount    NUMERIC(10,2),
    placed_at       TIMESTAMPTZ,
    city_split      SMALLINT     NOT NULL DEFAULT 1,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);`, tbl, brandCols, brandStatus)
}

func buildOrderItemsTableDDL(city string, split int) string {
	tbl := fmt.Sprintf("order_items_%s_%d", city, split)
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    item_id         BIGSERIAL    PRIMARY KEY,
    order_id        BIGINT,
    product_id      BIGINT,
    product_name    VARCHAR(100),
    quantity        INT,
    unit_price      NUMERIC(10,2),
    city_split      SMALLINT     NOT NULL DEFAULT 1,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);`, tbl)
}

func buildOrderStatusEventsTableDDL(city string, split int) string {
	tbl := fmt.Sprintf("order_status_events_%s_%d", city, split)
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    event_id        BIGSERIAL    PRIMARY KEY,
    order_id        BIGINT,
    status          VARCHAR(30),
    event_time      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    city_split      SMALLINT     NOT NULL DEFAULT 1
);`, tbl)
}

func buildDeliveryAssignmentsTableDDL(brandName, city string, split int) string {
	var cols string
	switch brandName {
	case "zomato_food", "blinkit":
		cols = `
    rider_id        BIGINT,
    assigned_at     TIMESTAMPTZ,
    delivered_at    TIMESTAMPTZ,`
	case "hyperpure":
		cols = `
    truck_id        BIGINT,
    assigned_at     TIMESTAMPTZ,
    received_at     TIMESTAMPTZ,`
	case "district":
		cols = `
    gate_id         BIGINT,
    scanned_at      TIMESTAMPTZ,
    attended_at     TIMESTAMPTZ,`
	}
	tbl := fmt.Sprintf("delivery_assignments_%s_%d", city, split)
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    assignment_id   BIGSERIAL    PRIMARY KEY,
    order_id        BIGINT,%s
    city_split      SMALLINT     NOT NULL DEFAULT 1
);`, tbl, cols)
}

func batchInsertRaw(db *sql.DB, prefix string, rows []string, args []interface{}) error {
	if len(rows) == 0 {
		return nil
	}
	argsPerRow := len(args) / len(rows)
	for start := 0; start < len(rows); start += insertBatchSize {
		end := start + insertBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		chunkArgs := args[start*argsPerRow : end*argsPerRow]
		query := prefix + strings.Join(rows[start:end], ", ") + " ON CONFLICT DO NOTHING"
		if _, err := db.Exec(query, chunkArgs...); err != nil {
			return err
		}
	}
	return nil
}

func countRows(db *sql.DB, table string) (int, error) {
	var n int
	err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&n)
	return n, err
}

func connectWithRetry(dsn string, retries int, label string) (*sql.DB, error) {
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
		log.Printf("  [%s] waiting for DB... (%d/%d): %v", label, i+1, retries, lastErr)
		time.Sleep(3 * time.Second)
	}
	return nil, fmt.Errorf("could not connect after %d retries: %w", retries, lastErr)
}

func randomDate(fromYear, toYear int) string {
	year := fromYear + rand.Intn(toYear-fromYear)
	month := rand.Intn(12) + 1
	day := rand.Intn(28) + 1
	return fmt.Sprintf("%04d-%02d-%02d", year, month, day)
}

func randomTimestamp(fromYear, toYear int) string {
	year := fromYear + rand.Intn(toYear-fromYear)
	month := rand.Intn(12) + 1
	day := rand.Intn(28) + 1
	hour := rand.Intn(24)
	min := rand.Intn(60)
	sec := rand.Intn(60)
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d+05:30", year, month, day, hour, min, sec)
}
