package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/streamcraft/zomato-etl/db_setup/internal/brands"
	"github.com/streamcraft/zomato-etl/db_setup/internal/config"
)

func main() {
	log.Println("=== Postgres Schema Creator ===")

	for _, brand := range brands.Brands {
		dsn := config.PostgresDSN(config.DBHost, brand.Port, brand.DBName)
		log.Printf("[%s] connecting on port %d ...", brand.Name, brand.Port)

		db, err := connectWithRetry(dsn, 15)
		if err != nil {
			log.Fatalf("[%s] failed to connect: %v", brand.Name, err)
		}

		n, err := createInitialSplits(db, brand.Name)
		db.Close()
		if err != nil {
			log.Fatalf("[%s] schema creation failed: %v", brand.Name, err)
		}
		log.Printf("[%s] %d tables created (split _1 only; seeder creates _2+ dynamically)", brand.Name, n)
	}

	log.Println("=== Postgres schema setup complete ===")
}

func createInitialSplits(db *sql.DB, brandName string) (int, error) {
	count := 0
	for _, city := range brands.Cities {
		ddls := buildDDLs(brandName, city.Name)
		for _, ddl := range ddls {
			if _, err := db.Exec(ddl); err != nil {
				return count, fmt.Errorf("brand=%s city=%s: %w", brandName, city.Name, err)
			}
			count++
		}
	}
	return count, nil
}

func buildDDLs(brandName, city string) []string {
	return []string{
		ordersTable(brandName, city),
		orderItemsTable(city),
		orderStatusEventsTable(city),
		deliveryAssignmentsTable(brandName, city),
	}
}

func ordersTable(brandName, city string) string {
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

	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS orders_%s_1 (
    order_id        BIGSERIAL    PRIMARY KEY,
    customer_id     BIGINT,%s
    %s
    city_id         INT,
    total_amount    NUMERIC(10,2),
    placed_at       TIMESTAMPTZ,
    city_split      SMALLINT     NOT NULL DEFAULT 1,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_orders_%s_1_customer ON orders_%s_1 (customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_%s_1_placed ON orders_%s_1 (placed_at);
CREATE INDEX IF NOT EXISTS idx_orders_%s_1_status ON orders_%s_1 (order_status);`,
		city, brandCols, brandStatus,
		city, city,
		city, city,
		city, city,
	)
}

func orderItemsTable(city string) string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS order_items_%s_1 (
    item_id         BIGSERIAL    PRIMARY KEY,
    order_id        BIGINT,
    product_id      BIGINT,
    product_name    VARCHAR(100),
    quantity        INT,
    unit_price      NUMERIC(10,2),
    city_split      SMALLINT     NOT NULL DEFAULT 1,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_order_items_%s_1_order ON order_items_%s_1 (order_id);`,
		city,
		city, city,
	)
}

func orderStatusEventsTable(city string) string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS order_status_events_%s_1 (
    event_id        BIGSERIAL    PRIMARY KEY,
    order_id        BIGINT,
    status          VARCHAR(30),
    event_time      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    city_split      SMALLINT     NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_ose_%s_1_order ON order_status_events_%s_1 (order_id);`,
		city,
		city, city,
	)
}

func deliveryAssignmentsTable(brandName, city string) string {
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

	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS delivery_assignments_%s_1 (
    assignment_id   BIGSERIAL    PRIMARY KEY,
    order_id        BIGINT,%s
    city_split      SMALLINT     NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_da_%s_1_order ON delivery_assignments_%s_1 (order_id);`,
		city, cols,
		city, city,
	)
}

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
