package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/streamcraft/zomato-etl/db_setup/internal/brands"
	"github.com/streamcraft/zomato-etl/db_setup/internal/config"
)

var flagAuxDB = flag.String("auxdb", config.PostgresDSN(config.DBHost, config.AuxDBPort, config.AuxDB), "AuxDB DSN")

func main() {
	flag.Parse()
	log.Println("=== WAL Enabler ===")

	auxDB, err := connectWithRetry(*flagAuxDB, 15, "auxdb")
	if err != nil {
		log.Fatalf("failed to connect to AuxDB: %v", err)
	}
	defer auxDB.Close()

	var wg sync.WaitGroup
	errs := make(chan error, len(brands.Brands))

	for _, brand := range brands.Brands {
		wg.Add(1)
		go func(b brands.Brand) {
			defer wg.Done()
			if err := enableWAL(b, auxDB); err != nil {
				errs <- fmt.Errorf("[%s] %w", b.Name, err)
			}
		}(brand)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		log.Printf("ERROR: %v", err)
	}

	log.Println("=== WAL Enabler complete ===")
}

func enableWAL(brand brands.Brand, auxDB *sql.DB) error {
	dsn := config.PostgresDSN(config.DBHost, brand.Port, brand.DBName)
	db, err := connectWithRetry(dsn, 15, brand.Name)
	if err != nil {
		return err
	}
	defer db.Close()

	// Check if replication slot already exists.
	var slotExists bool
	err = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, brand.ReplicationSlot).Scan(&slotExists)
	if err != nil {
		return fmt.Errorf("check replication slot: %w", err)
	}

	if !slotExists {
		_, err = db.Exec(`SELECT pg_create_logical_replication_slot($1, 'pgoutput')`, brand.ReplicationSlot)
		if err != nil {
			return fmt.Errorf("create replication slot: %w", err)
		}
		log.Printf("[%s] replication slot %q created", brand.Name, brand.ReplicationSlot)
	} else {
		log.Printf("[%s] replication slot %q already exists", brand.Name, brand.ReplicationSlot)
	}

	// Check if publication already exists.
	pubName := brand.Name + "_pub"
	var pubExists bool
	err = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)`, pubName).Scan(&pubExists)
	if err != nil {
		return fmt.Errorf("check publication: %w", err)
	}

	if !pubExists {
		_, err = db.Exec(fmt.Sprintf(`CREATE PUBLICATION %s FOR ALL TABLES`, pubName))
		if err != nil {
			return fmt.Errorf("create publication: %w", err)
		}
		log.Printf("[%s] publication %q created", brand.Name, pubName)
	} else {
		log.Printf("[%s] publication %q already exists", brand.Name, pubName)
	}

	// Read current LSN.
	var lsn string
	if err := db.QueryRow(`SELECT pg_current_wal_lsn()`).Scan(&lsn); err != nil {
		return fmt.Errorf("read LSN: %w", err)
	}
	log.Printf("[%s] current WAL LSN: %s", brand.Name, lsn)

	// Write LSN to AuxDB.
	_, err = auxDB.Exec(`
INSERT INTO wal_positions (sub_brand, lsn, publication_name, slot_name)
VALUES ($1, $2, $3, $4)
ON CONFLICT (sub_brand) DO UPDATE SET lsn = $2, recorded_at = NOW()`,
		brand.Name, lsn, pubName, brand.ReplicationSlot,
	)
	if err != nil {
		return fmt.Errorf("upsert wal_positions: %w", err)
	}
	log.Printf("[%s] LSN recorded in AuxDB", brand.Name)
	return nil
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
