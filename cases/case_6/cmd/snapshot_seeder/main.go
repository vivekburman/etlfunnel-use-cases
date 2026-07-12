package main

// snapshot_seeder — writes one day's Parquet inventory snapshot directory.
//
// Unlike case 4/5's HTTP mock seeders, there is no live API here — this
// tool writes real Spark/Hive-style numbered Parquet part files directly to
// disk, using the same default "part" prefix connector_62's GenerateScan
// expects via helper.ListParts.
//
// USAGE:
//   go run ./cmd/snapshot_seeder
//   go run ./cmd/snapshot_seeder -date 2026-07-11 -total 1500 -rows-per-part 1000 -fault-rate 5
//   make seed-snapshot

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/streamcraft/fc-inventory-etl/case6/cmd/snapshot_seeder/generators"
	"github.com/streamcraft/fc-inventory-etl/case6/internal/config"
)

func main() {
	cfg := config.Load()

	flagDate := flag.String("date", os.Getenv("SNAPSHOT_DATE"), "Snapshot date (YYYY-MM-DD), defaults to today")
	flagTotal := flag.Int("total", envInt("TOTAL_ROWS", 1500), "Total snapshot rows to generate")
	flagRowsPerPart := flag.Int("rows-per-part", envInt("ROWS_PER_PART", 1000), "Rows per Parquet part file")
	flagFaultRate := flag.Int("fault-rate", envInt("FAULT_RATE", 5), "Percent of rows replaced by fault rows")
	flag.Parse()

	snapshotDate := *flagDate
	if snapshotDate == "" {
		snapshotDate = time.Now().UTC().Format("2006-01-02")
	}

	log.SetFlags(log.Ltime | log.Lmicroseconds)

	directory := filepath.Join(cfg.FCSnapshotDir, fmt.Sprintf("dt=%s", snapshotDate))
	if err := os.MkdirAll(directory, 0o755); err != nil {
		log.Fatalf("create directory %s: %v", directory, err)
	}

	rows := generators.GenerateMixed(*flagTotal, *flagFaultRate, snapshotDate)

	partCount := 0
	for start := 0; start < len(rows); start += *flagRowsPerPart {
		end := start + *flagRowsPerPart
		if end > len(rows) {
			end = len(rows)
		}
		path := filepath.Join(directory, fmt.Sprintf("part-%05d.parquet", partCount))
		if err := parquet.WriteFile(path, rows[start:end]); err != nil {
			log.Fatalf("write %s: %v", path, err)
		}
		log.Printf("  wrote %s (%d rows)", path, end-start)
		partCount++
	}

	log.Printf("=== Snapshot Seeder complete: %d rows across %d parts in %s ===", len(rows), partCount, directory)
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
		return def
	}
	return n
}
