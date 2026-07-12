package main

// movement_seeder — appends new Avro part files to the flat (never
// rotating) WMS movement directory.
//
// Existing part files are never touched: this tool always finds the next
// free "part-NNNNN.avro" sequence number and appends from there, so running
// it again simulates "WMS exported another batch" — connector_64's
// GenerateScan (helper.ListParts, called fresh every invocation) picks up
// the new parts automatically on the pipeline's next run.
//
// USAGE:
//   go run ./cmd/movement_seeder
//   go run ./cmd/movement_seeder -new 2000 -rows-per-part 500 -fault-rate 5
//   make seed-movement

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"

	"github.com/hamba/avro/v2/ocf"

	"github.com/streamcraft/fc-inventory-etl/case6/cmd/movement_seeder/generators"
	"github.com/streamcraft/fc-inventory-etl/case6/internal/config"
)

const movementSchema = `{
	"type": "record",
	"name": "StockMovement",
	"fields": [
		{"name": "movement_id", "type": "string"},
		{"name": "fc_id", "type": "string"},
		{"name": "sku_id", "type": "string"},
		{"name": "movement_type", "type": "string"},
		{"name": "quantity_delta", "type": "int"},
		{"name": "reason_code", "type": "string"},
		{"name": "operator_id", "type": "string"},
		{"name": "recorded_at", "type": "string"}
	]
}`

var partPattern = regexp.MustCompile(`^part-(\d+)\.avro$`)

func main() {
	cfg := config.Load()

	flagNew := flag.Int("new", envInt("NEW_MOVEMENTS", 2000), "New movement records to append")
	flagRowsPerPart := flag.Int("rows-per-part", envInt("ROWS_PER_PART", 500), "Records per Avro part file")
	flagFaultRate := flag.Int("fault-rate", envInt("FAULT_RATE", 5), "Percent of records replaced by fault records")
	flag.Parse()

	log.SetFlags(log.Ltime | log.Lmicroseconds)

	directory := cfg.FCMovementDir
	if err := os.MkdirAll(directory, 0o755); err != nil {
		log.Fatalf("create directory %s: %v", directory, err)
	}

	nextSeq, err := nextPartSeq(directory)
	if err != nil {
		log.Fatalf("find next part sequence: %v", err)
	}

	rows := generators.GenerateMixed(*flagNew, *flagFaultRate)

	partCount := 0
	for start := 0; start < len(rows); start += *flagRowsPerPart {
		end := start + *flagRowsPerPart
		if end > len(rows) {
			end = len(rows)
		}
		path := filepath.Join(directory, fmt.Sprintf("part-%05d.avro", nextSeq+partCount))
		if err := writePart(path, rows[start:end]); err != nil {
			log.Fatalf("write %s: %v", path, err)
		}
		log.Printf("  wrote %s (%d records)", path, end-start)
		partCount++
	}

	log.Printf("=== Movement Seeder complete: %d records across %d new parts (starting at part-%05d) in %s ===",
		len(rows), partCount, nextSeq, directory)
}

func writePart(path string, rows []generators.MovementRow) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	enc, err := ocf.NewEncoder(movementSchema, f)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := enc.Encode(row); err != nil {
			return err
		}
	}
	return enc.Close()
}

// nextPartSeq mirrors the framework's helper.NextPartSeq: one past the
// highest existing "part-<n>.avro" sequence number in dir, or 0 if dir has
// no part files yet.
func nextPartSeq(dir string) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	var seqs []int
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		m := partPattern.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		seq, err := strconv.Atoi(m[1])
		if err != nil {
			continue
		}
		seqs = append(seqs, seq)
	}
	if len(seqs) == 0 {
		return 0, nil
	}
	sort.Ints(seqs)
	return seqs[len(seqs)-1] + 1, nil
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
