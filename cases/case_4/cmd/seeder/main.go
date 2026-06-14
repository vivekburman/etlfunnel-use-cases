package main

// Zepto Order Events Mock Seeder — HTTP server that mimics the Zepto internal
// Order Events REST API.
//
// Exposes one endpoint:
//
//   GET /api/v2/order-events?cursor=<seq>&limit=<n>
//
// Cursor-based pagination: each response includes "next_cursor" and "has_more".
// When has_more=false the cursor feed is exhausted — Flow 1 will terminate.
//
// USAGE:
//   go run ./cmd/seeder                         # defaults: 2000 events on :11334
//   TOTAL_EVENTS=5000 SEEDER_PORT=11334 go run ./cmd/seeder
//   make seed

import (
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/streamcraft/zepto-etl/case4/cmd/seeder/generators"
	"github.com/streamcraft/zepto-etl/case4/cmd/seeder/handlers"
)

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	port := getEnv("SEEDER_PORT", "11334")
	totalEvents := parseInt(getEnv("TOTAL_EVENTS", "2000"), 2000)
	faultRate := parseInt(getEnv("FAULT_RATE", "0"), 0)

	log.Printf("=== Zepto Order Events Mock Seeder ===")
	log.Printf("  port:         :%s", port)
	log.Printf("  total events: %d", totalEvents)
	log.Printf("  fault rate:   %d%% (drop / storage-backlog / ingestion-backlog cycling)", faultRate)
	log.Printf("  endpoint:     GET /api/v2/order-events?cursor=<seq>&limit=<n>")
	log.Printf("  auth:         X-Internal-Token header (any non-empty value)")

	pool := generators.GenerateMixed(totalEvents, faultRate)
	faultCount := 0
	for _, ev := range pool {
		if ev.City == "" || ev.CreatedAt == "INVALID_TIMESTAMP" {
			faultCount++
		}
	}
	log.Printf("  generated %d events (%d fault records) across %d cities", len(pool), faultCount, 7)

	mux := http.NewServeMux()
	mux.Handle("/api/v2/order-events", handlers.NewOrderEventsHandler(pool))
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	addr := ":" + port
	log.Printf(">>> Listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("listen: %v", err)
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func parseInt(s string, def int) int {
	if n, err := strconv.Atoi(s); err == nil && n > 0 {
		return n
	}
	return def
}
