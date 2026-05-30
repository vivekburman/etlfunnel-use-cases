package main

// GA4 Mock Seeder — HTTP server that mimics the Google Analytics 4 Data API.
//
// Exposes two endpoints:
//
//   POST /v1beta/{property}:runReport         — paginated session data
//   POST /v1beta/{property}:runRealtimeReport — live active-user snapshot
//
// All responses are deterministic for (property, date, offset) so repeated
// requests return identical rows — safe for pagination and idempotent re-runs.
//
// USAGE:
//   go run ./cmd/seeder                            # defaults
//   DATE_FROM=2024-01-01 DATE_TO=2025-12-31 ROWS_PER_DAY=15000 go run ./cmd/seeder
//   make seed

import (
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/streamcraft/myntra-etl/db_setup/cmd/seeder/handlers"
	"github.com/streamcraft/myntra-etl/db_setup/cmd/seeder/state"
)

// surfaceMap maps GA4 property IDs → surface label.
// Must match the property IDs defined in orchestrator_1 and orchestrator_2.
var surfaceMap = map[string]string{
	"properties/123456789": "web",
	"properties/987654321": "android",
	"properties/567891234": "ios",
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	port := getEnv("SEEDER_PORT", "9090")
	rowsPerDay := parseInt(getEnv("ROWS_PER_DAY", "15000"), 15000)
	dateFrom := getEnv("DATE_FROM", "2024-01-01")
	dateTo := getEnv("DATE_TO", "2025-12-31")

	log.Printf("=== GA4 Mock Seeder ===")
	log.Printf("  port:         :%s", port)
	log.Printf("  rows/day:     %d", rowsPerDay)
	log.Printf("  date range:   %s → %s", dateFrom, dateTo)
	log.Printf("  properties:   %v", propertyList())

	quota := state.NewQuotaStore()

	mux := http.NewServeMux()

	// Route both endpoints through the same mux using path prefix matching.
	mux.HandleFunc("/v1beta/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case isRunReportPath(r.URL.Path):
			handlers.NewReportHandler(quota, rowsPerDay, surfaceMap)(w, r)
		case isRealtimePath(r.URL.Path):
			handlers.NewRealtimeHandler(quota, surfaceMap)(w, r)
		default:
			http.NotFound(w, r)
		}
	})

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

func isRunReportPath(path string) bool {
	// /v1beta/properties/NNN:runReport
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == ':' {
			return path[i:] == ":runReport"
		}
	}
	return false
}

func isRealtimePath(path string) bool {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == ':' {
			return path[i:] == ":runRealtimeReport"
		}
	}
	return false
}

func propertyList() []string {
	keys := make([]string, 0, len(surfaceMap))
	for k := range surfaceMap {
		keys = append(keys, k)
	}
	return keys
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
