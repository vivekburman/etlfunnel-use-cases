package main

// realtime — Realtime Pulse runner.
//
// Polls the GA4 Realtime API (runRealtimeReport) for all three properties
// every 60 seconds and streams rows directly into dbo.realtime_sessions.
// After each INSERT batch a TTL DELETE removes rows older than 2 hours,
// keeping the table as a rolling 2-hour active-user buffer.
//
// USAGE:
//   make realtime          # runs until Ctrl+C
//   go run ./cmd/realtime --once   # single snapshot, then exit (for testing)

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/microsoft/go-mssqldb"

	"github.com/streamcraft/myntra-etl/db_setup/internal/config"
)

var flagOnce = flag.Bool("once", false, "Run a single snapshot and exit (for testing)")

type property struct {
	ID      string
	Surface string
}

var properties = []property{
	{"properties/123456789", "web"},
	{"properties/987654321", "android"},
	{"properties/567891234", "ios"},
}

const pollInterval = 60 * time.Second
const ttlHours = 2

func main() {
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	cfg := config.Load()

	log.Printf("=== Realtime Pulse Flow — Myntra GA4 → SQL Server ===")
	log.Printf("  seeder:   %s", cfg.SeedURL)
	log.Printf("  mssql:    %s:%s/%s", cfg.MSSQLHost, cfg.MSSQLPort, cfg.MSSQLDB)
	if *flagOnce {
		log.Printf("  mode:     single snapshot (--once)")
	} else {
		log.Printf("  interval: %s", pollInterval)
	}

	db, err := sql.Open("sqlserver", cfg.MSSQLDSN())
	if err != nil {
		log.Fatalf("open mssql: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	if err := db.Ping(); err != nil {
		log.Fatalf("ping mssql: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	runSnapshot := func() {
		snapAt := time.Now().UTC()
		total := 0
		for _, prop := range properties {
			rows, err := fetchRealtime(ctx, cfg.SeedURL, prop)
			if err != nil {
				log.Printf("  [%s] fetch error: %v", prop.Surface, err)
				continue
			}
			n, err := insertRealtimeRows(ctx, db, prop, rows, snapAt)
			if err != nil {
				log.Printf("  [%s] insert error: %v", prop.Surface, err)
				continue
			}
			total += n
		}
		log.Printf("  snapshot at %s — inserted %d rows across 3 properties", snapAt.Format("15:04:05"), total)
		if err := deleteStalRows(ctx, db); err != nil {
			log.Printf("  TTL delete error: %v", err)
		}
	}

	// First snapshot immediately.
	runSnapshot()

	if *flagOnce {
		log.Println("=== Realtime snapshot complete ===")
		return
	}

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("=== Realtime pulse stopped ===")
			return
		case <-ticker.C:
			runSnapshot()
		}
	}
}

func fetchRealtime(ctx context.Context, baseURL string, prop property) ([]map[string]any, error) {
	body := map[string]any{
		"dimensions": []map[string]string{
			{"name": "city"}, {"name": "deviceCategory"},
			{"name": "pagePath"}, {"name": "eventName"},
		},
		"metrics": []map[string]string{{"name": "activeUsers"}},
	}
	bodyBytes, _ := json.Marshal(body)

	url := fmt.Sprintf("%s/v1beta/%s:runRealtimeReport", baseURL, prop.ID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer test-token")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, b)
	}

	var ga4Resp struct {
		DimensionHeaders []struct{ Name string `json:"name"` } `json:"dimensionHeaders"`
		MetricHeaders    []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"metricHeaders"`
		Rows []struct {
			DimensionValues []struct{ Value string `json:"value"` } `json:"dimensionValues"`
			MetricValues    []struct{ Value string `json:"value"` } `json:"metricValues"`
		} `json:"rows"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&ga4Resp); err != nil {
		return nil, err
	}

	rows := make([]map[string]any, 0, len(ga4Resp.Rows))
	for _, row := range ga4Resp.Rows {
		rec := map[string]any{
			"property_id": prop.ID,
			"surface":     prop.Surface,
		}
		for i, dh := range ga4Resp.DimensionHeaders {
			if i < len(row.DimensionValues) {
				rec[dh.Name] = row.DimensionValues[i].Value
			}
		}
		for i, mh := range ga4Resp.MetricHeaders {
			if i < len(row.MetricValues) {
				var n int64
				fmt.Sscanf(row.MetricValues[i].Value, "%d", &n)
				rec[mh.Name] = n
			}
		}
		rows = append(rows, rec)
	}
	return rows, nil
}

func insertRealtimeRows(ctx context.Context, db *sql.DB, prop property, rows []map[string]any, snapAt time.Time) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	cols := []string{"snapshot_at", "property_id", "surface", "active_users", "city", "device_category", "page_path", "event_name"}
	var phs []string
	var args []any
	argIdx := 1

	for _, rec := range rows {
		var rph []string
		vals := []any{
			snapAt,
			rec["property_id"],
			rec["surface"],
			nullableInt(rec["activeUsers"]),
			nullableStr(rec["city"]),
			nullableStr(rec["deviceCategory"]),
			nullableStr(rec["pagePath"]),
			nullableStr(rec["eventName"]),
		}
		for _, v := range vals {
			rph = append(rph, fmt.Sprintf("@p%d", argIdx))
			args = append(args, v)
			argIdx++
		}
		phs = append(phs, "("+strings.Join(rph, ", ")+")")
	}

	q := fmt.Sprintf("INSERT INTO dbo.realtime_sessions (%s) VALUES %s",
		strings.Join(cols, ", "), strings.Join(phs, ", "))

	if _, err := db.ExecContext(ctx, q, args...); err != nil {
		return 0, err
	}
	return len(rows), nil
}

func deleteStalRows(ctx context.Context, db *sql.DB) error {
	q := fmt.Sprintf(
		"DELETE FROM dbo.realtime_sessions WHERE snapshot_at < DATEADD(HOUR, -%d, GETUTCDATE())",
		ttlHours,
	)
	res, err := db.ExecContext(ctx, q)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		log.Printf("  TTL: deleted %d stale realtime rows", n)
	}
	return nil
}

func nullableStr(v any) any {
	if s, ok := v.(string); ok && s != "" {
		return s
	}
	return nil
}

func nullableInt(v any) any {
	switch t := v.(type) {
	case int64:
		return t
	case int:
		return int64(t)
	}
	return nil
}
