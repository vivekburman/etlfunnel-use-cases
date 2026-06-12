package main

// metrics_watcher — live dashboard for the Myntra GA4 Analytics ETL.
//
// Polls SQL Server (row counts, pipeline log) and AuxDB (checkpoints)
// every INTERVAL seconds and redraws the terminal.
//
// USAGE:
//   go run ./cmd/metrics_watcher
//   make watch
//   make watch INTERVAL=10s

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/microsoft/go-mssqldb"

	"github.com/streamcraft/myntra-etl/db_setup/internal/config"
)

func main() {
	interval := flag.Duration("interval", 5*time.Second, "Poll interval")
	flag.Parse()

	cfg := config.Load()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	mssqlDB, err := sql.Open("sqlserver", cfg.MSSQLDSN())
	if err != nil {
		log.Fatalf("mssql open: %v", err)
	}
	defer mssqlDB.Close()

	auxDB, err := pgx.Connect(ctx, cfg.AuxDBDSN)
	if err != nil {
		log.Fatalf("auxdb connect: %v", err)
	}
	defer auxDB.Close(ctx)

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	render(ctx, mssqlDB, auxDB)
	for {
		select {
		case <-ctx.Done():
			fmt.Println("\nStopped.")
			return
		case <-ticker.C:
			render(ctx, mssqlDB, auxDB)
		}
	}
}

func render(ctx context.Context, mssql *sql.DB, aux *pgx.Conn) {
	clearScreen()
	now := time.Now().Format("15:04:05")
	fmt.Printf("=== Myntra GA4 Analytics ETL — Live Metrics [%s] ===\n\n", now)

	printMSSQLStats(ctx, mssql)
	printCheckpoints(ctx, aux)
	printRecentRuns(ctx, aux)

	fmt.Printf("\n(refreshes every few seconds — Ctrl+C to exit)\n")
}

// ── SQL Server ───────────────────────────────────────────────────────────────

type sessionStat struct {
	PropertyID string
	Surface    string
	Rows       int64
	MinDate    string
	MaxDate    string
}

func printMSSQLStats(ctx context.Context, db *sql.DB) {
	fmt.Println("── SQL Server: ga4_sessions ─────────────────────────────────────────────")

	rows, err := db.QueryContext(ctx, `
		SELECT property_id, surface, COUNT(*) AS rows,
		       MIN(report_date), MAX(report_date)
		FROM dbo.ga4_sessions
		GROUP BY property_id, surface
		ORDER BY property_id, surface`)
	if err != nil {
		fmt.Printf("  [error: %v]\n\n", err)
		return
	}
	defer rows.Close()

	var stats []sessionStat
	for rows.Next() {
		var s sessionStat
		if err := rows.Scan(&s.PropertyID, &s.Surface, &s.Rows, &s.MinDate, &s.MaxDate); err == nil {
			stats = append(stats, s)
		}
	}

	if len(stats) == 0 {
		fmt.Println("  (no data yet)")
	} else {
		fmt.Printf("  %-30s %-10s %12s  %-12s %-12s\n", "property_id", "surface", "rows", "min_date", "max_date")
		for _, s := range stats {
			fmt.Printf("  %-30s %-10s %12s  %-12s %-12s\n",
				s.PropertyID, s.Surface, formatInt(s.Rows), s.MinDate, s.MaxDate)
		}
	}

	var realtimeCount, runLogCount int64
	db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dbo.realtime_sessions`).Scan(&realtimeCount)
	db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dbo.pipeline_run_log`).Scan(&runLogCount)
	fmt.Printf("\n  realtime_sessions : %s\n", formatInt(realtimeCount))
	fmt.Printf("  pipeline_run_log  : %s\n\n", formatInt(runLogCount))
}

// ── AuxDB checkpoints ────────────────────────────────────────────────────────

type checkpoint struct {
	PropertyID     string
	Surface        string
	LastMergedDate string
	RunID          string
	UpdatedAt      string
}

func printCheckpoints(ctx context.Context, db *pgx.Conn) {
	fmt.Println("── AuxDB: pipeline_checkpoints ──────────────────────────────────────────")

	rows, err := db.Query(ctx, `
		SELECT property_id, surface,
		       COALESCE(last_merged_date::text, '—'),
		       COALESCE(pipeline_run_id, '—'),
		       updated_at::text
		FROM pipeline_checkpoints
		ORDER BY property_id, surface`)
	if err != nil {
		fmt.Printf("  [error: %v]\n\n", err)
		return
	}
	defer rows.Close()

	var cps []checkpoint
	for rows.Next() {
		var c checkpoint
		if err := rows.Scan(&c.PropertyID, &c.Surface, &c.LastMergedDate, &c.RunID, &c.UpdatedAt); err == nil {
			cps = append(cps, c)
		}
	}

	if len(cps) == 0 {
		fmt.Println("  (no checkpoints yet — run make backfill)")
	} else {
		fmt.Printf("  %-30s %-10s %-12s  %-26s  %s\n", "property_id", "surface", "last_merged", "run_id", "updated_at")
		for _, c := range cps {
			fmt.Printf("  %-30s %-10s %-12s  %-26s  %s\n",
				c.PropertyID, c.Surface, c.LastMergedDate, truncate(c.RunID, 26), c.UpdatedAt[:19])
		}
	}
	fmt.Println()
}

// ── Recent pipeline runs ─────────────────────────────────────────────────────

type runEntry struct {
	RunID        string
	PipelineName string
	PropertyID   string
	Surface      string
	RowsFetched  int64
	RowsMerged   int64
	Status       string
	StartedAt    string
}

func printRecentRuns(ctx context.Context, db *pgx.Conn) {
	fmt.Println("── AuxDB: recent pipeline runs (last 10) ────────────────────────────────")

	rows, err := db.Query(ctx, `
		SELECT run_id, pipeline_name,
		       COALESCE(property_id, '—'), COALESCE(surface, '—'),
		       rows_fetched, rows_merged, status,
		       started_at::text
		FROM pipeline_run_log
		ORDER BY started_at DESC
		LIMIT 10`)
	if err != nil {
		fmt.Printf("  [error: %v]\n\n", err)
		return
	}
	defer rows.Close()

	var runs []runEntry
	for rows.Next() {
		var r runEntry
		if err := rows.Scan(&r.RunID, &r.PipelineName, &r.PropertyID, &r.Surface,
			&r.RowsFetched, &r.RowsMerged, &r.Status, &r.StartedAt); err == nil {
			runs = append(runs, r)
		}
	}

	if len(runs) == 0 {
		fmt.Println("  (no runs yet)")
		return
	}

	fmt.Printf("  %-28s %-18s %-10s %-8s %10s %10s %-8s  %s\n",
		"run_id", "pipeline", "property", "surface", "fetched", "merged", "status", "started_at")
	for _, r := range runs {
		fmt.Printf("  %-28s %-18s %-10s %-8s %10s %10s %-8s  %s\n",
			truncate(r.RunID, 28), truncate(r.PipelineName, 18),
			truncate(r.PropertyID, 10), r.Surface,
			formatInt(r.RowsFetched), formatInt(r.RowsMerged),
			r.Status, r.StartedAt[:19])
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

func clearScreen() {
	fmt.Print("\033[H\033[2J")
}

func formatInt(n int64) string {
	if n == 0 {
		return "0"
	}
	s := fmt.Sprintf("%d", n)
	result := make([]byte, 0, len(s)+(len(s)-1)/3)
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	return string(result)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
