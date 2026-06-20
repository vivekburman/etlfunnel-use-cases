package main

// metrics_watcher — live dashboard for the Zepto Order Events pipeline.
//
// Polls AuxDB every INTERVAL seconds and shows:
//   - Flow 1 cursor checkpoint (last published cursor position)
//   - Flow 1 ingestion backlog count
//   - Flow 2 Kafka offset checkpoints per partition
//   - Flow 2 storage backlog count
//
// USAGE:
//   go run ./cmd/metrics_watcher
//   make watch
//   make watch INTERVAL=10s

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/streamcraft/zepto-etl/case4/internal/config"
)

func main() {
	interval := flag.Duration("interval", 5*time.Second, "Poll interval")
	flag.Parse()

	cfg := config.Load()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	auxDB, err := pgx.Connect(ctx, cfg.AuxDBDSN)
	if err != nil {
		log.Fatalf("auxdb connect: %v", err)
	}
	defer auxDB.Close(ctx)

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	render(ctx, auxDB)
	for {
		select {
		case <-ctx.Done():
			fmt.Println("\nStopped.")
			return
		case <-ticker.C:
			render(ctx, auxDB)
		}
	}
}

func render(ctx context.Context, aux *pgx.Conn) {
	clearScreen()
	now := time.Now().Format("15:04:05")
	fmt.Printf("=== Zepto Order Events Pipeline — Live Metrics [%s] ===\n\n", now)

	printIngestionStatus(ctx, aux)
	printStorageStatus(ctx, aux)
	printBacklogCounts(ctx, aux)

	fmt.Printf("\n(refreshes every few seconds — Ctrl+C to exit)\n")
}

func printIngestionStatus(ctx context.Context, db *pgx.Conn) {
	fmt.Println("── Flow 1: REST API → Kafka (cursor checkpoints) ────────────────────────")

	rows, err := db.Query(ctx, `
		SELECT pipeline, last_cursor, updated_at::text
		FROM zepto_ingestion_cursors
		ORDER BY pipeline`)
	if err != nil {
		fmt.Printf("  [error: %v]\n\n", err)
		return
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		var pipeline, cursor, updatedAt string
		if err := rows.Scan(&pipeline, &cursor, &updatedAt); err == nil {
			fmt.Printf("  pipeline=%-20s  cursor=%-25s  updated=%s\n",
				pipeline, cursor, updatedAt[:19])
			found = true
		}
	}
	if !found {
		fmt.Println("  (no cursor checkpoint yet — Flow 1 has not committed any cursor)")
	}
	fmt.Println()
}

func printStorageStatus(ctx context.Context, db *pgx.Conn) {
	fmt.Println("── Flow 2: Kafka → Cassandra (offset checkpoints) ───────────────────────")

	rows, err := db.Query(ctx, `
		SELECT topic, partition, last_offset, updated_at::text
		FROM zepto_storage_offsets
		ORDER BY topic, partition`)
	if err != nil {
		fmt.Printf("  [error: %v]\n\n", err)
		return
	}
	defer rows.Close()

	found := false
	fmt.Printf("  %-30s  %9s  %12s  %s\n", "topic", "partition", "last_offset", "updated_at")
	for rows.Next() {
		var topic string
		var partition int32
		var lastOffset int64
		var updatedAt string
		if err := rows.Scan(&topic, &partition, &lastOffset, &updatedAt); err == nil {
			fmt.Printf("  %-30s  %9d  %12s  %s\n",
				topic, partition, formatInt(lastOffset), updatedAt[:19])
			found = true
		}
	}
	if !found {
		fmt.Println("  (no offset checkpoint yet — Flow 2 has not committed any offsets)")
	}
	fmt.Println()
}

func printBacklogCounts(ctx context.Context, db *pgx.Conn) {
	fmt.Println("── Backlogs ──────────────────────────────────────────────────────────────")

	var ingestionCount, storageCount int64
	db.QueryRow(ctx, `SELECT COUNT(*) FROM zepto_ingestion_backlog`).Scan(&ingestionCount)
	db.QueryRow(ctx, `SELECT COUNT(*) FROM zepto_storage_backlog`).Scan(&storageCount)

	fmt.Printf("  zepto_ingestion_backlog (Flow 1 failed publishes) : %s\n", formatInt(ingestionCount))
	fmt.Printf("  zepto_storage_backlog   (Flow 2 failed writes)    : %s\n", formatInt(storageCount))

	if ingestionCount > 0 {
		fmt.Println()
		fmt.Println("  Recent ingestion backlog entries:")
		rows, err := db.Query(ctx, `
			SELECT order_id, event_id, failure_stage, error_message, created_at::text
			FROM zepto_ingestion_backlog
			ORDER BY created_at DESC LIMIT 5`)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var orderID, eventID, stage, errMsg, at string
				if err := rows.Scan(&orderID, &eventID, &stage, &errMsg, &at); err == nil {
					fmt.Printf("    %-20s  %-36s  %-15s  %s  %.60s\n",
						orderID, eventID, stage, at[:19], errMsg)
				}
			}
		}
	}
}

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
