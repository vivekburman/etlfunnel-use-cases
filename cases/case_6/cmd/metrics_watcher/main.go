package main

// metrics_watcher — live dashboard for the FC Inventory Ops pipeline.
//
// Polls AuxDB and MongoDB every INTERVAL seconds and shows:
//   - Flow 36 snapshot progress (per-directory checkpoints)
//   - Flow 37 movement progress (single, ever-advancing checkpoint)
//   - MongoDB collection counts
//   - Backlog counts + recent entries for both flows
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
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/streamcraft/fc-inventory-etl/case6/internal/config"
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

	mongoClient, err := mongo.Connect(options.Client().ApplyURI(cfg.MongoURI))
	if err != nil {
		log.Fatalf("mongo connect: %v", err)
	}
	defer mongoClient.Disconnect(ctx)
	mongoDB := mongoClient.Database(cfg.MongoDatabase)

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	render(ctx, auxDB, mongoDB)
	for {
		select {
		case <-ctx.Done():
			fmt.Println("\nStopped.")
			return
		case <-ticker.C:
			render(ctx, auxDB, mongoDB)
		}
	}
}

func render(ctx context.Context, aux *pgx.Conn, mongoDB *mongo.Database) {
	clearScreen()
	now := time.Now().Format("15:04:05")
	fmt.Printf("=== FC Inventory Ops Pipeline — Live Metrics [%s] ===\n\n", now)

	printSnapshotProgress(ctx, aux)
	printMovementProgress(ctx, aux)
	printMongoCounts(ctx, mongoDB)
	printBacklogCounts(ctx, aux)

	fmt.Printf("\n(refreshes every few seconds — Ctrl+C to exit)\n")
}

func printSnapshotProgress(ctx context.Context, db *pgx.Conn) {
	fmt.Println("── Flow 36: Parquet → MongoDB (snapshot progress) ───────────────────────────")

	rows, err := db.Query(ctx, `
		SELECT directory, last_part, last_row, status, updated_at::text
		FROM fc_snapshot_progress
		ORDER BY directory DESC
		LIMIT 5`)
	if err != nil {
		fmt.Printf("  [error: %v]\n\n", err)
		return
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		var directory, status, updatedAt string
		var lastPart, lastRow int
		if err := rows.Scan(&directory, &lastPart, &lastRow, &status, &updatedAt); err == nil {
			fmt.Printf("  directory=%-45s part=%-4d row=%-6d status=%-11s updated=%s\n",
				directory, lastPart, lastRow, status, updatedAt[:19])
			found = true
		}
	}
	if !found {
		fmt.Println("  (no snapshot checkpoint yet — Flow 36 has not committed any progress)")
	}
	fmt.Println()
}

func printMovementProgress(ctx context.Context, db *pgx.Conn) {
	fmt.Println("── Flow 37: Avro → MongoDB (movement progress) ──────────────────────────────")

	rows, err := db.Query(ctx, `
		SELECT directory, last_part, last_row, updated_at::text
		FROM fc_movement_progress`)
	if err != nil {
		fmt.Printf("  [error: %v]\n\n", err)
		return
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		var directory, updatedAt string
		var lastPart, lastRow int
		if err := rows.Scan(&directory, &lastPart, &lastRow, &updatedAt); err == nil {
			fmt.Printf("  directory=%-35s part=%-4d row=%-6d updated=%s\n",
				directory, lastPart, lastRow, updatedAt[:19])
			found = true
		}
	}
	if !found {
		fmt.Println("  (no movement checkpoint yet — Flow 37 has not committed any progress)")
	}
	fmt.Println()
}

func printMongoCounts(ctx context.Context, db *mongo.Database) {
	fmt.Println("── MongoDB collection counts ─────────────────────────────────────────────────")

	snapshotCount, err := db.Collection("inventory_snapshots").CountDocuments(ctx, bson.D{})
	if err != nil {
		fmt.Printf("  fc_inventory.inventory_snapshots : [error: %v]\n", err)
	} else {
		fmt.Printf("  fc_inventory.inventory_snapshots : %d\n", snapshotCount)
	}

	movementCount, err := db.Collection("stock_movements").CountDocuments(ctx, bson.D{})
	if err != nil {
		fmt.Printf("  fc_inventory.stock_movements     : [error: %v]\n", err)
	} else {
		fmt.Printf("  fc_inventory.stock_movements     : %d\n", movementCount)
	}
	fmt.Println()
}

func printBacklogCounts(ctx context.Context, db *pgx.Conn) {
	fmt.Println("── Backlogs ───────────────────────────────────────────────────────────────────")

	var snapshotBacklogCount, movementBacklogCount int64
	_ = db.QueryRow(ctx, "SELECT COUNT(*) FROM fc_snapshot_backlog").Scan(&snapshotBacklogCount)
	_ = db.QueryRow(ctx, "SELECT COUNT(*) FROM fc_movement_backlog").Scan(&movementBacklogCount)

	fmt.Printf("  fc_snapshot_backlog (Flow 36 failed rows)    : %d\n", snapshotBacklogCount)
	fmt.Printf("  fc_movement_backlog (Flow 37 failed records) : %d\n", movementBacklogCount)

	rows, err := db.Query(ctx, `
		SELECT fc_id, sku_id, failure_stage, error_message, created_at::text
		FROM fc_snapshot_backlog
		ORDER BY created_at DESC
		LIMIT 5`)
	if err == nil {
		defer rows.Close()
		printedHeader := false
		for rows.Next() {
			var fcID, skuID, stage, errMsg, createdAt string
			if err := rows.Scan(&fcID, &skuID, &stage, &errMsg, &createdAt); err == nil {
				if !printedHeader {
					fmt.Println("\n  Recent snapshot backlog entries:")
					printedHeader = true
				}
				fmt.Printf("    %-12s %-14s %-11s %s  %s\n", fcID, skuID, stage, createdAt[:19], errMsg)
			}
		}
	}
	fmt.Println()
}

func clearScreen() {
	fmt.Print("\033[H\033[2J")
}
