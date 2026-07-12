package main

// backlog_seeder — directly seeds both AuxDB backlog tables with synthetic
// failed rows so the metrics dashboard shows non-zero values immediately,
// without needing to run the full pipeline with fault injection first.
//
// USAGE:
//   go run ./cmd/backlog_seeder
//   make seed-backlog
//   make seed-backlog N=50

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"

	movementgen "github.com/streamcraft/fc-inventory-etl/case6/cmd/movement_seeder/generators"
	snapshotgen "github.com/streamcraft/fc-inventory-etl/case6/cmd/snapshot_seeder/generators"
	"github.com/streamcraft/fc-inventory-etl/case6/internal/config"
)

var flagN = flag.Int("n", 20, "Number of synthetic backlog rows to insert per table")

func main() {
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	cfg := config.Load()
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, cfg.AuxDBDSN)
	if err != nil {
		log.Fatalf("auxdb connect: %v", err)
	}
	defer conn.Close(ctx)

	n := *flagN
	if n <= 0 {
		n = 20
	}

	log.Printf("=== Backlog Seeder — inserting %d rows per table ===", n)

	snapshotDate := time.Now().UTC().Format("2006-01-02")
	rows := snapshotgen.Generate(n*2, snapshotDate)
	movements := movementgen.Generate(n * 2)

	snapshotInserted := seedSnapshotBacklog(ctx, conn, rows, n)
	movementInserted := seedMovementBacklog(ctx, conn, movements, n)

	log.Printf("=== Done: %d snapshot backlog, %d movement backlog rows inserted ===",
		snapshotInserted, movementInserted)
}

// seedSnapshotBacklog inserts synthetic Flow 36 (Parquet → MongoDB) failures.
func seedSnapshotBacklog(ctx context.Context, conn *pgx.Conn, rows []snapshotgen.SnapshotRow, n int) int {
	failureStages := []string{"transform", "destination", "transform"}
	errorMessages := []string{
		"transformer_98: negative quantity_on_hand -1",
		"mongo: connection pool exhausted",
		"transformer_98: quantity_on_hand: unexpected type <nil>",
	}

	inserted := 0
	for i := 0; i < n && i < len(rows); i++ {
		row := rows[i]
		stage := failureStages[i%len(failureStages)]
		errMsg := errorMessages[i%len(errorMessages)]
		runID := fmt.Sprintf("seeded-flow36-%04d", i)
		filePart := i / 1000
		fileRow := i % 1000

		payload := fmt.Sprintf(
			`{"fc_id":%q,"sku_id":%q,"quantity_on_hand":%d,"snapshot_date":%q}`,
			row.FCID, row.SKUID, row.QuantityOnHand, row.SnapshotDate,
		)

		_, execErr := conn.Exec(ctx, `
			INSERT INTO fc_snapshot_backlog
				(fc_id, sku_id, file_part, file_row, failure_stage, error_message, record_payload, pipeline_run_id, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			row.FCID, row.SKUID, filePart, fileRow, stage, errMsg, payload, runID,
			time.Now().UTC().Add(-time.Duration(n-i)*time.Minute),
		)
		if execErr != nil {
			log.Printf("  [warn] fc_snapshot_backlog insert %d: %v", i, execErr)
			continue
		}
		inserted++
	}

	log.Printf("  fc_snapshot_backlog: %d rows inserted", inserted)
	return inserted
}

// seedMovementBacklog inserts synthetic Flow 37 (Avro → MongoDB) failures.
func seedMovementBacklog(ctx context.Context, conn *pgx.Conn, rows []movementgen.MovementRow, n int) int {
	failureStages := []string{"transform", "destination", "transform"}
	errorMessages := []string{
		`transformer_101: unknown movement_type "UNKNOWN"`,
		"mongo: connection pool exhausted",
		`transformer_101: unknown movement_type ""`,
	}

	inserted := 0
	for i := 0; i < n && i < len(rows); i++ {
		row := rows[i]
		stage := failureStages[i%len(failureStages)]
		errMsg := errorMessages[i%len(errorMessages)]
		runID := fmt.Sprintf("seeded-flow37-%04d", i)
		filePart := i / 500
		fileRow := i % 500

		payload := fmt.Sprintf(
			`{"movement_id":%q,"fc_id":%q,"movement_type":%q,"quantity_delta":%d}`,
			row.MovementID, row.FCID, row.MovementType, row.QuantityDelta,
		)

		_, execErr := conn.Exec(ctx, `
			INSERT INTO fc_movement_backlog
				(movement_id, fc_id, file_part, file_row, failure_stage, error_message, record_payload, pipeline_run_id, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			row.MovementID, row.FCID, filePart, fileRow, stage, errMsg, payload, runID,
			time.Now().UTC().Add(-time.Duration(n-i)*time.Minute),
		)
		if execErr != nil {
			log.Printf("  [warn] fc_movement_backlog insert %d: %v", i, execErr)
			continue
		}
		inserted++
	}

	log.Printf("  fc_movement_backlog: %d rows inserted", inserted)
	return inserted
}
