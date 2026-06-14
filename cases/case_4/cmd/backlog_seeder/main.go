package main

// backlog_seeder — directly seeds both AuxDB backlog tables with synthetic
// failed records so Loki widgets show non-zero values immediately, without
// needing to run the full pipeline with fault injection.
//
// Inserts N records into each table with realistic-looking failure data.
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
	"github.com/streamcraft/zepto-etl/case4/cmd/seeder/generators"
	"github.com/streamcraft/zepto-etl/case4/internal/config"
)

var flagN = flag.Int("n", 20, "Number of synthetic backlog records to insert per table")

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

	log.Printf("=== Backlog Seeder — inserting %d records per table ===", n)

	events := generators.Generate(n * 2) // generate more than we need for variety

	ingestionInserted := seedIngestionBacklog(ctx, conn, events, n)
	storageInserted := seedStorageBacklog(ctx, conn, events, n)

	log.Printf("=== Done: %d ingestion backlog, %d storage backlog records inserted ===",
		ingestionInserted, storageInserted)
}

// seedIngestionBacklog inserts synthetic Flow 1 (REST API → Kafka) failures.
func seedIngestionBacklog(ctx context.Context, conn *pgx.Conn, events []generators.OrderEvent, n int) int {
	failureStages := []string{"destination", "transform", "destination"}
	errorMessages := []string{
		"kafka: leader not available for partition 0",
		"transformer_81: json: unsupported type: chan int",
		"kafka: request timed out after 30s",
	}

	inserted := 0
	for i := 0; i < n && i < len(events); i++ {
		ev := events[i]
		stage := failureStages[i%len(failureStages)]
		errMsg := errorMessages[i%len(errorMessages)]
		runID := fmt.Sprintf("seeded-flow1-%04d", i)

		payload := fmt.Sprintf(
			`{"event_id":%q,"order_id":%q,"city":%q,"event_type":%q,"amount":%g}`,
			ev.EventID, ev.OrderID, ev.City, ev.EventType, ev.Amount,
		)

		_, execErr := conn.Exec(ctx, `
			INSERT INTO zepto_ingestion_backlog
				(order_id, event_id, failure_stage, error_message, record_payload, pipeline_run_id, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			ev.OrderID, ev.EventID, stage, errMsg, payload, runID,
			time.Now().UTC().Add(-time.Duration(n-i)*time.Minute),
		)
		if execErr != nil {
			log.Printf("  [warn] ingestion_backlog insert %d: %v", i, execErr)
			continue
		}
		inserted++
	}

	log.Printf("  zepto_ingestion_backlog: %d records inserted", inserted)
	return inserted
}

// seedStorageBacklog inserts synthetic Flow 2 (Kafka → Cassandra) failures.
func seedStorageBacklog(ctx context.Context, conn *pgx.Conn, events []generators.OrderEvent, n int) int {
	failureStages := []string{"transform", "destination", "transform"}
	errorMessages := []string{
		`transformer_88: parse created_at "INVALID_TIMESTAMP": parsing time "INVALID_TIMESTAMP" as "2006-01-02T15:04:05Z07:00"`,
		"gocql: no connections available in the pool",
		`transformer_88: created_at is missing`,
	}
	topic := "zepto.order.events"

	inserted := 0
	for i := 0; i < n && i < len(events); i++ {
		ev := events[i]
		stage := failureStages[i%len(failureStages)]
		errMsg := errorMessages[i%len(errorMessages)]
		runID := fmt.Sprintf("seeded-flow2-%04d", i)
		partition := int32(i % 3)
		offset := int64(1000 + i)

		payload := fmt.Sprintf(
			`{"event_id":%q,"order_id":%q,"city":%q,"event_type":%q,"_kafka_topic":%q,"_kafka_partition":%d,"_kafka_offset":%d}`,
			ev.EventID, ev.OrderID, ev.City, ev.EventType, topic, partition, offset,
		)

		_, execErr := conn.Exec(ctx, `
			INSERT INTO zepto_storage_backlog
				(order_id, event_id, kafka_topic, kafka_partition, kafka_offset,
				 failure_stage, error_message, record_payload, pipeline_run_id, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			ev.OrderID, ev.EventID, topic, partition, offset,
			stage, errMsg, payload, runID,
			time.Now().UTC().Add(-time.Duration(n-i)*time.Minute),
		)
		if execErr != nil {
			log.Printf("  [warn] storage_backlog insert %d: %v", i, execErr)
			continue
		}
		inserted++
	}

	log.Printf("  zepto_storage_backlog:   %d records inserted", inserted)
	return inserted
}
