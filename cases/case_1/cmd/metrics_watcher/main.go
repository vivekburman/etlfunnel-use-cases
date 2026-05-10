package main

// Telecom ETL — Live Metrics Watcher
//
// Connects to AuxDB and the destination DB and polls on a configurable
// interval, printing a dashboard until Ctrl+C. Every tick shows:
//   - Checkpoint progress per active shard (records moved, throughput/tick)
//   - Active pipeline count (checkpoints written in the last 30 s)
//   - Destination raw row counts per table
//   - Backlog summary by status and failure stage
//   - Backlog rate (backlog inserts vs checkpoint records processed)
//   - Write tune config snapshot
//   - Data movement summary: did anything actually move this tick?
//
// When --log is set all output goes exclusively to that file (not stdout).
// Tail the file to watch live: tail -f metrics.log
//
// Usage:
//
//	go run ./cmd/metrics_watcher \
//	  --auxdb "host=localhost port=5435 dbname=auxdb user=etl_user password=etl_pass sslmode=disable" \
//	  --destdb "host=localhost port=5434 dbname=destination_db user=etl_user password=etl_pass sslmode=disable" \
//	  --interval 5s \
//	  --log metrics.log

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
)

// ── flags ─────────────────────────────────────────────────────────────────────

var (
	flagAuxDSN   = flag.String("auxdb", "host=localhost port=5435 dbname=auxdb user=etl_user password=etl_pass sslmode=disable", "AuxDB connection string")
	flagDestDSN  = flag.String("destdb", "host=localhost port=5434 dbname=destination_db user=etl_user password=etl_pass sslmode=disable", "Destination DB connection string")
	flagInterval = flag.Duration("interval", 5*time.Second, "Poll interval (e.g. 5s, 10s, 1m)")
	flagLogFile  = flag.String("log", "", "Path to write output (file is cleared each run). When set, output goes only to the file, not stdout.")
)

// out is the writer used by all print functions — stdout, file, or both.
var out io.Writer = os.Stdout

// ── checkpoint row ────────────────────────────────────────────────────────────

type checkpointRow struct {
	Company          string
	Zone             string
	State            string
	SplitIndex       int
	LastProcessedPK  int64
	BatchID          int64
	Phase            string
	Status           string
	RecordsProcessed int64
	CheckpointTS     time.Time
}

func (c checkpointRow) key() string {
	return fmt.Sprintf("%s|%s|%s|%d", c.Company, c.Zone, c.State, c.SplitIndex)
}

// ── write tune ────────────────────────────────────────────────────────────────

type writeTune struct {
	Normal   int
	Turbo    int
	Throttle int
}

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	flag.Parse()

	if *flagLogFile != "" {
		f, err := os.OpenFile(*flagLogFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "fatal: cannot open log file %s: %v\n", *flagLogFile, err)
			os.Exit(1)
		}
		defer f.Close()
		out = f // file only — tail the file to watch live
		fmt.Fprintf(os.Stderr, "Logging to %s (tail -f to follow)\n", *flagLogFile)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	auxConn := mustConnect(ctx, *flagAuxDSN, "AuxDB")
	destConn := mustConnect(ctx, *flagDestDSN, "DestDB")
	defer auxConn.Close(context.Background())
	defer destConn.Close(context.Background())

	fmt.Fprintln(out, "Telecom ETL Metrics Watcher started.")
	fmt.Fprintf(out, "Polling every %s\n\n", *flagInterval)

	prevRecords := make(map[string]int64)
	prevDestCounts := make(map[string]int64)
	var prevBacklogTotal int64 = -1 // -1 = first tick, no delta yet
	tick := 0

	ticker := time.NewTicker(*flagInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Fprintln(out, "\nStopped.")
			return
		case <-ticker.C:
			tick++
			printDashboard(ctx, auxConn, destConn, tick, prevRecords, prevDestCounts, &prevBacklogTotal)
		}
	}
}

// ── dashboard ─────────────────────────────────────────────────────────────────

func printDashboard(ctx context.Context, aux, dest *pgx.Conn, tick int,
	prevRecords map[string]int64, prevDestCounts map[string]int64, prevBacklogTotal *int64) {

	now := time.Now().Format("2006-01-02 15:04:05")
	fmt.Fprintf(out, "\n%s\n", strings.Repeat("═", 90))
	fmt.Fprintf(out, "  TELECOM ETL METRICS  —  %s  (tick #%d)\n", now, tick)
	fmt.Fprintf(out, "%s\n", strings.Repeat("═", 90))

	checkpointDelta := printCheckpoints(ctx, aux, tick, prevRecords)
	printActivePipelines(ctx, aux)
	destDelta := printDestinationCounts(ctx, dest, prevDestCounts)
	backlogDelta := printBacklog(ctx, aux, prevBacklogTotal)
	printBacklogRate(ctx, aux)
	printWriteTune(ctx, aux)
	printMovement(tick, checkpointDelta, destDelta, backlogDelta)
}

// ── checkpoints ───────────────────────────────────────────────────────────────

// printCheckpoints returns total records-processed delta across all shards this tick.
func printCheckpoints(ctx context.Context, conn *pgx.Conn, tick int, prev map[string]int64) int64 {
	fmt.Fprintln(out, "\n  CHECKPOINT PROGRESS")
	fmt.Fprintf(out, "  %-14s %-12s %-16s %5s %12s %8s  %-14s %-12s %s\n",
		"Company", "Zone", "State", "Split", "Last PK", "Batch", "Phase", "Status", "Rows (+/tick)")
	fmt.Fprintf(out, "  %s\n", strings.Repeat("─", 112))

	rows, err := conn.Query(ctx, `
		SELECT source_company, zone, state, table_split_index,
		       last_processed_pk, batch_id, phase, status, records_processed, checkpoint_ts
		FROM pipeline_checkpoints
		ORDER BY source_company, zone, state, table_split_index`)
	if err != nil {
		fmt.Fprintf(out, "  [warn] cannot read pipeline_checkpoints: %v\n", err)
		return 0
	}
	defer rows.Close()

	var totalDelta int64
	for rows.Next() {
		var r checkpointRow
		if scanErr := rows.Scan(
			&r.Company, &r.Zone, &r.State, &r.SplitIndex,
			&r.LastProcessedPK, &r.BatchID, &r.Phase, &r.Status,
			&r.RecordsProcessed, &r.CheckpointTS,
		); scanErr != nil {
			fmt.Fprintf(out, "  [warn] scan error: %v\n", scanErr)
			continue
		}

		k := r.key()
		delta := r.RecordsProcessed - prev[k]
		if tick == 1 {
			delta = 0
		}
		prev[k] = r.RecordsProcessed
		totalDelta += delta

		deltaStr := fmt.Sprintf("+%d", delta)
		if delta <= 0 {
			deltaStr = "  idle"
		}

		fmt.Fprintf(out, "  %-14s %-12s %-16s %5d %12d %8d  %-14s %-12s %s (%s)\n",
			r.Company, r.Zone, r.State, r.SplitIndex,
			r.LastProcessedPK, r.BatchID%100000,
			r.Phase, r.Status,
			fmtInt(r.RecordsProcessed), deltaStr,
		)
	}
	if rows.Err() != nil {
		fmt.Fprintf(out, "  [warn] row error: %v\n", rows.Err())
	}
	return totalDelta
}

// ── active pipelines ──────────────────────────────────────────────────────────

func printActivePipelines(ctx context.Context, conn *pgx.Conn) {
	var active, total int
	conn.QueryRow(ctx, `SELECT COUNT(*) FROM pipeline_checkpoints`).Scan(&total)
	conn.QueryRow(ctx, `
		SELECT COUNT(*) FROM pipeline_checkpoints
		WHERE checkpoint_ts > now() - INTERVAL '30 seconds'`).Scan(&active)

	fmt.Fprintf(out, "\n  ACTIVE PIPELINES (updated in last 30s): %d / %d\n", active, total)
}

// ── destination row counts ────────────────────────────────────────────────────

var destTables = []string{
	"customers",
	"subscriptions",
	"billing_accounts",
	"sim_inventory",
	"port_history",
}

// printDestinationCounts returns total new rows landed in destination raw schema this tick.
func printDestinationCounts(ctx context.Context, conn *pgx.Conn, prev map[string]int64) int64 {
	fmt.Fprintln(out, "\n  DESTINATION ROW COUNTS (raw schema)")
	var totalDelta int64
	for _, tbl := range destTables {
		var n int64
		err := conn.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM raw.%s`, tbl)).Scan(&n)
		if err != nil {
			fmt.Fprintf(out, "  %-20s  [unavailable: %v]\n", tbl+":", err)
			continue
		}
		delta := n - prev[tbl]
		prev[tbl] = n
		totalDelta += delta
		deltaStr := ""
		if delta > 0 {
			deltaStr = fmt.Sprintf("  (+%s)", fmtInt(delta))
		}
		fmt.Fprintf(out, "  %-20s  %s%s\n", tbl+":", fmtInt(n), deltaStr)
	}
	return totalDelta
}

// ── backlog summary ───────────────────────────────────────────────────────────

// printBacklog returns the change in total backlog size since the last tick.
// prevTotal is updated in-place; pass -1 on first tick to suppress delta display.
func printBacklog(ctx context.Context, conn *pgx.Conn, prevTotal *int64) int64 {
	fmt.Fprintln(out, "\n  BACKLOG SUMMARY  (status × failure_stage)")
	fmt.Fprintf(out, "  %-12s %-14s %8s\n", "Status", "Stage", "Count")
	fmt.Fprintf(out, "  %s\n", strings.Repeat("─", 38))

	rows, err := conn.Query(ctx, `
		SELECT status, failure_stage, COUNT(*) AS cnt
		FROM backlog_records
		GROUP BY status, failure_stage
		ORDER BY status, failure_stage`)
	if err != nil {
		fmt.Fprintf(out, "  [warn] cannot read backlog_records: %v\n", err)
		return 0
	}
	defer rows.Close()

	var total int64
	for rows.Next() {
		var status, stage string
		var cnt int64
		if scanErr := rows.Scan(&status, &stage, &cnt); scanErr != nil {
			continue
		}
		total += cnt
		fmt.Fprintf(out, "  %-12s %-14s %8s\n", status, stage, fmtInt(cnt))
	}

	var delta int64
	if *prevTotal >= 0 {
		delta = total - *prevTotal
	}
	*prevTotal = total
	return delta
}

// ── backlog rate ──────────────────────────────────────────────────────────────

func printBacklogRate(ctx context.Context, conn *pgx.Conn) {
	var totalProcessed, totalBacklog int64
	conn.QueryRow(ctx, `SELECT COALESCE(SUM(records_processed),0) FROM pipeline_checkpoints`).Scan(&totalProcessed)
	conn.QueryRow(ctx, `SELECT COUNT(*) FROM backlog_records`).Scan(&totalBacklog)

	// totalIn = every record that entered the pipeline: those that made it through
	// (checkpointed) plus those that didn't (backlogged). Dividing backlog by only
	// the checkpointed count inflates the rate whenever backlog grows faster than
	// checkpoints commit — especially at pipeline start.
	totalIn := totalProcessed + totalBacklog
	rate := 0.0
	if totalIn > 0 {
		rate = float64(totalBacklog) / float64(totalIn) * 100.0
	}
	fmt.Fprintf(out, "\n  Total records in: %s  |  Processed: %s  |  Backlog: %s  |  Backlog rate: %.4f%%\n",
		fmtInt(totalIn), fmtInt(totalProcessed), fmtInt(totalBacklog), rate)
}

// ── write tune ────────────────────────────────────────────────────────────────

func printWriteTune(ctx context.Context, conn *pgx.Conn) {
	var wt writeTune
	err := conn.QueryRow(ctx, `
		SELECT batch_size_normal, batch_size_turbo, batch_size_throttle
		FROM write_tune_config LIMIT 1`).Scan(&wt.Normal, &wt.Turbo, &wt.Throttle)
	if err != nil {
		fmt.Fprintf(out, "\n  WRITE TUNE CONFIG  [unavailable: %v]\n", err)
		return
	}
	fmt.Fprintf(out, "\n  WRITE TUNE CONFIG  —  Normal: %s  |  Turbo: %s  |  Throttle: %s\n",
		fmtInt(int64(wt.Normal)), fmtInt(int64(wt.Turbo)), fmtInt(int64(wt.Throttle)))
}

// ── data movement summary ──────────────────────────────────────────────────────

// printMovement emits a one-line verdict on whether data moved this tick.
// checkpointDelta: new records written to checkpoints.
// destDelta:       new rows landed in destination raw schema.
// backlogDelta:    change in backlog total (positive = new failures, negative = resolved).
func printMovement(tick int, checkpointDelta, destDelta, backlogDelta int64) {
	fmt.Fprintf(out, "\n  %s\n", strings.Repeat("─", 90))
	fmt.Fprintln(out, "  DATA MOVEMENT THIS TICK")

	moved := checkpointDelta > 0 || destDelta > 0

	cpStr := "idle"
	if checkpointDelta > 0 {
		cpStr = fmt.Sprintf("+%s records checkpointed", fmtInt(checkpointDelta))
	}

	destStr := "idle"
	if destDelta > 0 {
		destStr = fmt.Sprintf("+%s rows landed in destination", fmtInt(destDelta))
	}

	backlogStr := "no change"
	if backlogDelta > 0 {
		backlogStr = fmt.Sprintf("+%s new failures routed to backlog", fmtInt(backlogDelta))
	} else if backlogDelta < 0 {
		backlogStr = fmt.Sprintf("%s records resolved/abandoned from backlog", fmtInt(backlogDelta))
	}

	verdict := "NO MOVEMENT"
	if tick == 1 {
		verdict = "FIRST TICK — establishing baseline"
	} else if moved {
		verdict = "MOVING"
	}

	fmt.Fprintf(out, "  Checkpoints : %s\n", cpStr)
	fmt.Fprintf(out, "  Destination : %s\n", destStr)
	fmt.Fprintf(out, "  Backlog     : %s\n", backlogStr)
	fmt.Fprintf(out, "  >> %s\n", verdict)
	fmt.Fprintf(out, "  %s\n", strings.Repeat("─", 90))
}

// ── helpers ───────────────────────────────────────────────────────────────────

func fmtInt(n int64) string {
	if n == 0 {
		return "0"
	}
	s := fmt.Sprintf("%d", n)
	out := make([]byte, 0, len(s)+len(s)/3)
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, byte(c))
	}
	return string(out)
}

func mustConnect(ctx context.Context, dsn, label string) *pgx.Conn {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fatal: cannot connect to %s (%s): %v\n", label, dsn, err)
		os.Exit(1)
	}
	fmt.Fprintf(out, "Connected to %s\n", label)
	return conn
}
