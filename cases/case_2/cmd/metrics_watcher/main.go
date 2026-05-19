package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

var (
	flagAuxDSN    = flag.String("auxdb", "host=localhost port=5445 dbname=auxdb user=etl_user password=etl_pass sslmode=disable", "AuxDB connection string")
	flagES        = flag.String("es", "http://localhost:9200", "Elasticsearch base URL")
	flagESUser    = flag.String("es-user", "elastic", "Elasticsearch username")
	flagESPass    = flag.String("es-pass", "etl_pass", "Elasticsearch password")
	flagRedis     = flag.String("redis", "localhost:6379", "Redis address")
	flagRedisPass = flag.String("redis-pass", "etl_pass", "Redis password")
	flagInterval  = flag.Duration("interval", 5*time.Second, "Poll interval")
	flagLogFile   = flag.String("log", "", "Path to write output (file is cleared each run)")
)

var out io.Writer = os.Stdout

var redisStreams = []string{
	"zomato:orders:stream",
	"blinkit:orders:stream",
	"hyperpure:orders:stream",
	"district:orders:stream",
}

// streamLabel trims the common suffix for compact display.
func streamLabel(s string) string {
	return strings.TrimSuffix(s, ":orders:stream")
}

func main() {
	flag.Parse()

	if *flagLogFile != "" {
		f, err := os.OpenFile(*flagLogFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "fatal: cannot open log file %s: %v\n", *flagLogFile, err)
			os.Exit(1)
		}
		defer f.Close()
		out = f
		fmt.Fprintf(os.Stderr, "Logging to %s (tail -f to follow)\n", *flagLogFile)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	auxConn := mustConnectPG(ctx, *flagAuxDSN, "AuxDB")
	defer auxConn.Close(context.Background())

	rdb := redis.NewClient(&redis.Options{
		Addr:     *flagRedis,
		Password: *flagRedisPass,
	})
	defer rdb.Close()

	fmt.Fprintln(out, "Zomato Platform Order Intelligence — Metrics Watcher started.")
	fmt.Fprintf(out, "Polling every %s\n\n", *flagInterval)

	prevRecords := make(map[string]int64)
	prevStreamLens := make(map[string]int64)
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
			printDashboard(ctx, auxConn, rdb, tick, prevRecords, prevStreamLens)
		}
	}
}

func printDashboard(ctx context.Context, aux *pgx.Conn, rdb *redis.Client, tick int, prevRecords map[string]int64, prevStreamLens map[string]int64) {
	now := time.Now().Format("2006-01-02 15:04:05")
	fmt.Fprintf(out, "\n%s\n", strings.Repeat("═", 100))
	fmt.Fprintf(out, "  ZOMATO ORDER INTELLIGENCE METRICS  —  %s  (tick #%d)\n", now, tick)
	fmt.Fprintf(out, "%s\n", strings.Repeat("═", 100))

	printColdCheckpoints(ctx, aux, tick, prevRecords)
	printBackfillCompletion(ctx, aux)
	printHotStage1(ctx, rdb, tick, prevStreamLens)
	printHotStage2(ctx, rdb)
	printESDocCounts(*flagES, *flagESUser, *flagESPass)
	printBacklogSummary(ctx, aux)
	printWriteTuneConfig(ctx, aux)
	printESWriteLog(ctx, aux)
}

func printColdCheckpoints(ctx context.Context, conn *pgx.Conn, tick int, prev map[string]int64) {
	fmt.Fprintln(out, "\n  COLD CHECKPOINT PROGRESS")
	fmt.Fprintf(out, "  %-14s %-12s %-26s %5s %12s %12s  %-12s %-14s %s\n",
		"Brand", "City", "Entity", "Split", "Last PK", "Records", "Phase", "Status", "Delta/tick")
	fmt.Fprintf(out, "  %s\n", strings.Repeat("─", 115))

	rows, err := conn.Query(ctx, `
		SELECT sub_brand, city, entity, table_split_index,
		       last_processed_pk, records_processed, phase, status, checkpoint_ts
		FROM pipeline_checkpoints
		WHERE flow_type = 'cold'
		ORDER BY sub_brand, city, entity, table_split_index`)
	if err != nil {
		fmt.Fprintf(out, "  [warn] cannot read pipeline_checkpoints: %v\n", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var brand, city, entity, phase, status string
		var split int
		var lastPK, records int64
		var ts time.Time
		if scanErr := rows.Scan(&brand, &city, &entity, &split, &lastPK, &records, &phase, &status, &ts); scanErr != nil {
			continue
		}
		key := fmt.Sprintf("%s|%s|%s|%d", brand, city, entity, split)
		delta := records - prev[key]
		if tick == 1 {
			delta = 0
		}
		prev[key] = records

		deltaStr := fmt.Sprintf("+%d", delta)
		if delta <= 0 {
			deltaStr = "  idle"
		}
		fmt.Fprintf(out, "  %-14s %-12s %-26s %5d %12d %12s  %-12s %-14s %s\n",
			brand, city, entity, split, lastPK, fmtInt(records), phase, status, deltaStr)
	}
}

func printBackfillCompletion(ctx context.Context, conn *pgx.Conn) {
	var count int64
	conn.QueryRow(ctx, `SELECT COUNT(*) FROM backfill_completion_log`).Scan(&count)
	const total = 160 // 4 brands × 10 cities × 4 entities
	fmt.Fprintf(out, "\n  COLD BACKFILL COMPLETION: %d / %d  (%.1f%%)\n",
		count, total, float64(count)/float64(total)*100.0)
}

// printHotStage1 shows WAL → Redis ingestion: stream length and writes per tick.
func printHotStage1(ctx context.Context, rdb *redis.Client, tick int, prev map[string]int64) {
	fmt.Fprintln(out, "\n  HOT STAGE 1 — WAL → Redis  (messages written into streams)")
	fmt.Fprintf(out, "  %-14s  %14s  %14s\n", "Brand", "StreamLen", "Delta/tick")
	fmt.Fprintf(out, "  %s\n", strings.Repeat("─", 48))

	for _, stream := range redisStreams {
		length, err := rdb.XLen(ctx, stream).Result()
		if err != nil {
			fmt.Fprintf(out, "  %-14s  [unavailable: %v]\n", streamLabel(stream), err)
			continue
		}
		delta := length - prev[stream]
		if tick == 1 {
			delta = 0
		}
		prev[stream] = length

		deltaStr := fmt.Sprintf("+%s", fmtInt(delta))
		if delta <= 0 {
			deltaStr = "  idle"
		}
		fmt.Fprintf(out, "  %-14s  %14s  %14s\n", streamLabel(stream), fmtInt(length), deltaStr)
	}
}

// printHotStage2 shows Redis → Elasticsearch consumer progress per stream.
func printHotStage2(ctx context.Context, rdb *redis.Client) {
	fmt.Fprintln(out, "\n  HOT STAGE 2 — Redis → Elasticsearch  (consumer group progress)")
	fmt.Fprintf(out, "  %-14s  %-16s  %10s  %10s  %8s  %8s\n",
		"Brand", "Group", "Pending", "Delivered", "Lag", "Workers")
	fmt.Fprintf(out, "  %s\n", strings.Repeat("─", 74))

	for _, stream := range redisStreams {
		groups, err := rdb.XInfoGroups(ctx, stream).Result()
		if err != nil {
			fmt.Fprintf(out, "  %-14s  [unavailable: %v]\n", streamLabel(stream), err)
			continue
		}
		if len(groups) == 0 {
			fmt.Fprintf(out, "  %-14s  no consumer groups\n", streamLabel(stream))
			continue
		}
		for _, g := range groups {
			fmt.Fprintf(out, "  %-14s  %-16s  %10s  %10s  %8s  %8d\n",
				streamLabel(stream), g.Name,
				fmtInt(g.Pending), fmtInt(g.EntriesRead),
				fmtInt(g.Lag), g.Consumers)
		}
	}
}

// esRequest performs an authenticated HTTP request against Elasticsearch.
func esRequest(method, url, user, pass string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(user, pass)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return http.DefaultClient.Do(req)
}

func printESDocCounts(base, esUser, esPass string) {
	fmt.Fprintln(out, "\n  ELASTICSEARCH DOC COUNTS")

	// Total count
	resp, err := esRequest(http.MethodGet, fmt.Sprintf("%s/platform_orders/_count", base), esUser, esPass, nil)
	if err != nil {
		fmt.Fprintf(out, "  [warn] ES count: %v\n", err)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var countResp struct {
		Count int64 `json:"count"`
	}
	json.Unmarshal(body, &countResp)
	fmt.Fprintf(out, "  Total docs in platform_orders: %s\n", fmtInt(countResp.Count))

	// Agg by sub_brand
	aggReq := `{"size":0,"aggs":{"by_brand":{"terms":{"field":"sub_brand","size":10}}}}`
	aggResp, err := esRequest(http.MethodPost, fmt.Sprintf("%s/platform_orders/_search", base), esUser, esPass, strings.NewReader(aggReq))
	if err != nil {
		fmt.Fprintf(out, "  [warn] ES agg: %v\n", err)
		return
	}
	defer aggResp.Body.Close()
	aggBody, _ := io.ReadAll(aggResp.Body)

	var aggResult struct {
		Aggregations struct {
			ByBrand struct {
				Buckets []struct {
					Key      string `json:"key"`
					DocCount int64  `json:"doc_count"`
				} `json:"buckets"`
			} `json:"by_brand"`
		} `json:"aggregations"`
	}
	if err := json.Unmarshal(aggBody, &aggResult); err == nil {
		fmt.Fprintf(out, "  %-20s %12s\n", "sub_brand", "doc_count")
		for _, b := range aggResult.Aggregations.ByBrand.Buckets {
			fmt.Fprintf(out, "  %-20s %12s\n", b.Key, fmtInt(b.DocCount))
		}
	}
}

func printBacklogSummary(ctx context.Context, conn *pgx.Conn) {
	fmt.Fprintln(out, "\n  BACKLOG SUMMARY  (flow_type × failure_stage × status)")
	fmt.Fprintf(out, "  %-10s %-14s %-12s %8s\n", "FlowType", "Stage", "Status", "Count")
	fmt.Fprintf(out, "  %s\n", strings.Repeat("─", 48))

	rows, err := conn.Query(ctx, `
		SELECT COALESCE(flow_type,'?'), COALESCE(failure_stage,'?'), status, COUNT(*)
		FROM backlog_records
		GROUP BY flow_type, failure_stage, status
		ORDER BY flow_type, failure_stage, status`)
	if err != nil {
		fmt.Fprintf(out, "  [warn] cannot read backlog_records: %v\n", err)
		return
	}
	defer rows.Close()

	var total int64
	for rows.Next() {
		var flowType, stage, status string
		var cnt int64
		if scanErr := rows.Scan(&flowType, &stage, &status, &cnt); scanErr != nil {
			continue
		}
		total += cnt
		fmt.Fprintf(out, "  %-10s %-14s %-12s %8s\n", flowType, stage, status, fmtInt(cnt))
	}
	fmt.Fprintf(out, "  %s\n  Total backlog records: %s\n", strings.Repeat("─", 48), fmtInt(total))
}

func printWriteTuneConfig(ctx context.Context, conn *pgx.Conn) {
	var normal, turbo, throttle int
	err := conn.QueryRow(ctx, `
		SELECT batch_size_normal, batch_size_turbo, batch_size_throttle
		FROM write_tune_config LIMIT 1`).Scan(&normal, &turbo, &throttle)
	if err != nil {
		fmt.Fprintf(out, "\n  WRITE TUNE CONFIG  [unavailable: %v]\n", err)
		return
	}
	fmt.Fprintf(out, "\n  WRITE TUNE CONFIG  —  Normal: %s  |  Turbo: %s  |  Throttle: %s\n",
		fmtInt(int64(normal)), fmtInt(int64(turbo)), fmtInt(int64(throttle)))
}

func printESWriteLog(ctx context.Context, conn *pgx.Conn) {
	fmt.Fprintln(out, "\n  ES WRITE LOG (last 5 entries)")
	fmt.Fprintf(out, "  %-14s %-12s %-26s %-8s %8s %8s %8s  %s\n",
		"Brand", "City", "Entity", "FlowType", "Success", "Failure", "Retry", "LoggedAt")
	fmt.Fprintf(out, "  %s\n", strings.Repeat("─", 100))

	rows, err := conn.Query(ctx, `
		SELECT sub_brand, city, entity, COALESCE(flow_type,'?'), success_count, failure_count, retry_count, logged_at
		FROM es_write_log
		ORDER BY logged_at DESC
		LIMIT 5`)
	if err != nil {
		fmt.Fprintf(out, "  [warn] cannot read es_write_log: %v\n", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var brand, city, entity, flowType string
		var success, failure, retry int
		var loggedAt time.Time
		if scanErr := rows.Scan(&brand, &city, &entity, &flowType, &success, &failure, &retry, &loggedAt); scanErr != nil {
			continue
		}
		fmt.Fprintf(out, "  %-14s %-12s %-26s %-8s %8d %8d %8d  %s\n",
			brand, city, entity, flowType, success, failure, retry,
			loggedAt.Format("2006-01-02 15:04:05"))
	}
}

func fmtInt(n int64) string {
	if n == 0 {
		return "0"
	}
	s := fmt.Sprintf("%d", n)
	b := make([]byte, 0, len(s)+len(s)/3)
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b = append(b, ',')
		}
		b = append(b, byte(c))
	}
	return string(b)
}

func mustConnectPG(ctx context.Context, dsn, label string) *pgx.Conn {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fatal: cannot connect to %s (%s): %v\n", label, dsn, err)
		os.Exit(1)
	}
	fmt.Fprintf(out, "Connected to %s\n", label)
	return conn
}
