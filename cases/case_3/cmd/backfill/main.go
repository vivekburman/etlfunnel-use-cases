package main

// backfill — Historical Backfill runner.
//
// Fetches 730 days of GA4 session data from the seeder (or real GA4) for all
// three Myntra properties and upserts into dbo.ga4_sessions via
// stage.ga4_sessions + MERGE.
//
// Properties run sequentially with a 2-minute gap between them to respect the
// shared GCP service-account hourly quota.  Within each property, up to 3
// dates run concurrently (MaxConcurrent from orchestrator_1).
//
// USAGE:
//   make backfill
//   DATE_FROM=2024-06-01 DATE_TO=2024-12-31 make backfill
//   go run ./cmd/backfill

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
	"sync"
	"syscall"
	"time"

	_ "github.com/microsoft/go-mssqldb"

	"github.com/streamcraft/myntra-etl/db_setup/internal/config"
)

var (
	flagDateFrom = flag.String("date-from", "", "Start date YYYY-MM-DD (default: env DATE_FROM or 2024-01-01)")
	flagDateTo   = flag.String("date-to", "", "End date YYYY-MM-DD (default: env DATE_TO or 2025-12-31)")
	flagWorkers  = flag.Int("workers", 3, "Max parallel date requests per property")
)

type property struct {
	ID      string
	Surface string
}

var properties = []property{
	{"properties/123456789", "web"},
	{"properties/987654321", "android"},
	{"properties/567891234", "ios"},
}

const ga4DateLayout = "2006-01-02"
const ga4DateDimLayout = "20060102"
const batchSize = 1000
const maxRetries = 5

func main() {
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	cfg := config.Load()

	dateFrom := firstNonEmpty(*flagDateFrom, os.Getenv("DATE_FROM"), "2024-01-01")
	dateTo := firstNonEmpty(*flagDateTo, os.Getenv("DATE_TO"), "2025-12-31")

	log.Printf("=== Historical Backfill — Myntra GA4 → SQL Server ===")
	log.Printf("  date range: %s → %s", dateFrom, dateTo)
	log.Printf("  seeder:     %s", cfg.SeedURL)
	log.Printf("  mssql:      %s:%s/%s", cfg.MSSQLHost, cfg.MSSQLPort, cfg.MSSQLDB)
	log.Printf("  workers:    %d dates/property", *flagWorkers)

	dates, err := enumerateDates(dateFrom, dateTo)
	if err != nil {
		log.Fatalf("enumerate dates: %v", err)
	}
	log.Printf("  total dates: %d", len(dates))

	db, err := sql.Open("sqlserver", cfg.MSSQLDSN())
	if err != nil {
		log.Fatalf("open mssql: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	if err := db.Ping(); err != nil {
		log.Fatalf("ping mssql: %v", err)
	}
	log.Printf("  mssql connected")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Process each property sequentially to respect the shared service-account quota.
	for i, prop := range properties {
		if ctx.Err() != nil {
			break
		}
		log.Printf("\n>>> Property %d/3: %s (%s) — %d dates", i+1, prop.ID, prop.Surface, len(dates))
		if err := backfillProperty(ctx, cfg, db, prop, dates); err != nil {
			log.Printf("  ERROR: %v", err)
		}

		// 2-minute gap between properties (respect shared quota).
		if i < len(properties)-1 {
			log.Printf("  waiting 2m before next property...")
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Minute):
			}
		}
	}

	log.Println("\n=== Backfill complete ===")
}

func backfillProperty(ctx context.Context, cfg *config.Config, db *sql.DB, prop property, dates []string) error {
	sem := make(chan struct{}, *flagWorkers)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []string

	for _, date := range dates {
		if ctx.Err() != nil {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(d string) {
			defer func() { <-sem; wg.Done() }()
			if err := backfillDate(ctx, cfg, db, prop, d); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Sprintf("%s: %v", d, err))
				mu.Unlock()
				log.Printf("  [%s][%s] ERROR: %v", prop.Surface, d, err)
			}
		}(date)
	}

	wg.Wait()

	if len(errs) > 0 {
		return fmt.Errorf("%d dates failed: %s", len(errs), strings.Join(errs, "; "))
	}
	return nil
}

func backfillDate(ctx context.Context, cfg *config.Config, db *sql.DB, prop property, date string) error {
	offset := int64(0)
	pageNum := 0
	totalRows := 0

	for {
		rows, rowCount, err := fetchPage(ctx, cfg.SeedURL, prop, date, offset)
		if err != nil {
			return fmt.Errorf("fetchPage offset=%d: %w", offset, err)
		}

		if len(rows) == 0 {
			break
		}

		pageNum++

		// Transform and stage
		staged, err := stageRows(ctx, db, prop, rows)
		if err != nil {
			return fmt.Errorf("stage offset=%d: %w", offset, err)
		}
		totalRows += staged

		// Execute MERGE after each page
		merged, err := executeMerge(ctx, db)
		if err != nil {
			return fmt.Errorf("merge offset=%d: %w", offset, err)
		}

		log.Printf("  [%s][%s] page %d: fetched=%d staged=%d merged=%d",
			prop.Surface, date, pageNum, len(rows), staged, merged)

		if offset+int64(len(rows)) >= rowCount {
			break
		}
		offset += int64(len(rows))
	}

	log.Printf("  [%s][%s] done: %d total rows", prop.Surface, date, totalRows)
	return nil
}

// fetchPage calls the seeder's runReport endpoint with offset/limit and returns
// a flattened []map[string]any + total rowCount.
func fetchPage(ctx context.Context, baseURL string, prop property, date string, offset int64) ([]map[string]any, int64, error) {
	body := map[string]any{
		"dimensions": buildDimensions(prop.Surface),
		"metrics":    buildMetrics(),
		"dateRanges": []map[string]string{{"startDate": date, "endDate": date}},
		"limit":      100_000,
		"offset":     offset,
	}
	bodyBytes, _ := json.Marshal(body)

	url := fmt.Sprintf("%s/v1beta/%s:runReport", baseURL, prop.ID)

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<uint(attempt)) * time.Second
			select {
			case <-ctx.Done():
				return nil, 0, ctx.Err()
			case <-time.After(backoff):
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
		if err != nil {
			return nil, 0, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer test-token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			resp.Body.Close()
			log.Printf("  [%s][%s] 429 quota exhausted — sleeping until next hour", prop.Surface, date)
			sleepUntilNextHour(ctx)
			lastErr = fmt.Errorf("quota exhausted")
			continue
		}

		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, b)
			continue
		}

		var ga4Resp ga4Response
		if err := json.NewDecoder(resp.Body).Decode(&ga4Resp); err != nil {
			resp.Body.Close()
			lastErr = err
			continue
		}
		resp.Body.Close()

		records := parseGA4Response(&ga4Resp, prop, date)
		return records, ga4Resp.RowCount, nil
	}

	return nil, 0, fmt.Errorf("all %d attempts failed: %w", maxRetries, lastErr)
}

// stageRows truncates stage.ga4_sessions and bulk-inserts the given rows.
func stageRows(ctx context.Context, db *sql.DB, prop property, rows []map[string]any) (int, error) {
	if _, err := db.ExecContext(ctx, "TRUNCATE TABLE stage.ga4_sessions"); err != nil {
		return 0, fmt.Errorf("truncate stage: %w", err)
	}

	cols := stagingColumns()
	inserted := 0

	for start := 0; start < len(rows); start += batchSize {
		end := start + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		chunk := rows[start:end]

		var placeholders []string
		var args []any
		argIdx := 1

		for _, rec := range chunk {
			var rowPH []string
			for _, col := range cols {
				rowPH = append(rowPH, fmt.Sprintf("@p%d", argIdx))
				args = append(args, rec[col])
				argIdx++
			}
			placeholders = append(placeholders, "("+strings.Join(rowPH, ", ")+")")
		}

		query := fmt.Sprintf("INSERT INTO stage.ga4_sessions (%s) VALUES %s",
			strings.Join(cols, ", "), strings.Join(placeholders, ", "))

		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			return inserted, fmt.Errorf("insert chunk at %d: %w", start, err)
		}
		inserted += len(chunk)
	}

	return inserted, nil
}

// executeMerge runs the MERGE from stage.ga4_sessions into dbo.ga4_sessions.
func executeMerge(ctx context.Context, db *sql.DB) (int64, error) {
	mergeSQL := `
	MERGE dbo.ga4_sessions AS target
	USING stage.ga4_sessions AS source
	ON  target.property_id = source.property_id
	AND target.report_date  = source.report_date
	AND target.session_id   = source.session_id
	WHEN MATCHED THEN
		UPDATE SET
			sessions                  = source.sessions,
			engaged_sessions          = source.engaged_sessions,
			total_users               = source.total_users,
			new_users                 = source.new_users,
			bounce_rate               = source.bounce_rate,
			avg_session_duration_secs = source.avg_session_duration_secs,
			conversions               = source.conversions,
			purchase_revenue_inr      = source.purchase_revenue_inr,
			event_count               = source.event_count,
			screen_page_views         = source.screen_page_views,
			ingested_at               = source.ingested_at,
			pipeline_run_id           = source.pipeline_run_id
	WHEN NOT MATCHED BY TARGET THEN
		INSERT (property_id, surface, report_date, session_id, user_pseudo_id,
			device_category, city, country, source, medium, campaign,
			product_category, payment_method, wishlisted, app_version, os_version,
			sessions, engaged_sessions, total_users, new_users, bounce_rate,
			avg_session_duration_secs, conversions, purchase_revenue_inr,
			event_count, screen_page_views, ingested_at, pipeline_run_id)
		VALUES (source.property_id, source.surface, source.report_date, source.session_id,
			source.user_pseudo_id, source.device_category, source.city, source.country,
			source.source, source.medium, source.campaign, source.product_category,
			source.payment_method, source.wishlisted, source.app_version, source.os_version,
			source.sessions, source.engaged_sessions, source.total_users, source.new_users,
			source.bounce_rate, source.avg_session_duration_secs, source.conversions,
			source.purchase_revenue_inr, source.event_count, source.screen_page_views,
			source.ingested_at, source.pipeline_run_id);`

	res, err := db.ExecContext(ctx, mergeSQL)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// ── GA4 response types ─────────────────────────────────────────────────────

type ga4Response struct {
	DimensionHeaders []struct{ Name string `json:"name"` } `json:"dimensionHeaders"`
	MetricHeaders    []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"metricHeaders"`
	Rows []struct {
		DimensionValues []struct{ Value string `json:"value"` } `json:"dimensionValues"`
		MetricValues    []struct{ Value string `json:"value"` } `json:"metricValues"`
	} `json:"rows"`
	RowCount int64 `json:"rowCount"`
}

// dimensionRenames maps GA4 camelCase → canonical snake_case column names.
var dimensionRenames = map[string]string{
	"sessionId": "session_id", "userPseudoId": "user_pseudo_id",
	"deviceCategory": "device_category", "sessionSource": "source",
	"sessionMedium": "medium", "sessionCampaignName": "campaign",
	"appVersion": "app_version", "operatingSystemVersion": "os_version",
	"customEvent:product_category": "product_category",
	"customEvent:category_slug":    "product_category",
	"customEvent:item_category":    "product_category",
	"customEvent:payment_method":   "payment_method",
	"customEvent:payment_type":     "payment_method",
	"customEvent:pay_method":       "payment_method",
	"customEvent:wishlisted":       "wishlisted",
	"customEvent:is_wishlisted":    "wishlisted",
}

var metricRenames = map[string]string{
	"engagedSessions": "engaged_sessions", "totalUsers": "total_users",
	"newUsers": "new_users", "bounceRate": "bounce_rate",
	"averageSessionDuration": "avg_session_duration_secs",
	"purchaseRevenue":        "purchase_revenue_inr",
	"eventCount":             "event_count", "screenPageViews": "screen_page_views",
}

func parseGA4Response(resp *ga4Response, prop property, date string) []map[string]any {
	// Parse date for report_date
	t, _ := time.Parse(ga4DateDimLayout, date)
	if t.IsZero() {
		t, _ = time.Parse(ga4DateLayout, date)
	}

	runID := fmt.Sprintf("backfill-%s-%s", prop.Surface, date)
	now := time.Now().UTC()

	records := make([]map[string]any, 0, len(resp.Rows))
	for _, row := range resp.Rows {
		rec := map[string]any{
			"property_id":     prop.ID,
			"surface":         prop.Surface,
			"report_date":     t,
			"pipeline_run_id": runID,
			"ingested_at":     now,
		}
		for i, dh := range resp.DimensionHeaders {
			if i >= len(row.DimensionValues) {
				break
			}
			val := row.DimensionValues[i].Value
			col := dh.Name
			if dh.Name == "date" {
				// Already handled above via report_date
				continue
			}
			if canonical, ok := dimensionRenames[col]; ok {
				col = canonical
			}
			rec[col] = val
		}
		for i, mh := range resp.MetricHeaders {
			if i >= len(row.MetricValues) {
				break
			}
			val := row.MetricValues[i].Value
			col := mh.Name
			if canonical, ok := metricRenames[col]; ok {
				col = canonical
			}
			rec[col] = parseMetric(val, mh.Type)
		}
		// Fill nulls
		for _, c := range []string{"session_id", "user_pseudo_id", "device_category",
			"city", "country", "source", "medium", "campaign",
			"product_category", "payment_method", "wishlisted", "app_version", "os_version"} {
			if _, ok := rec[c]; !ok {
				rec[c] = ""
			}
		}
		records = append(records, rec)
	}
	return records
}

func parseMetric(raw, typ string) any {
	switch typ {
	case "TYPE_FLOAT", "TYPE_CURRENCY", "TYPE_SECONDS":
		var f float64
		fmt.Sscanf(raw, "%f", &f)
		return f
	default:
		var i int64
		fmt.Sscanf(raw, "%d", &i)
		return i
	}
}

// ── dimension / metric request builders ───────────────────────────────────

var surfaceDims = map[string][]string{
	"web":     {"customEvent:product_category", "customEvent:wishlisted", "customEvent:payment_method"},
	"android": {"customEvent:category_slug", "customEvent:is_wishlisted", "customEvent:payment_type", "appVersion", "operatingSystemVersion"},
	"ios":     {"customEvent:item_category", "customEvent:pay_method", "appVersion", "operatingSystemVersion"},
}

func buildDimensions(surface string) []map[string]string {
	core := []string{"date", "sessionId", "userPseudoId", "deviceCategory", "city", "country",
		"sessionSource", "sessionMedium", "sessionCampaignName"}
	all := append(core, surfaceDims[surface]...)
	out := make([]map[string]string, len(all))
	for i, d := range all {
		out[i] = map[string]string{"name": d}
	}
	return out
}

func buildMetrics() []map[string]string {
	metrics := []string{"sessions", "engagedSessions", "totalUsers", "newUsers",
		"bounceRate", "averageSessionDuration", "conversions",
		"purchaseRevenue", "eventCount", "screenPageViews"}
	out := make([]map[string]string, len(metrics))
	for i, m := range metrics {
		out[i] = map[string]string{"name": m}
	}
	return out
}

func stagingColumns() []string {
	return []string{
		"property_id", "surface", "report_date", "session_id", "user_pseudo_id",
		"device_category", "city", "country", "source", "medium", "campaign",
		"product_category", "payment_method", "wishlisted", "app_version", "os_version",
		"sessions", "engaged_sessions", "total_users", "new_users",
		"bounce_rate", "avg_session_duration_secs",
		"conversions", "purchase_revenue_inr", "event_count", "screen_page_views",
		"ingested_at", "pipeline_run_id",
	}
}

// ── helpers ────────────────────────────────────────────────────────────────

func enumerateDates(from, to string) ([]string, error) {
	start, err := time.Parse(ga4DateLayout, from)
	if err != nil {
		return nil, err
	}
	end, err := time.Parse(ga4DateLayout, to)
	if err != nil {
		return nil, err
	}
	var out []string
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		out = append(out, d.Format(ga4DateLayout))
	}
	return out, nil
}

func sleepUntilNextHour(ctx context.Context) {
	now := time.Now()
	next := now.Truncate(time.Hour).Add(time.Hour)
	select {
	case <-ctx.Done():
	case <-time.After(time.Until(next)):
	}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
