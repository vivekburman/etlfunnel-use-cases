package main

// daily — Incremental Daily Flow runner.
//
// Fetches T-2 (fully settled) and T-1 (precautionary) GA4 data for all three
// properties and upserts into dbo.ga4_sessions.  Runs daily at 06:00 IST
// (scheduled externally, e.g. cron or the Streamcraft scheduler).
//
// Logic is identical to the backfill runner but limited to two target dates
// calculated dynamically at runtime.
//
// USAGE:
//   make daily
//   go run ./cmd/daily

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
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

const (
	ga4DateLayout    = "2006-01-02"
	ga4DateDimLayout = "20060102"
	batchSize        = 1000
	maxRetries       = 5
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

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	cfg := config.Load()

	today := time.Now().UTC().Truncate(24 * time.Hour)
	targetDates := []string{
		today.AddDate(0, 0, -2).Format(ga4DateLayout),
		today.AddDate(0, 0, -1).Format(ga4DateLayout),
	}

	log.Printf("=== Incremental Daily Flow — Myntra GA4 → SQL Server ===")
	log.Printf("  target dates: %v (T-2 and T-1)", targetDates)
	log.Printf("  seeder:       %s", cfg.SeedURL)
	log.Printf("  mssql:        %s:%s/%s", cfg.MSSQLHost, cfg.MSSQLPort, cfg.MSSQLDB)

	db, err := sql.Open("sqlserver", cfg.MSSQLDSN())
	if err != nil {
		log.Fatalf("open mssql: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(3)
	if err := db.Ping(); err != nil {
		log.Fatalf("ping mssql: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	total := 0
	for _, prop := range properties {
		for _, date := range targetDates {
			if ctx.Err() != nil {
				break
			}
			rows, err := ingestDate(ctx, cfg, db, prop, date)
			if err != nil {
				log.Printf("  [%s][%s] ERROR: %v", prop.Surface, date, err)
				continue
			}
			total += rows
			log.Printf("  [%s][%s] done: %d rows merged", prop.Surface, date, rows)
		}
	}

	log.Printf("=== Daily flow complete — %d total rows merged ===", total)
}

func ingestDate(ctx context.Context, cfg *config.Config, db *sql.DB, prop property, date string) (int, error) {
	offset := int64(0)
	totalMerged := 0

	for {
		records, rowCount, err := fetchPage(ctx, cfg.SeedURL, prop, date, offset)
		if err != nil {
			return totalMerged, err
		}
		if len(records) == 0 {
			break
		}

		if _, err := db.ExecContext(ctx, "TRUNCATE TABLE stage.ga4_sessions"); err != nil {
			return totalMerged, fmt.Errorf("truncate stage: %w", err)
		}

		if err := bulkInsertStage(ctx, db, records); err != nil {
			return totalMerged, fmt.Errorf("stage insert: %w", err)
		}

		merged, err := executeMerge(ctx, db)
		if err != nil {
			return totalMerged, fmt.Errorf("merge: %w", err)
		}
		totalMerged += int(merged)

		if offset+int64(len(records)) >= rowCount {
			break
		}
		offset += int64(len(records))
	}

	return totalMerged, nil
}

func fetchPage(ctx context.Context, baseURL string, prop property, date string, offset int64) ([]map[string]any, int64, error) {
	surfaceDims := map[string][]string{
		"web":     {"customEvent:product_category", "customEvent:wishlisted", "customEvent:payment_method"},
		"android": {"customEvent:category_slug", "customEvent:is_wishlisted", "customEvent:payment_type", "appVersion", "operatingSystemVersion"},
		"ios":     {"customEvent:item_category", "customEvent:pay_method", "appVersion", "operatingSystemVersion"},
	}
	coreDims := []string{"date", "sessionId", "userPseudoId", "deviceCategory", "city", "country",
		"sessionSource", "sessionMedium", "sessionCampaignName"}
	allDims := append(coreDims, surfaceDims[prop.Surface]...)
	dims := make([]map[string]string, len(allDims))
	for i, d := range allDims {
		dims[i] = map[string]string{"name": d}
	}

	body := map[string]any{
		"dimensions": dims,
		"metrics": []map[string]string{
			{"name": "sessions"}, {"name": "engagedSessions"}, {"name": "totalUsers"},
			{"name": "newUsers"}, {"name": "bounceRate"}, {"name": "averageSessionDuration"},
			{"name": "conversions"}, {"name": "purchaseRevenue"}, {"name": "eventCount"},
			{"name": "screenPageViews"},
		},
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

		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer test-token")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			resp.Body.Close()
			now := time.Now()
			next := now.Truncate(time.Hour).Add(time.Hour)
			log.Printf("  [%s][%s] 429 quota — sleeping until %s", prop.Surface, date, next.Format("15:04:05"))
			select {
			case <-ctx.Done():
				return nil, 0, ctx.Err()
			case <-time.After(time.Until(next)):
			}
			lastErr = fmt.Errorf("quota exhausted")
			continue
		}
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, b)
			continue
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
			RowCount int64 `json:"rowCount"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&ga4Resp); err != nil {
			resp.Body.Close()
			lastErr = err
			continue
		}
		resp.Body.Close()

		dimRenames := map[string]string{
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
		metRenames := map[string]string{
			"engagedSessions": "engaged_sessions", "totalUsers": "total_users",
			"newUsers": "new_users", "bounceRate": "bounce_rate",
			"averageSessionDuration": "avg_session_duration_secs",
			"purchaseRevenue":        "purchase_revenue_inr",
			"eventCount":             "event_count", "screenPageViews": "screen_page_views",
		}

		t, _ := time.Parse(ga4DateLayout, date)
		runID := fmt.Sprintf("daily-%s-%s", prop.Surface, date)
		now := time.Now().UTC()

		records := make([]map[string]any, 0, len(ga4Resp.Rows))
		for _, row := range ga4Resp.Rows {
			rec := map[string]any{
				"property_id": prop.ID, "surface": prop.Surface,
				"report_date": t, "pipeline_run_id": runID, "ingested_at": now,
			}
			for i, dh := range ga4Resp.DimensionHeaders {
				if i >= len(row.DimensionValues) {
					break
				}
				if dh.Name == "date" {
					continue
				}
				col := dh.Name
				if c, ok := dimRenames[col]; ok {
					col = c
				}
				rec[col] = row.DimensionValues[i].Value
			}
			for i, mh := range ga4Resp.MetricHeaders {
				if i >= len(row.MetricValues) {
					break
				}
				col := mh.Name
				if c, ok := metRenames[col]; ok {
					col = c
				}
				val := row.MetricValues[i].Value
				switch mh.Type {
				case "TYPE_FLOAT", "TYPE_CURRENCY":
					var f float64
					fmt.Sscanf(val, "%f", &f)
					rec[col] = f
				default:
					var n int64
					fmt.Sscanf(val, "%d", &n)
					rec[col] = n
				}
			}
			for _, c := range []string{"session_id", "user_pseudo_id", "device_category",
				"city", "country", "source", "medium", "campaign",
				"product_category", "payment_method", "wishlisted", "app_version", "os_version"} {
				if _, ok := rec[c]; !ok {
					rec[c] = ""
				}
			}
			records = append(records, rec)
		}

		return records, ga4Resp.RowCount, nil
	}

	return nil, 0, fmt.Errorf("all attempts failed: %w", lastErr)
}

func bulkInsertStage(ctx context.Context, db *sql.DB, rows []map[string]any) error {
	cols := []string{
		"property_id", "surface", "report_date", "session_id", "user_pseudo_id",
		"device_category", "city", "country", "source", "medium", "campaign",
		"product_category", "payment_method", "wishlisted", "app_version", "os_version",
		"sessions", "engaged_sessions", "total_users", "new_users",
		"bounce_rate", "avg_session_duration_secs",
		"conversions", "purchase_revenue_inr", "event_count", "screen_page_views",
		"ingested_at", "pipeline_run_id",
	}
	for start := 0; start < len(rows); start += batchSize {
		end := start + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		var phs []string
		var args []any
		argIdx := 1
		for _, rec := range rows[start:end] {
			var rph []string
			for _, col := range cols {
				rph = append(rph, fmt.Sprintf("@p%d", argIdx))
				args = append(args, rec[col])
				argIdx++
			}
			phs = append(phs, "("+strings.Join(rph, ", ")+")")
		}
		q := fmt.Sprintf("INSERT INTO stage.ga4_sessions (%s) VALUES %s",
			strings.Join(cols, ", "), strings.Join(phs, ", "))
		if _, err := db.ExecContext(ctx, q, args...); err != nil {
			return err
		}
	}
	return nil
}

func executeMerge(ctx context.Context, db *sql.DB) (int64, error) {
	res, err := db.ExecContext(ctx, mergeSQL)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

const mergeSQL = `
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
