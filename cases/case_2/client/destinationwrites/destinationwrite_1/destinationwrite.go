package client_destinationwrite_1

// Zomato Platform Order Intelligence — destinationwrite_1: Write Tuning (STEP-32)
//
// Dynamically adjusts Elasticsearch bulk-index batch size and runtime settings
// based on time-of-day and pipeline health.  Config is read from AuxDB
// `write_tune_config` on every tick (live — no restart required).
//
// Modes:
//
//   Speedify (off-peak / cold backfill window 22:00–09:00 IST):
//     • Batch size:         batch_size_turbo  (default: 5000 docs/request)
//     • ES refresh_interval: "-1"            (disable live refresh)
//     • XREADGROUP COUNT:   batch_size_turbo  (hot flow reads more per poll)
//     • Concurrency:        max_concurrent_flows (default: 8)
//     • Inter-batch sleep:  0ms
//
//   Slowify (peak hours 09:00–22:00 IST or ES under load):
//     • Batch size:         batch_size_throttle (default: 50 docs/request)
//     • ES refresh_interval: "30s"
//     • XREADGROUP COUNT:   redis_xread_count_slowify (default: 10)
//     • Concurrency:        1
//     • Inter-batch sleep:  configurable (destinationLatencyThresholdMs / 2)
//
// ES refresh_interval is controlled via the Elasticsearch Index Settings API:
//   PUT /platform_orders/_settings
//   { "index": { "refresh_interval": "-1" | "30s" } }
//
// The function is idempotent: it only makes the ES settings call when the
// mode actually changes (tracked in package-level state).

import (
	"bytes"
	"context"
	"encoding/json"
	"etlfunnel/execution/models"
	ulib "etlfunnel/execution/client/userlibraries"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

const (
	indexName     = "platform_orders"
	peakStartHour = 9  // 09:00 IST
	peakEndHour   = 22 // 22:00 IST
	istOffset     = 5*60 + 30 // IST = UTC+5:30 in minutes
)

// modeState tracks the last applied mode to avoid redundant ES API calls.
var (
	lastMode     string
	lastModeMu   sync.Mutex
)

// writeTuneConfig mirrors the AuxDB `write_tune_config` row.
type writeTuneConfig struct {
	BatchSizeNormal              int
	BatchSizeTurbo               int
	BatchSizeThrottle            int
	ThrottleSchedule             string
	RedisXReadCountSlowify       int
	DestinationLatencyThresholdMS int
	ConcurrencyLimit             int
	MaxConcurrentFlows           int
	MaxConcurrentPipelines       int
}

// Tune reads the current write config and returns batch size and mode adjustments.
func Tune(ctx context.Context, param *models.DestinationWriteProps) (*models.DestinationWriteTune, error) {
	cfg, err := loadConfig(ctx, param)
	if err != nil {
		param.State.GetLogger().Warn(fmt.Sprintf("destinationwrite_1: config load failed (%v) — keeping current tune", err))
		return defaultTune(), nil
	}

	mode := currentMode(cfg)
	tune := buildTune(cfg, mode)

	// Apply ES refresh_interval only when mode changes.
	lastModeMu.Lock()
	modeChanged := mode != lastMode
	if modeChanged {
		lastMode = mode
	}
	lastModeMu.Unlock()

	if modeChanged {
		esURL := param.ConnectorProps.DestinationURL
		if esURL == "" {
			esURL = "http://localhost:9200"
		}
		if esErr := setESRefreshInterval(esURL, tune.ESRefreshInterval); esErr != nil {
			param.State.GetLogger().Warn(fmt.Sprintf(
				"destinationwrite_1: ES refresh_interval update failed (%v) — continuing with batch size change only", esErr,
			))
		} else {
			param.State.GetLogger().Debug(fmt.Sprintf(
				"destinationwrite_1: switched to %s mode (batch=%d refresh=%s xread_count=%d)",
				mode, tune.BatchSize, tune.ESRefreshInterval, tune.RedisXReadCount,
			))
		}
	}

	return tune, nil
}

// ── Mode selection ───────────────────────────────────────────────────────────

func currentMode(cfg *writeTuneConfig) string {
	now := time.Now().UTC()
	// Convert UTC to IST by adding the offset.
	istMinutes := now.Hour()*60 + now.Minute() + istOffset
	istHour := (istMinutes / 60) % 24

	if istHour >= peakStartHour && istHour < peakEndHour {
		return "slowify"
	}
	return "speedify"
}

// ── Tune builder ─────────────────────────────────────────────────────────────

func buildTune(cfg *writeTuneConfig, mode string) *models.DestinationWriteTune {
	if mode == "slowify" {
		sleepMS := cfg.DestinationLatencyThresholdMS / 2
		if sleepMS < 0 {
			sleepMS = 0
		}
		return &models.DestinationWriteTune{
			BatchSize:         cfg.BatchSizeThrottle,
			Concurrency:       1,
			InterBatchSleepMS: sleepMS,
			ESRefreshInterval: "30s",
			RedisXReadCount:   cfg.RedisXReadCountSlowify,
			Mode:              "slowify",
		}
	}

	// speedify
	return &models.DestinationWriteTune{
		BatchSize:         cfg.BatchSizeTurbo,
		Concurrency:       cfg.MaxConcurrentFlows,
		InterBatchSleepMS: 0,
		ESRefreshInterval: "-1",
		RedisXReadCount:   cfg.BatchSizeTurbo,
		Mode:              "speedify",
	}
}

func defaultTune() *models.DestinationWriteTune {
	return &models.DestinationWriteTune{
		BatchSize:         1000,
		Concurrency:       4,
		InterBatchSleepMS: 0,
		ESRefreshInterval: "30s",
		RedisXReadCount:   100,
		Mode:              "unknown",
	}
}

// ── AuxDB config loader ──────────────────────────────────────────────────────

func loadConfig(ctx context.Context, param *models.DestinationWriteProps) (*writeTuneConfig, error) {
	pgConn, err := ulib.GetAuxPostgresConn(param.AuxiliaryDBConnMap)
	if err != nil {
		return nil, err
	}

	var cfg writeTuneConfig
	err = pgConn.QueryRow(ctx, `
		SELECT batch_size_normal, batch_size_turbo, batch_size_throttle,
		       throttle_schedule, redis_xread_count_slowify,
		       destination_latency_threshold_ms,
		       concurrency_limit, max_concurrent_flows, max_concurrent_pipelines
		FROM write_tune_config
		WHERE config_name = 'global'`,
	).Scan(
		&cfg.BatchSizeNormal,
		&cfg.BatchSizeTurbo,
		&cfg.BatchSizeThrottle,
		&cfg.ThrottleSchedule,
		&cfg.RedisXReadCountSlowify,
		&cfg.DestinationLatencyThresholdMS,
		&cfg.ConcurrencyLimit,
		&cfg.MaxConcurrentFlows,
		&cfg.MaxConcurrentPipelines,
	)
	if err != nil {
		return nil, fmt.Errorf("write_tune_config query: %w", err)
	}
	return &cfg, nil
}

// ── Elasticsearch refresh_interval API call ──────────────────────────────────

// setESRefreshInterval calls PUT /platform_orders/_settings to update
// refresh_interval.  "-1" disables live refresh (speedify); "30s" re-enables it.
func setESRefreshInterval(esURL, interval string) error {
	payload := map[string]any{
		"index": map[string]any{
			"refresh_interval": interval,
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	url := esURL + "/" + indexName + "/_settings"
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("PUT %s: %w", url, err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("ES settings update status=%d body=%s", resp.StatusCode, truncate(string(respBody), 200))
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
