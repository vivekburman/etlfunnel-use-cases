package client_terminate_1

// Zomato Platform Order Intelligence — terminate_1: Pipeline Termination Rules (STEP-31)
//
// Evaluates 9 configurable rules on each tick interval and returns
// ActionStop when any threshold is breached, ActionContinue otherwise.
// Thresholds are read from AuxDB `terminate_rules` on every tick (live
// config — no restart needed to adjust thresholds).
//
// Rules:
//   1. ERROR_RATE_BREACH       — backlog rate > threshold% of batch records
//   2. SOURCE_UNREACHABLE      — consecutive source connection failures > threshold
//   3. DESTINATION_SATURATION  — ES bulk latency (ms) > threshold
//   4. INTEGRITY_VIOLATION     — null order_id rate > threshold% in batch
//   5. IDLE_TIMEOUT            — no records received for > threshold seconds
//   6. MANUAL_KILL             — operator set force_stop=true in write_tune_config
//   7. MAX_RECORDS_REACHED     — total processed >= cap (0 = disabled)
//   8. REDIS_STREAM_LAG        — consumer group pending count > threshold [HOT ONLY]
//   9. WAL_SLOT_INACTIVE       — replication slot active=false         [HOT ONLY]
//
// Rules 8–9 are skipped for cold flow pipelines (no Redis/WAL context).
// All rules load thresholds lazily per tick from AuxDB; a missing row means
// the rule is disabled for that tick.

import (
	"context"
	"encoding/json"
	"etlfunnel/execution/models"
	ulib "etlfunnel/execution/client/userlibraries"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Terminate evaluates all active rules and returns ActionStop on the first breach.
func Terminate(ctx context.Context, param *models.TerminateProps) (*models.TerminateTune, error) {
	thresholds, err := loadThresholds(ctx, param)
	if err != nil {
		// If we can't read thresholds, fail open (continue) — don't halt healthy pipelines.
		param.State.GetLogger().Warn(fmt.Sprintf("terminate_1: threshold load failed (%v) — all rules skipped", err))
		return continueAction(), nil
	}

	flowType := ulib.FlowType(param.State)

	// ── Rule 1: ERROR_RATE_BREACH ──────────────────────────────────────────────
	if t, ok := thresholds["ERROR_RATE_BREACH"]; ok && param.BatchSize > 0 {
		rate := float64(param.BacklogCount) / float64(param.BatchSize) * 100
		if rate > t {
			return stopAction("ERROR_RATE_BREACH", fmt.Sprintf("backlog rate %.1f%% > threshold %.1f%%", rate, t)), nil
		}
	}

	// ── Rule 2: SOURCE_UNREACHABLE ────────────────────────────────────────────
	if t, ok := thresholds["SOURCE_UNREACHABLE"]; ok {
		if float64(param.ConsecutiveSourceErrors) > t {
			return stopAction("SOURCE_UNREACHABLE", fmt.Sprintf("%d consecutive source failures > %.0f", param.ConsecutiveSourceErrors, t)), nil
		}
	}

	// ── Rule 3: DESTINATION_SATURATION ───────────────────────────────────────
	if t, ok := thresholds["DESTINATION_SATURATION"]; ok {
		latencyMS := float64(param.LastDestLatency.Milliseconds())
		if latencyMS > t {
			return stopAction("DESTINATION_SATURATION", fmt.Sprintf("ES latency %.0fms > %.0fms", latencyMS, t)), nil
		}
	}

	// ── Rule 4: INTEGRITY_VIOLATION ──────────────────────────────────────────
	if t, ok := thresholds["INTEGRITY_VIOLATION"]; ok && param.BatchSize > 0 {
		nullRate := float64(param.NullOrderIDCount) / float64(param.BatchSize) * 100
		if nullRate > t {
			return stopAction("INTEGRITY_VIOLATION", fmt.Sprintf("null order_id rate %.1f%% > %.1f%%", nullRate, t)), nil
		}
	}

	// ── Rule 5: IDLE_TIMEOUT ─────────────────────────────────────────────────
	if t, ok := thresholds["IDLE_TIMEOUT"]; ok {
		idleSecs := time.Since(param.LastRecordReceivedAt).Seconds()
		if idleSecs > t && !param.LastRecordReceivedAt.IsZero() {
			return stopAction("IDLE_TIMEOUT", fmt.Sprintf("no records for %.0fs > %.0fs", idleSecs, t)), nil
		}
	}

	// ── Rule 6: MANUAL_KILL ───────────────────────────────────────────────────
	if _, ok := thresholds["MANUAL_KILL"]; ok {
		if forceStop, _ := readForceStop(ctx, param); forceStop {
			return stopAction("MANUAL_KILL", "operator set force_stop=true in write_tune_config"), nil
		}
	}

	// ── Rule 7: MAX_RECORDS_REACHED ──────────────────────────────────────────
	if t, ok := thresholds["MAX_RECORDS_REACHED"]; ok && t > 0 {
		if float64(param.TotalRecordsProcessed) >= t {
			return stopAction("MAX_RECORDS_REACHED", fmt.Sprintf("processed %d >= cap %.0f", param.TotalRecordsProcessed, t)), nil
		}
	}

	// ── Rules 8–9: hot-flow only ──────────────────────────────────────────────
	if flowType == "hot" {
		// Rule 8: REDIS_STREAM_LAG
		if t, ok := thresholds["REDIS_STREAM_LAG"]; ok {
			lag, lagErr := getStreamLag(ctx, param)
			if lagErr != nil {
				param.State.GetLogger().Warn(fmt.Sprintf("terminate_1: REDIS_STREAM_LAG check failed: %v", lagErr))
			} else if float64(lag) > t {
				return stopAction("REDIS_STREAM_LAG", fmt.Sprintf("stream pending %d > %.0f", lag, t)), nil
			}
		}

		// Rule 9: WAL_SLOT_INACTIVE
		if _, ok := thresholds["WAL_SLOT_INACTIVE"]; ok {
			inactive, slotErr := isWALSlotInactive(ctx, param)
			if slotErr != nil {
				param.State.GetLogger().Warn(fmt.Sprintf("terminate_1: WAL_SLOT_INACTIVE check failed: %v", slotErr))
			} else if inactive {
				return stopAction("WAL_SLOT_INACTIVE", "replication slot is inactive — WAL consumer disconnected"), nil
			}
		}
	}

	return continueAction(), nil
}

// ── Threshold loader ────────────────────────────────────────────────────────

func loadThresholds(ctx context.Context, param *models.TerminateProps) (map[string]float64, error) {
	pgConn, err := ulib.GetAuxPostgresConn(param.AuxiliaryDBConnMap)
	if err != nil {
		return nil, err
	}

	rows, err := pgConn.Query(ctx, `
		SELECT rule_name, threshold_value
		FROM terminate_rules
		WHERE pipeline_name IN ('global', $1)`,
		param.State.GetName(),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]float64)
	for rows.Next() {
		var name string
		var val float64
		if scanErr := rows.Scan(&name, &val); scanErr == nil {
			result[name] = val
		}
	}
	return result, rows.Err()
}

// ── Rule 6 helper: force_stop flag ─────────────────────────────────────────

func readForceStop(ctx context.Context, param *models.TerminateProps) (bool, error) {
	pgConn, err := ulib.GetAuxPostgresConn(param.AuxiliaryDBConnMap)
	if err != nil {
		return false, err
	}
	var forceStop bool
	err = pgConn.QueryRow(ctx,
		"SELECT force_stop FROM write_tune_config WHERE config_name = 'global'",
	).Scan(&forceStop)
	if err != nil {
		return false, err
	}
	return forceStop, nil
}

// ── Rule 8 helper: Redis stream lag via XINFO GROUPS ───────────────────────

func getStreamLag(ctx context.Context, param *models.TerminateProps) (int64, error) {
	streamKey := param.ConnectorProps.StreamKey
	if streamKey == "" {
		return 0, nil
	}

	groups, err := param.RedisClient.XInfoGroups(ctx, streamKey)
	if err != nil {
		return 0, fmt.Errorf("XINFO GROUPS %s: %w", streamKey, err)
	}

	const consumerGroup = "elastic_writer_group"
	for _, g := range groups {
		if g.Name == consumerGroup {
			return g.Lag, nil
		}
	}
	return 0, nil
}

// ── Rule 9 helper: WAL slot active check via pg_replication_slots ──────────

// walSlotStatus holds the fields we read from pg_replication_slots.
type walSlotStatus struct {
	Active bool
}

func isWALSlotInactive(ctx context.Context, param *models.TerminateProps) (bool, error) {
	slotName := param.ConnectorProps.ReplicationSlot
	if slotName == "" {
		return false, nil
	}

	// We need to check the source brand's Postgres, not AuxDB.
	brand := param.ConnectorProps.Brand
	sourceConn, err := param.SourceDBConn(brand)
	if err != nil {
		return false, fmt.Errorf("source DB connect for %s: %w", brand, err)
	}

	var active bool
	err = sourceConn.QueryRow(ctx,
		"SELECT active FROM pg_replication_slots WHERE slot_name = $1",
		slotName,
	).Scan(&active)
	if err != nil {
		// Row not found = slot was dropped — that is also inactive.
		if strings.Contains(err.Error(), "no rows") {
			return true, nil
		}
		return false, err
	}
	return !active, nil
}

// ── ES latency helper: used by DESTINATION_SATURATION ──────────────────────
// The framework passes LastDestLatency in TerminateProps populated by
// iso_entity_33/34 after each bulk write.  No additional query needed.

// ── Elasticsearch _cluster/health fallback ──────────────────────────────────
// If the framework does not populate LastDestLatency, we can probe ES directly.
func probeESLatency(esURL string) (time.Duration, error) {
	start := time.Now()
	resp, err := http.Get(esURL + "/_cluster/health?timeout=5s")
	elapsed := time.Since(start)
	if err != nil {
		return elapsed, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return elapsed, nil
}

// ── JSON helpers ─────────────────────────────────────────────────────────────

func marshalStop(rule, reason string) string {
	b, _ := json.Marshal(map[string]string{"rule": rule, "reason": reason})
	return string(b)
}

// ── Action builders ──────────────────────────────────────────────────────────

func stopAction(rule, reason string) *models.TerminateTune {
	return &models.TerminateTune{
		Action: models.ActionStop,
		Reason: marshalStop(rule, reason),
	}
}

func continueAction() *models.TerminateTune {
	return &models.TerminateTune{Action: models.ActionContinue}
}
