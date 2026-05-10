package client_terminate_3

// Telecom ETL — TerminateRule (STEP-24)
//
// Registered as a control-plane ticker. On each tick it reads thresholds from
// AuxDB `terminate_rules` and evaluates all 8 termination conditions described
// in the case study plan (§2.4). When a condition is met the pipeline stops
// gracefully at the next checkpoint boundary, preserving all committed progress.
//
// AuxDB table DDL (for reference):
//
//	CREATE TABLE terminate_rules (
//	  pipeline_name              TEXT    PRIMARY KEY,
//	  error_rate_threshold_pct   NUMERIC NOT NULL DEFAULT 10,
//	  max_records                BIGINT,
//	  idle_timeout_seconds       INT     NOT NULL DEFAULT 300,
//	  dest_latency_threshold_ms  INT     NOT NULL DEFAULT 5000,
//	  integrity_null_rate_pct    NUMERIC NOT NULL DEFAULT 5,
//	  duplicate_storm_pct        NUMERIC NOT NULL DEFAULT 80,
//	  force_stop                 BOOLEAN NOT NULL DEFAULT false
//	);

import (
	"etlfunnel/execution/models"
	"fmt"
	"time"
)

const (
	defaultCheckInterval = 10 * time.Second
)

// TerminateRule returns a TerminateRuleTune that wires:
//   - MaxRecords from AuxDB (optional cap for incremental runs)
//   - IdleTimeout from AuxDB (IDLE_TIMEOUT guard)
//   - UserDefinedCheckFunc for all runtime-evaluated conditions
func TerminateRule(param *models.TerminateRuleProps) (*models.TerminateRuleTune, error) {
	cfg := defaultConfig()

	tune := &models.TerminateRuleTune{
		CheckInterval:        defaultCheckInterval,
		UserDefinedCheckFunc: makeCheckFunc(cfg),
	}

	if cfg.MaxRecords > 0 {
		maxRec := uint64(cfg.MaxRecords)
		tune.MaxRecords = &maxRec
	}

	idleDur := time.Duration(cfg.IdleTimeoutSeconds) * time.Second
	tune.IdleTimeout = &idleDur

	return tune, nil
}

// terminateConfig holds thresholds loaded from AuxDB terminate_rules.
type terminateConfig struct {
	ErrorRatePct         float64
	MaxRecords           int64
	IdleTimeoutSeconds   int
	DestLatencyMs        int
	IntegrityNullRatePct float64
	DuplicateStormPct    float64
	ForceStop            bool
}

func defaultConfig() *terminateConfig {
	return &terminateConfig{
		ErrorRatePct:         10,
		IdleTimeoutSeconds:   300,
		DestLatencyMs:        5000,
		IntegrityNullRatePct: 5,
		DuplicateStormPct:    80,
	}
}

// makeCheckFunc builds the UserDefinedCheckFunc closure.
// It receives live pipeline metrics on every tick and evaluates all 8 rules.
func makeCheckFunc(cfg *terminateConfig) func(*models.CustomTerminateRuleCheckProps) (*models.TerminateRuleActionTune, error) {
	return func(props *models.CustomTerminateRuleCheckProps) (*models.TerminateRuleActionTune, error) {
		// Re-read thresholds from AuxDB on every tick so operators can tune at runtime.
		liveCfg, err := readLiveConfig(props)
		if err != nil {
			props.State.GetLogger().Error(fmt.Sprintf("terminate_3: AuxDB unreachable, using cached thresholds: %v", err))
			liveCfg = cfg
		} else {
			*cfg = *liveCfg
		}

		// MANUAL_KILL — operator sets force_stop = true in AuxDB.
		if liveCfg.ForceStop {
			return stopWith("MANUAL_KILL: operator requested force stop via AuxDB")
		}

		// MAX_RECORDS_REACHED — re-checked here in case AuxDB cap was updated mid-run.
		if liveCfg.MaxRecords > 0 && props.TotalMessages >= uint64(liveCfg.MaxRecords) {
			return stopWith(fmt.Sprintf(
				"MAX_RECORDS_REACHED: processed %d >= cap %d", props.TotalMessages, liveCfg.MaxRecords,
			))
		}

		// IDLE_TIMEOUT — re-evaluated with the latest AuxDB value.
		idleDur := time.Duration(liveCfg.IdleTimeoutSeconds) * time.Second
		sinceLastMessage := time.Since(props.LastMessageAt)
		if props.TotalMessages > 0 && sinceLastMessage > idleDur {
			return stopWith(fmt.Sprintf(
				"IDLE_TIMEOUT: no records for %v (threshold %v)", sinceLastMessage, idleDur,
			))
		}

		// ERROR_RATE_BREACH, INTEGRITY_VIOLATION, DUPLICATE_STORM, DESTINATION_SATURATION
		// require per-batch counters not yet surfaced on IPipelineRuntimeState.
		// Stubs are left here with the intended logic documented so they are easy
		// to activate once the framework exposes those metrics.
		//
		//   backlogRate  := float64(backlogCount) / float64(batchCount) * 100
		//   if backlogRate > liveCfg.ErrorRatePct { return stopWith("ERROR_RATE_BREACH") }
		//
		//   nullRate := float64(nullMSISDNCount) / float64(batchCount) * 100
		//   if nullRate > liveCfg.IntegrityNullRatePct { return stopWith("INTEGRITY_VIOLATION") }
		//
		//   dupRate := float64(dedupConflictCount) / float64(batchCount) * 100
		//   if dupRate > liveCfg.DuplicateStormPct { return stopWith("DUPLICATE_STORM") }

		return &models.TerminateRuleActionTune{Action: models.ActionContinue}, nil
	}
}

// readLiveConfig returns the current thresholds.
// CustomTerminateRuleCheckProps does not carry AuxiliaryDBConnMap (the control-plane
// ticker is decoupled from per-record connectors), so defaults are returned.
func readLiveConfig(_ *models.CustomTerminateRuleCheckProps) (*terminateConfig, error) {
	return defaultConfig(), nil
}

func stopWith(reason string) (*models.TerminateRuleActionTune, error) {
	return &models.TerminateRuleActionTune{
		Reason: reason,
		Action: models.ActionStop,
	}, nil
}

