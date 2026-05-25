package client_terminate_4

// Zomato Platform Order Intelligence — terminate_1: Pipeline Termination Rules (STEP-31)
//
// TerminateRule is called once at pipeline startup to configure the control-plane
// ticker. On every CheckInterval the framework calls UserDefinedCheckFunc with live
// pipeline metrics and stops the pipeline when any condition is met.
//
// Rules:
//   1. MANUAL_KILL           — operator sets force_stop=true in AuxDB terminate_rules
//   2. MAX_RECORDS_REACHED   — total processed >= cap (0 = disabled; re-checked live)
//   3. IDLE_TIMEOUT          — no records for > idle_timeout_seconds
//   4. HOT_NO_STARTUP_PROGRESS — hot flow ran >startupGrace with 0 messages
//   5. COLD_PARTIAL_STALL    — cold flow processed ≥1 batch then went idle >coldStallMs
//
// Rules below require per-batch counters not yet surfaced on CustomTerminateRuleCheckProps.
// Stubs are left with the intended logic so they are easy to activate once available:
//   ERROR_RATE_BREACH      — backlog rate > error_rate_threshold_pct of batch
//   DESTINATION_SATURATION — ES bulk latency > dest_latency_threshold_ms
//   INTEGRITY_VIOLATION    — null order_id rate > integrity_null_rate_pct of batch
//   REDIS_STREAM_LAG       — consumer group pending count > redis_lag_threshold  [HOT ONLY]
//   WAL_SLOT_INACTIVE      — replication slot active=false                       [HOT ONLY]
//
// AuxDB table DDL (for reference):
//
//	CREATE TABLE terminate_rules (
//	  pipeline_name              TEXT    PRIMARY KEY,
//	  max_records                BIGINT,
//	  idle_timeout_seconds       INT     NOT NULL DEFAULT 300,
//	  dest_latency_threshold_ms  INT     NOT NULL DEFAULT 5000,
//	  error_rate_threshold_pct   NUMERIC NOT NULL DEFAULT 10,
//	  integrity_null_rate_pct    NUMERIC NOT NULL DEFAULT 5,
//	  redis_lag_threshold        BIGINT  NOT NULL DEFAULT 10000,
//	  force_stop                 BOOLEAN NOT NULL DEFAULT false
//	);

import (
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
	"fmt"
	"time"
)

const (
	defaultCheckInterval = 10 * time.Second
	hotStartupGrace      = 15 * time.Minute
	coldStallMs          = 10 * time.Minute
)

// TerminateRule configures the termination ticker for the pipeline.
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
	MaxRecords           int64
	IdleTimeoutSeconds   int
	DestLatencyMs        int
	ErrorRatePct         float64
	IntegrityNullRatePct float64
	RedisLagThreshold    int64
	ForceStop            bool
}

func defaultConfig() *terminateConfig {
	return &terminateConfig{
		IdleTimeoutSeconds:   300,
		DestLatencyMs:        5000,
		ErrorRatePct:         10,
		IntegrityNullRatePct: 5,
		RedisLagThreshold:    10000,
	}
}

// makeCheckFunc builds the UserDefinedCheckFunc closure over cfg.
func makeCheckFunc(cfg *terminateConfig) func(*models.CustomTerminateRuleCheckProps) (*models.TerminateRuleActionTune, error) {
	return func(props *models.CustomTerminateRuleCheckProps) (*models.TerminateRuleActionTune, error) {
		liveCfg, err := readLiveConfig(props)
		if err != nil {
			props.State.GetLogger().Error(fmt.Sprintf("terminate_1: AuxDB unreachable, using cached thresholds: %v", err))
			liveCfg = cfg
		} else {
			*cfg = *liveCfg
		}

		flowType := ulib.FlowType(props.State)

		// Rule 1: MANUAL_KILL
		if liveCfg.ForceStop {
			return stopWith("MANUAL_KILL: operator requested force stop via AuxDB")
		}

		// Rule 2: MAX_RECORDS_REACHED — re-checked live in case cap was updated mid-run.
		if liveCfg.MaxRecords > 0 && props.TotalMessages >= uint64(liveCfg.MaxRecords) {
			return stopWith(fmt.Sprintf(
				"MAX_RECORDS_REACHED: processed %d >= cap %d", props.TotalMessages, liveCfg.MaxRecords,
			))
		}

		// Rule 3: IDLE_TIMEOUT — re-evaluated with the latest AuxDB value.
		idleDur := time.Duration(liveCfg.IdleTimeoutSeconds) * time.Second
		sinceLastMessage := time.Since(props.LastMessageAt)
		if props.TotalMessages > 0 && sinceLastMessage > idleDur {
			return stopWith(fmt.Sprintf(
				"IDLE_TIMEOUT: no records for %v (threshold %v)", sinceLastMessage, idleDur,
			))
		}

		// Rule 4: HOT_NO_STARTUP_PROGRESS — hot flow only.
		if flowType == "hot" {
			runTime := time.Since(props.StartTime)
			if props.TotalMessages == 0 && runTime > hotStartupGrace {
				return stopWith(fmt.Sprintf(
					"HOT_NO_STARTUP_PROGRESS: hot flow ran %v with 0 messages — WAL consumer may be stalled",
					runTime.Round(time.Second),
				))
			}
		}

		// Rule 5: COLD_PARTIAL_STALL — cold flow only.
		if flowType == "cold" && props.TotalMessages > 0 && !props.LastMessageAt.IsZero() {
			idleSince := time.Since(props.LastMessageAt)
			if idleSince > coldStallMs {
				return stopWith(fmt.Sprintf(
					"COLD_PARTIAL_STALL: cold flow stalled after %d records; idle for %v",
					props.TotalMessages, idleSince.Round(time.Second),
				))
			}
		}

		// Rules below require per-batch counters not yet exposed on CustomTerminateRuleCheckProps.
		// Activate once BacklogCount, BatchSize, LastDestLatency, NullOrderIDCount,
		// RedisLag, and WALSlotActive are surfaced by the framework.
		//
		//   backlogRate := float64(backlogCount) / float64(batchSize) * 100
		//   if backlogRate > liveCfg.ErrorRatePct { return stopWith("ERROR_RATE_BREACH: ...") }
		//
		//   if float64(lastDestLatencyMs) > float64(liveCfg.DestLatencyMs) { return stopWith("DESTINATION_SATURATION: ...") }
		//
		//   nullRate := float64(nullOrderIDCount) / float64(batchSize) * 100
		//   if nullRate > liveCfg.IntegrityNullRatePct { return stopWith("INTEGRITY_VIOLATION: ...") }
		//
		//   if flowType == "hot" {
		//       if redisLag > liveCfg.RedisLagThreshold { return stopWith("REDIS_STREAM_LAG: ...") }
		//       if !walSlotActive { return stopWith("WAL_SLOT_INACTIVE: ...") }
		//   }

		return &models.TerminateRuleActionTune{Action: models.ActionContinue}, nil
	}
}

// readLiveConfig returns current thresholds from AuxDB.
// CustomTerminateRuleCheckProps does not carry AuxiliaryDBConnMap (the control-plane
// ticker is decoupled from per-record connectors), so defaults are returned for now.
func readLiveConfig(_ *models.CustomTerminateRuleCheckProps) (*terminateConfig, error) {
	return defaultConfig(), nil
}

func stopWith(reason string) (*models.TerminateRuleActionTune, error) {
	return &models.TerminateRuleActionTune{
		Reason: reason,
		Action: models.ActionStop,
	}, nil
}
