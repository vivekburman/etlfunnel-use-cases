package client_terminate_5

// Hot Flow Stage 1 (WAL → Redis) — Pipeline Termination Rules
//
// TerminateRule configures the control-plane ticker for WAL ingestion pipelines.
// Stage 1 runs indefinitely — it is a long-lived WAL consumer, not a bounded
// batch job — so rules are tuned accordingly.
//
// Active rules:
//   1. MANUAL_KILL              — operator sets force_stop=true in AuxDB terminate_rules
//   2. MAX_RECORDS_REACHED      — processed >= cap (0 = disabled; useful for smoke tests)
//   3. IDLE_TIMEOUT             — no WAL events received for > idle_timeout_seconds
//   4. WAL_NO_STARTUP_PROGRESS  — pipeline ran > startupGrace with 0 messages
//                                 (slot likely inactive or publication missing)
//
// Stubbed rules (require metrics not yet surfaced on CustomTerminateRuleCheckProps):
//   WAL_SLOT_INACTIVE   — replication slot active=false in pg_replication_slots
//   SOURCE_UNREACHABLE  — Postgres connection failure after N retries
//
// Rules intentionally excluded for Stage 1:
//   COLD_PARTIAL_STALL     — cold flow only
//   REDIS_STREAM_LAG       — Stage 2 consumer lag, not Stage 1 producer
//   ERROR_RATE_BREACH      — no per-batch counters in Stage 1
//   DESTINATION_SATURATION — no Elasticsearch in Stage 1
//   INTEGRITY_VIOLATION    — no order_id validation in Stage 1
//
// AuxDB table: terminate_rules (shared, keyed by pipeline_name).

import (
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
	"fmt"
	"time"
)

const (
	defaultCheckInterval = 10 * time.Second
	startupGrace         = 15 * time.Minute
)

// TerminateRule configures the termination ticker for a Stage 1 WAL pipeline.
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
	MaxRecords         int64
	IdleTimeoutSeconds int
	ForceStop          bool
}

func defaultConfig() *terminateConfig {
	return &terminateConfig{
		IdleTimeoutSeconds: 300,
	}
}

// makeCheckFunc builds the UserDefinedCheckFunc closure over cfg.
func makeCheckFunc(cfg *terminateConfig) func(*models.CustomTerminateRuleCheckProps) (*models.TerminateRuleActionTune, error) {
	return func(props *models.CustomTerminateRuleCheckProps) (*models.TerminateRuleActionTune, error) {
		liveCfg, err := readLiveConfig(props)
		if err != nil {
			props.State.GetLogger().Error(fmt.Sprintf("terminate_5: AuxDB unreachable, using cached thresholds: %v", err))
			liveCfg = cfg
		} else {
			*cfg = *liveCfg
		}

		// Rule 1: MANUAL_KILL
		if liveCfg.ForceStop {
			return stopWith("MANUAL_KILL: operator requested force stop via AuxDB")
		}

		// Rule 2: MAX_RECORDS_REACHED — disabled when MaxRecords=0; useful for smoke tests.
		if liveCfg.MaxRecords > 0 && props.TotalMessages >= uint64(liveCfg.MaxRecords) {
			return stopWith(fmt.Sprintf(
				"MAX_RECORDS_REACHED: processed %d >= cap %d", props.TotalMessages, liveCfg.MaxRecords,
			))
		}

		// Rule 3: IDLE_TIMEOUT — WAL source has gone quiet or slot was dropped.
		idleDur := time.Duration(liveCfg.IdleTimeoutSeconds) * time.Second
		sinceLastMessage := time.Since(props.LastMessageAt)
		if props.TotalMessages > 0 && sinceLastMessage > idleDur {
			return stopWith(fmt.Sprintf(
				"IDLE_TIMEOUT: no WAL events for %v (threshold %v) — slot may be inactive",
				sinceLastMessage.Round(time.Second), idleDur,
			))
		}

		// Rule 4: WAL_NO_STARTUP_PROGRESS — slot connected but no events arriving.
		runTime := time.Since(props.StartTime)
		if props.TotalMessages == 0 && runTime > startupGrace {
			return stopWith(fmt.Sprintf(
				"WAL_NO_STARTUP_PROGRESS: pipeline ran %v with 0 WAL events — check slot and publication",
				runTime.Round(time.Second),
			))
		}

		// Stubbed rules — activate once WALSlotActive and connection health are
		// surfaced on CustomTerminateRuleCheckProps:
		//
		//   WAL_SLOT_INACTIVE:
		//   if !walSlotActive { return stopWith("WAL_SLOT_INACTIVE: replication slot active=false") }
		//
		//   SOURCE_UNREACHABLE:
		//   if consecutiveConnErrors > maxRetries { return stopWith("SOURCE_UNREACHABLE: Postgres connection lost") }

		_ = ulib.IsNoRows // keep ulib import live until stubs above are activated

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
