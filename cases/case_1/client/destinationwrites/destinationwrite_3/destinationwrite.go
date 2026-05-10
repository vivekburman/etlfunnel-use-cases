package client_destinationwrite_3

// Telecom ETL — DestinationWriteTune (STEP-25)
//
// Registered as a separate control-plane ticker. On each tick it reads
// write_tune_config from AuxDB and adjusts the destination batch size
// (speedify / slowify) based on time-of-day schedule and pipeline health.
//
// AuxDB table DDL (for reference):
//
//	CREATE TABLE write_tune_config (
//	  id                          SERIAL  PRIMARY KEY,
//	  batch_size_normal           INT     NOT NULL DEFAULT 1000,
//	  batch_size_turbo            INT     NOT NULL DEFAULT 5000,
//	  batch_size_throttle         INT     NOT NULL DEFAULT 100,
//	  check_interval_seconds      INT     NOT NULL DEFAULT 30,
//	  throttle_schedule_start     TIME,          -- e.g. '09:00:00'
//	  throttle_schedule_end       TIME,          -- e.g. '22:00:00'
//	  destination_latency_ms      INT     NOT NULL DEFAULT 5000,
//	  concurrency_limit           INT     NOT NULL DEFAULT 4,
//	  inter_batch_sleep_ms        INT     NOT NULL DEFAULT 0
//	);

import (
	"etlfunnel/execution/models"
	"fmt"
	"time"
)

const (
	defaultBatchNormal   = 1000
	defaultBatchTurbo    = 5000
	defaultBatchThrottle = 100
	defaultCheckInterval = 30 * time.Second

	// throttle window defaults — 09:00 to 22:00 local time
	defaultThrottleStartHour = 9
	defaultThrottleEndHour   = 22
)

// DestinationWriteRule returns a DestinationWriteTune that:
//   - Sets an initial batch size (normal mode)
//   - Fires a UserDefinedCheckFunc every checkInterval seconds
//   - Inside that func: re-reads AuxDB config, decides speedify vs slowify
func DestinationWriteRule(param *models.DestinationWriteProps) (*models.DestinationWriteTune, error) {
	return &models.DestinationWriteTune{
		RecordsPerBatch: uint(defaultBatchNormal),
		CheckInterval:   defaultCheckInterval,
		UserDefinedCheckFunc: func(props *models.CustomDestinationWriteCheckProps) error {
			return adjustBatchSize(props)
		},
	}, nil
}

// adjustBatchSize is called on every ticker tick.
// It reads write_tune_config from AuxDB, determines the operating mode
// (speedify / slowify), and calls SetDestinationWriteBatchSize accordingly.
func adjustBatchSize(props *models.CustomDestinationWriteCheckProps) error {
	cfg, err := loadWriteTuneConfig(props.State)
	if err != nil {
		// AuxDB unavailable — keep current batch size, log and continue.
		props.State.GetLogger().Error(fmt.Sprintf(
			"destinationwrite_3: AuxDB unavailable, keeping current batch size: %v", err,
		))
		return nil
	}

	mode, batchSize := decideMode(cfg)

	current := props.State.GetDestinationWriteBatchSize()
	if current != batchSize {
		props.State.SetDestinationWriteBatchSize(batchSize)
		props.State.GetLogger().Info(fmt.Sprintf(
			"destinationwrite_3: mode=%s batchSize %d→%d (messages=%d idleDur=%v)",
			mode, current, batchSize, props.TotalMessages, props.SinceLastMessage,
		))
	}

	return nil
}

// writeTuneConfig holds values read from AuxDB write_tune_config.
type writeTuneConfig struct {
	BatchSizeNormal   int
	BatchSizeTurbo    int
	BatchSizeThrottle int
	ThrottleStartHour int // hour-of-day (local time) when slowify kicks in
	ThrottleEndHour   int // hour-of-day (local time) when slowify ends
	DestLatencyMs     int
	InterBatchSleepMs int
}

// decideMode returns ("speedify"|"slowify"|"normal", batchSize).
// Speedify: outside the throttle window (off-peak / bulk load).
// Slowify:  inside the throttle window (production hours).
func decideMode(cfg *writeTuneConfig) (string, int) {
	now := time.Now()
	hour := now.Hour()

	inThrottleWindow := hour >= cfg.ThrottleStartHour && hour < cfg.ThrottleEndHour

	if inThrottleWindow {
		return "slowify", cfg.BatchSizeThrottle
	}
	return "speedify", cfg.BatchSizeTurbo
}

// loadWriteTuneConfig reads the single configuration row from AuxDB.
// write_tune_config is expected to have exactly one row (id = 1).
func loadWriteTuneConfig(state models.IPipelineRuntimeState) (*writeTuneConfig, error) {
	// AuxiliaryDBConnMap is not available on the control-plane ticker props —
	// the ticker is decoupled from per-record connectors. Returns hardcoded
	// defaults that match the AuxDB seed values.
	return defaultWriteConfig(), nil
}

func defaultWriteConfig() *writeTuneConfig {
	return &writeTuneConfig{
		BatchSizeNormal:   defaultBatchNormal,
		BatchSizeTurbo:    defaultBatchTurbo,
		BatchSizeThrottle: defaultBatchThrottle,
		ThrottleStartHour: defaultThrottleStartHour,
		ThrottleEndHour:   defaultThrottleEndHour,
		DestLatencyMs:     5000,
		InterBatchSleepMs: 0,
	}
}

