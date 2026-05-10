package client_backlog_3

// Telecom ETL — Incident Backlog (STEP-23)
//
// Routes failed records to AuxDB `backlog_records` instead of crashing the
// pipeline. Each row carries the full raw record JSON so it can be
// re-injected by the reprocessing flow (STEP-33/35) after the root cause is fixed.
//
// AuxDB table DDL (for reference):
//
//	CREATE TABLE backlog_records (
//	  id                 BIGSERIAL PRIMARY KEY,
//	  source_company     TEXT    NOT NULL,
//	  zone               TEXT    NOT NULL,
//	  state              TEXT    NOT NULL,
//	  table_split_index  INT     NOT NULL DEFAULT 1,
//	  batch_id           BIGINT,
//	  failure_stage      TEXT    NOT NULL,
//	  error_code         TEXT,
//	  error_message      TEXT,
//	  raw_record         JSONB   NOT NULL,
//	  retry_count        INT     NOT NULL DEFAULT 0,
//	  status             TEXT    NOT NULL DEFAULT 'PENDING',
//	  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
//	  last_attempted_at  TIMESTAMPTZ NOT NULL DEFAULT now()
//	);

import (
	"context"
	"encoding/json"
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
	"fmt"
	"time"
)

// Backlog writes each failed record to AuxDB backlog_records and always
// returns ActionContinue so the pipeline keeps processing subsequent records.
// Only a destination connection error escalates to ActionStop.
func Backlog(param *models.BacklogProps) (*models.BacklogTune, error) {
	pgConn, err := ulib.GetAuxPostgresConn(param.AuxiliaryDBConnMap)
	if err != nil {
		param.State.GetLogger().Error(fmt.Sprintf("backlog: %v — dropping failed records", err))
		return continueAction(), nil
	}

	// Derive shard context from the records themselves, not from the flow name.
	// source_company is stamped by the company-specific schema mapper (transformer_26
	// for Aircel, transformer_23 for Vodafone, etc.). zone and state are native columns
	// in every source table — SELECT * brings them through unchanged for all sharding
	// types (zone+state, zone-only, state-only). Falls back to "unknown" if absent.
	company, zone, state := recordShardContext(param.Records)
	stage := failureStageLabel(param.FailureStage)
	batchID := time.Now().UnixMilli()

	errMsg := ""
	if param.Err != nil {
		errMsg = param.Err.Error()
	}

	for _, record := range param.Records {
		rawJSON, jsonErr := json.Marshal(record)
		if jsonErr != nil {
			param.State.GetLogger().Error(fmt.Sprintf("backlog: failed to marshal record to JSON: %v", jsonErr))
			continue
		}

		insertQ := `
			INSERT INTO backlog_records
				(source_company, zone, state, table_split_index,
				 batch_id, failure_stage, error_code, error_message,
				 raw_record, retry_count, status, created_at, last_attempted_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 0, 'PENDING', now(), now())`

		_, execErr := pgConn.Exec(context.Background(), insertQ,
			company, zone, state, ulib.ExtractSplitIndex(record),
			batchID, stage, stage, errMsg,
			string(rawJSON),
		)
		if execErr != nil {
			param.State.GetLogger().Error(fmt.Sprintf(
				"backlog: failed to insert backlog record: %v", execErr,
			))
			// A destination write error to AuxDB is serious — stop the pipeline.
			if param.DestDBConn.IsConnectionError(execErr) {
				return stopAction(), execErr
			}
		}
	}

	param.State.GetLogger().Debug(fmt.Sprintf(
		"backlog: routed %d record(s) to backlog [stage=%s company=%s zone=%s state=%s]",
		len(param.Records), stage, company, zone, state,
	))

	return continueAction(), nil
}

// failureStageLabel maps the FailureStage enum to a human-readable string
// stored in the backlog_records.failure_stage column.
func failureStageLabel(stage models.FailureStage) string {
	switch stage {
	case models.FailureStageTransform:
		return "Transform"
	case models.FailureStageDestination:
		return "Destination"
	default:
		return "Unknown"
	}
}

func continueAction() *models.BacklogTune {
	return &models.BacklogTune{Action: models.ActionContinue}
}

func stopAction() *models.BacklogTune {
	return &models.BacklogTune{Action: models.ActionStop}
}

// recordShardContext extracts (source_company, zone, state) from the first available
// record. These fields are always present in the record by the time it reaches the
// backlog:
//   - source_company is stamped by the company-specific schema mapper (first transformer)
//   - zone and state are native geo columns in every source table; SELECT * brings them
//     through unchanged for all sharding types (zone+state / zone-only / state-only)
//
// Falls back to "unknown" if a field is missing — this only happens if the schema
// mapper itself failed, which is a fatal misconfiguration, not a data-quality issue.
func recordShardContext(records []map[string]any) (company, zone, state string) {
	strField := func(rec map[string]any, key string) string {
		if v, ok := rec[key].(string); ok && v != "" {
			return v
		}
		return "unknown"
	}
	if len(records) == 0 {
		return "unknown", "unknown", "unknown"
	}
	rec := records[0]
	return strField(rec, "source_company"), strField(rec, "zone"), strField(rec, "state")
}
