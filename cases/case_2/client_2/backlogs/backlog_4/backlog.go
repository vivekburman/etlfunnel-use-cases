package client_backlog_4

// Zomato Platform Order Intelligence — Incident Backlog (STEP-30)
//
// Routes failed records to AuxDB `backlog_records` instead of halting the
// pipeline. The full raw record is stored as JSONB so it can be replayed
// after the root cause is fixed.
//
// Failure stages:
//   - Transform    — a transformer rejected or errored on the record
//   - Destination  — Elasticsearch bulk-index rejected the document
//   - WALUnwrap    — transformer_14 failed to parse the WAL event envelope
//                    (hot flow only; set via _failure_stage record metadata)
//
// AuxDB table DDL (for reference):
//
//	CREATE TABLE backlog_records (
//	  id                 BIGSERIAL    PRIMARY KEY,
//	  sub_brand          TEXT         NOT NULL,
//	  city               TEXT         NOT NULL,
//	  entity             TEXT         NOT NULL,
//	  table_split_index  INT          NOT NULL DEFAULT 1,
//	  flow_type          VARCHAR(10)  NOT NULL DEFAULT 'cold',
//	  batch_id           BIGINT,
//	  failure_stage      TEXT         NOT NULL,
//	  error_code         TEXT,
//	  error_message      TEXT,
//	  raw_record         JSONB        NOT NULL,
//	  redis_stream_id    TEXT         NOT NULL DEFAULT '',
//	  retry_count        INT          NOT NULL DEFAULT 0,
//	  status             TEXT         NOT NULL DEFAULT 'PENDING',
//	  created_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
//	  last_attempted_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
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
// returns ActionContinue so subsequent records keep processing.
// Only an AuxDB connection error escalates to ActionStop.
func Backlog(param *models.BacklogProps) (*models.BacklogTune, error) {
	pgConn, err := ulib.GetAuxPostgresConn(param.AuxiliaryDBConnMap)
	if err != nil {
		param.State.GetLogger().Error(fmt.Sprintf("backlog: %v — dropping failed records", err))
		return continueAction(), nil
	}

	// sub_brand and city are stamped on records by transformer_1 (SubBrandTagger)
	// and the source tables respectively. entity comes from the pipeline name.
	subBrand, city, entity := ulib.ParseBrandContext(param.State, param.Records)
	flowType := ulib.FlowType(param.State)
	defaultStage := failureStageLabel(param.FailureStage)
	batchID := time.Now().UnixMilli()

	errMsg := ""
	if param.Err != nil {
		errMsg = param.Err.Error()
	}

	for _, record := range param.Records {
		stage := ulib.ExtractFailureStageLabel(record, defaultStage)
		splitIndex := ulib.ExtractSplitIndex(record)
		redisStreamID := ulib.ExtractRedisStreamID([]map[string]any{record})

		rawJSON, jsonErr := json.Marshal(record)
		if jsonErr != nil {
			param.State.GetLogger().Error(fmt.Sprintf("backlog: failed to marshal record: %v", jsonErr))
			continue
		}

		insertQ := `
			INSERT INTO backlog_records
				(sub_brand, city, entity, table_split_index, flow_type,
				 batch_id, failure_stage, error_code, error_message,
				 raw_record, redis_stream_id,
				 retry_count, status, created_at, last_attempted_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 0, 'PENDING', now(), now())`

		_, execErr := pgConn.Exec(context.Background(), insertQ,
			subBrand, city, entity, splitIndex, flowType,
			batchID, stage, stage, errMsg,
			string(rawJSON), redisStreamID,
		)
		if execErr != nil {
			param.State.GetLogger().Error(fmt.Sprintf("backlog: failed to insert record: %v", execErr))
			if param.DestDBConn.IsConnectionError(execErr) {
				return stopAction(), execErr
			}
		}
	}

	param.State.GetLogger().Debug(fmt.Sprintf(
		"backlog: routed %d record(s) [stage=%s sub_brand=%s city=%s entity=%s flow=%s]",
		len(param.Records), defaultStage, subBrand, city, entity, flowType,
	))

	return continueAction(), nil
}

// failureStageLabel maps the FailureStage enum to the string stored in backlog_records.
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
