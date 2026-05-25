package client_backlog_5

// Hot Flow Stage 1 (WAL → Redis) — Incident Backlog
//
// Routes failed Stage 1 records to AuxDB `backlog_records`. Stage 1 has no
// transformer chain, so the only failure stages are:
//
//   - WALDecode    — framework failed to decode the raw WAL binary event
//   - RedisPublish — XADD to the brand's Redis stream failed
//
// The FailureStage enum values are remapped to these Stage 1 labels:
//   FailureStageTransform   → "WALDecode"    (decode failure before publish)
//   FailureStageDestination → "RedisPublish" (XADD failure)
//
// Records can also carry _failure_stage metadata to override the label, which
// allows the framework's WAL reader to stamp more specific sub-causes.
//
// AuxDB table: backlog_records (shared with cold and Stage 2).

import (
	"context"
	"encoding/json"
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
	"fmt"
	"time"
)

const flowType = "hot_stage1"

// Backlog writes each failed record to AuxDB backlog_records and always
// returns ActionContinue. Only an AuxDB connection failure escalates to ActionStop.
func Backlog(param *models.BacklogProps) (*models.BacklogTune, error) {
	pgConn, err := ulib.GetAuxPostgresConn(param.AuxiliaryDBConnMap)
	if err != nil {
		param.State.GetLogger().Error(fmt.Sprintf("backlog_5: %v — dropping failed records", err))
		return continueAction(), nil
	}

	subBrand, _, _ := ulib.ParseBrandContext(param.State, param.Records)
	defaultStage := failureStageLabel(param.FailureStage)
	batchID := time.Now().UnixMilli()

	errMsg := ""
	if param.Err != nil {
		errMsg = param.Err.Error()
	}

	for _, record := range param.Records {
		stage := ulib.ExtractFailureStageLabel(record, defaultStage)
		redisStreamID := ulib.ExtractRedisStreamID([]map[string]any{record})

		rawJSON, jsonErr := json.Marshal(record)
		if jsonErr != nil {
			param.State.GetLogger().Error(fmt.Sprintf("backlog_5: failed to marshal record: %v", jsonErr))
			continue
		}

		insertQ := `
			INSERT INTO backlog_records
				(sub_brand, city, entity, table_split_index, flow_type,
				 batch_id, failure_stage, error_code, error_message,
				 raw_record, redis_stream_id,
				 retry_count, status, created_at, last_attempted_at)
			VALUES ($1, 'n/a', 'wal_ingestion', 0, $2, $3, $4, $5, $6, $7, $8, 0, 'PENDING', now(), now())`

		_, execErr := pgConn.Exec(context.Background(), insertQ,
			subBrand, flowType,
			batchID, stage, stage, errMsg,
			string(rawJSON), redisStreamID,
		)
		if execErr != nil {
			param.State.GetLogger().Error(fmt.Sprintf("backlog_5: failed to insert record: %v", execErr))
			if param.DestDBConn.IsConnectionError(execErr) {
				return stopAction(), execErr
			}
		}
	}

	param.State.GetLogger().Debug(fmt.Sprintf(
		"backlog_5: routed %d record(s) [stage=%s sub_brand=%s flow=%s]",
		len(param.Records), defaultStage, subBrand, flowType,
	))

	return continueAction(), nil
}

// failureStageLabel maps FailureStage to Stage 1-specific labels.
func failureStageLabel(stage models.FailureStage) string {
	switch stage {
	case models.FailureStageTransform:
		return "WALDecode"
	case models.FailureStageDestination:
		return "RedisPublish"
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
