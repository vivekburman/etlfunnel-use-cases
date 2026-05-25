package client_checkpoint_5

// Hot Flow Stage 1 (WAL → Redis) — Pipeline Checkpoint
//
// Tracks the dual cursor for WAL ingestion: wal_lsn (last confirmed LSN
// advanced on the replication slot) and redis_stream_id (last XADD entry ID).
// On restart the WAL consumer resumes from this LSN rather than replaying
// from the slot origin.
//
// flow_type is hardcoded to "hot_stage1" so Stage 1 checkpoint rows coexist
// independently from Stage 2 ("hot") and cold flow rows for the same brand.
//
// entity is fixed to "wal_ingestion" — Stage 1 reads all four tables from one
// slot and does not split by entity.  table_split_index is always 0 (N/A).
//
// AuxDB table: pipeline_checkpoints (shared with cold and Stage 2).

import (
	"context"
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
	"fmt"
	"time"
)

const (
	flowType = "hot_stage1"
	entity   = "wal_ingestion"
	phase    = "WAL/Publish"
)

// Checkpoint upserts the WAL LSN + Redis stream ID cursor into pipeline_checkpoints.
func Checkpoint(param *models.CheckpointProps) (*models.CheckpointTune, error) {
	pgConn, err := ulib.GetAuxPostgresConn(param.AuxiliaryDBConnMap)
	if err != nil {
		return continueAction(), fmt.Errorf("checkpoint_5: %w", err)
	}

	subBrand, _, _ := ulib.ParseBrandContext(param.State, param.Records)
	redisStreamID := ulib.ExtractRedisStreamID(param.Records)
	walLSN := ulib.ExtractWALLSN(param.Records)
	batchID := time.Now().UnixMilli()

	query := `
		INSERT INTO pipeline_checkpoints
			(sub_brand, city, entity, table_split_index, flow_type,
			 last_processed_pk, redis_stream_id, wal_lsn,
			 batch_id, phase, status, records_processed,
			 checkpoint_ts, created_at, updated_at)
		VALUES ($1, 'n/a', $2, 0, $3, 0, $4, $5, $6, $7, 'IN_PROGRESS', $8, now(), now(), now())
		ON CONFLICT (sub_brand, city, entity, table_split_index, flow_type)
		DO UPDATE SET
			redis_stream_id   = EXCLUDED.redis_stream_id,
			wal_lsn           = EXCLUDED.wal_lsn,
			batch_id          = EXCLUDED.batch_id,
			phase             = EXCLUDED.phase,
			status            = EXCLUDED.status,
			records_processed = pipeline_checkpoints.records_processed + EXCLUDED.records_processed,
			checkpoint_ts     = EXCLUDED.checkpoint_ts,
			updated_at        = EXCLUDED.updated_at`

	_, execErr := pgConn.Exec(context.Background(), query,
		subBrand, entity, flowType,
		redisStreamID, walLSN,
		batchID, phase, int64(len(param.Records)),
	)
	if execErr != nil {
		return continueAction(), fmt.Errorf("checkpoint_5: failed to write pipeline_checkpoints: %w", execErr)
	}

	param.State.GetLogger().Debug(fmt.Sprintf(
		"checkpoint_5 written: sub_brand=%s flow=%s streamID=%s lsn=%s batchID=%d",
		subBrand, flowType, redisStreamID, walLSN, batchID,
	))

	return continueAction(), nil
}

func continueAction() *models.CheckpointTune {
	return &models.CheckpointTune{Action: models.ActionContinue}
}

