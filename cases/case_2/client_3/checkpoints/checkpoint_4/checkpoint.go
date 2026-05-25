package client_checkpoint_4

// Zomato Platform Order Intelligence — Pipeline Checkpoint (STEP-29)
//
// Writes per-shard progress to AuxDB `pipeline_checkpoints` after every
// committed Elasticsearch bulk-index batch.
//
// Cold flow: tracks last_processed_pk so the source connector can resume
//   with WHERE order_id > last_processed_pk on restart.
//
// Hot flow: tracks redis_stream_id and wal_lsn so the Redis consumer group
//   can resume via XREADGROUP from the last acknowledged stream entry.
//
// AuxDB table DDL (for reference):
//
//	CREATE TABLE pipeline_checkpoints (
//	  id                 BIGSERIAL    PRIMARY KEY,
//	  sub_brand          VARCHAR(30)  NOT NULL,
//	  city               VARCHAR(30)  NOT NULL,
//	  entity             VARCHAR(50)  NOT NULL,
//	  table_split_index  INT          NOT NULL DEFAULT 1,
//	  flow_type          VARCHAR(10)  NOT NULL DEFAULT 'cold',
//	  last_processed_pk  BIGINT       NOT NULL DEFAULT 0,
//	  redis_stream_id    TEXT         NOT NULL DEFAULT '',
//	  wal_lsn            TEXT         NOT NULL DEFAULT '',
//	  batch_id           BIGINT       NOT NULL DEFAULT 0,
//	  phase              VARCHAR(30)  NOT NULL DEFAULT 'Extract',
//	  status             VARCHAR(20)  NOT NULL DEFAULT 'IN_PROGRESS',
//	  records_processed  BIGINT       NOT NULL DEFAULT 0,
//	  checkpoint_ts      TIMESTAMPTZ  NOT NULL DEFAULT now(),
//	  created_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
//	  updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
//	  UNIQUE (sub_brand, city, entity, table_split_index, flow_type)
//	);

import (
	"context"
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
	"fmt"
	"time"
)

// Checkpoint upserts a row in pipeline_checkpoints for the current shard.
// The conflict key is (sub_brand, city, entity, table_split_index, flow_type)
// so cold and hot rows for the same shard coexist independently.
func Checkpoint(param *models.CheckpointProps) (*models.CheckpointTune, error) {
	pgConn, err := ulib.GetAuxPostgresConn(param.AuxiliaryDBConnMap)
	if err != nil {
		return continueAction(), fmt.Errorf("checkpoint: %w", err)
	}

	subBrand, city, entity := ulib.ParseBrandContext(param.State, param.Records)
	flowType := ulib.FlowType(param.State)
	splitIndex := ulib.ExtractSplitIndex(param.Records[0])
	batchID := time.Now().UnixMilli()

	var lastPK int64
	var redisStreamID, walLSN, phase string

	if flowType == "hot" {
		redisStreamID = ulib.ExtractRedisStreamID(param.Records)
		walLSN = ulib.ExtractWALLSN(param.Records)
		phase = "Stream/Xform/Load"
	} else {
		lastPK = ulib.ExtractLastPK(param.Records)
		phase = "Extract/Xform/Load"
	}

	query := `
		INSERT INTO pipeline_checkpoints
			(sub_brand, city, entity, table_split_index, flow_type,
			 last_processed_pk, redis_stream_id, wal_lsn,
			 batch_id, phase, status, records_processed,
			 checkpoint_ts, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'IN_PROGRESS', $11, now(), now(), now())
		ON CONFLICT (sub_brand, city, entity, table_split_index, flow_type)
		DO UPDATE SET
			last_processed_pk = EXCLUDED.last_processed_pk,
			redis_stream_id   = EXCLUDED.redis_stream_id,
			wal_lsn           = EXCLUDED.wal_lsn,
			batch_id          = EXCLUDED.batch_id,
			phase             = EXCLUDED.phase,
			status            = EXCLUDED.status,
			records_processed = pipeline_checkpoints.records_processed + EXCLUDED.records_processed,
			checkpoint_ts     = EXCLUDED.checkpoint_ts,
			updated_at        = EXCLUDED.updated_at`

	_, execErr := pgConn.Exec(context.Background(), query,
		subBrand, city, entity, splitIndex, flowType,
		lastPK, redisStreamID, walLSN,
		batchID, phase, int64(len(param.Records)),
	)
	if execErr != nil {
		return continueAction(), fmt.Errorf("checkpoint: failed to write pipeline_checkpoints: %w", execErr)
	}

	param.State.GetLogger().Debug(fmt.Sprintf(
		"checkpoint written: sub_brand=%s city=%s entity=%s split=%d flow=%s lastPK=%d streamID=%s lsn=%s batchID=%d",
		subBrand, city, entity, splitIndex, flowType, lastPK, redisStreamID, walLSN, batchID,
	))

	return continueAction(), nil
}

func continueAction() *models.CheckpointTune {
	return &models.CheckpointTune{Action: models.ActionContinue}
}
