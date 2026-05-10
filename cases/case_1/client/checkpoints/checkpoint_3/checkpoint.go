package client_checkpoint_3

// Telecom ETL — Pipeline Checkpoint (STEP-22)
//
// Writes shard-level progress to AuxDB `pipeline_checkpoints` after every
// committed destination batch. On pipeline resume the source connector reads
// the row for its (company, zone, state, split) key and restarts from
// last_processed_pk + 1.
//
// AuxDB table DDL (for reference):
//
//	CREATE TABLE pipeline_checkpoints (
//	  id                 BIGSERIAL PRIMARY KEY,
//	  source_company     VARCHAR(30) NOT NULL,
//	  zone               VARCHAR(20) NOT NULL,
//	  state              VARCHAR(50) NOT NULL,
//	  table_split_index  INT         NOT NULL DEFAULT 1,
//	  last_processed_pk  BIGINT      NOT NULL DEFAULT 0,
//	  batch_id           BIGINT      NOT NULL DEFAULT 0,
//	  phase              VARCHAR(20) NOT NULL DEFAULT 'Extract',
//	  status             VARCHAR(20) NOT NULL DEFAULT 'IN_PROGRESS',
//	  records_processed  BIGINT      NOT NULL DEFAULT 0,
//	  checkpoint_ts      TIMESTAMPTZ NOT NULL DEFAULT now(),
//	  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
//	  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
//	  UNIQUE (source_company, zone, state, table_split_index)
//	);

import (
	"context"
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
	"fmt"
	"time"
)

// Checkpoint upserts a row in pipeline_checkpoints for the current shard.
// It derives company/zone/state from the pipeline runtime state names and
// extracts last_processed_pk from the last committed record's primary key.
func Checkpoint(param *models.CheckpointProps) (*models.CheckpointTune, error) {
	pgConn, err := ulib.GetAuxPostgresConn(param.AuxiliaryDBConnMap)
	if err != nil {
		return continueAction(), fmt.Errorf("checkpoint: %w", err)
	}

	company, zone, state := ulib.ParseShardContext(param.State)
	if zone == "unknown" {
		props := param.State.GetReplicaProps()
		entityBase, _ := props["entityBaseName"].(string)
		table, _ := props["table"].(string)
		if entityBase != "" {
			company = entityBase
			if z, zErr := ulib.ParseZoneFromTable(table, entityBase); zErr == nil {
				zone = z
			}
		}
	}
	splitIndex := ulib.ExtractSplitIndex(param.Records[0])
	lastPK := extractLastPK(param.Records)
	batchID := time.Now().UnixMilli()

	query := `
		INSERT INTO pipeline_checkpoints
			(source_company, zone, state, table_split_index, last_processed_pk, batch_id, phase,
			 status, records_processed, checkpoint_ts, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 'IN_PROGRESS', $8, now(), now())
		ON CONFLICT (source_company, zone, state, table_split_index)
		DO UPDATE SET
			last_processed_pk = EXCLUDED.last_processed_pk,
			batch_id          = EXCLUDED.batch_id,
			phase             = EXCLUDED.phase,
			status            = EXCLUDED.status,
			records_processed = pipeline_checkpoints.records_processed + EXCLUDED.records_processed,
			checkpoint_ts     = EXCLUDED.checkpoint_ts,
			updated_at        = EXCLUDED.updated_at`

	_, execErr := pgConn.Exec(context.Background(), query,
		company, zone, state, splitIndex, lastPK, batchID, "Transform/Load", int64(len(param.Records)),
	)
	if execErr != nil {
		return continueAction(), fmt.Errorf("checkpoint: failed to write to pipeline_checkpoints: %w", execErr)
	}

	param.State.GetLogger().Debug(fmt.Sprintf(
		"checkpoint written: company=%s zone=%s state=%s split=%d lastPK=%d batchID=%d",
		company, zone, state, splitIndex, lastPK, batchID,
	))

	return continueAction(), nil
}

// extractLastPK returns the primary key of the last record in the batch.
// Tries "id", then "customer_id", then falls back to 0.
func extractLastPK(records []map[string]any) int64 {
	if len(records) == 0 {
		return 0
	}
	last := records[len(records)-1]
	for _, key := range []string{"id", "customer_id", "pk"} {
		if v, ok := last[key]; ok {
			switch n := v.(type) {
			case int64:
				return n
			case int:
				return int64(n)
			case float64:
				return int64(n)
			}
		}
	}
	return 0
}

func continueAction() *models.CheckpointTune {
	return &models.CheckpointTune{Action: models.ActionContinue}
}
