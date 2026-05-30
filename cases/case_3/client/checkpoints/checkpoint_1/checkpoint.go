package client_checkpoint_1

// Checkpoint — records the last successfully MERGEd date for a given
// (property_id, surface) pair in AuxDB.pipeline_checkpoints.
//
// On restart the backfill orchestrator queries this table and skips dates
// that are already checkpointed, enabling safe resume without re-processing
// or duplicating data.

import (
	"context"
	"fmt"
	"time"

	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
)

func Checkpoint(param *models.CheckpointProps) (*models.CheckpointTune, error) {
	if len(param.Records) == 0 {
		return continueAction(), nil
	}

	conn, err := ulib.GetAuxPostgresConn(param.AuxiliaryDBConnMap)
	if err != nil {
		return continueAction(), fmt.Errorf("checkpoint_1: auxdb connect: %w", err)
	}
	defer conn.Close(context.Background())

	rp := param.State.GetReplicaProps()
	propertyID, _ := rp["property_id"].(string)
	surface, _ := rp["surface"].(string)
	date, _ := rp["date_from"].(string)
	runID, _ := rp["pipeline_run_id"].(string)

	query := `
		INSERT INTO pipeline_checkpoints
			(property_id, surface, last_merged_date, pipeline_run_id, updated_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (property_id, surface)
		DO UPDATE SET
			last_merged_date = EXCLUDED.last_merged_date,
			pipeline_run_id  = EXCLUDED.pipeline_run_id,
			updated_at       = EXCLUDED.updated_at`

	_, execErr := conn.Exec(context.Background(), query,
		propertyID, surface, date, runID, time.Now().UTC())
	if execErr != nil {
		return continueAction(), fmt.Errorf("checkpoint_1: upsert: %w", execErr)
	}

	return continueAction(), nil
}

func continueAction() *models.CheckpointTune {
	return &models.CheckpointTune{Action: models.ActionContinue}
}
