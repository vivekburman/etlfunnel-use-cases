package client_backlog_1

// Backlog — writes failed records to AuxDB.pipeline_backlog for later
// inspection or retry.  The pipeline continues after logging the failure
// (ActionContinue) so a single bad GA4 row does not abort the entire date's
// backfill.

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
)

func Backlog(param *models.BacklogProps) (*models.BacklogTune, error) {
	conn, err := ulib.GetAuxPostgresConn(param.AuxiliaryDBConnMap)
	if err != nil {
		return continueAction(), fmt.Errorf("backlog_1: auxdb connect: %w", err)
	}
	defer conn.Close(context.Background())

	rp := param.State.GetReplicaProps()
	propertyID, _ := rp["property_id"].(string)
	surface, _ := rp["surface"].(string)
	runID, _ := rp["pipeline_run_id"].(string)

	errMsg := ""
	if param.Err != nil {
		errMsg = param.Err.Error()
	}

	for _, rec := range param.Records {
		payload, _ := json.Marshal(rec)
		_, execErr := conn.Exec(context.Background(), `
			INSERT INTO pipeline_backlog
				(property_id, surface, failure_stage, error_message, record_payload, pipeline_run_id, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			propertyID, surface, string(param.FailureStage), errMsg, payload, runID, time.Now().UTC())
		if execErr != nil {
			return continueAction(), fmt.Errorf("backlog_1: insert: %w", execErr)
		}
	}

	return continueAction(), nil
}

func continueAction() *models.BacklogTune {
	return &models.BacklogTune{Action: models.ActionContinue}
}
