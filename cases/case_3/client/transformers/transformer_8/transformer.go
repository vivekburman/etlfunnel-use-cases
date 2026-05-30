package client_transformer_8

// RunIDStamper — injects two traceability fields into every record:
//
//	pipeline_run_id  — UUID string set once per pipeline run, read from ReplicaProps.
//	ingested_at      — time.Time set to the current wall clock when this transformer runs.
//
// These columns populate dbo.ga4_sessions.pipeline_run_id and ingested_at,
// enabling point-in-time audit ("which pipeline run wrote this row?").

import (
	"fmt"
	"time"

	"etlfunnel/execution/models"
)

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	rp := param.State.GetReplicaProps()

	runID, _ := rp["pipeline_run_id"].(string)
	if runID == "" {
		return nil, fmt.Errorf("RunIDStamper: replica prop 'pipeline_run_id' is empty")
	}

	out := make(map[string]any, len(param.Record)+2)
	for k, v := range param.Record {
		out[k] = v
	}
	out["pipeline_run_id"] = runID
	out["ingested_at"] = time.Now().UTC()

	return out, nil
}
