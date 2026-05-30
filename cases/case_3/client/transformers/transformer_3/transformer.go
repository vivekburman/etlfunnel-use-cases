package client_transformer_3

// SurfaceInjector — injects the "surface" label (web / android / ios) and the
// normalised "property_id" into every record, reading both from the pipeline's
// ReplicaProps.  Without this, dbo.ga4_sessions rows have no source label and
// the composite primary key (property_id, report_date, session_id) is broken.

import (
	"fmt"

	"etlfunnel/execution/models"
)

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	rp := param.State.GetReplicaProps()

	surface, _ := rp["surface"].(string)
	if surface == "" {
		return nil, fmt.Errorf("SurfaceInjector: replica prop 'surface' is empty")
	}

	out := make(map[string]any, len(param.Record)+1)
	for k, v := range param.Record {
		out[k] = v
	}
	out["surface"] = surface

	return out, nil
}
