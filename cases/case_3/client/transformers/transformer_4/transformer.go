package client_transformer_4

// PropertyInjector — injects the GA4 property ID string into every record from
// ReplicaProps.  Needed as part of the composite primary key
// (property_id, report_date, session_id) in dbo.ga4_sessions.

import (
	"fmt"

	"etlfunnel/execution/models"
)

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	rp := param.State.GetReplicaProps()

	propertyID, _ := rp["property_id"].(string)
	if propertyID == "" {
		return nil, fmt.Errorf("PropertyInjector: replica prop 'property_id' is empty")
	}

	out := make(map[string]any, len(param.Record)+1)
	for k, v := range param.Record {
		out[k] = v
	}
	out["property_id"] = propertyID

	return out, nil
}
