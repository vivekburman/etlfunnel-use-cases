package client_transformer_27

import (
	"etlfunnel/execution/models"
)

// GeoTagger: inject the missing "state" field for Idea records.
//
// Idea uses zone-only sharding — source tables contain a "zone" column but no "state" column.
// The destination schema partitions by both zone and state, so a placeholder must be injected.
// "NA" signals that state was not captured at source and must not be used for geographic filtering.

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	rec := param.Record
	if _, ok := rec["state"]; !ok {
		rec["state"] = "NA"
	}
	return rec, nil
}
