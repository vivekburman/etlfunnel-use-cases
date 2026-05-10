package client_transformer_29

import (
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
	"fmt"
)

// GeoTagger (Tata Docomo): Tata Docomo shards by state — inject zone from the replica table name.
// Table format: "{entityBaseName}_{zone}_{splitIndex}" e.g. "customers_maharashtra_1".
func Transformer(param *models.TransformerProps) (map[string]any, error) {
	props := param.State.GetReplicaProps()
	table, _ := props["table"].(string)
	entityBase, _ := props["entityBaseName"].(string)
	zone, err := ulib.ParseZoneFromTable(table, entityBase)
	if err != nil {
		return nil, fmt.Errorf("GeoTagger(tata_docomo): %w", err)
	}

	rec := param.Record
	rec["zone"] = zone
	return rec, nil
}
