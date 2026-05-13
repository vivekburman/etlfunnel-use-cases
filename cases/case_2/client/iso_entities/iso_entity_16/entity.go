package client_iso_entity_16

// iso_entity_16 — district / delivery_assignments / cold flow (STEP-12)
//
// Note: District's delivery_assignments table stores gate scan events
// (scanned_at, gate_id) rather than rider dispatch.  The shared SELECT
// pattern is identical; schema divergence is handled by transformer_5.

import (
	"context"
	"etlfunnel/execution/models"
	conn "github.com/streamcraft/zomato-etl/db_setup/client/connectors/connector_4"
)

const (
	EntityBaseName = "delivery_assignments"
	Brand          = "district"
	FlowType       = "cold"
)

func FetchBatch(ctx context.Context, dbConn models.IDBConn, props *models.ConnectorProps) ([]map[string]any, error) {
	return conn.FetchBatch(ctx, dbConn, props)
}

func GenerateQuery(props *models.ConnectorProps) string {
	return conn.GenerateQuery(props)
}
