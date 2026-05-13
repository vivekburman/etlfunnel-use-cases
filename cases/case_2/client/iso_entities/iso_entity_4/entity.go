package client_iso_entity_4

// iso_entity_4 — zomato_food / delivery_assignments / cold flow (STEP-12)

import (
	"context"
	"etlfunnel/execution/models"
	conn "github.com/streamcraft/zomato-etl/db_setup/client/connectors/connector_1"
)

const (
	EntityBaseName = "delivery_assignments"
	Brand          = "zomato_food"
	FlowType       = "cold"
)

func FetchBatch(ctx context.Context, dbConn models.IDBConn, props *models.ConnectorProps) ([]map[string]any, error) {
	return conn.FetchBatch(ctx, dbConn, props)
}

func GenerateQuery(props *models.ConnectorProps) string {
	return conn.GenerateQuery(props)
}
