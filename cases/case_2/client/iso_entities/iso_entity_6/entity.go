package client_iso_entity_6

// iso_entity_6 — blinkit / order_items / cold flow (STEP-12)

import (
	"context"
	"etlfunnel/execution/models"
	conn "github.com/streamcraft/zomato-etl/db_setup/client/connectors/connector_2"
)

const (
	EntityBaseName = "order_items"
	Brand          = "blinkit"
	FlowType       = "cold"
)

func FetchBatch(ctx context.Context, dbConn models.IDBConn, props *models.ConnectorProps) ([]map[string]any, error) {
	return conn.FetchBatch(ctx, dbConn, props)
}

func GenerateQuery(props *models.ConnectorProps) string {
	return conn.GenerateQuery(props)
}
