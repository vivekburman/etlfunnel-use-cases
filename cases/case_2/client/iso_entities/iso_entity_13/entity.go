package client_iso_entity_13

// iso_entity_13 — district / orders / cold flow (STEP-12)

import (
	"context"
	"etlfunnel/execution/models"
	conn "github.com/streamcraft/zomato-etl/db_setup/client/connectors/connector_4"
)

const (
	EntityBaseName = "orders"
	Brand          = "district"
	FlowType       = "cold"
)

func FetchBatch(ctx context.Context, dbConn models.IDBConn, props *models.ConnectorProps) ([]map[string]any, error) {
	return conn.FetchBatch(ctx, dbConn, props)
}

func GenerateQuery(props *models.ConnectorProps) string {
	return conn.GenerateQuery(props)
}
