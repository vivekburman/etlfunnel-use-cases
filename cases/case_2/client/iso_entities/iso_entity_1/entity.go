package client_iso_entity_1

// iso_entity_1 — zomato_food / orders / cold flow (STEP-12)
//
// Thin stub that registers this pipeline as the "orders" entity for the
// zomato_food brand cold flow.  The orchestrator_1 discovers all city-split
// tables matching "orders_{city}_{n}" on the zomato_food DB and sets
// ConnectorProps.TableName for each iteration.

import (
	"context"
	"etlfunnel/execution/models"
	conn "github.com/streamcraft/zomato-etl/db_setup/client/connectors/connector_1"
)

const (
	EntityBaseName = "orders"
	Brand          = "zomato_food"
	FlowType       = "cold"
)

func FetchBatch(ctx context.Context, dbConn models.IDBConn, props *models.ConnectorProps) ([]map[string]any, error) {
	return conn.FetchBatch(ctx, dbConn, props)
}

func GenerateQuery(props *models.ConnectorProps) string {
	return conn.GenerateQuery(props)
}
