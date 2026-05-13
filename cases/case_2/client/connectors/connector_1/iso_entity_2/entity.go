package client_iso_entity_2

// iso_entity_2 — zomato_food / order_items / cold flow (STEP-12)
// Belongs to connector_1 (zomato_food Postgres, port 5441).

import (
	"context"
	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
	conn "github.com/streamcraft/zomato-etl/db_setup/client/connectors/connector_1"
)

const (
	EntityBaseName = "order_items"
	Brand          = "zomato_food"
	FlowType       = "cold"
)

type IUseConnector struct{}

var _ coreinterface.IClientDBPostgresSource = (*IUseConnector)(nil)

func (d *IUseConnector) FetchRecords(param *models.PostgresSourceFetch) <-chan map[string]any {
	panic("unimplemented")
}

func (d *IUseConnector) GenerateQuery(param *models.PostgresSourceQuery) (*models.PostgresSourceQueryTune, error) {
	rp := param.State.GetReplicaProps()
	tableName, _ := rp["table_name"].(string)
	lastPK, _ := rp["last_pk"].(int64)
	batchSize, _ := rp["batch_size"].(int)
	props := &models.ConnectorProps{
		TableName: tableName,
		LastPK:    lastPK,
		BatchSize: batchSize,
	}
	return &models.PostgresSourceQueryTune{Query: conn.GenerateQuery(props)}, nil
}

func (d *IUseConnector) GenerateNotification(param *models.PostgresSourceNotification) (*models.PostgresSourceNotificationTune, error) {
	panic("unimplemented")
}

func (d *IUseConnector) GenerateWAL(param *models.PostgresSourceWAL) (*models.PostgresSourceWALTune, error) {
	panic("unimplemented")
}

func FetchBatch(ctx context.Context, dbConn models.IDBConn, props *models.ConnectorProps) ([]map[string]any, error) {
	return conn.FetchBatch(ctx, dbConn, props)
}

func GenerateQuery(props *models.ConnectorProps) string {
	return conn.GenerateQuery(props)
}
