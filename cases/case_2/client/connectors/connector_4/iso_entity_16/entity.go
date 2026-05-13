package client_iso_entity_16

// iso_entity_16 — district / delivery_assignments / cold flow (STEP-12)
// Belongs to connector_4 (district Postgres, port 5444).
// District's delivery_assignments carries gate scan events (scanned_at, gate_id)
// rather than rider dispatch; schema divergence handled by transformer_5.

import (
	"context"
	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
	conn "github.com/streamcraft/zomato-etl/db_setup/client/connectors/connector_4"
)

const (
	EntityBaseName = "delivery_assignments"
	Brand          = "district"
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
