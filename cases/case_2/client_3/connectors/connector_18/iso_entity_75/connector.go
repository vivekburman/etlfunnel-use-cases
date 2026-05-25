package client_connector_18_iso_entity_75

import (
	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
	"fmt"
)

const (
	EntityBaseName = "order_items"
	Brand          = "blinkit"
	FlowType       = "cold"
)

type IUseConnector struct{}

var _ coreinterface.IClientDBPostgresSource = (*IUseConnector)(nil)

func (d *IUseConnector) FetchRecords(param *models.PostgresSourceFetch) <-chan map[string]any {
	panic("unimplemented")
}
func (d *IUseConnector) GenerateQuery(param *models.PostgresSourceQuery) (*models.PostgresSourceQueryTune, error) {
	rp := param.State.GetReplicaProps()
	tableName, _ := rp["table"].(string)
	lastPK, _ := rp["last_pk"].(int64)
	batchSize, _ := rp["batch_size"].(int)
	if batchSize <= 0 {
		batchSize = 1000
	}
	return &models.PostgresSourceQueryTune{
		Query: fmt.Sprintf("SELECT * FROM %s WHERE order_id > %d ORDER BY order_id ASC LIMIT %d", tableName, lastPK, batchSize),
	}, nil
}
func (d *IUseConnector) GenerateNotification(param *models.PostgresSourceNotification) (*models.PostgresSourceNotificationTune, error) {
	panic("unimplemented")
}
func (d *IUseConnector) GenerateWAL(param *models.PostgresSourceWAL) (*models.PostgresSourceWALTune, error) {
	panic("unimplemented")
}