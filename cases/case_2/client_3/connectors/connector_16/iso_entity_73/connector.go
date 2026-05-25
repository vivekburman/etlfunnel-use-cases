package client_connector_16_iso_entity_73

import (
	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
)

type IUseConnector struct{}

var _ coreinterface.IClientDBPostgresSource = (*IUseConnector)(nil)

func (d *IUseConnector) FetchRecords(param *models.PostgresSourceFetch) <-chan map[string]any {
	panic("unimplemented")
}
func (d *IUseConnector) GenerateQuery(param *models.PostgresSourceQuery) (*models.PostgresSourceQueryTune, error) {
	panic("unimplemented")
}
func (d *IUseConnector) GenerateNotification(param *models.PostgresSourceNotification) (*models.PostgresSourceNotificationTune, error) {
	panic("unimplemented")
}
func (d *IUseConnector) GenerateWAL(param *models.PostgresSourceWAL) (*models.PostgresSourceWALTune, error) {
	panic("unimplemented")
}