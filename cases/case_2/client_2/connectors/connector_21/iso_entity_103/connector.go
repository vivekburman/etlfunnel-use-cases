package client_connector_21_iso_entity_103

import (
	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
)

type IUseConnector struct{}

var _ coreinterface.IClientDBElasticDest = (*IUseConnector)(nil)

func (d *IUseConnector) GenerateQuery(param *models.ElasticDestQuery) (*models.ElasticDestQueryTune, error) {
	panic("unimplemented")
}