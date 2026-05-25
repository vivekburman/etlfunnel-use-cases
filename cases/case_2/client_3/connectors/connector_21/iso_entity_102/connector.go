package client_connector_21_iso_entity_102

import (
	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
	"fmt"
)
const (
	FlowType = "cold"
)

type IUseConnector struct{}

var _ coreinterface.IClientDBElasticDest = (*IUseConnector)(nil)

func (d *IUseConnector) GenerateQuery(param *models.ElasticDestQuery) (*models.ElasticDestQueryTune, error) {
	rec := param.Record
	subBrand, _ := rec["sub_brand"].(string)
	orderID := fmt.Sprintf("%v", rec["order_id"])
	docID := ""
	if subBrand != "" && orderID != "" && orderID != "<nil>" {
		docID = subBrand + "_" + orderID
	}
	return &models.ElasticDestQueryTune{
		Index:     "platform_orders",
		DocID:     docID,
		Document:  rec,
		Operation: models.ElasticWriteIndex,
	}, nil
}