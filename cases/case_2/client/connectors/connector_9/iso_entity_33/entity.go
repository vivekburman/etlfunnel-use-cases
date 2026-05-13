package client_iso_entity_33

// iso_entity_33 — cold flow ES write (STEP-13)
// Belongs to connector_9 (Elasticsearch destination).
// Receives transformed records from the cold Postgres backfill chain and
// bulk-writes them to the platform_orders index.

import (
	"context"
	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
	"fmt"
	conn "github.com/streamcraft/zomato-etl/db_setup/client/connectors/connector_9"
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

func BulkWrite(ctx context.Context, props *models.ConnectorProps, records []map[string]any) (*conn.BulkResult, error) {
	return conn.BulkWrite(ctx, props, records)
}
