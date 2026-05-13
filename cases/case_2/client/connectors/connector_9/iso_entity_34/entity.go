package client_iso_entity_34

// iso_entity_34 — hot flow ES write + XACK (STEP-13)
// Belongs to connector_9 (Elasticsearch destination).
// Receives transformed WAL records from the hot Redis stream chain,
// bulk-writes to platform_orders, then signals ACK readiness via the
// returned BulkResult so the caller can XACK only confirmed documents.

import (
	"context"
	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
	"fmt"
	conn "github.com/streamcraft/zomato-etl/db_setup/client/connectors/connector_9"
)

const (
	FlowType = "hot"
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

// BulkWrite writes records to ES and returns the result.
// The caller must XACK only the IDs corresponding to Indexed documents —
// Rejected entries must NOT be acked so they can be reprocessed or backlogged.
func BulkWrite(ctx context.Context, props *models.ConnectorProps, records []map[string]any) (*conn.BulkResult, error) {
	return conn.BulkWrite(ctx, props, records)
}
