package client_connector_30_iso_entity_109

import (
	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
)

// Hot Flow Stage 1 — Redis Stream destination for Zomato Food WAL ingestion.
// Publishes raw WAL change events via XADD to zomato:orders:stream.

const (
	Brand     = "zomato_food"
	StreamKey = "zomato:orders:stream"
	FlowType  = "hot_stage1"
)

type IUseConnector struct{}

var _ coreinterface.IClientDBRedisDest = (*IUseConnector)(nil)

// GenerateQuery constructs an XADD command targeting zomato:orders:stream.
// The WAL change event record (op, table, city, lsn, ts, after) is published
// as-is — Stage 1 performs no transformation.
//
// ReplicaProps consumed (all optional):
//
//	"max_len" int64 — MAXLEN trim threshold (default 0 = no trim)
//	"approx"  bool  — use approximate trimming MAXLEN ~ (default false)
func (d *IUseConnector) GenerateQuery(param *models.RedisDestQuery) (*models.RedisDestQueryTune, error) {
	rp := param.State.GetReplicaProps()

	var maxLen int64
	if v, ok := rp["max_len"].(int64); ok {
		maxLen = v
	}

	approx := false
	if v, ok := rp["approx"].(bool); ok {
		approx = v
	}

	return &models.RedisDestQueryTune{
		Operation: models.RedisDestOpXAdd,
		Key:       StreamKey,
		Value:     param.Record,
		MaxLen:    maxLen,
		Approx:    approx,
	}, nil
}
