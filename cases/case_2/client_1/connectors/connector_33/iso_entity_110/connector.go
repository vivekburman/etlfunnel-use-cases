package client_connector_33_iso_entity_110

import (
	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
)

// Hot Flow Stage 1 — Redis Stream destination for Blinkit WAL ingestion.
// Publishes raw WAL change events via XADD to blinkit:orders:stream.

const (
	Brand     = "blinkit"
	StreamKey = "blinkit:orders:stream"
	FlowType  = "hot_stage1"
)

type IUseConnector struct{}

var _ coreinterface.IClientDBRedisDest = (*IUseConnector)(nil)

// GenerateQuery constructs an XADD command targeting blinkit:orders:stream.
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
