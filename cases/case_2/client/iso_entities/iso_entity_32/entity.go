package client_iso_entity_32

// iso_entity_32 — district / delivery_assignments / hot flow (STEP-14)
//
// District's delivery_assignments in the hot flow carries gate scan events
// (scanned_at, gate_id) rather than rider dispatch — same stream, different
// schema.  transformer_5 handles the mapping.

import (
	"context"
	"etlfunnel/execution/models"
	rs "github.com/streamcraft/zomato-etl/db_setup/client/connectors/redis_source"
)

const (
	EntityBaseName = "delivery_assignments"
	Brand          = "district"
	StreamKey      = "district:orders:stream"
	FlowType       = "hot"
)

func ReadBatch(ctx context.Context, redisClient models.IRedisClient, props *models.ConnectorProps) ([]map[string]any, error) {
	return rs.ReadBatch(ctx, redisClient, props, StreamKey, EntityBaseName)
}

func AckBatch(ctx context.Context, redisClient models.IRedisClient, ids []string) error {
	return rs.AckBatch(ctx, redisClient, StreamKey, ids)
}
