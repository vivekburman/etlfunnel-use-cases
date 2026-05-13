package client_iso_entity_21

// iso_entity_21 — blinkit / orders / hot flow (STEP-14)

import (
	"context"
	"etlfunnel/execution/models"
	rs "github.com/streamcraft/zomato-etl/db_setup/client/connectors/redis_source"
)

const (
	EntityBaseName = "orders"
	Brand          = "blinkit"
	StreamKey      = "blinkit:orders:stream"
	FlowType       = "hot"
)

func ReadBatch(ctx context.Context, redisClient models.IRedisClient, props *models.ConnectorProps) ([]map[string]any, error) {
	return rs.ReadBatch(ctx, redisClient, props, StreamKey, EntityBaseName)
}

func AckBatch(ctx context.Context, redisClient models.IRedisClient, ids []string) error {
	return rs.AckBatch(ctx, redisClient, StreamKey, ids)
}
