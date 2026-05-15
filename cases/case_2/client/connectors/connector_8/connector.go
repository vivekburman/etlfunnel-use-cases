package client_connector_8

// Zomato Platform Order Intelligence — connector_8: District hot source (STEP-14)
//
// Redis stream source connector for the district brand hot flow.
// Reads WAL change events from "district:orders:stream" via XREADGROUP.
// Implements ReadBatch / AckBatch directly.
//
// iso_entities owned by this connector:
//   iso_entity_29 — orders
//   iso_entity_30 — order_items
//   iso_entity_31 — order_status_events
//   iso_entity_32 — delivery_assignments

import (
	"context"
	"encoding/json"
	"etlfunnel/execution/models"
	"fmt"
	"strings"
)

const (
	Brand         = "district"
	StreamKey     = "district:orders:stream"
	FlowType      = "hot"
	consumerGroup = "elastic_writer_group"
	blockMS       = 2000
)

func ReadBatch(ctx context.Context, redisClient models.IRedisClient, props *models.ConnectorProps, entityTable string) ([]map[string]any, error) {
	count := props.BatchSize
	if count <= 0 {
		count = 100
	}

	consumerName := props.ConsumerName
	if consumerName == "" {
		consumerName = "consumer_default"
	}

	msgs, err := redisClient.XReadGroup(ctx, &models.XReadGroupArgs{
		Group:    consumerGroup,
		Consumer: consumerName,
		Streams:  []string{StreamKey, ">"},
		Count:    int64(count),
		Block:    blockMS,
	})
	if err != nil {
		if strings.Contains(err.Error(), "NOGROUP") {
			return nil, fmt.Errorf("connector_8(%s): consumer group %q not found — ensure it is created before starting the hot flow", Brand, consumerGroup)
		}
		if isTimeout(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("connector_8(%s): XREADGROUP: %w", Brand, err)
	}

	var records []map[string]any
	for _, stream := range msgs {
		for _, msg := range stream.Messages {
			rec, decodeErr := decodeMessage(msg)
			if decodeErr != nil {
				continue
			}
			msgTable, _ := rec["table"].(string)
			if !tableMatches(msgTable, entityTable) {
				_ = redisClient.XAck(ctx, StreamKey, consumerGroup, msg.ID)
				continue
			}
			rec["_redis_stream_id"] = msg.ID
			records = append(records, rec)
		}
	}

	return records, nil
}

func AckBatch(ctx context.Context, redisClient models.IRedisClient, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	return redisClient.XAck(ctx, StreamKey, consumerGroup, ids...)
}

func decodeMessage(msg models.XMessage) (map[string]any, error) {
	if raw, ok := msg.Values["data"].(string); ok {
		var rec map[string]any
		if err := json.Unmarshal([]byte(raw), &rec); err != nil {
			return nil, fmt.Errorf("decode %s: %w", msg.ID, err)
		}
		return rec, nil
	}
	rec := make(map[string]any, len(msg.Values))
	for k, v := range msg.Values {
		if s, ok := v.(string); ok {
			var sub any
			if json.Unmarshal([]byte(s), &sub) == nil {
				rec[k] = sub
				continue
			}
		}
		rec[k] = v
	}
	return rec, nil
}

func tableMatches(msgTable, entity string) bool {
	return msgTable == entity || strings.HasPrefix(msgTable, entity+"_")
}

func isTimeout(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "redis: nil") ||
		strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "EOF")
}
