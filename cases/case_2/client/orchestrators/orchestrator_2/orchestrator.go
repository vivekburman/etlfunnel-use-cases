package client_orchestrator_2

// Zomato Platform Order Intelligence — orchestrator_2: Hot Flow (STEP-16)
//
// Prepares the Redis consumer group for a brand's hot flow and returns one
// OrchestratorTune per entity so the framework can spin up the hot pipeline.
//
// Startup sequence:
//   1. Ensure consumer group `elastic_writer_group` exists on the brand's
//      Redis stream (`XGROUP CREATE {stream} elastic_writer_group $ MKSTREAM`).
//      The `$` start ID means new messages only — the WAL LSN bookmark ensures
//      the cold and hot flows cover the full history without gap.
//   2. For resume after a restart: read the last acknowledged stream ID from
//      AuxDB `pipeline_checkpoints`. If a checkpoint exists, the hot iso-entity
//      will use XAUTOCLAIM to recover pending (unacknowledged) entries.
//   3. Return one OrchestratorTune per entity (4 per brand) so each entity's
//      hot pipeline lane reads from the same stream but filters by table name.
//
// Consumer group idempotency:
//   `XGROUP CREATE ... MKSTREAM` returns BUSYGROUP if the group already exists.
//   This is treated as success (idempotent startup).
//
// Stream-per-brand, not stream-per-entity:
//   All four entities share one Redis stream per brand. The hot iso-entity
//   filters messages by the `table` field. This keeps the WAL consumer simple
//   (one publisher per brand) while allowing entity-level parallelism.

import (
	"context"
	"etlfunnel/execution/models"
	ulib "etlfunnel/execution/client/userlibraries"
	"fmt"
	"strings"
)

const (
	consumerGroup = "elastic_writer_group"
	entities      = 4 // orders, order_items, order_status_events, delivery_assignments
)

var entityNames = [entities]string{
	"orders",
	"order_items",
	"order_status_events",
	"delivery_assignments",
}

// Orchestrate registers the consumer group and returns one tune per entity.
func Orchestrate(ctx context.Context, param *models.OrchestratorProps) ([]*models.OrchestratorTune, error) {
	brand := param.Brand
	streamKey := param.StreamKey // e.g. "zomato:orders:stream", set by pipeline config

	if err := ensureConsumerGroup(ctx, param.RedisClient, streamKey); err != nil {
		return nil, fmt.Errorf("orchestrator_2: consumer group setup for %s: %w", brand, err)
	}

	// Load last checkpointed stream IDs from AuxDB (one per entity).
	checkpoints, err := loadHotCheckpoints(ctx, param, brand)
	if err != nil {
		param.State.GetLogger().Warn(fmt.Sprintf(
			"orchestrator_2: checkpoint load failed (%v) — starting from stream head for %s", err, brand,
		))
		checkpoints = map[string]string{}
	}

	tunes := make([]*models.OrchestratorTune, 0, entities)
	for _, entity := range entityNames {
		lastStreamID := checkpoints[entity]
		if lastStreamID == "" {
			lastStreamID = ">" // ">" = only new messages; consumer group tracks cursor
		}

		tunes = append(tunes, &models.OrchestratorTune{
			ConnectorProps: &models.ConnectorProps{
				StreamKey:    streamKey,
				Entity:       entity,
				Brand:        brand,
				ConsumerName: consumerNameFor(brand, entity),
				LastStreamID: lastStreamID,
				BatchSize:    param.BatchSize,
			},
			Concurrency: 1, // one consumer per entity per brand
		})
	}

	param.State.GetLogger().Debug(fmt.Sprintf(
		"orchestrator_2: consumer group %q ready on stream %s — %d entity tune(s) returned",
		consumerGroup, streamKey, len(tunes),
	))

	return tunes, nil
}

// ensureConsumerGroup runs XGROUP CREATE ... MKSTREAM idempotently.
// BUSYGROUP (group already exists) is treated as success.
func ensureConsumerGroup(ctx context.Context, redisClient models.IRedisClient, streamKey string) error {
	err := redisClient.XGroupCreateMkStream(ctx, streamKey, consumerGroup, "$")
	if err != nil && !isBUSYGROUP(err) {
		return err
	}
	return nil
}

// loadHotCheckpoints reads the last redis_stream_id per entity for this brand
// from AuxDB `pipeline_checkpoints` (flow_type = 'hot').
func loadHotCheckpoints(
	ctx context.Context,
	param *models.OrchestratorProps,
	brand string,
) (map[string]string, error) {
	pgConn, err := ulib.GetAuxPostgresConn(param.AuxiliaryDBConnMap)
	if err != nil {
		return nil, err
	}

	rows, err := pgConn.Query(ctx, `
		SELECT entity, redis_stream_id
		FROM pipeline_checkpoints
		WHERE sub_brand = $1
		  AND flow_type = 'hot'
		  AND redis_stream_id IS NOT NULL
		  AND redis_stream_id != ''
		ORDER BY checkpoint_ts DESC`,
		brand,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Take the most recent stream ID per entity.
	result := make(map[string]string)
	for rows.Next() {
		var entity, streamID string
		if scanErr := rows.Scan(&entity, &streamID); scanErr == nil {
			// Because ORDER BY checkpoint_ts DESC, first row per entity wins.
			if _, seen := result[entity]; !seen {
				result[strings.TrimPrefix(entity, "pipeline_")] = streamID
			}
		}
	}
	return result, rows.Err()
}

func consumerNameFor(brand, entity string) string {
	// e.g. "zomato_food:orders:consumer"
	return brand + ":" + entity + ":consumer"
}

func isBUSYGROUP(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}
