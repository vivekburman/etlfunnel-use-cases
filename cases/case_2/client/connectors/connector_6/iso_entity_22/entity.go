package client_iso_entity_22

// iso_entity_22 — blinkit / order_items / hot flow (STEP-14)
// Belongs to connector_6 (blinkit Redis stream).

import (
	"context"
	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
	conn "github.com/streamcraft/zomato-etl/db_setup/client/connectors/connector_6"
)

const (
	EntityBaseName = "order_items"
	Brand          = "blinkit"
	FlowType       = "hot"
)

type IUseConnector struct{}

var _ coreinterface.IClientDBRedisSource = (*IUseConnector)(nil)

func (d *IUseConnector) GenerateKeys(param *models.RedisSourceKeys) (*models.RedisSourceKeysTune, error) {
	panic("unimplemented")
}

func (d *IUseConnector) GenerateStreams(param *models.RedisSourceStreams) (*models.RedisSourceStreamsTune, error) {
	rp := param.State.GetReplicaProps()
	consumerName, _ := rp["consumer_name"].(string)
	batchSize, _ := rp["batch_size"].(int)
	if consumerName == "" {
		consumerName = "consumer_default"
	}
	if batchSize <= 0 {
		batchSize = 100
	}
	return &models.RedisSourceStreamsTune{
		StreamNames:   []string{conn.StreamKey},
		ConsumerGroup: "elastic_writer_group",
		ConsumerName:  consumerName,
		BatchSize:     batchSize,
		BlockTime:     2000,
	}, nil
}

func (d *IUseConnector) GenerateKeyspace(param *models.RedisSourceKeyspace) (*models.RedisSourceKeySpacesTune, error) {
	panic("unimplemented")
}

func (d *IUseConnector) FetchRecords(param *models.RedisSourceFetch) <-chan map[string]any {
	panic("unimplemented")
}

func ReadBatch(ctx context.Context, redisClient models.IRedisClient, props *models.ConnectorProps) ([]map[string]any, error) {
	return conn.ReadBatch(ctx, redisClient, props, EntityBaseName)
}

func AckBatch(ctx context.Context, redisClient models.IRedisClient, ids []string) error {
	return conn.AckBatch(ctx, redisClient, ids)
}
