package client_connector_24_iso_entity_100

import (
	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
)
const (
	EntityBaseName = "order_status_events"
	Brand          = "district"
	StreamKey      = "district:orders:stream"
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
		StreamNames:   []string{StreamKey},
		ConsumerGroup: "elastic_writer_group",
		ConsumerName:  consumerName,
		BatchSize:     batchSize,
		BlockTime:     2000,
		StartFrom:     "0",
	}, nil
}
func (d *IUseConnector) GenerateKeyspace(param *models.RedisSourceKeyspace) (*models.RedisSourceKeySpacesTune, error) {
	panic("unimplemented")
}
func (d *IUseConnector) FetchRecords(param *models.RedisSourceFetch) <-chan map[string]any {
	panic("unimplemented")
}