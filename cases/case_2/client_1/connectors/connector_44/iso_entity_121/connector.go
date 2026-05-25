package client_connector_44_iso_entity_121

import (
	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
)

// Hot Flow Stage 1 — WAL Ingestion for District DB.
// Reads from the district_slot logical replication slot and publishes raw
// change events to the district:orders:stream Redis stream.

const (
	Brand           = "district"
	SlotName        = "district_slot"
	PublicationName = "district_pub"
	RedisStream     = "district:orders:stream"
	FlowType        = "hot_stage1"
)

type IUseConnector struct{}

var _ coreinterface.IClientDBPostgresSource = (*IUseConnector)(nil)

func (d *IUseConnector) FetchRecords(param *models.PostgresSourceFetch) <-chan map[string]any {
	panic("unimplemented")
}
func (d *IUseConnector) GenerateQuery(param *models.PostgresSourceQuery) (*models.PostgresSourceQueryTune, error) {
	panic("unimplemented")
}
func (d *IUseConnector) GenerateNotification(param *models.PostgresSourceNotification) (*models.PostgresSourceNotificationTune, error) {
	panic("unimplemented")
}

// GenerateWAL returns the replication configuration for the District logical
// replication slot. ReplicaProps consumed (all optional):
//
//	"slot_name"        string  — override the replication slot name
//	"publication_name" string  — override the publication name
//	"streaming"        bool    — set false to disable streaming mode (default true)
func (d *IUseConnector) GenerateWAL(param *models.PostgresSourceWAL) (*models.PostgresSourceWALTune, error) {
	rp := param.State.GetReplicaProps()

	slotName := SlotName
	if v, ok := rp["slot_name"].(string); ok && v != "" {
		slotName = v
	}

	publicationName := PublicationName
	if v, ok := rp["publication_name"].(string); ok && v != "" {
		publicationName = v
	}

	streaming := true
	if v, ok := rp["streaming"].(bool); ok {
		streaming = v
	}

	return &models.PostgresSourceWALTune{
		SlotName:        slotName,
		OutputPlugin:    models.PostgresCDCTypePGOutput,
		PublicationName: publicationName,
		Streaming:       streaming,
	}, nil
}
