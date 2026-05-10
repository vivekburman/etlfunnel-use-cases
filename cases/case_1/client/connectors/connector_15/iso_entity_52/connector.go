package client_connector_15_iso_entity_52

// Idea source connector — customers table, zone-only sharding.
// Table name pattern: customers_{zone}_{split}
//
// Idea did not shard by state before the merger, so the table name encodes only
// the zone. The GeoTagger transformer injects the missing "state" column
// from the pipeline context so the destination always has both dimensions.
//
//	flow name  "idea_north" → zone = "north"
//	table      customers_north_1

import (
	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
	"fmt"
)

type IUseConnector struct{}

var _ coreinterface.IClientDBMySQLSource = (*IUseConnector)(nil)

func (d *IUseConnector) FetchRecords(param *models.MySQLSourceFetch) <-chan map[string]any {
	panic("unimplemented")
}
func (d *IUseConnector) GenerateQuery(param *models.MySQLSourceQuery) (*models.MySQLSourceQueryTune, error) {
	table, ok := param.State.GetReplicaProps()["table"].(string)
	if !ok || table == "" {
		return nil, fmt.Errorf("missing 'table' key in replica props")
	}
	return &models.MySQLSourceQueryTune{Query: fmt.Sprintf("SELECT * FROM %s ORDER BY id", table)}, nil
}
func (d *IUseConnector) GenerateBinLog(param *models.MySQLSourceBinlog) (*models.MySQLSourceBinlogTune, error) {
	panic("unimplemented")
}
