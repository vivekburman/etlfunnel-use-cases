package client_connector_15_iso_entity_50

// Idea source connector — sim_inventory table, zone-only sharding.
// Table name pattern: sim_inventory_{zone}_{split}
//
// GeoTagger transformer injects the missing "state" column from pipeline context.
//
//	flow name  "idea_north" → zone = "north"
//	table      sim_inventory_north_1

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
	return &models.MySQLSourceQueryTune{Query: fmt.Sprintf("SELECT *, status AS sim_status FROM %s ORDER BY id", table)}, nil
}
func (d *IUseConnector) GenerateBinLog(param *models.MySQLSourceBinlog) (*models.MySQLSourceBinlogTune, error) {
	panic("unimplemented")
}
