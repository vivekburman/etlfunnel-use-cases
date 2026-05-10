// Idea source connector — port_history table, zone-only sharding.
// Table name pattern: port_history_{zone}_{split}
//
// GeoTagger transformer injects the missing "state" column from pipeline context.
//
//	flow name  "idea_north" → zone = "north"
//	table      port_history_north_1
package client_connector_15_iso_entity_51

import (
	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
	"fmt"
	"strings"
)

type IUseConnector struct{}

var _ coreinterface.IClientDBMySQLSource = (*IUseConnector)(nil)

func (d *IUseConnector) FetchRecords(param *models.MySQLSourceFetch) <-chan map[string]any {
	panic("unimplemented")
}
func (d *IUseConnector) GenerateQuery(param *models.MySQLSourceQuery) (*models.MySQLSourceQueryTune, error) {
	zone, err := parseZone(param.State.GetFlowName())
	if err != nil {
		return nil, err
	}
	table := fmt.Sprintf("port_history_%s_1", zone)
	query := fmt.Sprintf("SELECT * FROM %s ORDER BY id", table)
	return &models.MySQLSourceQueryTune{Query: query}, nil
}
func (d *IUseConnector) GenerateBinLog(param *models.MySQLSourceBinlog) (*models.MySQLSourceBinlogTune, error) {
	panic("unimplemented")
}
func parseZone(flowName string) (string, error) {
	parts := strings.Split(flowName, "_")
	if len(parts) < 2 {
		return "", fmt.Errorf("unexpected flow name %q: expected {company}_{zone}", flowName)
	}
	zone := parts[len(parts)-1]
	if zone == "" {
		return "", fmt.Errorf("could not derive zone from flow=%q", flowName)
	}
	return zone, nil
}
