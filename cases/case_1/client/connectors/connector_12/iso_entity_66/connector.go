// Postgres destination connector — writes to raw.sim_inventory (landing zone, plain INSERT).
package client_connector_12_iso_entity_66

import (
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
)

var destinationColumns = []string{
	"source_company",
	"msisdn",
	"sim_serial",
	"imsi",
	"sim_status",
	"activated_date",
	"deactivated_date",
	"zone",
	"state",
}

var requiredColumns = map[string]bool{
	"msisdn":         true,
	"source_company": true,
	"zone":           true,
	"state":          true,
	"sim_serial":     true,
}

type IUseConnector struct{}

var _ coreinterface.IClientDBPostgresDest = (*IUseConnector)(nil)

func (d *IUseConnector) GenerateQuery(param *models.PostgresDestQuery) (*models.PostgresDestQueryTune, error) {
	if err := ulib.ValidateRecord(param.Record, requiredColumns); err != nil {
		return nil, err
	}
	return &models.PostgresDestQueryTune{
		Query: ulib.BuildInsertQuery("raw.sim_inventory", destinationColumns),
		Value: ulib.ExtractValues(param.Record, destinationColumns),
	}, nil
}
