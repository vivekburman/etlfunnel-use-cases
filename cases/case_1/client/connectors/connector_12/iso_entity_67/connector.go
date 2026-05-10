// Postgres destination connector — writes to raw.port_history (landing zone, plain INSERT).
package client_connector_12_iso_entity_67

import (
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
)

var destinationColumns = []string{
	"source_company",
	"msisdn",
	"port_direction",
	"from_carrier",
	"to_carrier",
	"port_date",
	"porting_ref",
	"zone",
	"state",
}

var requiredColumns = map[string]bool{
	"msisdn":         true,
	"source_company": true,
	"zone":           true,
	"state":          true,
	"port_direction": true,
	"port_date":      true,
}

type IUseConnector struct{}

var _ coreinterface.IClientDBPostgresDest = (*IUseConnector)(nil)

func (d *IUseConnector) GenerateQuery(param *models.PostgresDestQuery) (*models.PostgresDestQueryTune, error) {
	if err := ulib.ValidateRecord(param.Record, requiredColumns); err != nil {
		return nil, err
	}
	return &models.PostgresDestQueryTune{
		Query: ulib.BuildInsertQuery("raw.port_history", destinationColumns),
		Value: ulib.ExtractValues(param.Record, destinationColumns),
	}, nil
}
