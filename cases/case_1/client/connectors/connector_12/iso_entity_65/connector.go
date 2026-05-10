// Postgres destination connector — writes to raw.billing_accounts (landing zone, plain INSERT).
package client_connector_12_iso_entity_65

import (
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
)

var destinationColumns = []string{
	"source_company",
	"msisdn",
	"amount_due",
	"amount_paid",
	"cycle_start",
	"cycle_end",
	"zone",
	"state",
}

var requiredColumns = map[string]bool{
	"msisdn":         true,
	"source_company": true,
	"zone":           true,
	"state":          true,
}

type IUseConnector struct{}

var _ coreinterface.IClientDBPostgresDest = (*IUseConnector)(nil)

func (d *IUseConnector) GenerateQuery(param *models.PostgresDestQuery) (*models.PostgresDestQueryTune, error) {
	if err := ulib.ValidateRecord(param.Record, requiredColumns); err != nil {
		return nil, err
	}
	return &models.PostgresDestQueryTune{
		Query: ulib.BuildInsertQuery("raw.billing_accounts", destinationColumns),
		Value: ulib.ExtractValues(param.Record, destinationColumns),
	}, nil
}
