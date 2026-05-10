// Postgres destination connector — writes to raw.subscriptions (landing zone, plain INSERT).
package client_connector_12_iso_entity_64

import (
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
)

var destinationColumns = []string{
	"source_company",
	"msisdn",
	"plan_code",
	"plan_start",
	"plan_end",
	"status",
	"zone",
	"state",
}

var requiredColumns = map[string]bool{
	"msisdn":         true,
	"source_company": true,
	"zone":           true,
	"state":          true,
	"plan_code":      true,
}

type IUseConnector struct{}

var _ coreinterface.IClientDBPostgresDest = (*IUseConnector)(nil)

func (d *IUseConnector) GenerateQuery(param *models.PostgresDestQuery) (*models.PostgresDestQueryTune, error) {
	if err := ulib.ValidateRecord(param.Record, requiredColumns); err != nil {
		return nil, err
	}
	return &models.PostgresDestQueryTune{
		Query: ulib.BuildInsertQuery("raw.subscriptions", destinationColumns),
		Value: ulib.ExtractValues(param.Record, destinationColumns),
	}, nil
}
