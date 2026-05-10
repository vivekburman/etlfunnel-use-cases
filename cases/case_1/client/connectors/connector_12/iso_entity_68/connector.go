// Postgres destination connector — writes to raw.customers.
//
// Uses an upsert (ON CONFLICT DO UPDATE) because raw.customers has a unique
// constraint on (msisdn, source_company), making the write idempotent across retries.
package client_connector_12_iso_entity_68

import (
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
)

var destinationColumns = []string{
	"msisdn",
	"name",
	"dob",
	"aadhaar_hash",
	"pan_hash",
	"email",
	"address",
	"activation_date",
	"zone",
	"state",
	"source_company",
	"table_split",
	"batch_id",
	"raw_record",
}

var requiredColumns = map[string]bool{
	"msisdn":         true,
	"source_company": true,
	"zone":           true,
	"state":          true,
}

// skipOnUpdate keeps identity and audit keys immutable when a conflict is resolved.
var skipOnUpdate = map[string]bool{
	"msisdn":         true,
	"source_company": true,
}

type IUseConnector struct{}

var _ coreinterface.IClientDBPostgresDest = (*IUseConnector)(nil)

func (d *IUseConnector) GenerateQuery(param *models.PostgresDestQuery) (*models.PostgresDestQueryTune, error) {
	if err := ulib.ValidateRecord(param.Record, requiredColumns); err != nil {
		return nil, err
	}
	return &models.PostgresDestQueryTune{
		Query: ulib.BuildUpsertQuery("raw.customers", destinationColumns, []string{"msisdn", "source_company"}, skipOnUpdate),
		Value: ulib.ExtractValues(param.Record, destinationColumns),
	}, nil
}
