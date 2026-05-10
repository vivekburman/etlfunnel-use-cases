package client_transformer_19

import (
	"context"
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
	"fmt"
)

// PlanMapper: resolve source plan_code to destination plan_code via AuxDB plan_mapping table.
//
// AuxDB table schema:
//   plan_mapping(source_company TEXT, source_plan_code TEXT, destination_plan_code TEXT)
//
// If no mapping is found the record is not failed — plan_code is set to "UNMAPPED_<original>"
// so it is visible in the destination and can be corrected via the backlog reprocessing flow.

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	rec := param.Record

	sourcePlanCode, ok := rec["plan_code"]
	if !ok || sourcePlanCode == nil {
		// NullHandler should have defaulted this to "UNKNOWN" already; nothing to map.
		return rec, nil
	}

	planCodeStr, ok := sourcePlanCode.(string)
	if !ok {
		return nil, fmt.Errorf("PlanMapper: plan_code is not a string, got %T", sourcePlanCode)
	}

	company, _ := rec["source_company"].(string)

	pgConn, err := ulib.GetAuxPostgresConn(param.AuxiliaryDBConnMap)
	if err != nil {
		return nil, fmt.Errorf("PlanMapper: %w", err)
	}

	var destPlanCode string
	query := `SELECT destination_plan_code FROM plan_mapping WHERE source_company = $1 AND source_plan_code = $2 LIMIT 1`
	row := pgConn.QueryRow(context.Background(), query, company, planCodeStr)
	if err := row.Scan(&destPlanCode); err != nil {
		// No mapping found — mark as unmapped rather than failing the record.
		rec["plan_code"] = "UNMAPPED_" + planCodeStr
		return rec, nil
	}

	rec["plan_code"] = destPlanCode
	return rec, nil
}
