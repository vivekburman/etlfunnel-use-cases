package client_transformer_39

import (
	"etlfunnel/execution/models"
	"fmt"
)

// NullHandler: apply per-field null rules before the record reaches the destination.
//
// Rules:
//   msisdn     → nil is a hard error: record goes to backlog (return error)
//   plan_code  → nil defaults to "UNKNOWN"
//   name       → nil defaults to "N/A"
//   amount_due → nil defaults to 0.0
//   amount_paid → nil defaults to 0.0
//   dob        → nil allowed; flag dob_flagged = true for manual review

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	rec := param.Record

	// Critical field — cannot continue without MSISDN
	if msisdn, ok := rec["msisdn"]; !ok || msisdn == nil {
		return nil, fmt.Errorf("NullHandler: msisdn is null or missing — record cannot be routed")
	}

	// String defaults
	stringDefaults := map[string]string{
		"plan_code": "UNKNOWN",
		"name":      "N/A",
	}
	for col, def := range stringDefaults {
		if v, ok := rec[col]; !ok || v == nil {
			rec[col] = def
		}
	}

	// Numeric defaults
	numericDefaults := []string{"amount_due", "amount_paid"}
	for _, col := range numericDefaults {
		if v, ok := rec[col]; !ok || v == nil {
			rec[col] = 0.0
		}
	}

	// DOB null — flag for manual review, do not fail
	if v, ok := rec["dob"]; !ok || v == nil {
		rec["dob"] = nil
		rec["dob_flagged"] = true
	}

	return rec, nil
}
