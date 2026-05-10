package client_transformer_33

import (
	"etlfunnel/execution/models"
	"fmt"
)

// NullHandler: apply per-field null rules before the record reaches the destination.
//
// Rules:
//   msisdn          → nil is a hard error: record goes to backlog (return error)
//   plan_code       → nil defaults to "UNKNOWN"
//   name            → nil defaults to "N/A"
//   email           → nil allowed; no default
//   pincode         → nil defaults to "000000"
//   dob             → nil allowed; flag dob_flagged = true for manual review
//   outstanding_dues → nil defaults to 0.0
//   last_payment_amount → nil defaults to 0.0
//   bill_amount     → nil defaults to 0.0
//   data_mb         → nil defaults to 0.0

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
		"pincode":   "000000",
	}
	for col, def := range stringDefaults {
		if v, ok := rec[col]; !ok || v == nil {
			rec[col] = def
		}
	}

	// Numeric defaults
	numericDefaults := []string{"amount_due", "amount_paid", "data_mb"}
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
