package client_transformer_26

import (
	"etlfunnel/execution/models"
)

// columnMap normalises Aircel's source column names to the unified destination schema.
// Columns not present here pass through unchanged (e.g. activated_date, deactivated_date,
// port_date, porting_ref — all hardcoded in the Aircel DDL and already canonical).
var columnMap = map[string]string{
	// ----- shared / identity -----
	"msisdn": "msisdn",
	"name":   "name",
	"dob":    "dob",
	"zone":   "zone",
	"state":  "state",

	// ----- customers -----
	"aadhaar_id":    "aadhaar",          // PIIMasker hashes → aadhaar_hash
	"pan_id":        "pan",              // PIIMasker hashes → pan_hash
	"email_address": "email",
	"full_address":  "address",
	"activated_at":  "activation_date",

	// ----- subscriptions -----
	"product_code":       "plan_code",
	"start_date":         "plan_start",
	"end_date":           "plan_end",
	"subscription_state": "status",

	// ----- billing_accounts -----
	"balance_due":  "amount_due",
	"balance_paid": "amount_paid",
	"cycle_open":   "cycle_start",
	"cycle_close":  "cycle_end",

	// ----- sim_inventory -----
	"sim_id":      "sim_serial",
	"imsi_number": "imsi",
	"active_flag": "sim_status",

	// ----- port_history -----
	"port_event":   "port_direction",
	"from_network": "from_carrier",
	"to_network":   "to_carrier",
}

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	mapped := make(map[string]any, len(param.Record)+1)
	for srcCol, val := range param.Record {
		if destCol, found := columnMap[srcCol]; found {
			mapped[destCol] = val
		} else {
			mapped[srcCol] = val
		}
	}

	// This transformer is Aircel-specific. The company is known at compile time —
	// each source company has its own schema mapper. (Vodafone uses its own mapper
	// with const company = "vodafone", etc.)
	mapped["source_company"] = "aircel"

	return mapped, nil
}
