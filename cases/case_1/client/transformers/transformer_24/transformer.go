package client_transformer_24

import (
	"etlfunnel/execution/models"
)

var columnMap = map[string]string{
	// customers
	"phone_number":        "msisdn",
	"subscriber_name":     "name",
	"birth_date":          "dob",
	"id_aadhaar":          "aadhaar",
	"id_pan":              "pan",
	"subscriber_email":    "email",
	"residential_address": "address",
	"sim_activation_dt":   "activation_date",
	// subscriptions
	"pack_code":           "plan_code",
	"pack_start_dt":       "plan_start",
	"pack_end_dt":         "plan_end",
	"pack_status":         "status",
	// billing_accounts
	"dues":                "amount_due",
	"payments_received":   "amount_paid",
	"period_start":        "cycle_start",
	"period_end":          "cycle_end",
	// sim_inventory
	"iccid":               "sim_serial",
	"imsi_code":           "imsi",
	"sim_active":          "sim_status",
	// port_history
	"port_in_out":         "port_direction",
	"prev_carrier":        "from_carrier",
	"new_carrier":         "to_carrier",
}

const company = "tata_docomo"

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	mapped := make(map[string]any, len(param.Record)+1)
	for srcCol, val := range param.Record {
		if destCol, found := columnMap[srcCol]; found {
			mapped[destCol] = val
		} else {
			mapped[srcCol] = val
		}
	}
	mapped["source_company"] = company
	return mapped, nil
}
