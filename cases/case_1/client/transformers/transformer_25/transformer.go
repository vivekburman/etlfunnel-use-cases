package client_transformer_25

import (
	"etlfunnel/execution/models"
)

var columnMap = map[string]string{
	// customers
	"contact":      "msisdn",
	"full_name":    "name",
	"dob":          "dob",
	"aadhaar":      "aadhaar",
	"pan":          "pan",
	"email":        "email",
	"addr":         "address",
	"activated_on": "activation_date",
	"zone":         "zone",

	// subscriptions
	"tariff_code":    "plan_code",
	"validity_start": "plan_start",
	"validity_end":   "plan_end",
	"status":         "status",

	// billing_accounts
	"outstanding": "amount_due",
	"paid":        "amount_paid",
	"bill_from":   "cycle_start",
	"bill_to":     "cycle_end",

	// sim_inventory
	"serial_no": "sim_serial",
	"imsi":      "imsi",

	// port_history
	"mnp_type":        "port_direction",
	"source_operator": "from_carrier",
	"dest_operator":   "to_carrier",
}

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	mapped := make(map[string]any, len(param.Record))
	for srcCol, val := range param.Record {
		if destCol, found := columnMap[srcCol]; found {
			mapped[destCol] = val
		} else {
			mapped[srcCol] = val
		}
	}
	mapped["source_company"] = "idea"
	return mapped, nil
}
