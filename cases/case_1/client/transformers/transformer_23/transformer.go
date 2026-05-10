package client_transformer_23

import (
	"etlfunnel/execution/models"
)

var columnMap = map[string]string{
	// identity renames — source name differs from canonical name
	"mob_no":          "msisdn",
	"cust_name":       "name",
	"date_of_birth":   "dob",
	"aadhaar_num":     "aadhaar",
	"pan_num":         "pan",
	"email_id":        "email",
	"imsi_no":         "imsi",
	"cycle_from":      "cycle_start",
	"cycle_to":        "cycle_end",
	"sub_status":      "status",
	"activation_date": "activated_date",
	"from_operator":   "from_carrier",
	"to_operator":     "to_carrier",
}

const company = "vodafone"

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	mapped := make(map[string]any, len(param.Record)+1)
	for srcCol, val := range param.Record {
		if b, ok := val.([]byte); ok {
			val = string(b)
		}
		if destCol, found := columnMap[srcCol]; found {
			mapped[destCol] = val
		} else {
			mapped[srcCol] = val
		}
	}
	mapped["source_company"] = company
	return mapped, nil
}