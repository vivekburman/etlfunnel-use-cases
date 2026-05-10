package client_transformer_34

import (
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
)

// UnitNormalizer: standardize units before destination write.
//
// Conversions:
//   data_mb  (float64, megabytes)  → data_gb (float64, gigabytes, 3 decimal places)
//
// Monetary fields (amount_due, amount_paid) are already in rupees from the source
// and are passed through unchanged.

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	rec := param.Record

	// data_mb → data_gb
	if raw, ok := rec["data_mb"]; ok && raw != nil {
		mb, err := ulib.ParseDecimal(raw)
		if err != nil {
			return nil, err
		}
		rec["data_gb"] = ulib.RoundTo(mb/1024.0, 3)
		delete(rec, "data_mb")
	}

	return rec, nil
}
