package client_transformer_22

import (
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
)

// UnitNormalizer: standardize units and currency before destination write.
//
// Conversions:
//   data_mb  (float64, megabytes)  → data_gb (float64, gigabytes, 3 decimal places)
//   bill_amount, outstanding_dues, last_payment_amount
//            (float64, paise)      → same fields in rupees (divide by 100, 2 decimal places)

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

	// Paise → Rupees for all monetary fields
	for _, col := range []string{"bill_amount", "outstanding_dues", "last_payment_amount"} {
		raw, ok := rec[col]
		if !ok || raw == nil {
			continue
		}
		paise, err := ulib.ParseDecimal(raw)
		if err != nil {
			return nil, err
		}
		rec[col] = ulib.RoundTo(paise/100.0, 2)
	}

	return rec, nil
}
