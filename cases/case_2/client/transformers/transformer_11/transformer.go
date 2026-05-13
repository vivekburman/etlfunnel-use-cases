package client_transformer_11

// Zomato Platform Order Intelligence — transformer_11: OrderValueBander (STEP-24)
//
// Buckets total_amount into order_value_band for coarse-grained analytics
// segmentation (e.g. high-value vs. impulse orders).
//
// Bands:
//   total_amount <  100  → "0-100"
//   100 ≤ total_amount < 500   → "100-500"
//   500 ≤ total_amount < 2000  → "500-2000"
//   total_amount ≥ 2000        → "2000+"
//
// District exception:
//   District sells event tickets, not food. The total_amount maps to ticket
//   face value × ticket_count which is surfaced separately in ES; banding it
//   alongside food/grocery would distort band distributions. For District
//   records, order_value_band is set to "na".
//
// If total_amount is absent or zero for non-District brands, the band is
// set to "0-100" (zero-value orders are a known data quality issue in the
// seeder — they should not block indexing).

import (
	"etlfunnel/execution/models"
)

func Transform(param *models.TransformerProps) (*models.TransformerTune, error) {
	out := make([]map[string]any, 0, len(param.Records))
	for _, rec := range param.Records {
		r := shallowClone(rec)
		r["order_value_band"] = band(r)
		out = append(out, r)
	}
	return &models.TransformerTune{Action: models.ActionContinue, Records: out}, nil
}

func band(r map[string]any) string {
	brand, _ := r["sub_brand"].(string)
	if brand == "district" {
		return "na"
	}

	amount := toFloat64(r["total_amount"])
	switch {
	case amount < 100:
		return "0-100"
	case amount < 500:
		return "100-500"
	case amount < 2000:
		return "500-2000"
	default:
		return "2000+"
	}
}

func toFloat64(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int64:
		return float64(n)
	case int:
		return float64(n)
	}
	return 0
}

func shallowClone(src map[string]any) map[string]any {
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
