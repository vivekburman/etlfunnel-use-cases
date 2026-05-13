package client_transformer_2

// Zomato Platform Order Intelligence — transformer_2: UnifiedSchemaMapper_ZomatoFood (STEP-18)
//
// Maps Zomato Food-specific column names to the unified order model so that
// all downstream transformers can work against a single field vocabulary.
//
// Brand-specific → unified:
//   restaurant_id   → fulfilment_source_id
//   cuisine_type    → catalogue_label
//   meal_type       → order_subtype
//   prep_time_secs  → promised_minutes  (converted: secs / 60)
//   delivered_at    → completed_at      (also set as sla_anchor)
//   fulfilment_type → "delivery"        (hardcoded for this brand)
//
// Records belonging to other brands are passed through unchanged.

import (
	"etlfunnel/execution/models"
)

const targetBrand = "zomato_food"

func Transform(param *models.TransformerProps) (*models.TransformerTune, error) {
	out := make([]map[string]any, 0, len(param.Records))
	for _, rec := range param.Records {
		if brand, _ := rec["sub_brand"].(string); brand != targetBrand {
			out = append(out, rec)
			continue
		}
		out = append(out, mapRecord(rec))
	}
	return &models.TransformerTune{Action: models.ActionContinue, Records: out}, nil
}

func mapRecord(src map[string]any) map[string]any {
	r := shallowClone(src)

	move(r, "restaurant_id", "fulfilment_source_id")
	move(r, "cuisine_type", "catalogue_label")
	move(r, "meal_type", "order_subtype")

	// prep_time_secs → promised_minutes (integer, rounded down)
	if v, ok := r["prep_time_secs"]; ok {
		r["promised_minutes"] = toMinutesFromSecs(v)
		delete(r, "prep_time_secs")
	}

	// delivered_at is the SLA anchor and completion timestamp for this brand.
	if v, ok := r["delivered_at"]; ok {
		r["completed_at"] = v
		r["sla_anchor"] = v
	}

	r["fulfilment_type"] = "delivery"

	return r
}

// move renames src[from] → src[to] and deletes the old key.
func move(r map[string]any, from, to string) {
	if v, ok := r[from]; ok {
		r[to] = v
		delete(r, from)
	}
}

func toMinutesFromSecs(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n / 60
	case int:
		return int64(n) / 60
	case float64:
		return int64(n) / 60
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
