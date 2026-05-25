package client_transformer_48

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
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
)

const targetBrand = "zomato_food"

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	if brand, _ := param.Record["sub_brand"].(string); brand != targetBrand {
		return param.Record, nil
	}
	return mapRecord(param.Record), nil
}

func mapRecord(src map[string]any) map[string]any {
	r := ulib.ShallowClone(src)

	ulib.MoveKey(r, "restaurant_id", "fulfilment_source_id")
	ulib.MoveKey(r, "cuisine_type", "catalogue_label")
	ulib.MoveKey(r, "meal_type", "order_subtype")

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

