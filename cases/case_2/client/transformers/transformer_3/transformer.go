package client_transformer_3

// Zomato Platform Order Intelligence — transformer_3: UnifiedSchemaMapper_Blinkit (STEP-18)
//
// Maps Blinkit-specific column names to the unified order model.
//
// Brand-specific → unified:
//   dark_store_id    → fulfilment_source_id
//   slot_type        → catalogue_label
//   is_scheduled     → order_subtype   ("scheduled" | "instant")
//   promise_minutes  → promised_minutes (already in minutes, kept as-is)
//   delivered_at     → completed_at / sla_anchor
//   fulfilment_type  → "quick_commerce"
//
// Records belonging to other brands are passed through unchanged.

import (
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
	"fmt"
)

const targetBrand = "blinkit"

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	if brand, _ := param.Record["sub_brand"].(string); brand != targetBrand {
		return param.Record, nil
	}
	return mapRecord(param.Record), nil
}

func mapRecord(src map[string]any) map[string]any {
	r := ulib.ShallowClone(src)

	ulib.MoveKey(r, "dark_store_id", "fulfilment_source_id")
	ulib.MoveKey(r, "slot_type", "catalogue_label")

	// is_scheduled bool → order_subtype string
	if v, ok := r["is_scheduled"]; ok {
		r["order_subtype"] = scheduledLabel(v)
		delete(r, "is_scheduled")
	}

	// promise_minutes is already in minutes; copy to standard field name.
	ulib.MoveKey(r, "promise_minutes", "promised_minutes")

	if v, ok := r["delivered_at"]; ok {
		r["completed_at"] = v
		r["sla_anchor"] = v
	}

	r["fulfilment_type"] = "quick_commerce"

	return r
}

func scheduledLabel(v any) string {
	switch b := v.(type) {
	case bool:
		if b {
			return "scheduled"
		}
		return "instant"
	case string:
		if b == "true" || b == "1" {
			return "scheduled"
		}
		return "instant"
	}
	return fmt.Sprintf("%v", v)
}

