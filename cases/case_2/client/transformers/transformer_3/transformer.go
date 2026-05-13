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
	"etlfunnel/execution/models"
	"fmt"
)

const targetBrand = "blinkit"

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

	move(r, "dark_store_id", "fulfilment_source_id")
	move(r, "slot_type", "catalogue_label")

	// is_scheduled bool → order_subtype string
	if v, ok := r["is_scheduled"]; ok {
		r["order_subtype"] = scheduledLabel(v)
		delete(r, "is_scheduled")
	}

	// promise_minutes is already in minutes; copy to standard field name.
	move(r, "promise_minutes", "promised_minutes")

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

func move(r map[string]any, from, to string) {
	if v, ok := r[from]; ok {
		r[to] = v
		delete(r, from)
	}
}

func shallowClone(src map[string]any) map[string]any {
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
