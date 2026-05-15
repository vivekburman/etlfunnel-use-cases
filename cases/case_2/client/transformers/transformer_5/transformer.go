package client_transformer_5

// Zomato Platform Order Intelligence — transformer_5: UnifiedSchemaMapper_District (STEP-18)
//
// Maps District-specific column names to the unified order model.
// District is Zomato's live events / ticketing platform — structurally
// different from the other three brands: no delivery rider, no kitchen,
// fulfilment is gate attendance at a venue.
//
// Brand-specific → unified:
//   venue_id        → fulfilment_source_id
//   event_id        → catalogue_label
//   seat_category   → order_subtype
//   event_date      → sla_anchor        (District SLA anchor is the event date)
//   ticket_count    → item_count        (already the right concept, renamed)
//   attended_at     → completed_at      (null until the gate scan happens)
//   fulfilment_type → "live_event"
//
// District has no prep_time / promise_minutes equivalent; promised_minutes
// is left unset (TypeCaster will omit nil fields from the ES document).
//
// Records belonging to other brands are passed through unchanged.

import (
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
)

const targetBrand = "district"

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	if brand, _ := param.Record["sub_brand"].(string); brand != targetBrand {
		return param.Record, nil
	}
	return mapRecord(param.Record), nil
}

func mapRecord(src map[string]any) map[string]any {
	r := ulib.ShallowClone(src)

	ulib.MoveKey(r, "venue_id", "fulfilment_source_id")
	ulib.MoveKey(r, "event_id", "catalogue_label")
	ulib.MoveKey(r, "seat_category", "order_subtype")
	ulib.MoveKey(r, "ticket_count", "item_count")

	// event_date is the SLA anchor — the moment by which attendance is expected.
	if v, ok := r["event_date"]; ok {
		r["sla_anchor"] = v
		// Do NOT copy to completed_at here; completed_at = attended_at (gate scan).
	}

	// attended_at is the completion timestamp (may be null for future/pending events).
	if v, ok := r["attended_at"]; ok {
		r["completed_at"] = v
	}

	r["fulfilment_type"] = "live_event"

	// District has no promised_minutes — leave it absent so TypeCaster omits it.

	return r
}

