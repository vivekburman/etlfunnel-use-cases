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
	"etlfunnel/execution/models"
)

const targetBrand = "district"

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

	move(r, "venue_id", "fulfilment_source_id")
	move(r, "event_id", "catalogue_label")
	move(r, "seat_category", "order_subtype")
	move(r, "ticket_count", "item_count")

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
