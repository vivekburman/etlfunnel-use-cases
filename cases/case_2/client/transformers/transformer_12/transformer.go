package client_transformer_12

// Zomato Platform Order Intelligence — transformer_12: CancellationStageClassifier (STEP-25)
//
// For cancelled/rejected/refunded orders, determines at which operational
// stage the cancellation occurred. Skips non-cancelled records (pass-through).
//
// Classification logic reads `order_status_events` — an embedded slice of
// status transition events that the source connector joins into the record.
// Each event is a map with at least:
//   status     string  — the status reached at this event
//   created_at string  — timestamp of the transition
//
// Food brands (zomato_food, blinkit, hyperpure):
//   pre_accept   — cancelled before any ACCEPTED/CONFIRMED/PICKING transition
//   post_accept  — cancelled after acceptance but before pickup/dispatch
//   post_pickup  — cancelled after the order was picked up / dispatched
//
// District (live events):
//   pre_event    — refunded/no_show before TICKET_ISSUED was reached
//   post_event   — no_show after ticket was issued (gate was not scanned)
//
// If order_status_events is absent or empty, cancellation_stage = "unknown".
// Non-cancelled records get cancellation_stage = "na".

import (
	"etlfunnel/execution/models"
	"strings"
)

// stageMarkers defines, per brand group, which statuses mark the boundary
// between pre/post stages. The classifier walks the event list in order
// and checks whether any of these marker statuses were reached before cancel.
var foodAcceptedStatuses = map[string]bool{
	"accepted":  true,
	"confirmed": true, // hyperpure
	"picking":   true, // blinkit
}

var foodPickedUpStatuses = map[string]bool{
	"picked_up":   true,
	"dispatched":  true, // hyperpure
	"out_for_delivery": true, // blinkit
	"in_transit":  true, // hyperpure
}

var districtTicketIssuedStatuses = map[string]bool{
	"ticket_issued": true,
}

func Transform(param *models.TransformerProps) (*models.TransformerTune, error) {
	out := make([]map[string]any, 0, len(param.Records))
	for _, rec := range param.Records {
		r := shallowClone(rec)
		classify(r)
		out = append(out, r)
	}
	return &models.TransformerTune{Action: models.ActionContinue, Records: out}, nil
}

func classify(r map[string]any) {
	canonical, _ := r["canonical_status"].(string)
	if canonical != "cancelled" {
		r["cancellation_stage"] = "na"
		return
	}

	brand, _ := r["sub_brand"].(string)
	events := extractEvents(r["order_status_events"])

	if len(events) == 0 {
		r["cancellation_stage"] = "unknown"
		return
	}

	if brand == "district" {
		r["cancellation_stage"] = classifyDistrict(events)
	} else {
		r["cancellation_stage"] = classifyFood(events)
	}
}

func classifyFood(events []map[string]any) string {
	reachedAccepted := false
	reachedPickedUp := false

	for _, e := range events {
		status := strings.ToLower(statusFromEvent(e))
		if foodPickedUpStatuses[status] {
			reachedPickedUp = true
		}
		if foodAcceptedStatuses[status] {
			reachedAccepted = true
		}
	}

	switch {
	case reachedPickedUp:
		return "post_pickup"
	case reachedAccepted:
		return "post_accept"
	default:
		return "pre_accept"
	}
}

func classifyDistrict(events []map[string]any) string {
	for _, e := range events {
		status := strings.ToLower(statusFromEvent(e))
		if districtTicketIssuedStatuses[status] {
			return "post_event"
		}
	}
	return "pre_event"
}

// extractEvents tries both a typed []map[string]any slice and the raw
// []any form that JSON unmarshalling produces.
func extractEvents(v any) []map[string]any {
	switch ev := v.(type) {
	case []map[string]any:
		return ev
	case []any:
		out := make([]map[string]any, 0, len(ev))
		for _, item := range ev {
			if m, ok := item.(map[string]any); ok {
				out = append(out, m)
			}
		}
		return out
	}
	return nil
}

func statusFromEvent(e map[string]any) string {
	if s, ok := e["status"].(string); ok {
		return s
	}
	return ""
}

func shallowClone(src map[string]any) map[string]any {
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
