package client_transformer_53


// Zomato Platform Order Intelligence — transformer_7: SLACalculator (STEP-20)
//
// Computes sla_status (met | breached | na) per brand SLA rules.
//
// SLA models:
//
//   Zomato Food / Blinkit:
//     actual_minutes = (sla_anchor - placed_at).Minutes()
//     sla_status = met     if actual_minutes <= promised_minutes
//                  breached if actual_minutes > promised_minutes
//                  na       if sla_anchor or placed_at is absent/null
//                  na       if promised_minutes == 0 (data quality issue)
//
//   Hyperpure (B2B supply):
//     sla_status = met     if sla_anchor (received_at) within delivery_window of placed_at
//                           delivery_window = promised_minutes (stored as days×1440 by mapper)
//                  breached if received_at > placed_at + delivery_window
//                  na       if either timestamp is absent
//
//   District (live events):
//     sla_status = met     if attended_at is not null  (gate scan happened)
//                  na       if canonical_status = cancelled (refunded before event)
//                  breached if canonical_status = cancelled AND order_status = NO_SHOW
//                           (ticket was issued but customer did not attend)
//
// The transformer writes two additional fields:
//   actual_minutes  — computed delivery/transit duration (integer minutes; absent for District)
//   sla_status      — the verdict string

import (
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
)

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	r := ulib.ShallowClone(param.Record)
	computeSLA(r)
	return r, nil
}

func computeSLA(r map[string]any) {
	brand, _ := r["sub_brand"].(string)

	switch brand {
	case "district":
		computeDistrictSLA(r)
	case "hyperpure":
		computeTimestampSLA(r, false)
	default:
		// zomato_food and blinkit share the same model
		computeTimestampSLA(r, true)
	}
}

// computeTimestampSLA handles the delivery / quick-commerce SLA model.
// writeActualMinutes: if true, also writes actual_minutes to the record.
func computeTimestampSLA(r map[string]any, writeActualMinutes bool) {
	placedAt, okPlaced := ulib.ToTime(r["placed_at"])
	slaAnchor, okAnchor := ulib.ToTime(r["sla_anchor"])
	promised := toInt64(r["promised_minutes"])

	if !okPlaced || !okAnchor {
		r["sla_status"] = "na"
		return
	}
	if promised == 0 {
		r["sla_status"] = "na"
		return
	}

	actual := int64(slaAnchor.Sub(placedAt).Minutes())
	if writeActualMinutes {
		r["actual_minutes"] = actual
	}

	if actual <= promised {
		r["sla_status"] = "met"
	} else {
		r["sla_status"] = "breached"
	}
}

func computeDistrictSLA(r map[string]any) {
	canonicalStatus, _ := r["canonical_status"].(string)
	rawStatus, _ := r["order_status"].(string)

	// If refunded before the event (not a no_show), SLA is not applicable.
	if canonicalStatus == "cancelled" {
		if isNoShow(rawStatus) {
			r["sla_status"] = "breached"
		} else {
			r["sla_status"] = "na"
		}
		return
	}

	// For completed events, attended_at must be present.
	if _, ok := ulib.ToTime(r["completed_at"]); ok {
		r["sla_status"] = "met"
		return
	}

	// Still pending / in_progress (event hasn't happened yet).
	r["sla_status"] = "na"
}

func isNoShow(status string) bool {
	return status == "NO_SHOW" || status == "no_show"
}

func toInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	}
	return 0
}

