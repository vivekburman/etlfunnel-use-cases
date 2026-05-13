package client_transformer_6

// Zomato Platform Order Intelligence — transformer_6: OrderStatusNormaliser (STEP-19)
//
// Maps brand-specific order status strings to a canonical set used in
// Elasticsearch:
//
//	pending      — order placed but not yet accepted / confirmed
//	in_progress  — actively being prepared / picked / in transit
//	completed    — successfully delivered / received / attended
//	cancelled    — order cancelled, rejected, refunded, or no_show
//
// Status vocabularies (from seeder / brand_sla_rules):
//
//	Zomato Food : PLACED → ACCEPTED → PREPARING → PICKED_UP → DELIVERED / CANCELLED
//	Blinkit     : PLACED → PICKING → PACKED → OUT_FOR_DELIVERY → DELIVERED / CANCELLED
//	Hyperpure   : PLACED → CONFIRMED → DISPATCHED → IN_TRANSIT → RECEIVED / REJECTED
//	District    : BOOKED → PAYMENT_CONFIRMED → TICKET_ISSUED → ATTENDED / REFUNDED / NO_SHOW
//
// Records with an unmapped status are routed to backlog with error code
// UNKNOWN_STATUS and removed from the downstream batch.

import (
	"etlfunnel/execution/models"
	"strings"
)

// statusMap maps "<sub_brand>|<STATUS>" → canonical_status.
// Keys are lower-cased for case-insensitive matching.
var statusMap = map[string]string{
	// Zomato Food
	"zomato_food|placed":        "pending",
	"zomato_food|accepted":      "in_progress",
	"zomato_food|preparing":     "in_progress",
	"zomato_food|picked_up":     "in_progress",
	"zomato_food|delivered":     "completed",
	"zomato_food|cancelled":     "cancelled",

	// Blinkit
	"blinkit|placed":            "pending",
	"blinkit|picking":           "in_progress",
	"blinkit|packed":            "in_progress",
	"blinkit|out_for_delivery":  "in_progress",
	"blinkit|delivered":         "completed",
	"blinkit|cancelled":         "cancelled",

	// Hyperpure
	"hyperpure|placed":          "pending",
	"hyperpure|confirmed":       "in_progress",
	"hyperpure|dispatched":      "in_progress",
	"hyperpure|in_transit":      "in_progress",
	"hyperpure|received":        "completed",
	"hyperpure|rejected":        "cancelled",

	// District
	"district|booked":               "pending",
	"district|payment_confirmed":    "in_progress",
	"district|ticket_issued":        "in_progress",
	"district|attended":             "completed",
	"district|refunded":             "cancelled",
	"district|no_show":              "cancelled",
}

func Transform(param *models.TransformerProps) (*models.TransformerTune, error) {
	var good []map[string]any
	var failed []map[string]any

	for _, rec := range param.Records {
		brand, _ := rec["sub_brand"].(string)
		rawStatus, _ := rec["order_status"].(string)

		key := strings.ToLower(brand) + "|" + strings.ToLower(rawStatus)
		canonical, ok := statusMap[key]
		if !ok {
			rec["_failure_stage"] = "Transform"
			rec["_error_code"] = "UNKNOWN_STATUS"
			rec["_error_msg"] = "unmapped status: " + rawStatus + " for brand: " + brand
			failed = append(failed, rec)
			continue
		}

		r := shallowClone(rec)
		r["canonical_status"] = canonical
		good = append(good, r)
	}

	if len(failed) > 0 {
		param.State.GetLogger().Warn(
			"transformer_6: " + itoa(len(failed)) + " record(s) with unknown status routed to backlog",
		)
		param.BacklogFn(failed)
	}

	return &models.TransformerTune{Action: models.ActionContinue, Records: good}, nil
}

func shallowClone(src map[string]any) map[string]any {
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := [20]byte{}
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}
