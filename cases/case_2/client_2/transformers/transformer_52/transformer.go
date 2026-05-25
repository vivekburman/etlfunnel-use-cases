package client_transformer_52

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
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
	"fmt"
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

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	brand, _ := param.Record["sub_brand"].(string)
	rawStatus, _ := param.Record["order_status"].(string)

	// order_items, order_status_events, and delivery_assignments records do not
	// carry order_status — only the orders entity does. Pass through unchanged.
	if rawStatus == "" {
		return param.Record, nil
	}

	key := strings.ToLower(brand) + "|" + strings.ToLower(rawStatus)
	canonical, ok := statusMap[key]
	if !ok {
		return nil, fmt.Errorf("UNKNOWN_STATUS: unmapped status %q for brand %q", rawStatus, brand)
	}

	r := ulib.ShallowClone(param.Record)
	r["canonical_status"] = canonical
	return r, nil
}

