package client_transformer_4

// Zomato Platform Order Intelligence — transformer_4: UnifiedSchemaMapper_Hyperpure (STEP-18)
//
// Maps Hyperpure-specific column names to the unified order model.
// Hyperpure is Zomato's B2B ingredient supply chain — orders are invoices
// between Zomato and restaurants, not consumer orders.
//
// Brand-specific → unified:
//   supplier_id           → fulfilment_source_id
//   invoice_number        → catalogue_label
//   bulk_order_flag       → order_subtype   ("bulk" | "standard")
//   delivery_window_days  → promised_minutes (converted: days × 1440)
//   received_at           → completed_at / sla_anchor
//   fulfilment_type       → "b2b_supply"
//
// Records belonging to other brands are passed through unchanged.

import (
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
	"fmt"
)

const targetBrand = "hyperpure"

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	if brand, _ := param.Record["sub_brand"].(string); brand != targetBrand {
		return param.Record, nil
	}
	return mapRecord(param.Record), nil
}

func mapRecord(src map[string]any) map[string]any {
	r := ulib.ShallowClone(src)

	ulib.MoveKey(r, "supplier_id", "fulfilment_source_id")
	ulib.MoveKey(r, "invoice_number", "catalogue_label")

	// bulk_order_flag → order_subtype
	if v, ok := r["bulk_order_flag"]; ok {
		r["order_subtype"] = bulkLabel(v)
		delete(r, "bulk_order_flag")
	}

	// delivery_window_days → promised_minutes (1 day = 1440 minutes)
	if v, ok := r["delivery_window_days"]; ok {
		r["promised_minutes"] = daysToMinutes(v)
		delete(r, "delivery_window_days")
	}

	if v, ok := r["received_at"]; ok {
		r["completed_at"] = v
		r["sla_anchor"] = v
	}

	r["fulfilment_type"] = "b2b_supply"

	return r
}

func bulkLabel(v any) string {
	switch b := v.(type) {
	case bool:
		if b {
			return "bulk"
		}
		return "standard"
	case string:
		if b == "true" || b == "1" {
			return "bulk"
		}
		return "standard"
	}
	return fmt.Sprintf("%v", v)
}

func daysToMinutes(v any) int64 {
	const minsPerDay = 1440
	switch n := v.(type) {
	case int64:
		return n * minsPerDay
	case int:
		return int64(n) * minsPerDay
	case float64:
		return int64(n) * minsPerDay
	}
	return 0
}

