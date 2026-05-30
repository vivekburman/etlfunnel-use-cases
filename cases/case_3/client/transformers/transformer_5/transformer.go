package client_transformer_5

// NullFiller — replaces nil / missing values with typed zero-values for every
// column in dbo.ga4_sessions.  SQL Server NOT NULL columns cannot accept Go nil,
// and GA4 omits custom dimensions that are not tracked by a given property
// (e.g. ios does not track "wishlisted").

import "etlfunnel/execution/models"

// stringColumns receive "" when absent.
var stringColumns = []string{
	"session_id", "user_pseudo_id", "device_category", "city", "country",
	"source", "medium", "campaign", "product_category", "payment_method",
	"wishlisted", "app_version", "os_version", "surface", "property_id",
	"pipeline_run_id",
}

// numericColumns receive 0 when absent.
var numericColumns = []string{
	"sessions", "engaged_sessions", "total_users", "new_users",
	"bounce_rate", "avg_session_duration_secs",
	"conversions", "purchase_revenue_inr", "event_count", "screen_page_views",
}

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	out := make(map[string]any, len(param.Record))
	for k, v := range param.Record {
		out[k] = v
	}

	for _, col := range stringColumns {
		if v, ok := out[col]; !ok || v == nil {
			out[col] = ""
		}
	}

	for _, col := range numericColumns {
		if v, ok := out[col]; !ok || v == nil {
			out[col] = int64(0)
		}
	}

	return out, nil
}
