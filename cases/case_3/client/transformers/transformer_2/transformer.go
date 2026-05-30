package client_transformer_2

// DimensionNormaliser — maps property-specific custom dimension names to the
// canonical column names used in dbo.ga4_sessions, eliminating per-property
// schema divergence before the SQL Server writer sees any record.
//
// Custom dimension names differ across web / android / ios:
//
//	product_category: customEvent:product_category | customEvent:category_slug | customEvent:item_category
//	payment_method:   customEvent:payment_method   | customEvent:payment_type  | customEvent:pay_method
//	wishlisted:       customEvent:wishlisted        | customEvent:is_wishlisted | (not tracked on ios)

import "etlfunnel/execution/models"

// dimensionMap maps raw GA4 custom dimension names → canonical column names.
var dimensionMap = map[string]string{
	// product_category variants
	"customEvent:product_category": "product_category",
	"customEvent:category_slug":    "product_category",
	"customEvent:item_category":    "product_category",

	// payment_method variants
	"customEvent:payment_method": "payment_method",
	"customEvent:payment_type":   "payment_method",
	"customEvent:pay_method":     "payment_method",

	// wishlisted variants (mapped to a single boolean-ish column)
	"customEvent:wishlisted":    "wishlisted",
	"customEvent:is_wishlisted": "wishlisted",

	// Core dimension renames (GA4 camelCase → snake_case)
	"sessionId":          "session_id",
	"userPseudoId":       "user_pseudo_id",
	"deviceCategory":     "device_category",
	"sessionSource":      "source",
	"sessionMedium":      "medium",
	"sessionCampaignName": "campaign",
	"appVersion":          "app_version",
	"operatingSystemVersion": "os_version",

	// Metric renames
	"engagedSessions":       "engaged_sessions",
	"totalUsers":            "total_users",
	"newUsers":              "new_users",
	"bounceRate":            "bounce_rate",
	"averageSessionDuration": "avg_session_duration_secs",
	"purchaseRevenue":       "purchase_revenue_inr",
	"eventCount":            "event_count",
	"screenPageViews":       "screen_page_views",
}

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	out := make(map[string]any, len(param.Record))
	for k, v := range param.Record {
		if canonical, found := dimensionMap[k]; found {
			out[canonical] = v
		} else {
			out[k] = v
		}
	}
	return out, nil
}
