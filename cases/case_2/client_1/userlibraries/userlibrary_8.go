package client_userlibrary

import (
	"etlfunnel/execution/models"
	"strings"
)

// ParseBrandContext derives (sub_brand, city, entity) from pipeline runtime state
// and the first available record.
//
// sub_brand — stamped on every record by transformer_1 (SubBrandTagger).
// city      — native column in every source table; present on all records.
// entity    — pipeline name with the "pipeline_" prefix stripped
//             (e.g. "pipeline_orders" → "orders").
func ParseBrandContext(state models.IPipelineRuntimeState, records []map[string]any) (subBrand, city, entity string) {
	entity = strings.TrimPrefix(state.GetName(), "pipeline_")

	if len(records) == 0 {
		return "unknown", "unknown", entity
	}

	rec := records[0]
	subBrand = stringField(rec, "sub_brand")
	city = stringField(rec, "city")
	return
}

// FlowType returns "hot" if the flow name contains "hot", otherwise "cold".
func FlowType(state models.IPipelineRuntimeState) string {
	if strings.Contains(strings.ToLower(state.GetFlowName()), "hot") {
		return "hot"
	}
	return "cold"
}

func stringField(rec map[string]any, key string) string {
	if v, ok := rec[key].(string); ok && v != "" {
		return v
	}
	return "unknown"
}
