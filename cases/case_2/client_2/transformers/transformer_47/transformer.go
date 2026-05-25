package client_transformer_47

// Zomato Platform Order Intelligence — transformer_1: SubBrandTagger (STEP-17)
//
// Stamps the `sub_brand` field on every record so that all downstream
// transformers can branch on it without re-parsing the pipeline context.
//
// sub_brand values: zomato_food | blinkit | hyperpure | district
//
// The brand is derived from the flow name, which the orchestrator sets to
// "<sub_brand>_cold_flow" or "<sub_brand>_hot_flow" when creating the pipeline.
// Precedence: existing record field > flow-name derivation > "unknown".

import (
	"etlfunnel/execution/models"
	"maps"
	"strings"
)

// knownBrands is the ordered list used when scanning the flow name.
// Longer tokens first to avoid "blinkit" matching before "hyperpure" etc.
var knownBrands = []string{"zomato_food", "hyperpure", "blinkit", "district"}

// Transformer stamps sub_brand on the record.
// It never fails — records missing a derivable brand get sub_brand = "unknown"
// and continue through the chain (SLACalculator / SchemaMapper will backlog them).
func Transformer(param *models.TransformerProps) (map[string]any, error) {
	brand := brandFromFlowName(param.State.GetFlowName())

	r := shallowClone(param.Record)

	// Preserve an already-set sub_brand (e.g. hot-flow re-entry after backlog retry).
	if existing, ok := r["sub_brand"].(string); !ok || existing == "" {
		r["sub_brand"] = brand
	}

	return r, nil
}

// brandFromFlowName extracts the sub_brand token from a flow name like
// "zomato_food_cold_flow", "Zomato Food Cold", or "blinkit_hot_flow".
// Spaces are normalised to underscores before matching.
func brandFromFlowName(flowName string) string {
	lower := strings.ReplaceAll(strings.ToLower(flowName), " ", "_")
	for _, b := range knownBrands {
		if strings.Contains(lower, b) {
			return b
		}
	}
	return "unknown"
}

func shallowClone(src map[string]any) map[string]any {
	dst := make(map[string]any, len(src))
	maps.Copy(dst, src)
	return dst
}