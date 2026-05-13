package client_transformer_1

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
	"strings"
)

// knownBrands is the ordered list used when scanning the flow name.
// Longer tokens first to avoid "blinkit" matching before "hyperpure" etc.
var knownBrands = []string{"zomato_food", "hyperpure", "blinkit", "district"}

// Transform stamps sub_brand on every record.
// It never fails — records missing a derivable brand get sub_brand = "unknown"
// and continue through the chain (SLACalculator / SchemaMapper will backlog them).
func Transform(param *models.TransformerProps) (*models.TransformerTune, error) {
	brand := brandFromFlowName(param.State.GetFlowName())

	out := make([]map[string]any, 0, len(param.Records))
	for _, rec := range param.Records {
		// Clone to avoid mutating the original batch slice entries.
		r := shallowClone(rec)

		// Preserve an already-set sub_brand (e.g. hot-flow re-entry after backlog retry).
		if existing, ok := r["sub_brand"].(string); !ok || existing == "" {
			r["sub_brand"] = brand
		}
		out = append(out, r)
	}

	param.State.GetLogger().Debug(
		"transformer_1: stamped sub_brand=" + brand +
			" on " + itoa(len(out)) + " record(s)",
	)

	return &models.TransformerTune{
		Action:  models.ActionContinue,
		Records: out,
	}, nil
}

// brandFromFlowName extracts the sub_brand token from a flow name like
// "zomato_food_cold_flow" or "blinkit_hot_flow".
func brandFromFlowName(flowName string) string {
	lower := strings.ToLower(flowName)
	for _, b := range knownBrands {
		if strings.Contains(lower, b) {
			return b
		}
	}
	return "unknown"
}

func shallowClone(src map[string]any) map[string]any {
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// itoa avoids importing strconv for a single log line.
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
