package client_transformer_13

// Zomato Platform Order Intelligence — transformer_13: TypeCaster_PGtoElastic (STEP-26)
//
// Converts Postgres/pgx driver output types to the JSON-native types that
// Elasticsearch expects in a _bulk index request.  This is the last
// transformer in the shared chain — the output record is ES-ready.
//
// Conversion rules:
//   time.Time / *time.Time  → ISO8601 string (RFC3339Nano, UTC)
//   pgtype.Numeric          → float64 (via .Float64Value())
//   []byte                  → base64-encoded string
//   nil / zero-value ptr    → field omitted from output map
//   bool                    → bool (no change; already JSON-native)
//   int, int32, int64       → int64 (no change; JSON numbers)
//   float32, float64        → float64 (no change)
//   string                  → string (no change)
//
// Unknown types that do not fit any rule are kept as-is with a warning log.
// The transformer never fails a record — type coercion errors produce a
// best-effort cast rather than a backlog route.
//
// It also stamps two final metadata fields:
//   flow_type   — "cold" or "hot" (from pipeline state)
//   indexed_at  — UTC timestamp of this indexing run (ISO8601)

import (
	"encoding/base64"
	"etlfunnel/execution/models"
	"fmt"
	"strings"
	"time"
)

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	flowType := deriveFlowType(param.State.GetFlowName())
	indexedAt := time.Now().UTC().Format(time.RFC3339)

	cast := castRecord(param.Record, param.State.GetLogger())
	cast["flow_type"] = flowType
	cast["indexed_at"] = indexedAt
	return cast, nil
}

// castRecord returns a new map with all values converted to ES-compatible types.
// Nil values are omitted from the output map.
func castRecord(src map[string]any, logger models.ILogger) map[string]any {
	dst := make(map[string]any, len(src))
	for k, v := range src {
		if v == nil {
			continue
		}
		casted, ok := castValue(v)
		if !ok {
			logger.Warn(fmt.Sprintf("transformer_13: unhandled type %T for field %q — kept as-is", v, k))
			dst[k] = v
			continue
		}
		if casted != nil {
			dst[k] = casted
		}
		// nil casted = explicitly omit the field (zero-value pointer etc.)
	}
	return dst
}

// castValue converts a single value to an ES-compatible type.
// Returns (converted, true) on success, (nil, true) to omit the field,
// and (nil, false) when the type is entirely unknown.
func castValue(v any) (any, bool) {
	switch val := v.(type) {
	case time.Time:
		if val.IsZero() {
			return nil, true // omit zero times
		}
		return val.UTC().Format(time.RFC3339Nano), true

	case *time.Time:
		if val == nil || val.IsZero() {
			return nil, true
		}
		return val.UTC().Format(time.RFC3339Nano), true

	case []byte:
		if len(val) == 0 {
			return nil, true
		}
		return base64.StdEncoding.EncodeToString(val), true

	case float32:
		return float64(val), true

	case int:
		return int64(val), true

	case int32:
		return int64(val), true

	// pgtype.Numeric — handled via duck-typing to avoid importing pgtype.
	// pgtype.Numeric implements fmt.Stringer; we attempt a float parse.
	case fmt.Stringer:
		return parseNumericStringer(val), true

	case bool, int64, float64, string:
		return val, true

	case map[string]any, []any, []map[string]any:
		// Embedded JSONB / arrays — pass through; ES maps them as nested objects.
		return val, true
	}

	return nil, false
}

// parseNumericStringer handles pgtype.Numeric (and similar) which implement
// fmt.Stringer.  We parse the string representation to float64.
func parseNumericStringer(s fmt.Stringer) any {
	str := s.String()
	if str == "" || str == "NaN" || str == "Inf" {
		return nil
	}
	var f float64
	if _, err := fmt.Sscanf(str, "%f", &f); err == nil {
		return f
	}
	return str // fallback: keep as string
}

func deriveFlowType(flowName string) string {
	if strings.Contains(strings.ToLower(flowName), "hot") {
		return "hot"
	}
	return "cold"
}
