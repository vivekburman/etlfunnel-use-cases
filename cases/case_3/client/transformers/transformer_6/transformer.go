package client_transformer_6

// MetricTypeCaster — GA4 returns ALL metric values as strings.  SQL Server
// expects typed data (INT, DECIMAL).  This transformer converts:
//
//	integer metrics → int64
//	float metrics   → float64
//
// This runs after DimensionNormaliser so field names are already canonical.

import (
	"fmt"
	"strconv"

	"etlfunnel/execution/models"
)

var intMetrics = map[string]bool{
	"sessions": true, "engaged_sessions": true, "total_users": true,
	"new_users": true, "conversions": true, "event_count": true,
	"screen_page_views": true,
}

var floatMetrics = map[string]bool{
	"bounce_rate": true, "avg_session_duration_secs": true, "purchase_revenue_inr": true,
}

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	out := make(map[string]any, len(param.Record))
	for k, v := range param.Record {
		out[k] = v
	}

	for col := range intMetrics {
		if raw, ok := out[col]; ok {
			cast, err := toInt64(raw)
			if err != nil {
				return nil, fmt.Errorf("MetricTypeCaster: column %q: %w", col, err)
			}
			out[col] = cast
		}
	}

	for col := range floatMetrics {
		if raw, ok := out[col]; ok {
			cast, err := toFloat64(raw)
			if err != nil {
				return nil, fmt.Errorf("MetricTypeCaster: column %q: %w", col, err)
			}
			out[col] = cast
		}
	}

	return out, nil
}

func toInt64(v any) (int64, error) {
	switch t := v.(type) {
	case int64:
		return t, nil
	case int:
		return int64(t), nil
	case float64:
		return int64(t), nil
	case string:
		i, err := strconv.ParseInt(t, 10, 64)
		if err != nil {
			// Tolerate floats-as-strings returned by some GA4 environments.
			if f, ferr := strconv.ParseFloat(t, 64); ferr == nil {
				return int64(f), nil
			}
			return 0, err
		}
		return i, nil
	case nil:
		return 0, nil
	}
	return 0, fmt.Errorf("cannot cast %T to int64", v)
}

func toFloat64(v any) (float64, error) {
	switch t := v.(type) {
	case float64:
		return t, nil
	case int64:
		return float64(t), nil
	case int:
		return float64(t), nil
	case string:
		f, err := strconv.ParseFloat(t, 64)
		if err != nil {
			return 0, err
		}
		return f, nil
	case nil:
		return 0, nil
	}
	return 0, fmt.Errorf("cannot cast %T to float64", v)
}
