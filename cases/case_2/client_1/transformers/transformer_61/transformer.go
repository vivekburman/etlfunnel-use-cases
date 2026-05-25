package client_transformer_61

// Hot Flow Stage 1 — WAL record sanitizer for Zomato Food → Redis.
// Converts pgtype values (pgtype.Numeric, pgtype.Text, etc.) to plain Go types
// so go-redis can marshal them. All pgtype structs implement driver.Valuer and
// return string/int64/float64/[]byte/time.Time/nil — exactly what Redis accepts.

import (
	"database/sql/driver"
	"etlfunnel/execution/models"
)

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	out := make(map[string]any, len(param.Record))
	for k, v := range param.Record {
		out[k] = sanitize(v)
	}
	return out, nil
}

func sanitize(v any) any {
	if valuer, ok := v.(driver.Valuer); ok {
		dv, err := valuer.Value()
		if err == nil {
			return dv
		}
	}
	return v
}
