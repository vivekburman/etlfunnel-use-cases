package client_transformer_32

import (
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
	"fmt"
)

// TypeCaster: convert MySQL-native Go types to Postgres-compatible Go types.
//
// MySQL driver maps:
//   TINYINT(1)  → int8 / []byte("0"|"1")   → bool
//   DATETIME    → []byte / string           → time.Time (UTC)
//   DECIMAL     → []byte                   → float64
//   BIGINT      → int64 (pass through)
//   TEXT/VARCHAR → string (pass through)

// booleanColumns are TINYINT(1) fields that represent boolean flags.
var booleanColumns = map[string]bool{
	"is_active":      true,
	"is_postpaid":    true,
	"is_roaming":     true,
	"is_ported":      true,
	"is_blacklisted": true,
	"dob_flagged":    true,
	"pii_masked":     true,
}

// dateColumns are DATETIME/DATE fields that should become time.Time.
var dateColumns = map[string]bool{
	"dob":             true,
	"activated_date":  true,
	"port_in_date":    true,
	"port_out_date":   true,
	"cycle_start":     true,
	"cycle_end":       true,
	"port_date":       true,
	"plan_start":      true,
	"plan_end":        true,
}

// decimalColumns are DECIMAL/FLOAT fields stored as []byte by the MySQL driver.
var decimalColumns = map[string]bool{
	"data_mb":    true,
	"amount_due": true,
	"amount_paid": true,
}

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	out := make(map[string]any, len(param.Record))

	for col, val := range param.Record {
		if val == nil {
			out[col] = nil
			continue
		}

		switch {
		case booleanColumns[col]:
			b, err := ulib.ParseBool(val)
			if err != nil {
				return nil, fmt.Errorf("TypeCaster: column %q: %w", col, err)
			}
			out[col] = b

		case dateColumns[col]:
			t, err := ulib.ParseDate(val)
			if err != nil {
				return nil, fmt.Errorf("TypeCaster: column %q: %w", col, err)
			}
			out[col] = t

		case decimalColumns[col]:
			f, err := ulib.ParseDecimal(val)
			if err != nil {
				return nil, fmt.Errorf("TypeCaster: column %q: %w", col, err)
			}
			out[col] = f

		default:
			// Coerce remaining []byte values to string (MySQL TEXT/VARCHAR).
			if b, ok := val.([]byte); ok {
				out[col] = string(b)
			} else {
				out[col] = val
			}
		}
	}

	return out, nil
}
