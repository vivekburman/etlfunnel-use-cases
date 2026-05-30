package client_transformer_1

// DateParser — converts the GA4 date dimension from "YYYYMMDD" string to a
// time.Time value stored as "report_date".  Also deletes the raw "date" key so
// downstream writers do not see both forms.

import (
	"fmt"
	"time"

	"etlfunnel/execution/models"
)

const ga4DateLayout = "20060102"

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	raw, ok := param.Record["date"].(string)
	if !ok || raw == "" {
		return nil, fmt.Errorf("DateParser: 'date' field missing or not a string")
	}

	t, err := time.Parse(ga4DateLayout, raw)
	if err != nil {
		return nil, fmt.Errorf("DateParser: cannot parse %q as YYYYMMDD: %w", raw, err)
	}

	out := make(map[string]any, len(param.Record))
	for k, v := range param.Record {
		out[k] = v
	}
	delete(out, "date")
	out["report_date"] = t

	return out, nil
}
