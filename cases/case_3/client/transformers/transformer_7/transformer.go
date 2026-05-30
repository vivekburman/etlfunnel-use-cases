package client_transformer_7

// QuotaThrottle — monitors the GA4 Data API hourly token spend for the current
// property.  When per-property spend reaches 80% of the 40,000-token/hour
// budget (i.e. 32,000 tokens), the transformer suspends the goroutine until
// the hour window rolls over.
//
// Token state is maintained in the package-level GlobalQuota tracker
// (userlibrary_1).  The GA4 connector (connector_45) increments the tracker
// in FetchRecords; this transformer reads it and inserts time.Sleep when
// necessary.  The record is always returned unmodified — the only observable
// effect is backpressure timing.

import (
	"time"

	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
)

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	rp := param.State.GetReplicaProps()
	property, _ := rp["property_id"].(string)

	if property != "" {
		ulib.GlobalQuota.CheckAndThrottle(property)
	}

	// Pass through unmodified.
	out := make(map[string]any, len(param.Record))
	for k, v := range param.Record {
		out[k] = v
	}
	return out, nil
}

// SleepUntilNextHour is exposed for unit testing.
func SleepUntilNextHour() {
	now := time.Now()
	next := now.Truncate(time.Hour).Add(time.Hour)
	time.Sleep(time.Until(next))
}
