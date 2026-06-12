package handlers

// realtime.go — handles POST /v1beta/{property}:runRealtimeReport
//
// Returns a synthetic active-user snapshot for the last 30 minutes.
// Row count is time-of-day weighted.  Quota: 1 token per call; returns 429
// if the daily realtime budget (10,000 tokens) is exhausted.

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"

	"github.com/streamcraft/myntra-etl/db_setup/cmd/seeder/generators"
	"github.com/streamcraft/myntra-etl/db_setup/cmd/seeder/state"
)

var runRealtimePath = regexp.MustCompile(`^/v1beta/(properties/[^:]+):runRealtimeReport$`)

// NewRealtimeHandler returns an http.Handler for :runRealtimeReport.
func NewRealtimeHandler(quota *state.QuotaStore, surfaceMap map[string]string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		m := runRealtimePath.FindStringSubmatch(r.URL.Path)
		if m == nil {
			http.NotFound(w, r)
			return
		}
		property := m[1]
		if _, ok := surfaceMap[property]; !ok {
			http.Error(w, fmt.Sprintf("unknown property %q", property), http.StatusBadRequest)
			return
		}

		exhausted := quota.ConsumeRealtime(property)
		if exhausted {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"code":    429,
					"status":  "RESOURCE_EXHAUSTED",
					"message": "Realtime quota exceeded for property.",
				},
			})
			return
		}

		rows := generators.GenerateRealtimeRows(property)

		dimNames := generators.RealtimeDimensions()
		metricNames, metricTypes := generators.RealtimeMetrics()

		dimHeaders := make([]map[string]string, len(dimNames))
		for i, n := range dimNames {
			dimHeaders[i] = map[string]string{"name": n}
		}
		metHeaders := make([]map[string]string, len(metricNames))
		for i, n := range metricNames {
			metHeaders[i] = map[string]string{"name": n, "type": metricTypes[i]}
		}

		responseRows := make([]map[string]any, len(rows))
		for i, row := range rows {
			dimVals := make([]map[string]string, len(row.DimensionValues))
			for j, v := range row.DimensionValues {
				dimVals[j] = map[string]string{"value": v}
			}
			metVals := make([]map[string]string, len(row.MetricValues))
			for j, v := range row.MetricValues {
				metVals[j] = map[string]string{"value": v}
			}
			responseRows[i] = map[string]any{
				"dimensionValues": dimVals,
				"metricValues":    metVals,
			}
		}

		resp := map[string]any{
			"dimensionHeaders": dimHeaders,
			"metricHeaders":    metHeaders,
			"rows":             responseRows,
			"rowCount":         len(rows),
		}

		log.Printf("[seeder] runRealtimeReport property=%s rows=%d", property, len(rows))
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}
