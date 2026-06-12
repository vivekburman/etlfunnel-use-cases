package handlers

// report.go — handles POST /v1beta/{property}:runReport
//
// Parses the GA4 runReport request, generates synthetic session rows from the
// generators package, applies offset/limit pagination, enforces quota limits
// (returns HTTP 429 when exhausted), and returns the GA4 JSON response shape.

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"

	"github.com/streamcraft/myntra-etl/db_setup/cmd/seeder/generators"
	"github.com/streamcraft/myntra-etl/db_setup/cmd/seeder/state"
)

var runReportPath = regexp.MustCompile(`^/v1beta/(properties/[^:]+):runReport$`)

// reportRequest is the subset of the GA4 runReport request body we care about.
type reportRequest struct {
	DateRanges []struct {
		StartDate string `json:"startDate"`
		EndDate   string `json:"endDate"`
	} `json:"dateRanges"`
	Limit  int64 `json:"limit"`
	Offset int64 `json:"offset"`
}

// NewReportHandler returns an http.Handler for :runReport.
func NewReportHandler(quota *state.QuotaStore, rowsPerDay int, surfaceMap map[string]string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		m := runReportPath.FindStringSubmatch(r.URL.Path)
		if m == nil {
			http.NotFound(w, r)
			return
		}
		property := m[1]
		surface, ok := surfaceMap[property]
		if !ok {
			http.Error(w, fmt.Sprintf("unknown property %q", property), http.StatusBadRequest)
			return
		}

		var req reportRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}

		if len(req.DateRanges) == 0 {
			http.Error(w, "dateRanges is required", http.StatusBadRequest)
			return
		}
		date := req.DateRanges[0].StartDate

		limit := req.Limit
		if limit <= 0 || limit > 100_000 {
			limit = 100_000
		}

		// Estimate token cost for quota enforcement.
		tokenCost := int64(rowsPerDay/1000) + 1
		_, exhausted := quota.ConsumeReport(property, tokenCost)
		if exhausted {
			log.Printf("[seeder] 429 quota exhausted for property=%s hourly_spent=%d",
				property, quota.HourlySpent(property))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"code":    429,
					"status":  "RESOURCE_EXHAUSTED",
					"message": "Quota exceeded for quota metric 'analyticsdata.googleapis.com/quota/tokens_per_hour' and limit 'TOKENS_PER_HOUR'.",
				},
			})
			return
		}

		allRows := generators.GenerateSessions(property, surface, date, rowsPerDay)
		rowCount := int64(len(allRows))

		// Paginate
		start := req.Offset
		if start < 0 {
			start = 0
		}
		end := start + limit
		if end > rowCount {
			end = rowCount
		}
		pageRows := allRows[start:end]

		dimNames := generators.Dimensions(surface)
		metricNames, metricTypes := generators.Metrics()

		dimHeaders := make([]map[string]string, len(dimNames))
		for i, n := range dimNames {
			dimHeaders[i] = map[string]string{"name": n}
		}
		metHeaders := make([]map[string]string, len(metricNames))
		for i, n := range metricNames {
			metHeaders[i] = map[string]string{"name": n, "type": metricTypes[i]}
		}

		rows := make([]map[string]any, len(pageRows))
		for i, s := range pageRows {
			dimVals := make([]map[string]string, len(s.DimensionValues))
			for j, v := range s.DimensionValues {
				dimVals[j] = map[string]string{"value": v}
			}
			metVals := make([]map[string]string, len(s.MetricValues))
			for j, v := range s.MetricValues {
				metVals[j] = map[string]string{"value": v}
			}
			rows[i] = map[string]any{
				"dimensionValues": dimVals,
				"metricValues":    metVals,
			}
		}

		resp := map[string]any{
			"dimensionHeaders": dimHeaders,
			"metricHeaders":    metHeaders,
			"rows":             rows,
			"rowCount":         rowCount,
		}

		log.Printf("[seeder] runReport property=%s date=%s offset=%s limit=%d rows=%d/%d",
			property, date, strconv.FormatInt(req.Offset, 10), limit, len(pageRows), rowCount)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}
