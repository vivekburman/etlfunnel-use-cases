package client_orchestrator_1

// Historical Backfill orchestrator — generates one pipeline replica per
// (property, date) combination covering DATE_FROM … DATE_TO.
//
// Each replica carries ReplicaProps:
//   property_id    string  "properties/123456789"
//   surface        string  "web" | "android" | "ios"
//   date_from      string  "2024-06-15"
//   date_to        string  "2024-06-15"  (always same day; 1-day chunks)
//   auth_token     string  Bearer token for GA4 API
//   base_url       string  http://localhost:9090 (seeder) or GA4 base
//   pipeline_run_id string per-replica traceability ID
//
// Runtime config is read from environment variables so the orchestrator
// stays stateless and can be invoked by the framework without a GlobalProps:
//   DATE_FROM         start date (default: 2024-01-01)
//   DATE_TO           end date   (default: 2025-12-31)
//   GA4_AUTH_TOKEN    bearer token (default: test-token for seeder)
//   SEEDER_URL        GA4 base URL (default: http://localhost:9090)
//   PIPELINE_RUN_ID   parent run ID prefix (default: backfill)

import (
	"fmt"
	"os"
	"time"

	"etlfunnel/execution/models"
)

const ga4DateLayout = "2006-01-02"

var properties = []struct {
	ID      string
	Surface string
}{
	{"properties/123456789", "web"},
	{"properties/987654321", "android"},
	{"properties/567891234", "ios"},
}

func PipelineOrchestrator(param *models.PipelineOrchestratorProps) ([]models.PipelineOrchestratorTune, error) {
	dateFrom := envOr("DATE_FROM", "2024-01-01")
	dateTo := envOr("DATE_TO", "2025-12-31")
	authToken := envOr("GA4_AUTH_TOKEN", "test-token")
	baseURL := envOr("SEEDER_URL", "http://localhost:9090")
	runIDPrefix := envOr("PIPELINE_RUN_ID", "backfill")

	dates, err := enumerateDates(dateFrom, dateTo)
	if err != nil {
		return nil, fmt.Errorf("orchestrator_1: %w", err)
	}

	var tunes []models.PipelineOrchestratorTune

	for _, pipeline := range param.Pipelines {
		for _, prop := range properties {
			for _, date := range dates {
				replicaRunID := fmt.Sprintf("%s-%s-%s", runIDPrefix, prop.Surface, date)
				tunes = append(tunes, models.PipelineOrchestratorTune{
					ParentName:  pipeline.Name,
					ReplicaName: fmt.Sprintf("%s_%s_%s", pipeline.Name, prop.Surface, date),
					ReplicaProps: map[string]any{
						"property_id":     prop.ID,
						"surface":         prop.Surface,
						"date_from":       date,
						"date_to":         date,
						"auth_token":      authToken,
						"base_url":        baseURL,
						"pipeline_run_id": replicaRunID,
					},
				})
			}
		}
	}

	return tunes, nil
}

func enumerateDates(from, to string) ([]string, error) {
	start, err := time.Parse(ga4DateLayout, from)
	if err != nil {
		return nil, fmt.Errorf("parse date_from %q: %w", from, err)
	}
	end, err := time.Parse(ga4DateLayout, to)
	if err != nil {
		return nil, fmt.Errorf("parse date_to %q: %w", to, err)
	}
	if end.Before(start) {
		return nil, fmt.Errorf("date_to %q is before date_from %q", to, from)
	}

	var dates []string
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		dates = append(dates, d.Format(ga4DateLayout))
	}
	return dates, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
