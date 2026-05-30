package client_orchestrator_2

// Daily Incremental orchestrator — generates replicas for T-2 and T-1 across
// all three GA4 properties.
//
// GA4 data is not fully settled for 48-72 hours after the session date.  This
// orchestrator always fetches the last two completed days:
//
//   T-2  fully settled — primary upsert target
//   T-1  partially settled — precautionary re-upsert; overwrites any
//         preliminary rows inserted by yesterday's run
//
// Runtime config is read from environment variables:
//   GA4_AUTH_TOKEN    bearer token (default: test-token for seeder)
//   SEEDER_URL        GA4 base URL (default: http://localhost:9090)
//   PIPELINE_RUN_ID   parent run ID prefix (default: daily)

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
	authToken := envOr("GA4_AUTH_TOKEN", "test-token")
	baseURL := envOr("SEEDER_URL", "http://localhost:9090")
	runIDPrefix := envOr("PIPELINE_RUN_ID", "daily")

	today := time.Now().UTC().Truncate(24 * time.Hour)
	targetDates := []string{
		today.AddDate(0, 0, -2).Format(ga4DateLayout), // T-2: fully settled
		today.AddDate(0, 0, -1).Format(ga4DateLayout), // T-1: precautionary re-upsert
	}

	var tunes []models.PipelineOrchestratorTune

	for _, pipeline := range param.Pipelines {
		for _, prop := range properties {
			for _, date := range targetDates {
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

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
