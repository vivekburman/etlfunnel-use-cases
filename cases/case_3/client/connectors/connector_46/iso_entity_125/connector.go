package client_connector_46_iso_entity_125

// MSSQL staging destination connector for the Myntra analytics ETL.
// Targets stage.ga4_sessions — a HEAP table used as the landing zone before
// each MERGE into dbo.ga4_sessions.
//
// The table has no indexes so writes are purely sequential; the engine batches
// 1,000 rows per INSERT (configured via DestinationWriteRule in destinationwrite_1).

import (
	"fmt"
	"strings"

	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
)

// stagingColumns must match stage.ga4_sessions column order exactly.
var stagingColumns = []string{
	"property_id", "surface", "report_date", "session_id", "user_pseudo_id",
	"device_category", "city", "country", "source", "medium", "campaign",
	"product_category", "payment_method", "app_version", "os_version",
	"sessions", "engaged_sessions", "total_users", "new_users",
	"bounce_rate", "avg_session_duration_secs",
	"conversions", "purchase_revenue_inr", "event_count", "screen_page_views",
	"ingested_at", "pipeline_run_id",
}

type IUseConnector struct{}

var _ coreinterface.IClientDBMicrosoftServerDest = (*IUseConnector)(nil)

func (d *IUseConnector) GenerateQuery(param *models.MicrosoftServerDestQuery) ([]*models.MicrosoftServerDestQueryTune, error) {
	query := buildInsert("stage.ga4_sessions", stagingColumns)
	tunes := make([]*models.MicrosoftServerDestQueryTune, 0, len(param.Records))
	for _, rec := range param.Records {
		if err := validateRecord(rec); err != nil {
			return nil, err
		}
		tunes = append(tunes, &models.MicrosoftServerDestQueryTune{
			Query: query,
			Value: extractValues(rec, stagingColumns),
		})
	}
	return tunes, nil
}

// ── helpers ────────────────────────────────────────────────────────────────

func validateRecord(rec map[string]any) error {
	required := []string{"property_id", "surface", "report_date", "session_id"}
	for _, col := range required {
		if v, ok := rec[col]; !ok || v == nil || v == "" {
			return fmt.Errorf("staging connector: missing required field %q", col)
		}
	}
	return nil
}

func buildInsert(table string, columns []string) string {
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(table)
	sb.WriteString(" (")
	sb.WriteString(strings.Join(columns, ", "))
	sb.WriteString(") VALUES (")

	for i := range columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("@p%d", i+1))
	}
	sb.WriteString(")")
	return sb.String()
}

func extractValues(rec map[string]any, columns []string) []any {
	vals := make([]any, len(columns))
	for i, col := range columns {
		vals[i] = rec[col]
	}
	return vals
}
