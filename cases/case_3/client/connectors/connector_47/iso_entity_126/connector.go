package client_connector_47_iso_entity_126

// MSSQL realtime destination connector for the Myntra analytics ETL.
// Targets dbo.realtime_sessions — append-only rolling buffer.
//
// No staging table or MERGE; the Realtime Pulse bridge INSERTs rows directly
// and then issues a TTL DELETE (rows older than 2 hours) in the same pipeline
// tick.  The table is effectively a 2-hour sliding window for the live ops
// dashboard.

import (
	"fmt"
	"strings"

	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
)

var realtimeColumns = []string{
	"snapshot_at", "property_id", "surface",
	"active_users", "city", "device_category", "page_path", "event_name",
}

type IUseConnector struct{}

var _ coreinterface.IClientDBMicrosoftServerDest = (*IUseConnector)(nil)

func (d *IUseConnector) GenerateQuery(param *models.MicrosoftServerDestQuery) ([]*models.MicrosoftServerDestQueryTune, error) {
	query := buildInsert("dbo.realtime_sessions", realtimeColumns)
	tunes := make([]*models.MicrosoftServerDestQueryTune, 0, len(param.Records))
	for _, rec := range param.Records {
		if err := validateRecord(rec); err != nil {
			return nil, err
		}
		tunes = append(tunes, &models.MicrosoftServerDestQueryTune{
			Query: query,
			Value: extractValues(rec, realtimeColumns),
		})
	}
	return tunes, nil
}

// ── helpers (duplicated from connector_46 — each connector is self-contained) ─

func validateRecord(rec map[string]any) error {
	required := []string{"snapshot_at", "property_id", "surface"}
	for _, col := range required {
		if v, ok := rec[col]; !ok || v == nil || v == "" {
			return fmt.Errorf("realtime connector: missing required field %q", col)
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
