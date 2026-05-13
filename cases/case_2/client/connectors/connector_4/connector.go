package client_connector_4

// Zomato Platform Order Intelligence — connector_4: District cold source (STEP-11)
//
// Postgres source connector for the district brand DB (port 5444).
// Implements GenerateQuery / GenerateBinLog / FetchBatch directly.
//
// Note: district's delivery_assignments carries gate scan events
// (scanned_at, gate_id) rather than rider delivery data.
//
// iso_entities owned by this connector:
//   iso_entity_13 — orders
//   iso_entity_14 — order_items
//   iso_entity_15 — order_status_events
//   iso_entity_16 — delivery_assignments

import (
	"context"
	"etlfunnel/execution/models"
	"fmt"
)

const brand = "district"

func GenerateQuery(props *models.ConnectorProps) string {
	batchSize := props.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}
	return fmt.Sprintf(
		"SELECT * FROM %s WHERE order_id > %d ORDER BY order_id ASC LIMIT %d",
		props.TableName, props.LastPK, batchSize,
	)
}

func GenerateBinLog(_ *models.ConnectorProps) string {
	panic(fmt.Sprintf("connector_4(%s): GenerateBinLog called on cold-flow connector", brand))
}

func FetchBatch(ctx context.Context, dbConn models.IDBConn, props *models.ConnectorProps) ([]map[string]any, error) {
	query := GenerateQuery(props)

	rows, err := dbConn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("connector_4(%s): query %q: %w", brand, props.TableName, err)
	}
	defer rows.Close()

	cols := rows.FieldDescriptions()
	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = string(c.Name)
	}

	var records []map[string]any
	for rows.Next() {
		vals, scanErr := rows.Values()
		if scanErr != nil {
			return nil, fmt.Errorf("connector_4(%s): row scan: %w", brand, scanErr)
		}
		rec := make(map[string]any, len(colNames))
		for i, col := range colNames {
			rec[col] = vals[i]
		}
		records = append(records, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("connector_4(%s): rows iteration: %w", brand, err)
	}

	return records, nil
}
