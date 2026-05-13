package client_connector_1

// Zomato Platform Order Intelligence — connector_1: Zomato Food cold source (STEP-11)
//
// Postgres source connector for the zomato_food brand DB (port 5441).
// Implements GenerateQuery / GenerateBinLog / FetchBatch directly.
//
// iso_entities owned by this connector:
//   iso_entity_1 — orders
//   iso_entity_2 — order_items
//   iso_entity_3 — order_status_events
//   iso_entity_4 — delivery_assignments

import (
	"context"
	"etlfunnel/execution/models"
	"fmt"
)

const brand = "zomato_food"

// GenerateQuery returns a resumable paginated SELECT for the given city-split table.
// props.TableName  — e.g. "orders_delhi_1"
// props.LastPK     — last order_id processed (0 on first run)
// props.BatchSize  — rows per page (default 1000)
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

// GenerateBinLog is not applicable to the cold flow.
func GenerateBinLog(_ *models.ConnectorProps) string {
	panic(fmt.Sprintf("connector_1(%s): GenerateBinLog called on cold-flow connector", brand))
}

// FetchBatch executes the paginated query and returns decoded row maps.
func FetchBatch(ctx context.Context, dbConn models.IDBConn, props *models.ConnectorProps) ([]map[string]any, error) {
	query := GenerateQuery(props)

	rows, err := dbConn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("connector_1(%s): query %q: %w", brand, props.TableName, err)
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
			return nil, fmt.Errorf("connector_1(%s): row scan: %w", brand, scanErr)
		}
		rec := make(map[string]any, len(colNames))
		for i, col := range colNames {
			rec[col] = vals[i]
		}
		records = append(records, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("connector_1(%s): rows iteration: %w", brand, err)
	}

	return records, nil
}
