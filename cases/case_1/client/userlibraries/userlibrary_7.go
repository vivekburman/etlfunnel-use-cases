package client_userlibrary

import (
	"fmt"
	"strings"
)

// BuildInsertQuery returns a parameterised INSERT for the given table and column list.
func BuildInsertQuery(table string, columns []string) string {
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		table, strings.Join(columns, ", "), strings.Join(placeholders, ", "),
	)
}

// BuildUpsertQuery returns an INSERT … ON CONFLICT DO UPDATE for the given table.
// conflictCols are the columns in the ON CONFLICT target.
// skipOnUpdate columns are kept immutable on conflict (identity / audit keys).
func BuildUpsertQuery(table string, columns []string, conflictCols []string, skipOnUpdate map[string]bool) string {
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	var setClauses []string
	for _, col := range columns {
		if !skipOnUpdate[col] {
			setClauses = append(setClauses, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
		}
	}
	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
		table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(conflictCols, ", "),
		strings.Join(setClauses, ", "),
	)
}

// ExtractValues returns values from record in the order defined by columns.
// Missing keys produce a nil (NULL) entry; required-column validation upstream
// ensures critical fields are present before this is called.
func ExtractValues(record map[string]any, columns []string) []any {
	values := make([]any, len(columns))
	for i, col := range columns {
		values[i] = record[col]
	}
	return values
}

// ValidateRecord returns an error if any required column is absent or nil.
func ValidateRecord(record map[string]any, required map[string]bool) error {
	for col := range required {
		v, ok := record[col]
		if !ok || v == nil {
			return fmt.Errorf("missing required destination column %q — record routed to backlog", col)
		}
	}
	return nil
}
