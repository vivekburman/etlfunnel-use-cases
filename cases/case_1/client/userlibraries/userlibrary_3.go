package client_userlibrary

// AuxDBKey is the standard map key for the auxiliary PostgreSQL connection.
const AuxDBKey = "Aux Postgres"

// ExtractSplitIndex reads the table split number from record metadata.
// Returns 1 if not present.
func ExtractSplitIndex(record map[string]any) int {
	if v, ok := record["_split_index"]; ok {
		switch n := v.(type) {
		case int:
			return n
		case int64:
			return int(n)
		case float64:
			return int(n)
		}
	}
	return 1
}

// ExtractErrorMessage pulls a pre-set error message from the record if the
// transformer embedded one, otherwise returns an empty string.
func ExtractErrorMessage(record map[string]any) string {
	if v, ok := record["_error_message"]; ok {
		if msg, ok := v.(string); ok {
			return msg
		}
	}
	return ""
}

// IsNoRows detects a pgx "no rows in result set" scan error without importing pgx directly.
func IsNoRows(err error) bool {
	return err != nil && err.Error() == "no rows in result set"
}
