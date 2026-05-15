package client_userlibrary

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

// ExtractLastPK returns the order_id of the last record in the batch.
// Tries "order_id" then "id" then falls back to 0.
func ExtractLastPK(records []map[string]any) int64 {
	if len(records) == 0 {
		return 0
	}
	last := records[len(records)-1]
	for _, key := range []string{"order_id", "id"} {
		if v, ok := last[key]; ok {
			switch n := v.(type) {
			case int64:
				return n
			case int:
				return int64(n)
			case float64:
				return int64(n)
			}
		}
	}
	return 0
}

// ExtractRedisStreamID reads the Redis stream entry ID from record metadata.
// Set by the hot-flow source connector after XREADGROUP.
func ExtractRedisStreamID(records []map[string]any) string {
	if len(records) == 0 {
		return ""
	}
	last := records[len(records)-1]
	if v, ok := last["_redis_stream_id"].(string); ok {
		return v
	}
	return ""
}

// ExtractWALLSN reads the WAL LSN from record metadata.
// Set by the WAL consumer / hot-flow connector.
func ExtractWALLSN(records []map[string]any) string {
	if len(records) == 0 {
		return ""
	}
	last := records[len(records)-1]
	if v, ok := last["_wal_lsn"].(string); ok {
		return v
	}
	return ""
}

// ExtractFailureStageLabel returns a human-readable failure stage label.
// Checks record metadata for "_failure_stage" first (allows WALUnwrap to be
// set by transformer_14), then falls back to the numeric stage parameter.
func ExtractFailureStageLabel(record map[string]any, fallback string) string {
	if v, ok := record["_failure_stage"].(string); ok && v != "" {
		return v
	}
	return fallback
}

// IsNoRows detects a pgx "no rows in result set" scan error without importing pgx directly.
func IsNoRows(err error) bool {
	return err != nil && err.Error() == "no rows in result set"
}

// ShallowClone returns a shallow copy of a record map.
func ShallowClone(src map[string]any) map[string]any {
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// MoveKey renames r[from] to r[to] and deletes the old key.
// No-op if from is absent.
func MoveKey(r map[string]any, from, to string) {
	if v, ok := r[from]; ok {
		r[to] = v
		delete(r, from)
	}
}
