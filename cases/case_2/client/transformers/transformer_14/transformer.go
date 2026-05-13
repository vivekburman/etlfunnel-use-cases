package client_transformer_14

// Zomato Platform Order Intelligence — transformer_14: WALEventUnwrapper (STEP-27)
//
// Hot-flow ONLY. Runs before the shared 13-transformer chain.
//
// The WAL consumer (Redis stream producer) publishes Postgres logical
// replication change events as JSON with this envelope:
//
//   {
//     "op":    "INSERT" | "UPDATE" | "DELETE",
//     "table": "orders_delhi_1",
//     "lsn":   "0/1A2B3C4",
//     "ts":    "2026-01-15T14:00:00Z",
//     "before": { ... old row ... },   // only for UPDATE/DELETE
//     "after":  { ... new row ... }    // only for INSERT/UPDATE
//   }
//
// This transformer:
//   1. Parses the envelope from the raw Redis stream message.
//   2. For INSERT / UPDATE: extracts the `after` map and forwards it
//      as the record for downstream transformers. Attaches _wal_lsn,
//      _wal_op, _wal_table as metadata on the record.
//   3. For DELETE: constructs a tombstone record (order_id only) and
//      routes it to backlog with failure_stage = "WALDelete".
//      Elasticsearch does not support soft-delete via upsert without
//      explicit handling; the record is backlogged for manual review.
//   4. Malformed envelopes (missing op/after) are backlogged with
//      failure_stage = "WALUnwrap".
//
// Records that are already unwrapped (re-entry after backlog retry)
// are detected by the absence of the "op" key and passed through.

import (
	"encoding/json"
	"etlfunnel/execution/models"
	"fmt"
)

func Transform(param *models.TransformerProps) (*models.TransformerTune, error) {
	var good []map[string]any
	var backlogged []map[string]any

	for _, rec := range param.Records {
		// If the record has no "op" key it has already been unwrapped
		// (e.g. backlog retry re-entered this transformer). Pass through.
		if _, hasOp := rec["op"]; !hasOp {
			good = append(good, rec)
			continue
		}

		unwrapped, action := unwrap(rec)
		switch action {
		case actionForward:
			good = append(good, unwrapped)
		case actionBacklog:
			backlogged = append(backlogged, unwrapped)
		}
	}

	if len(backlogged) > 0 {
		param.State.GetLogger().Warn(
			fmt.Sprintf("transformer_14: %d WAL event(s) routed to backlog", len(backlogged)),
		)
		param.BacklogFn(backlogged)
	}

	return &models.TransformerTune{Action: models.ActionContinue, Records: good}, nil
}

type unwrapAction int

const (
	actionForward  unwrapAction = iota
	actionBacklog
)

func unwrap(envelope map[string]any) (map[string]any, unwrapAction) {
	op, _ := envelope["op"].(string)
	table, _ := envelope["table"].(string)
	lsn, _ := envelope["lsn"].(string)

	switch op {
	case "INSERT", "UPDATE":
		after := extractRow(envelope["after"])
		if after == nil {
			// Malformed: INSERT/UPDATE without an "after" block.
			tombstone := map[string]any{
				"_failure_stage": "WALUnwrap",
				"_error_code":    "MISSING_AFTER",
				"_error_msg":     fmt.Sprintf("WAL %s event for table %q has no 'after' block", op, table),
				"_wal_op":        op,
				"_wal_table":     table,
				"_wal_lsn":       lsn,
				"_raw_envelope":  rawJSON(envelope),
			}
			return tombstone, actionBacklog
		}

		r := shallowClone(after)
		r["_wal_op"] = op
		r["_wal_table"] = table
		r["_wal_lsn"] = lsn
		r["_redis_stream_id"] = envelope["_redis_stream_id"] // propagate from source connector
		return r, actionForward

	case "DELETE":
		// Extract the order_id from the "before" block for the tombstone.
		before := extractRow(envelope["before"])
		orderID := ""
		if before != nil {
			orderID = fmt.Sprintf("%v", before["order_id"])
		}
		tombstone := map[string]any{
			"_failure_stage": "WALDelete",
			"_error_code":    "WAL_DELETE",
			"_error_msg":     fmt.Sprintf("DELETE event for order_id=%s table=%s — Elasticsearch soft-delete requires explicit handling", orderID, table),
			"_wal_op":        op,
			"_wal_table":     table,
			"_wal_lsn":       lsn,
			"order_id":       orderID,
		}
		return tombstone, actionBacklog

	default:
		tombstone := map[string]any{
			"_failure_stage": "WALUnwrap",
			"_error_code":    "UNKNOWN_OP",
			"_error_msg":     fmt.Sprintf("unrecognised WAL op %q for table %q", op, table),
			"_wal_op":        op,
			"_wal_table":     table,
			"_wal_lsn":       lsn,
			"_raw_envelope":  rawJSON(envelope),
		}
		return tombstone, actionBacklog
	}
}

// extractRow handles both typed map[string]any and JSON-string representations.
func extractRow(v any) map[string]any {
	switch r := v.(type) {
	case map[string]any:
		return r
	case string:
		// Some Redis producers serialize the row as a JSON string within the envelope.
		var m map[string]any
		if err := json.Unmarshal([]byte(r), &m); err == nil {
			return m
		}
	}
	return nil
}

func rawJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}

func shallowClone(src map[string]any) map[string]any {
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
