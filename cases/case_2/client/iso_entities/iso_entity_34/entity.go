package client_iso_entity_34

// iso_entity_34 — Elasticsearch destination / hot flow (STEP-13)
//
// Identical write path to iso_entity_33 (cold) but tagged with FlowType="hot"
// for es_write_log attribution and downstream observability.
//
// After a successful bulk write, the iso-entity also drives XACK for the
// Redis stream entries that were included in this batch — ensuring at-least-
// once delivery without the hot-flow source needing to know the ES outcome.
//
// Used by all 4 hot-flow pipelines (zomato_food, blinkit, hyperpure, district).

import (
	"context"
	"etlfunnel/execution/models"
	ulib "etlfunnel/execution/client/userlibraries"
	conn "github.com/streamcraft/zomato-etl/db_setup/client/connectors/connector_5"
	rs "github.com/streamcraft/zomato-etl/db_setup/client/connectors/redis_source"
	"fmt"
	"time"
)

const FlowType = "hot"

func Write(ctx context.Context, param *models.DestinationProps) (*models.DestinationTune, error) {
	if len(param.Records) == 0 {
		return continueAction(0), nil
	}

	result, err := conn.BulkWrite(ctx, param.ConnectorProps, param.Records)
	if err != nil {
		param.State.GetLogger().Error(fmt.Sprintf("iso_entity_34: ES bulk write failed: %v", err))
		return nil, err
	}

	// Route rejected docs to backlog.
	if len(result.Rejected) > 0 {
		var failedRecs []map[string]any
		for _, rej := range result.Rejected {
			if rej.Record != nil {
				r := shallowClone(rej.Record)
				r["_failure_stage"] = "Destination"
				r["_error_code"] = fmt.Sprintf("ES_%d", rej.Status)
				r["_error_msg"] = rej.Reason
				failedRecs = append(failedRecs, r)
			}
		}
		if len(failedRecs) > 0 {
			param.BacklogFn(failedRecs)
		}
	}

	// XACK the successfully indexed records' Redis stream IDs.
	// This must happen AFTER ES confirms the write to preserve at-least-once.
	if result.Indexed > 0 {
		ackStreamEntries(ctx, param, result)
	}

	logESWrite(ctx, param, result)

	param.State.GetLogger().Debug(fmt.Sprintf(
		"iso_entity_34(hot): indexed=%d rejected=%d",
		result.Indexed, len(result.Rejected),
	))

	return continueAction(result.Indexed), nil
}

// ackStreamEntries collects _redis_stream_id from successfully written records
// and sends XACK per stream so the consumer group advances its cursor.
func ackStreamEntries(ctx context.Context, param *models.DestinationProps, result *conn.BulkResult) {
	// Build a set of rejected IDs to skip.
	rejectedIDs := make(map[string]bool, len(result.Rejected))
	for _, rej := range result.Rejected {
		if rej.Record != nil {
			if id, ok := rej.Record["_redis_stream_id"].(string); ok && id != "" {
				rejectedIDs[id] = true
			}
		}
	}

	// Group stream IDs by stream key (each record carries _wal_table which
	// allows us to derive the stream; simpler to group by brand from sub_brand).
	streamIDs := make(map[string][]string)
	for _, rec := range param.Records {
		streamID, _ := rec["_redis_stream_id"].(string)
		if streamID == "" || rejectedIDs[streamID] {
			continue
		}
		streamKey := brandToStreamKey(rec["sub_brand"])
		if streamKey != "" {
			streamIDs[streamKey] = append(streamIDs[streamKey], streamID)
		}
	}

	for streamKey, ids := range streamIDs {
		if ackErr := rs.AckBatch(ctx, param.RedisClient, streamKey, ids); ackErr != nil {
			param.State.GetLogger().Warn(fmt.Sprintf(
				"iso_entity_34: XACK failed for stream %s (%d ids): %v",
				streamKey, len(ids), ackErr,
			))
		}
	}
}

func brandToStreamKey(v any) string {
	switch brand, _ := v.(string); brand {
	case "zomato_food":
		return "zomato:orders:stream"
	case "blinkit":
		return "blinkit:orders:stream"
	case "hyperpure":
		return "hyperpure:orders:stream"
	case "district":
		return "district:orders:stream"
	}
	return ""
}

func logESWrite(ctx context.Context, param *models.DestinationProps, result *conn.BulkResult) {
	pgConn, err := ulib.GetAuxPostgresConn(param.AuxiliaryDBConnMap)
	if err != nil {
		return
	}

	subBrand, city, entity := ulib.ParseBrandContext(param.State, param.Records)
	_, _ = pgConn.Exec(ctx, `
		INSERT INTO es_write_log
		    (sub_brand, city, entity, flow_type, batch_id, success_count, failure_count, logged_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, now())`,
		subBrand, city, entity, FlowType,
		time.Now().UnixMilli(),
		result.Indexed,
		len(result.Rejected),
	)
}

func continueAction(indexed int) *models.DestinationTune {
	return &models.DestinationTune{
		Action:        models.ActionContinue,
		RecordsWritten: indexed,
	}
}

func shallowClone(src map[string]any) map[string]any {
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
