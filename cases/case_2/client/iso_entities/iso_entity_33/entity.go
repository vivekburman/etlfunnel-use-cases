package client_iso_entity_33

// iso_entity_33 — Elasticsearch destination / cold flow (STEP-13)
//
// Calls connector_5's BulkWrite to index transformed records into the
// `platform_orders` Elasticsearch index. Handles partial bulk failures by
// routing rejected documents to backlog and logging batch results to AuxDB
// `es_write_log`.
//
// Used by all 4 cold-flow pipelines (zomato_food, blinkit, hyperpure, district).

import (
	"context"
	"etlfunnel/execution/models"
	ulib "etlfunnel/execution/client/userlibraries"
	conn "github.com/streamcraft/zomato-etl/db_setup/client/connectors/connector_5"
	"fmt"
	"time"
)

const FlowType = "cold"

// Write indexes a batch of transformed records to Elasticsearch.
// Rejected documents are tagged for backlog routing and removed from the
// successful count reported to the pipeline.
func Write(ctx context.Context, param *models.DestinationProps) (*models.DestinationTune, error) {
	if len(param.Records) == 0 {
		return continueAction(0), nil
	}

	result, err := conn.BulkWrite(ctx, param.ConnectorProps, param.Records)
	if err != nil {
		// Full request failure — do not mark records as written.
		param.State.GetLogger().Error(fmt.Sprintf("iso_entity_33: ES bulk write failed: %v", err))
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

	// Log batch result to AuxDB es_write_log.
	logESWrite(ctx, param, result)

	param.State.GetLogger().Debug(fmt.Sprintf(
		"iso_entity_33(cold): indexed=%d rejected=%d",
		result.Indexed, len(result.Rejected),
	))

	return continueAction(result.Indexed), nil
}

func logESWrite(ctx context.Context, param *models.DestinationProps, result *conn.BulkResult) {
	pgConn, err := ulib.GetAuxPostgresConn(param.AuxiliaryDBConnMap)
	if err != nil {
		return // non-fatal: log failure is not worth stopping the pipeline
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
