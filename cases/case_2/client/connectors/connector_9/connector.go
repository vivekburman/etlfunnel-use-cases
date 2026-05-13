package client_connector_9

// Zomato Platform Order Intelligence — connector_9: Elasticsearch destination (STEP-13)
//
// Writes transformed order documents to the `platform_orders` Elasticsearch
// index using the _bulk API. Shared by both cold (iso_entity_33) and hot
// (iso_entity_34) flows.
//
// Document ID: "{sub_brand}_{order_id}" — globally unique across brands and
// idempotent across flows (last writer wins).
//
// iso_entities owned by this connector:
//   iso_entity_33 — cold flow write
//   iso_entity_34 — hot flow write (also drives XACK after confirmed ES write)

import (
	"bytes"
	"context"
	"encoding/json"
	"etlfunnel/execution/models"
	"fmt"
	"io"
	"net/http"
	"time"
)

const indexName = "platform_orders"

// BulkResult summarises the outcome of one _bulk call.
type BulkResult struct {
	Indexed  int
	Rejected []RejectedDoc
}

// RejectedDoc holds the ES error detail for a single rejected document.
type RejectedDoc struct {
	ID     string
	Status int
	Reason string
	Record map[string]any
}

// BulkWrite sends records to Elasticsearch via _bulk API.
func BulkWrite(ctx context.Context, props *models.ConnectorProps, records []map[string]any) (*BulkResult, error) {
	if len(records) == 0 {
		return &BulkResult{}, nil
	}

	body, ids, err := buildBulkBody(records)
	if err != nil {
		return nil, fmt.Errorf("connector_9: build bulk body: %w", err)
	}

	esURL := props.DestinationURL
	if esURL == "" {
		esURL = "http://localhost:9200"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, esURL+"/_bulk", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("connector_9: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("connector_9: HTTP POST /_bulk: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 500 {
		return nil, fmt.Errorf("connector_9: ES _bulk %d: %s", resp.StatusCode, truncate(string(respBody), 300))
	}

	return parseBulkResponse(respBody, ids, records)
}

func buildBulkBody(records []map[string]any) ([]byte, []string, error) {
	var buf bytes.Buffer
	ids := make([]string, 0, len(records))

	for _, rec := range records {
		docID := buildDocID(rec)
		ids = append(ids, docID)

		action := map[string]any{
			"index": map[string]any{"_index": indexName, "_id": docID},
		}
		actionLine, err := json.Marshal(action)
		if err != nil {
			return nil, nil, err
		}
		docLine, err := json.Marshal(rec)
		if err != nil {
			return nil, nil, err
		}
		buf.Write(actionLine)
		buf.WriteByte('\n')
		buf.Write(docLine)
		buf.WriteByte('\n')
	}

	return buf.Bytes(), ids, nil
}

func buildDocID(rec map[string]any) string {
	brand, _ := rec["sub_brand"].(string)
	orderID := fmt.Sprintf("%v", rec["order_id"])
	if brand == "" || orderID == "" || orderID == "<nil>" {
		return fmt.Sprintf("unknown_%d", time.Now().UnixNano())
	}
	return brand + "_" + orderID
}

func parseBulkResponse(body []byte, ids []string, records []map[string]any) (*BulkResult, error) {
	var esResp struct {
		Errors bool                         `json:"errors"`
		Items  []map[string]json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(body, &esResp); err != nil {
		return &BulkResult{Indexed: len(ids)}, nil
	}

	result := &BulkResult{}
	for i, item := range esResp.Items {
		for _, raw := range item {
			var doc struct {
				Status int `json:"status"`
				Error  *struct {
					Type   string `json:"type"`
					Reason string `json:"reason"`
				} `json:"error"`
			}
			if err := json.Unmarshal(raw, &doc); err != nil {
				continue
			}
			if doc.Status >= 200 && doc.Status < 300 {
				result.Indexed++
			} else {
				reason := ""
				if doc.Error != nil {
					reason = doc.Error.Type + ": " + doc.Error.Reason
				}
				var rec map[string]any
				if i < len(records) {
					rec = records[i]
				}
				docID := ""
				if i < len(ids) {
					docID = ids[i]
				}
				result.Rejected = append(result.Rejected, RejectedDoc{
					ID: docID, Status: doc.Status, Reason: reason, Record: rec,
				})
			}
		}
	}
	return result, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
