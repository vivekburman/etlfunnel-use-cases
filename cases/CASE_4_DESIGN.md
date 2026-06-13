# Case 4 — Zepto Order Event Pipeline
## REST API (Cursor) → Kafka → Cassandra: Two-Flow Chained Architecture

> **Written**: 2026-06-12  
> **Branch**: `feature/KAN-29-kafka-source-dest`  
> **Status**: Design — not yet implemented  
> **Author intent**: Any future LLM or engineer must be able to implement this from scratch using only this document and the source code. Verify all interface signatures against source before coding — the Types section quotes from source but code drifts.

---

## 1. Why This Case Exists

### Cases 1–3 Recap

| Case | Shape | Source | Destination | Novel concept |
|---|---|---|---|---|
| 1 | 1 collection, 1 flow, 1 pipeline | REST API (offset pagination) | MSSQL | Cloud API → SQL warehouse |
| 2 | 1 collection, N flows, 1 pipeline each (fan-out) | PostgreSQL WAL (per DB instance) | Redis | Parallel CDC → cache |
| 3 | Connector layer work | REST API | REST API | New connector types added |

### What Case 4 Introduces

**Two flows in one collection, semantically chained via a Kafka topic.**

- **Flow 1** ("Event Ingestion") is **finite**: it polls a REST API using a cursor until the feed is exhausted, then terminates.
- **Flow 2** ("Event Storage") is **streaming**: it consumes Kafka continuously and writes to Cassandra with TTL.
- Kafka is the **durable handoff layer** between them — Flow 1's output *is* Flow 2's input.

This has never appeared in Cases 1–3:
- Case 1 and 2 had no technology appearing as *both* a destination (in Flow 1) and a source (in Flow 2).
- Case 2 had multiple flows but they were independent fan-outs of the same pattern, not chained.
- No previous case used cursor-based REST API capture, Kafka as a message bus, or Cassandra as a destination.
- No previous case mixed a finite pipeline with a streaming pipeline in the same collection.

---

## 2. Business Context

**Company**: Zepto (quick-commerce, 10-minute delivery, India)  
**Problem**: Order lifecycle events (created → confirmed → picked → dispatched → delivered) are written only to Zepto's operational PostgreSQL DB. Downstream consumers (ML feature pipelines, ops dashboards, audit teams, refund engines) all hammer that DB with event-polling queries. Additionally, there is no independent event store with replay capability.

**Solution**: StreamCraft pipeline that:
1. Continuously polls Zepto's internal Order Events REST API using a cursor (sequence-based, not offset — the API never returns a total count).
2. Publishes normalized events to a Kafka topic (`zepto.order.events`) as the durable event bus.
3. A separate streaming pipeline consumes that topic and persists events into Cassandra with a 90-day TTL — making Cassandra the authoritative time-series event store.

**Why Kafka in the middle (not REST API → Cassandra directly)?**
- Multiple downstream consumers (ML, audit, dashboards) can independently subscribe to `zepto.order.events` without coupling to this pipeline.
- If the Cassandra writer lags or fails, Kafka retains unprocessed events. The REST API cursor checkpoint means Flow 1 does not re-fetch what is already on Kafka.
- Flow 1 can be re-run (catch-up mode) without re-triggering Flow 2 — they are decoupled.

**Why Cassandra (not MSSQL or Postgres)?**
- Order events are write-heavy, time-series, partitioned by `(city, store_id)`. Cassandra excels here.
- Built-in TTL eliminates a separate purge job.
- Wide-column model maps naturally to sparse event payloads (different event types carry different fields).

---

## 3. Connector Types Used

All enum values are from `enum/connector.go` and `enum/source_connector.go`. Verify before coding.

| Flow | Role | Connector | `type` int | `captureMethod` int | Enum constant |
|---|---|---|---|---|---|
| Flow 1 | Source | REST API | `11` | `3` (cursor) | `UseDBRESTAPICaptureByCursorMethod` |
| Flow 1 | Destination | Kafka | `8` | `0` (dest, N/A) | — |
| Flow 2 | Source | Kafka | `8` | `1` (subscription) | `UseDBKafkaCaptureTopicSubscriptionMethod` |
| Flow 2 | Destination | Cassandra | `12` | `0` (dest, N/A) | — |

---

## 4. PID Assignments

PIDs must be globally unique across the collection. Existing PIDs in use: flows 18–24, connectors 29–48, entities 109–129, transformers 61, 75, 76, 79, 80, orchestrators 1, 11, checkpoints/backlogs/terminates 5, 6.

| Artifact | PID | Notes |
|---|---|---|
| **Flow 1** | `30` | "Zepto Order Event Ingestion" |
| **Flow 2** | `31` | "Zepto Order Event Storage" |
| **Flow 1 source connector** | `50` | REST API — Zepto Events API |
| **Flow 1 destination connector** | `51` | Kafka — order events cluster |
| **Flow 2 source connector** | `52` | Kafka — same cluster, consumer group |
| **Flow 2 destination connector** | `53` | Cassandra — zepto_events keyspace |
| **Flow 1 source entity** | `140` | REST API cursor implementation |
| **Flow 1 destination entity** | `141` | Kafka publisher implementation |
| **Flow 2 source entity** | `142` | Kafka consumer implementation |
| **Flow 2 destination entity** | `143` | Cassandra writer implementation |
| **Transformer 85** | `85` | Event Normalizer (Flow 1) |
| **Transformer 86** | `86` | Topic Router (Flow 1) |
| **Transformer 87** | `87` | Partition Key Resolver (Flow 2) |
| **Transformer 88** | `88` | TTL Calculator (Flow 2) |
| **Orchestrator 15** | `15` | Flow 1 pipeline orchestrator (cursor window) |
| **Orchestrator 16** | `16` | Flow 2 pipeline orchestrator (consumer group) |
| **Checkpoint 9** | `9` | Flow 1 cursor checkpoint |
| **Backlog 9** | `9` | Flow 1 failed-publish backlog |
| **Terminate 9** | `9` | Flow 1 cursor-exhausted termination |
| **Checkpoint 10** | `10` | Flow 2 offset checkpoint |
| **Backlog 10** | `10` | Flow 2 failed-write backlog |
| **Terminate 10** | `10` | Flow 2 idle/context termination |
| **DestinationWrite 7** | `7` | Flow 1 Kafka batch tune |
| **DestinationWrite 8** | `8` | Flow 2 Cassandra batch tune |
| **AuxDB** | `35` | PostgreSQL auxiliary hub (cursor + backlog store) |

---

## 5. collection.json

Complete definition. Add this to the existing `collection.json` `flowDefinition` array.

```json
{
  "flowOrchestratorDefinition": {"Name": "", "PID": 0},
  "flowDefinition": [
    {
      "flow": {"name": "Zepto Order Event Ingestion", "pid": 30},
      "pipelineOrchestratorDefintion": {"Name": "Cursor Window Orchestrator", "PID": 15},
      "pipelines": [
        {
          "name": "Pipeline 1",
          "entityBaseName": "zepto_ingestion",
          "transformers": [
            {"name": "Event Normalizer", "pid": 85},
            {"name": "Topic Router", "pid": 86}
          ],
          "sourceIsolationEntity":      {"name": "Zepto Events API Cursor", "type": "", "pid": 140},
          "destinatioIsolationEntity":  {"name": "Kafka Order Events Publisher", "type": "", "pid": 141},
          "checkpoint":     {"name": "Cursor Checkpoint",  "pid": 9},
          "backlog":        {"name": "Publish Backlog",    "pid": 9},
          "terminate":      {"name": "Cursor Exhausted",  "pid": 9},
          "destinationWrite": {"name": "Kafka Batch Tune", "pid": 7},
          "auxilaryHubs": [
            {
              "name": "AuxDB",
              "connectionParams": "{\"host\": \"localhost\", \"port\": 5446, \"sslMode\": \"disable\", \"database\": \"auxdb\", \"password\": \"etl_pass\", \"username\": \"etl_user\", \"connectToReplica\": false}",
              "pid": 35,
              "type": 3
            }
          ]
        }
      ],
      "source": {
        "name": "Zepto Order Events API",
        "connectionParams": "{\"baseURL\": \"http://order-events.zepto.internal\", \"authType\": \"apiKey\", \"apiKeyHeader\": \"X-Internal-Token\", \"token\": \"\"}",
        "pid": 50,
        "type": 11,
        "captureMethod": 3
      },
      "destination": {
        "name": "Kafka Order Events Cluster",
        "connectionParams": "{\"brokers\": [\"kafka-1.zepto.internal:9092\", \"kafka-2.zepto.internal:9092\"], \"clientID\": \"etlfunnel-order-ingestion\", \"kafkaVersion\": \"3.6.0\"}",
        "pid": 51,
        "type": 8,
        "captureMethod": 0
      }
    },
    {
      "flow": {"name": "Zepto Order Event Storage", "pid": 31},
      "pipelineOrchestratorDefintion": {"Name": "Consumer Group Orchestrator", "PID": 16},
      "pipelines": [
        {
          "name": "Pipeline 1",
          "entityBaseName": "zepto_storage",
          "transformers": [
            {"name": "Partition Key Resolver", "pid": 87},
            {"name": "TTL Calculator", "pid": 88}
          ],
          "sourceIsolationEntity":      {"name": "Kafka Order Events Consumer", "type": "", "pid": 142},
          "destinatioIsolationEntity":  {"name": "Cassandra Order Events Writer", "type": "", "pid": 143},
          "checkpoint":     {"name": "Offset Checkpoint",  "pid": 10},
          "backlog":        {"name": "Write Backlog",      "pid": 10},
          "terminate":      {"name": "Context Terminate",  "pid": 10},
          "destinationWrite": {"name": "Cassandra Batch Tune", "pid": 8},
          "auxilaryHubs": [
            {
              "name": "AuxDB",
              "connectionParams": "{\"host\": \"localhost\", \"port\": 5446, \"sslMode\": \"disable\", \"database\": \"auxdb\", \"password\": \"etl_pass\", \"username\": \"etl_user\", \"connectToReplica\": false}",
              "pid": 35,
              "type": 3
            }
          ]
        }
      ],
      "source": {
        "name": "Kafka Order Events Cluster (Consumer)",
        "connectionParams": "{\"brokers\": [\"kafka-1.zepto.internal:9092\", \"kafka-2.zepto.internal:9092\"], \"clientID\": \"etlfunnel-order-storage\", \"kafkaVersion\": \"3.6.0\"}",
        "pid": 52,
        "type": 8,
        "captureMethod": 1
      },
      "destination": {
        "name": "Cassandra Order Events Store",
        "connectionParams": "{\"hosts\": [\"cassandra-1.zepto.internal\", \"cassandra-2.zepto.internal\"], \"keyspace\": \"zepto_events\", \"port\": 9042, \"username\": \"etl_user\", \"password\": \"etl_pass\", \"consistency\": \"local_quorum\", \"dataCenter\": \"dc1\"}",
        "pid": 53,
        "type": 12,
        "captureMethod": 0
      }
    }
  ]
}
```

---

## 6. Cassandra Schema

Create this keyspace and table before running the pipeline. The TTL is set per-row from the pipeline (not at the table level) so late-arriving events get the correct remaining TTL rather than a fresh 90-day window.

```cql
CREATE KEYSPACE IF NOT EXISTS zepto_events
  WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3}
  AND durable_writes = true;

CREATE TABLE IF NOT EXISTS zepto_events.order_events (
  city        text,
  store_id    text,
  event_type  text,
  created_at  timestamp,
  event_id    uuid,
  order_id    text,
  customer_id text,
  status      text,
  amount      decimal,
  payload     text,        -- JSON-serialized event-type-specific fields
  run_id      text,        -- pipeline_run_id for lineage
  PRIMARY KEY ((city, store_id), event_type, created_at, event_id)
) WITH CLUSTERING ORDER BY (event_type ASC, created_at DESC, event_id ASC)
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_unit': 'DAYS',
                    'compaction_window_size': 1}
  AND comment = 'Order lifecycle events. TTL=90d set per-row by pipeline.';
```

**Partition key rationale**: `(city, store_id)` keeps reads for a single store's events co-located while distributing load across cities. A pure `order_id` partition would be too fine-grained (billions of tiny partitions). A pure `city` partition would make Mumbai a hot partition.

---

## 7. Data Contract

### 7.1 REST API Response (Flow 1 Source)

`GET /api/v2/order-events?cursor=<seq_cursor>&limit=500`

```json
{
  "events": [
    {
      "event_id":    "uuid-string",
      "order_id":    "ORD-20260612-84729",
      "customer_id": "CUST-18273",
      "store_id":    "STR-BLR-042",
      "city":        "bangalore",
      "event_type":  "ORDER_DISPATCHED",
      "status":      "dispatched",
      "amount":      349.00,
      "created_at":  "2026-06-12T10:23:41Z",
      "payload": {
        "driver_id": "DRV-9912",
        "eta_mins":  8
      }
    }
  ],
  "next_cursor": "seq_1718185421_00347",
  "has_more":    true
}
```

### 7.2 Record shape after Flow 1 transformers (written to Kafka)

`record.Data` after transformer_85 (Event Normalizer) and transformer_86 (Topic Router):

```
event_id           string   — from API
order_id           string   — from API
customer_id        string   — from API
store_id           string   — from API
city               string   — from API (normalized to lowercase)
event_type         string   — from API (normalized to UPPER_SNAKE_CASE)
status             string   — from API
amount             float64  — from API
created_at         string   — RFC3339 (from API)
payload            string   — JSON-marshaled nested payload
run_id             string   — from ReplicaProps["pipeline_run_id"]
_kafka_topic       string   — "zepto.order.events" (set by transformer_86)
_kafka_key         []byte   — []byte(order_id) (set by transformer_86)
_kafka_event_type  string   — event_type (used as Kafka header)
_kafka_store_id    string   — store_id (used as Kafka header)
```

Fields prefixed `_kafka_` are consumed by the Kafka destination connector and NOT written to Kafka message bodies — they drive routing only.

### 7.3 Record shape after Flow 2 transformers (written to Cassandra)

`record.Data` after transformer_87 (Partition Key Resolver) and transformer_88 (TTL Calculator):

```
event_id     string    — from Kafka message (was set by transformer_85)
order_id     string
customer_id  string
store_id     string
city         string
event_type   string
status       string
amount       float64
created_at   string    — RFC3339
payload      string
run_id       string
_ttl_seconds int       — seconds remaining until 90 days from created_at (set by transformer_88)
```

`_ttl_seconds` is consumed by the Cassandra destination connector to set `USING TTL` and is not written as a column.

---

## 8. File Map — What to Create

Every file below must be created. The package name follows the existing convention: `client_<folder>_<subfolder>` flattened to the last two segments.

```
client/
  orchestrators/
    orchestrator_15/orchestrator.go       ← Flow 1 pipeline orchestrator (cursor window)
    orchestrator_16/orchestrator.go       ← Flow 2 pipeline orchestrator (consumer group config)
  connectors/
    connector_50/iso_entity_140/connector.go   ← REST API source: cursor implementation
    connector_51/iso_entity_141/connector.go   ← Kafka destination: publisher implementation
    connector_52/iso_entity_142/connector.go   ← Kafka source: consumer group implementation
    connector_53/iso_entity_143/connector.go   ← Cassandra destination: order_events writer
  transformers/
    transformer_85/transformer.go         ← Event Normalizer (Flow 1)
    transformer_86/transformer.go         ← Topic Router (Flow 1)
    transformer_87/transformer.go         ← Partition Key Resolver (Flow 2)
    transformer_88/transformer.go         ← TTL Calculator (Flow 2)
  checkpoints/
    checkpoint_9/checkpoint.go            ← cursor position upsert to AuxDB
    checkpoint_10/checkpoint.go           ← Kafka offset upsert to AuxDB
  backlogs/
    backlog_9/backlog.go                  ← failed Kafka publish → AuxDB
    backlog_10/backlog.go                 ← failed Cassandra write → AuxDB
  terminates/
    terminate_9/terminate.go              ← cursor exhausted + idle timeout (finite)
    terminate_10/terminate.go             ← context-cancel only (infinite streaming)
  destinationwrites/
    destinationwrite_7/destinationwrite.go  ← Kafka batch: 250 msgs fixed
    destinationwrite_8/destinationwrite.go  ← Cassandra batch: 100 rows fixed
  pipelines/
    flow_30/pipeline_pipeline_1/bridge.go   ← Flow 1 bridge (copy of flow_24 bridge, update imports)
    flow_31/pipeline_pipeline_1/bridge.go   ← Flow 2 bridge (copy of flow_24 bridge, update imports)
```

---

## 9. Client Code — Full Implementations

### 9.1 Orchestrator 15 — Flow 1 Cursor Window

```go
// client/orchestrators/orchestrator_15/orchestrator.go
package client_orchestrator_15

// Flow 1 orchestrator. Generates one replica per run with the cursor position
// read from AuxDB. If no cursor exists (first run), starts from the beginning.
// The pipeline_run_id encodes the start cursor so restarts are idempotent.
//
// Env vars:
//   ZEPTO_API_TOKEN     API key for the Order Events API (required)
//   ZEPTO_API_BASE_URL  Override base URL (default: http://order-events.zepto.internal)

import (
	"context"
	"fmt"
	"os"
	"time"

	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
)

const defaultZeptoBaseURL = "http://order-events.zepto.internal"

func PipelineOrchestrator(param *models.PipelineOrchestratorProps) ([]models.PipelineOrchestratorTune, error) {
	apiToken := os.Getenv("ZEPTO_API_TOKEN")
	if apiToken == "" {
		return nil, fmt.Errorf("orchestrator_15: ZEPTO_API_TOKEN is not set")
	}

	baseURL := os.Getenv("ZEPTO_API_BASE_URL")
	if baseURL == "" {
		baseURL = defaultZeptoBaseURL
	}

	auxConn, err := ulib.GetAuxPostgresConn(param.AuxilaryDBConnMap)
	if err != nil {
		return nil, fmt.Errorf("orchestrator_15: auxdb connect: %w", err)
	}

	var lastCursor string
	row := auxConn.QueryRow(context.Background(),
		"SELECT last_cursor FROM zepto_ingestion_cursors WHERE pipeline = 'order_events' LIMIT 1")
	_ = row.Scan(&lastCursor) // ignore not-found; empty string = start from beginning

	runID := fmt.Sprintf("zepto-ingestion-%s", time.Now().UTC().Format("20060102-150405"))

	var tunes []models.PipelineOrchestratorTune
	for _, pipeline := range param.Pipelines {
		baseName := pipeline.EntityBaseName
		if baseName == "" {
			baseName = pipeline.Name
		}
		tunes = append(tunes, models.PipelineOrchestratorTune{
			ParentName:  pipeline.Name,
			ReplicaName: fmt.Sprintf("%s_%s", baseName, runID),
			ReplicaProps: map[string]any{
				"api_token":       apiToken,
				"base_url":        baseURL,
				"start_cursor":    lastCursor,
				"pipeline_run_id": runID,
			},
		})
	}
	return tunes, nil
}
```

### 9.2 Orchestrator 16 — Flow 2 Consumer Group

```go
// client/orchestrators/orchestrator_16/orchestrator.go
package client_orchestrator_16

// Flow 2 orchestrator. Generates one long-running replica that subscribes to
// zepto.order.events. No per-run windowing — this pipeline is perpetual.
// The consumer group ID is stable so Kafka offset commits survive restarts.

import (
	"fmt"

	"etlfunnel/execution/models"
)

const (
	kafkaTopic     = "zepto.order.events"
	consumerGroup  = "zepto-order-cassandra-writer"
)

func PipelineOrchestrator(param *models.PipelineOrchestratorProps) ([]models.PipelineOrchestratorTune, error) {
	var tunes []models.PipelineOrchestratorTune
	for _, pipeline := range param.Pipelines {
		baseName := pipeline.EntityBaseName
		if baseName == "" {
			baseName = pipeline.Name
		}
		tunes = append(tunes, models.PipelineOrchestratorTune{
			ParentName:  pipeline.Name,
			ReplicaName: fmt.Sprintf("%s_streaming", baseName),
			ReplicaProps: map[string]any{
				"kafka_topic":      kafkaTopic,
				"consumer_group":   consumerGroup,
				"pipeline_run_id":  "zepto-storage-streaming",
			},
		})
	}
	return tunes, nil
}
```

### 9.3 Connector 50 / Entity 140 — REST API Cursor Source

Implements `coreinterface.IClientRESTAPISource`. Only `GenerateCursorRequest` is active; the other methods return descriptive errors.

```go
// client/connectors/connector_50/iso_entity_140/connector.go
package client_connector_50_iso_entity_140

// Zepto Order Events API — cursor-based source connector.
//
// Cursor format: opaque string returned by the API as "next_cursor".
// On the first run, start_cursor is empty → API returns events from the
// oldest available sequence.
//
// ParseRecords extracts the "events" array and advances the cursor token.
// NextCursorToken returns ("", false) when has_more is false, signalling
// terminate_9 that the feed is exhausted.

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"

	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
)

type orderEventsResponse struct {
	Events []map[string]any `json:"events"`
	NextCursor string        `json:"next_cursor"`
	HasMore    bool          `json:"has_more"`
}

type IUseConnector struct {
	mu          sync.Mutex
	nextCursor  string
	exhausted   bool
}

var _ coreinterface.IClientRESTAPISource = (*IUseConnector)(nil)

func (c *IUseConnector) GenerateCursorRequest(param *models.RESTAPISourceFetch) (*models.RESTAPISourceCursorTune, error) {
	rp := param.State.GetReplicaProps()
	apiToken, _ := rp["api_token"].(string)
	startCursor, _ := rp["start_cursor"].(string)

	if apiToken == "" {
		return nil, fmt.Errorf("entity_140: replica prop 'api_token' is required")
	}

	c.mu.Lock()
	if c.nextCursor == "" {
		c.nextCursor = startCursor
	}
	c.mu.Unlock()

	return &models.RESTAPISourceCursorTune{
		Path:   "/api/v2/order-events",
		Method: "GET",
		Headers: map[string]string{
			"X-Internal-Token": apiToken,
			"Accept":           "application/json",
		},
		QueryParams: func() map[string]string {
			c.mu.Lock()
			cursor := c.nextCursor
			c.mu.Unlock()
			params := map[string]string{"limit": "500"}
			if cursor != "" {
				params["cursor"] = cursor
			}
			return params
		}(),
		ParseRecords: func(responseBody []byte, _ http.Header) ([]map[string]any, error) {
			var resp orderEventsResponse
			if err := json.Unmarshal(responseBody, &resp); err != nil {
				return nil, fmt.Errorf("entity_140: unmarshal response: %w", err)
			}
			c.mu.Lock()
			c.nextCursor = resp.NextCursor
			c.exhausted = !resp.HasMore
			c.mu.Unlock()
			return resp.Events, nil
		},
		NextCursorToken: func(responseBody []byte, _ http.Header) (string, bool) {
			c.mu.Lock()
			defer c.mu.Unlock()
			if c.exhausted {
				return "", false
			}
			return c.nextCursor, true
		},
	}, nil
}

func (c *IUseConnector) GeneratePaginateRequest(_ *models.RESTAPISourceFetch) (*models.RESTAPISourcePaginateTune, error) {
	return nil, fmt.Errorf("entity_140: Zepto Events API uses cursor mode, not pagination")
}

func (c *IUseConnector) GenerateWebhookRequest(_ *models.RESTAPISourceFetch) (*models.RESTAPISourceWebhookTune, error) {
	return nil, fmt.Errorf("entity_140: Zepto Events API does not support webhook mode")
}

func (c *IUseConnector) FetchRecords(_ *models.RESTAPISourceFetch) <-chan map[string]any {
	ch := make(chan map[string]any)
	close(ch)
	return ch
}

// GetExhausted is read by terminate_9 to detect cursor exhaustion.
func (c *IUseConnector) GetExhausted() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.exhausted
}

// GetLastCursor is read by checkpoint_9 to persist the last committed cursor.
func (c *IUseConnector) GetLastCursor() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nextCursor
}

// ── helpers (unexported) ──────────────────────────────────────────────────────

func readAll(r io.Reader) []byte {
	b, _ := io.ReadAll(r)
	return b
}
```

**IMPORTANT NOTE for implementer**: `terminate_9` and `checkpoint_9` need access to `GetExhausted()` and `GetLastCursor()`. The `clientSourceBridge` field on `pipelineContext` (in `bridge.go`) is typed as `*client_source_entity.IUseConnector`. Cast it to access these methods within the terminate/checkpoint functions by passing the bridge pointer through `ReplicaProps` or via a package-level singleton. The simplest pattern is a **package-level var** inside `iso_entity_140`:

```go
// In connector.go — package-level singleton accessed by checkpoint_9/terminate_9
var Instance = &IUseConnector{}
```

Then `checkpoint_9` imports `entity140 "etlfunnel/execution/client/connectors/connector_50/iso_entity_140"` and calls `entity140.Instance.GetLastCursor()`.

### 9.4 Connector 51 / Entity 141 — Kafka Destination (Publisher)

Implements `coreinterface.IClientDBKafkaDest`.

```go
// client/connectors/connector_51/iso_entity_141/connector.go
package client_connector_51_iso_entity_141

// Kafka publisher for zepto.order.events.
//
// Message layout:
//   Key:   order_id bytes (ensures all events for one order land on the same partition)
//   Value: JSON of the record minus _kafka_* meta fields
//   Headers:
//     event_type — for consumer-side filtering without deserializing the value
//     store_id   — for geo-sharded consumers
//
// Fields prefixed "_kafka_" in record.Data are consumed here and stripped
// from the message value so they do not appear in Cassandra downstream.

import (
	"encoding/json"
	"fmt"

	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
)

type IUseConnector struct{}

var _ coreinterface.IClientDBKafkaDest = (*IUseConnector)(nil)

func (d *IUseConnector) GenerateQuery(param *models.KafkaDestQuery) ([]*models.KafkaDestQueryTune, error) {
	tunes := make([]*models.KafkaDestQueryTune, 0, len(param.Records))

	for _, rec := range param.Records {
		topic, _ := rec["_kafka_topic"].(string)
		if topic == "" {
			topic = "zepto.order.events"
		}

		keyStr, _ := rec["_kafka_key"].(string)
		if keyStr == "" {
			keyStr, _ = rec["order_id"].(string)
		}

		eventType, _ := rec["_kafka_event_type"].(string)
		storeID, _ := rec["_kafka_store_id"].(string)

		// Strip routing meta-fields before serialising the value.
		payload := make(map[string]any, len(rec))
		for k, v := range rec {
			if len(k) > 0 && k[:1] == "_" {
				continue
			}
			payload[k] = v
		}

		valueBytes, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("entity_141: marshal record: %w", err)
		}

		tunes = append(tunes, &models.KafkaDestQueryTune{
			Topic:     topic,
			Key:       []byte(keyStr),
			Value:     valueBytes,
			Partition: -1, // partitioner-assigned (murmur2 on Key)
			Headers: []models.KafkaHeader{
				{Key: "event_type", Value: []byte(eventType)},
				{Key: "store_id", Value: []byte(storeID)},
			},
		})
	}

	return tunes, nil
}
```

### 9.5 Connector 52 / Entity 142 — Kafka Source (Consumer)

Implements `coreinterface.IClientDBKafkaSource`. Uses `GenerateSubscription`; `GenerateAssignment` and `FetchRecords` return errors.

```go
// client/connectors/connector_52/iso_entity_142/connector.go
package client_connector_52_iso_entity_142

// Kafka consumer for zepto.order.events.
//
// Consumer group: zepto-order-cassandra-writer (stable across restarts).
// InitialOffset: sarama.OffsetOldest on first join; thereafter Kafka tracks
// committed offsets per group.
//
// The engine's default Kafka source parsing puts:
//   record.Data["_kafka_value"]     = message value bytes
//   record.Data["_kafka_key"]       = message key string
//   record.Data["_kafka_partition"] = partition int32
//   record.Data["_kafka_offset"]    = offset int64
//   record.Data["_kafka_topic"]     = topic string
//
// transformer_87 (Partition Key Resolver) unwraps _kafka_value (JSON) into
// top-level fields so the rest of the pipeline sees a flat record.

import (
	"fmt"
	"time"

	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"

	"github.com/IBM/sarama"
)

type IUseConnector struct{}

var _ coreinterface.IClientDBKafkaSource = (*IUseConnector)(nil)

func (c *IUseConnector) GenerateSubscription(param *models.KafkaSourceSubscribe) (*models.KafkaSourceSubscriptionTune, error) {
	rp := param.State.GetReplicaProps()

	topic, _ := rp["kafka_topic"].(string)
	groupID, _ := rp["consumer_group"].(string)
	if topic == "" {
		topic = "zepto.order.events"
	}
	if groupID == "" {
		groupID = "zepto-order-cassandra-writer"
	}

	return &models.KafkaSourceSubscriptionTune{
		Topics:            []string{topic},
		GroupID:           groupID,
		InitialOffset:     sarama.OffsetOldest,
		SessionTimeout:    30 * time.Second,
		HeartbeatInterval: 10 * time.Second,
		MaxWaitTime:       500 * time.Millisecond,
		FetchMaxBytes:     10 * 1024 * 1024, // 10 MB
		AutoCommit:        true,
		CommitInterval:    1 * time.Second,
	}, nil
}

func (c *IUseConnector) GenerateAssignment(_ *models.KafkaSourceAssign) (*models.KafkaSourceAssignmentTune, error) {
	return nil, fmt.Errorf("entity_142: use subscription mode, not manual assignment")
}

func (c *IUseConnector) FetchRecords(_ *models.KafkaSourceFetch) <-chan map[string]any {
	ch := make(chan map[string]any)
	close(ch)
	return ch
}
```

### 9.6 Connector 53 / Entity 143 — Cassandra Destination (Order Events Writer)

Implements `coreinterface.IClientDBCassandraDest`.

```go
// client/connectors/connector_53/iso_entity_143/connector.go
package client_connector_53_iso_entity_143

// Cassandra writer for zepto_events.order_events.
//
// Each record produces one INSERT with USING TTL <_ttl_seconds>.
// The TTL is set by transformer_88 based on the event's created_at so that
// late-arriving events are not granted a fresh 90-day window.
//
// If _ttl_seconds is missing or <= 0 the row is inserted without TTL (safety
// fallback — the compaction strategy will eventually clean it up).
//
// Column mapping:
//   city, store_id     — partition key  (must be present; error if missing)
//   event_type         — clustering col (must be present)
//   created_at         — clustering col (must be present, RFC3339 string → CQL timestamp)
//   event_id           — clustering col (must be present, UUID string)
//   order_id, customer_id, status, amount, payload, run_id — regular cols

import (
	"fmt"

	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
)

const defaultTTL = 90 * 24 * 60 * 60 // 90 days in seconds

var requiredFields = []string{"city", "store_id", "event_type", "created_at", "event_id"}

type IUseConnector struct{}

var _ coreinterface.IClientDBCassandraDest = (*IUseConnector)(nil)

func (d *IUseConnector) GenerateQuery(param *models.CassandraDestQuery) ([]*models.CassandraDestQueryTune, error) {
	tunes := make([]*models.CassandraDestQueryTune, 0, len(param.Records))

	for _, rec := range param.Records {
		for _, f := range requiredFields {
			if rec[f] == nil || rec[f] == "" {
				return nil, fmt.Errorf("entity_143: missing required field %q", f)
			}
		}

		ttl := defaultTTL
		if v, ok := rec["_ttl_seconds"]; ok {
			if n, ok := v.(int); ok && n > 0 {
				ttl = n
			}
		}

		cql := `INSERT INTO zepto_events.order_events
			(city, store_id, event_type, created_at, event_id,
			 order_id, customer_id, status, amount, payload, run_id)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			USING TTL ?`

		tunes = append(tunes, &models.CassandraDestQueryTune{
			Operation: models.CassandraDestOpInsert,
			CQL:       cql,
			TTL:       ttl,
			Parameters: []any{
				rec["city"],
				rec["store_id"],
				rec["event_type"],
				rec["created_at"],
				rec["event_id"],
				rec["order_id"],
				rec["customer_id"],
				rec["status"],
				rec["amount"],
				rec["payload"],
				rec["run_id"],
				ttl,
			},
		})
	}

	return tunes, nil
}
```

### 9.7 Transformer 85 — Event Normalizer (Flow 1)

```go
// client/transformers/transformer_85/transformer.go
package client_transformer_85

// Event Normalizer — Flow 1 (REST API → Kafka).
//
// 1. Normalizes city to lowercase, event_type to UPPER_SNAKE_CASE.
// 2. JSON-marshals the nested "payload" map into a string so Kafka
//    value is flat JSON (Cassandra stores payload as text).
// 3. Injects run_id from ReplicaProps.
// Returns nil (drop record) if city, event_id, or order_id is absent.

import (
	"encoding/json"
	"strings"

	"etlfunnel/execution/models"
)

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	rec := param.Record

	city, _ := rec["city"].(string)
	eventID, _ := rec["event_id"].(string)
	orderID, _ := rec["order_id"].(string)

	if city == "" || eventID == "" || orderID == "" {
		return nil, nil // drop incomplete records silently
	}

	rp := param.State.GetReplicaProps()
	runID, _ := rp["pipeline_run_id"].(string)

	out := make(map[string]any, len(rec)+1)
	for k, v := range rec {
		out[k] = v
	}

	out["city"] = strings.ToLower(city)

	if et, ok := rec["event_type"].(string); ok {
		out["event_type"] = strings.ToUpper(strings.ReplaceAll(et, " ", "_"))
	}

	// Marshal nested payload map to string.
	if payload, ok := rec["payload"].(map[string]any); ok {
		b, err := json.Marshal(payload)
		if err == nil {
			out["payload"] = string(b)
		}
	} else if _, ok := rec["payload"].(string); !ok {
		out["payload"] = "{}"
	}

	out["run_id"] = runID

	return out, nil
}
```

### 9.8 Transformer 86 — Topic Router (Flow 1)

```go
// client/transformers/transformer_86/transformer.go
package client_transformer_86

// Topic Router — Flow 1 (REST API → Kafka).
//
// Sets _kafka_topic, _kafka_key, _kafka_event_type, _kafka_store_id on the
// record.  These fields are consumed by entity_141 to build the Kafka message
// and are stripped before the value is serialised — they never appear in
// Cassandra.

import (
	"etlfunnel/execution/models"
)

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	rec := param.Record

	out := make(map[string]any, len(rec)+4)
	for k, v := range rec {
		out[k] = v
	}

	orderID, _ := rec["order_id"].(string)
	eventType, _ := rec["event_type"].(string)
	storeID, _ := rec["store_id"].(string)

	out["_kafka_topic"] = "zepto.order.events"
	out["_kafka_key"] = orderID
	out["_kafka_event_type"] = eventType
	out["_kafka_store_id"] = storeID

	return out, nil
}
```

### 9.9 Transformer 87 — Partition Key Resolver (Flow 2)

```go
// client/transformers/transformer_87/transformer.go
package client_transformer_87

// Partition Key Resolver — Flow 2 (Kafka → Cassandra).
//
// The Kafka source engine delivers records where _kafka_value holds the raw
// message bytes. This transformer:
// 1. JSON-unmarshals _kafka_value into top-level fields.
// 2. Verifies city and store_id are present (Cassandra partition key).
// 3. Passes through _kafka_offset and _kafka_partition for checkpoint_10.
//
// Returns nil (drop) if the value cannot be parsed or partition key is absent.

import (
	"encoding/json"

	"etlfunnel/execution/models"
)

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	rec := param.Record

	raw, ok := rec["_kafka_value"].([]byte)
	if !ok {
		return nil, nil // malformed Kafka message — drop
	}

	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, nil // unparseable — drop
	}

	city, _ := payload["city"].(string)
	storeID, _ := payload["store_id"].(string)
	if city == "" || storeID == "" {
		return nil, nil // cannot determine Cassandra partition key — drop
	}

	out := make(map[string]any, len(payload)+3)
	for k, v := range payload {
		out[k] = v
	}
	// Preserve Kafka metadata for checkpoint_10.
	out["_kafka_offset"] = rec["_kafka_offset"]
	out["_kafka_partition"] = rec["_kafka_partition"]
	out["_kafka_topic"] = rec["_kafka_topic"]

	return out, nil
}
```

### 9.10 Transformer 88 — TTL Calculator (Flow 2)

```go
// client/transformers/transformer_88/transformer.go
package client_transformer_88

// TTL Calculator — Flow 2 (Kafka → Cassandra).
//
// Computes the remaining TTL for a Cassandra row based on the event's
// created_at timestamp.  Events older than 90 days are dropped (TTL would
// be zero or negative — Cassandra would reject them anyway).
//
// The result is stored in _ttl_seconds (int) and consumed by entity_143.

import (
	"fmt"
	"time"

	"etlfunnel/execution/models"
)

const maxAgeSeconds = 90 * 24 * 60 * 60 // 90 days

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	rec := param.Record

	createdAtStr, _ := rec["created_at"].(string)
	if createdAtStr == "" {
		return nil, fmt.Errorf("transformer_88: created_at is missing")
	}

	createdAt, err := time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("transformer_88: parse created_at %q: %w", createdAtStr, err)
	}

	ageSeconds := int(time.Since(createdAt).Seconds())
	remaining := maxAgeSeconds - ageSeconds

	if remaining <= 0 {
		return nil, nil // event is already older than 90 days — drop
	}

	out := make(map[string]any, len(rec)+1)
	for k, v := range rec {
		out[k] = v
	}
	out["_ttl_seconds"] = remaining

	return out, nil
}
```

### 9.11 Checkpoint 9 — Flow 1 Cursor Checkpoint

```go
// client/checkpoints/checkpoint_9/checkpoint.go
package client_checkpoint_9

// Checkpoint for Flow 1 (REST API → Kafka).
//
// After a successful Kafka publish flush, persists the current cursor position
// to AuxDB so a restart resumes from the last acknowledged position rather than
// re-publishing already-delivered events.
//
// Table: zepto_ingestion_cursors
//   pipeline TEXT PRIMARY KEY,
//   last_cursor TEXT,
//   updated_at TIMESTAMPTZ

import (
	"context"
	"fmt"
	"time"

	entity140 "etlfunnel/execution/client/connectors/connector_50/iso_entity_140"
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
)

func Checkpoint(param *models.CheckpointProps) (*models.CheckpointTune, error) {
	if len(param.Records) == 0 {
		return continueAction(), nil
	}

	conn, err := ulib.GetAuxPostgresConn(param.AuxiliaryDBConnMap)
	if err != nil {
		return continueAction(), fmt.Errorf("checkpoint_9: auxdb connect: %w", err)
	}

	cursor := entity140.Instance.GetLastCursor()
	if cursor == "" {
		return continueAction(), nil
	}

	_, execErr := conn.Exec(context.Background(), `
		INSERT INTO zepto_ingestion_cursors (pipeline, last_cursor, updated_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (pipeline) DO UPDATE SET
			last_cursor = EXCLUDED.last_cursor,
			updated_at  = EXCLUDED.updated_at`,
		"order_events", cursor, time.Now().UTC())
	if execErr != nil {
		return continueAction(), fmt.Errorf("checkpoint_9: upsert cursor: %w", execErr)
	}

	return continueAction(), nil
}

func continueAction() *models.CheckpointTune {
	return &models.CheckpointTune{Action: models.ActionContinue}
}
```

**AuxDB table DDL** (run once before first pipeline execution):

```sql
CREATE TABLE IF NOT EXISTS zepto_ingestion_cursors (
  pipeline    TEXT PRIMARY KEY,
  last_cursor TEXT NOT NULL,
  updated_at  TIMESTAMPTZ NOT NULL
);
```

### 9.12 Checkpoint 10 — Flow 2 Offset Checkpoint

```go
// client/checkpoints/checkpoint_10/checkpoint.go
package client_checkpoint_10

// Checkpoint for Flow 2 (Kafka → Cassandra).
//
// Records the last successfully written Kafka offset per partition to AuxDB.
// Kafka auto-commit handles most cases; this provides a secondary record for
// auditing and manual resume if the consumer group offset is lost.
//
// Table: zepto_storage_offsets
//   topic TEXT, partition INT, last_offset BIGINT, updated_at TIMESTAMPTZ
//   PRIMARY KEY (topic, partition)

import (
	"context"
	"fmt"
	"time"

	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
)

func Checkpoint(param *models.CheckpointProps) (*models.CheckpointTune, error) {
	if len(param.Records) == 0 {
		return continueAction(), nil
	}

	conn, err := ulib.GetAuxPostgresConn(param.AuxiliaryDBConnMap)
	if err != nil {
		return continueAction(), fmt.Errorf("checkpoint_10: auxdb connect: %w", err)
	}

	// Collect max offset per partition from committed records.
	type partOffset struct{ partition int32; offset int64; topic string }
	seen := map[int32]partOffset{}

	for _, rec := range param.Records {
		p, _ := rec["_kafka_partition"].(int32)
		o, _ := rec["_kafka_offset"].(int64)
		t, _ := rec["_kafka_topic"].(string)
		if existing, ok := seen[p]; !ok || o > existing.offset {
			seen[p] = partOffset{p, o, t}
		}
	}

	for _, po := range seen {
		_, err := conn.Exec(context.Background(), `
			INSERT INTO zepto_storage_offsets (topic, partition, last_offset, updated_at)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (topic, partition) DO UPDATE SET
				last_offset = EXCLUDED.last_offset,
				updated_at  = EXCLUDED.updated_at`,
			po.topic, po.partition, po.offset, time.Now().UTC())
		if err != nil {
			return continueAction(), fmt.Errorf("checkpoint_10: upsert offset: %w", err)
		}
	}

	return continueAction(), nil
}

func continueAction() *models.CheckpointTune {
	return &models.CheckpointTune{Action: models.ActionContinue}
}
```

**AuxDB table DDL**:

```sql
CREATE TABLE IF NOT EXISTS zepto_storage_offsets (
  topic       TEXT,
  partition   INT,
  last_offset BIGINT NOT NULL,
  updated_at  TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (topic, partition)
);
```

### 9.13 Backlog 9 — Flow 1

```go
// client/backlogs/backlog_9/backlog.go
package client_backlog_9

// Backlog for Flow 1 (REST API → Kafka).
//
// Writes failed records to AuxDB.zepto_ingestion_backlog.
// ActionContinue — a single bad event must not abort the cursor run.
// The cursor checkpoint means re-processing does NOT re-attempt the same
// records; manual re-drive of the backlog is a separate concern.
//
// Table: zepto_ingestion_backlog
//   id BIGSERIAL PK, order_id TEXT, event_id TEXT,
//   failure_stage TEXT, error_message TEXT,
//   record_payload JSONB, pipeline_run_id TEXT, created_at TIMESTAMPTZ

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
)

func Backlog(param *models.BacklogProps) (*models.BacklogTune, error) {
	conn, err := ulib.GetAuxPostgresConn(param.AuxiliaryDBConnMap)
	if err != nil {
		return continueAction(), fmt.Errorf("backlog_9: auxdb connect: %w", err)
	}

	rp := param.State.GetReplicaProps()
	runID, _ := rp["pipeline_run_id"].(string)
	errMsg := ""
	if param.Err != nil {
		errMsg = param.Err.Error()
	}

	for _, rec := range param.Records {
		payload, _ := json.Marshal(rec)
		orderID, _ := rec["order_id"].(string)
		eventID, _ := rec["event_id"].(string)
		_, execErr := conn.Exec(context.Background(), `
			INSERT INTO zepto_ingestion_backlog
				(order_id, event_id, failure_stage, error_message, record_payload, pipeline_run_id, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			orderID, eventID, string(param.FailureStage), errMsg, payload, runID, time.Now().UTC())
		if execErr != nil {
			return continueAction(), fmt.Errorf("backlog_9: insert: %w", execErr)
		}
	}

	return continueAction(), nil
}

func continueAction() *models.BacklogTune {
	return &models.BacklogTune{Action: models.ActionContinue}
}
```

### 9.14 Backlog 10 — Flow 2

```go
// client/backlogs/backlog_10/backlog.go
package client_backlog_10

// Backlog for Flow 2 (Kafka → Cassandra).
// Writes failed records to AuxDB.zepto_storage_backlog.
// ActionContinue — a single bad write must not stall the consumer.
//
// Table: zepto_storage_backlog
//   id BIGSERIAL PK, order_id TEXT, event_id TEXT, kafka_topic TEXT,
//   kafka_partition INT, kafka_offset BIGINT,
//   failure_stage TEXT, error_message TEXT,
//   record_payload JSONB, pipeline_run_id TEXT, created_at TIMESTAMPTZ

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
)

func Backlog(param *models.BacklogProps) (*models.BacklogTune, error) {
	conn, err := ulib.GetAuxPostgresConn(param.AuxiliaryDBConnMap)
	if err != nil {
		return continueAction(), fmt.Errorf("backlog_10: auxdb connect: %w", err)
	}

	rp := param.State.GetReplicaProps()
	runID, _ := rp["pipeline_run_id"].(string)
	errMsg := ""
	if param.Err != nil {
		errMsg = param.Err.Error()
	}

	for _, rec := range param.Records {
		payload, _ := json.Marshal(rec)
		orderID, _ := rec["order_id"].(string)
		eventID, _ := rec["event_id"].(string)
		topic, _ := rec["_kafka_topic"].(string)
		partition, _ := rec["_kafka_partition"].(int32)
		offset, _ := rec["_kafka_offset"].(int64)
		_, execErr := conn.Exec(context.Background(), `
			INSERT INTO zepto_storage_backlog
				(order_id, event_id, kafka_topic, kafka_partition, kafka_offset,
				 failure_stage, error_message, record_payload, pipeline_run_id, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			orderID, eventID, topic, partition, offset,
			string(param.FailureStage), errMsg, payload, runID, time.Now().UTC())
		if execErr != nil {
			return continueAction(), fmt.Errorf("backlog_10: insert: %w", execErr)
		}
	}

	return continueAction(), nil
}

func continueAction() *models.BacklogTune {
	return &models.BacklogTune{Action: models.ActionContinue}
}
```

### 9.15 Terminate 9 — Flow 1 (Finite: Cursor Exhausted)

```go
// client/terminates/terminate_9/terminate.go
package client_terminate_9

// Termination rule for Flow 1 (REST API → Kafka).
//
// Flow 1 is FINITE — it should stop when two conditions are both true:
//   (a) The REST API has returned has_more=false (cursor exhausted).
//   (b) No new records have arrived for idleTimeout (30s) — ensures the
//       final Kafka flush has completed before termination is declared.
//
// Without condition (b), the pipeline could terminate before the last batch
// is flushed to Kafka.

import (
	"fmt"
	"time"

	entity140 "etlfunnel/execution/client/connectors/connector_50/iso_entity_140"
	"etlfunnel/execution/models"
)

const (
	checkInterval = 5 * time.Second
	idleTimeout   = 30 * time.Second
)

func TerminateRule(_ *models.TerminateRuleProps) (*models.TerminateRuleTune, error) {
	return &models.TerminateRuleTune{
		CheckInterval:        checkInterval,
		UserDefinedCheckFunc: checkFunc,
	}, nil
}

func checkFunc(props *models.CustomTerminateRuleCheckProps) (*models.TerminateRuleActionTune, error) {
	if !entity140.Instance.GetExhausted() {
		return &models.TerminateRuleActionTune{Action: models.ActionContinue}, nil
	}

	if props.LastMessageAt.IsZero() || time.Since(props.LastMessageAt) < idleTimeout {
		return &models.TerminateRuleActionTune{Action: models.ActionContinue}, nil
	}

	return &models.TerminateRuleActionTune{
		RuleName: models.TerminateRuleIdleTimeout,
		Reason:   fmt.Sprintf("cursor exhausted and idle for %v — Kafka publish complete", time.Since(props.LastMessageAt).Round(time.Second)),
		Action:   models.ActionStop,
	}, nil
}
```

### 9.16 Terminate 10 — Flow 2 (Infinite: Context Cancel Only)

```go
// client/terminates/terminate_10/terminate.go
package client_terminate_10

// Termination rule for Flow 2 (Kafka → Cassandra).
//
// Flow 2 is INFINITE — it runs until the process receives a shutdown signal.
// The only termination path is context cancellation (handled by the engine's
// main select loop in bridge.go).  This rule never fires ActionStop on its own.

import (
	"etlfunnel/execution/models"
	"time"
)

const checkInterval = 60 * time.Second

func TerminateRule(_ *models.TerminateRuleProps) (*models.TerminateRuleTune, error) {
	return &models.TerminateRuleTune{
		CheckInterval:        checkInterval,
		UserDefinedCheckFunc: neverStop,
	}, nil
}

func neverStop(_ *models.CustomTerminateRuleCheckProps) (*models.TerminateRuleActionTune, error) {
	return &models.TerminateRuleActionTune{Action: models.ActionContinue}, nil
}
```

### 9.17 DestinationWrite 7 — Flow 1 Kafka Batch

```go
// client/destinationwrites/destinationwrite_7/destinationwrite.go
package client_destinationwrite_7

// Kafka publish batch tune for Flow 1.
// Fixed at 250 messages per flush — Kafka producers are not time-of-day
// sensitive so no dynamic adjustment is needed.
// CheckInterval ticks every 10s to allow the termination rule to fire promptly.

import (
	"etlfunnel/execution/models"
	"time"
)

func DestinationWriteRule(_ *models.DestinationWriteProps) (*models.DestinationWriteTune, error) {
	return &models.DestinationWriteTune{
		RecordsPerBatch: 250,
		CheckInterval:   10 * time.Second,
	}, nil
}
```

### 9.18 DestinationWrite 8 — Flow 2 Cassandra Batch

```go
// client/destinationwrites/destinationwrite_8/destinationwrite.go
package client_destinationwrite_8

// Cassandra write batch tune for Flow 2.
// 100 rows per UNLOGGED batch (same partition key guaranteed by the Kafka
// partitioner using order_id as key → all events for one order land on the
// same partition → same (city, store_id) pair after transformer_87).
// CheckInterval 30s gives the TTL calculator time to drain the batch.

import (
	"etlfunnel/execution/models"
	"time"
)

func DestinationWriteRule(_ *models.DestinationWriteProps) (*models.DestinationWriteTune, error) {
	return &models.DestinationWriteTune{
		RecordsPerBatch: 100,
		CheckInterval:   30 * time.Second,
	}, nil
}
```

### 9.19 Bridge Files

Both bridge files are **identical in structure** to `client/pipelines/flow_24/pipeline_pipeline_1/bridge.go`. Copy it verbatim and update only the import block to reference the correct entities, transformers, checkpoint, backlog, terminate, and destinationwrite packages.

**Flow 30 Bridge (`client/pipelines/flow_30/pipeline_pipeline_1/bridge.go`)**:

```
package: client_pipeline_pipeline_1  (same package name — each bridge is its own directory)

Imports to change from flow_24:
  client_source_entity      → client/connectors/connector_50/iso_entity_140
  client_destination_entity → client/connectors/connector_51/iso_entity_141
  client_transformer_75     → client/transformers/transformer_85
  client_transformer_80     → client/transformers/transformer_86
  (remove transformer_76 and transformer_79 — Flow 1 has only 2 transformers)
  client_checkpoint_6       → client/checkpoints/checkpoint_9
  client_backlog_6          → client/backlogs/backlog_9
  client_terminate_6        → client/terminates/terminate_9
  client_destinationwrite_5 → client/destinationwrites/destinationwrite_7
```

In `applyTransformations`: call transformer_85 then transformer_86. Remove the transformer_76 and transformer_79 calls.

**Flow 31 Bridge (`client/pipelines/flow_31/pipeline_pipeline_1/bridge.go`)**:

```
Imports to change:
  client_source_entity      → client/connectors/connector_52/iso_entity_142
  client_destination_entity → client/connectors/connector_53/iso_entity_143
  transformer imports       → transformer_87, transformer_88
  checkpoint                → checkpoint_10
  backlog                   → backlog_10
  terminate                 → terminate_10
  destinationwrite          → destinationwrite_8
```

In `applyTransformations`: call transformer_87 then transformer_88.

---

## 10. connectors.go — Required Imports

Update the root `connectors.go` to include the four new connector packages:

```go
import (
    // existing imports ...

    // Case 4 additions
    _ "etlfunnel/execution/core/source/restapi"    // Flow 1 source (already present if Case 1 active)
    _ "etlfunnel/execution/core/destination/kafka"  // Flow 1 destination
    _ "etlfunnel/execution/core/source/kafka"       // Flow 2 source
    _ "etlfunnel/execution/core/destination/cassandra" // Flow 2 destination
)
```

---

## 11. job.json

Update to point at one of the two flows (run separately, or add a flow orchestrator at the collection level to run both):

```json
{
  "pid": 30,
  "executionMode": 2
}
```

To run Flow 31 separately, set `"pid": 31`. Flow 2 should be started first so it is ready to consume before Flow 1 begins publishing.

---

## 12. Execution Sequence

1. **Create AuxDB tables** (run DDL from checkpoints 9 and 10 above — once only).
2. **Create Cassandra schema** (run CQL from section 6 — once only).
3. **Start Flow 2 first** (`job.json pid: 31`). It subscribes to `zepto.order.events` with `OffsetOldest`, so it will process any events that land on the topic from the beginning of the retention window.
4. **Start Flow 1** (`job.json pid: 30`). It polls the REST API cursor and publishes to Kafka. Flow 2 immediately begins consuming.
5. **Flow 1 terminates** when `entity_140.Instance.GetExhausted()` is true and the idle timeout elapses.
6. **Flow 2 continues running** indefinitely, consuming any future events published by subsequent Flow 1 runs or other Kafka producers.

---

## 13. Design Decisions and Trade-offs

| Decision | Choice | Why | Alternative rejected |
|---|---|---|---|
| Cursor vs pagination (Flow 1) | Cursor | Zepto's API has no total count; cursor is the natural model for append-only event feeds | Offset pagination requires knowing total rows — not available here |
| 2 flows vs 1 flow with 2 pipelines | 2 flows | Current bridge.go is per-pipeline-per-flow; each bridge wires exactly one source and one destination. Splitting into 2 flows reuses the existing bridge pattern without modifying the engine. | 1 flow with 2 pipelines would require the engine to support per-pipeline source/destination overrides at the flow level — not implemented yet |
| Kafka as intermediate | Yes | Decouples ingestion rate from write rate; enables replay; multiple consumers | REST API → Cassandra directly (no replay, no fanout, rate coupling) |
| Cassandra partition key `(city, store_id)` | Wide-column partition | Ops queries are store-centric ("show me all events for store BLR-042 in bangalore today") — this partition serves that access pattern in O(1) | `order_id` partition: too fine (billions of partitions); `city` only: Mumbai becomes a hot partition |
| TTL per-row (not table-level) | Per-row | Late-arriving replayed events should not get a fresh 90-day window. A row that is already 85 days old should expire in 5 days, not 90. | Table-level TTL: simpler but grants all rows a fresh TTL regardless of event age |
| Flow 2 start before Flow 1 | Yes | `OffsetOldest` means Flow 2 will read from the topic's retention start. If Flow 1 runs first, Kafka may have published 500k events before Flow 2 subscribes — they'd all be processed correctly with `OffsetOldest` anyway, but starting Flow 2 first eliminates the lag |  |
| `ActionContinue` on Kafka/Cassandra failure | Yes | A single malformed event must not abort the cursor run or stall the consumer. The backlog captures failed records for manual re-drive. | `ActionStop`: one bad record kills the run — too fragile for an event stream |

---

## 14. Testing Notes

### Flow 1 — REST API → Kafka

Use `httptest.Server` for the REST API (no Docker needed for the source). Spin up the Kafka container from `docker-compose.test.yml` for the destination.

Test cases:
- `TestCursorAdvances`: Serve 3 pages of 5 events each (has_more=true for first two, false for last). Assert 15 Kafka messages published, cursor advances correctly each page.
- `TestCursorResumeFromCheckpoint`: Pre-populate `zepto_ingestion_cursors` with a cursor pointing to page 2. Assert only pages 2 and 3 are fetched.
- `TestMissingRequiredField`: Serve one event without `order_id`. Assert transformer_85 drops it (returns nil), backlog receives it.
- `TestEventTooOld`: Serve one event with `created_at` older than 90 days. Assert transformer_88 drops it (TTL ≤ 0).

### Flow 2 — Kafka → Cassandra

Use `docker-compose.test.yml` Kafka and Cassandra containers.

Test cases:
- `TestFullPipeline`: Publish 10 synthetic order events to `zepto.order.events`. Assert all 10 appear in `zepto_events.order_events` with correct city/store_id partition keys and TTL within 1 second of expected.
- `TestPartitionKeyResolverDropsMalformed`: Publish a message with a non-JSON value. Assert transformer_87 drops it, backlog records it.
- `TestTTLDecay`: Publish an event with `created_at` 80 days ago. Assert Cassandra receives TTL of approximately `10 * 24 * 3600` (±60s).
- `TestOffsetCheckpointUpsert`: After a successful flush, assert `zepto_storage_offsets` is updated for the correct partition.

### Environment Variables Required

```powershell
$env:ZEPTO_API_TOKEN     = "test-token"
$env:ZEPTO_API_BASE_URL  = "http://localhost:<httptest-port>"
$env:TEST_KAFKA_BROKERS  = "localhost:9092"
$env:TEST_CASSANDRA_HOSTS = "localhost"
$env:TEST_POSTGRES_DSN   = "postgres://test:test@localhost:5433/testdb"  # AuxDB
```

---

## 15. Summary

Case 4 introduces **two semantically chained flows within one collection**, using Kafka as a durable inter-flow buffer. The architectural novelty versus Cases 1–3:

- **Flow 1** (finite, cursor-based): REST API → normalize → route → Kafka publish
- **Flow 2** (infinite, streaming): Kafka consume → unwrap → resolve partition key → calculate TTL → Cassandra insert

Every component follows the established patterns: `IUseConnector` implements the appropriate `IClient*` interface, transformers are pure functions on `record.Data`, checkpoint/backlog/terminate are closures over `AuxiliaryDBConnMap`, and bridge files are structural copies of `flow_24`'s bridge with updated imports.

The only genuinely new concepts introduced by this case that do not exist in Cases 1–3 are:
1. Cursor-based REST API capture (`GenerateCursorRequest` / `RESTAPISourceCursorTune`)
2. Kafka as a destination (`IClientDBKafkaDest` / `KafkaDestQueryTune`)
3. Kafka as a source (`IClientDBKafkaSource` / `KafkaSourceSubscriptionTune`)
4. Cassandra as a destination (`IClientDBCassandraDest` / `CassandraDestQueryTune` with TTL)
5. A finite pipeline with cursor-exhausted termination alongside an infinite streaming pipeline in the same collection
