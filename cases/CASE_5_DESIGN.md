# Case 5 — Product Catalog AI Enrichment Pipeline
## Postgres + Oracle → Kafka → Mac Enrich Service (Ollama) → Elasticsearch: Four-Flow Fan-In + AI Architecture

> **Written**: 2026-06-13
> **Branch**: `feature/005-case-study-ai-enrichment`
> **Status**: Design — not yet implemented
> **Author intent**: Any future LLM or engineer must be able to implement this from scratch using only this document and the source code at `C:\Users\vivek\streamcraftexecution`. Verify all interface signatures against source before coding — the Types section quotes from source but code drifts.

---

## 1. Why This Case Exists

### Cases 1–4 Recap

| Case | Shape | Source | Destination | Novel concept |
|---|---|---|---|---|
| 1 | 1 collection, 1 flow, 1 pipeline | REST API (offset pagination) | MSSQL | Cloud API → SQL warehouse |
| 2 | 1 collection, N flows, 1 pipeline each (fan-out) | PostgreSQL WAL (per DB instance) | Redis | Parallel CDC → cache |
| 3 | Connector layer work | REST API | REST API | New connector types added |
| 4 | 2 flows chained via Kafka | REST API (cursor) → Kafka → Cassandra | Kafka as source and dest | Mixed finite + infinite flows in one collection |

### What Case 5 Introduces

**Four flows in one collection: two DB sources fan into a shared Kafka topic, then an AI enrichment service acts as both a REST sink (receive) and a REST source (return), with Elasticsearch as the final semantic store.**

Novel concepts not present in Cases 1–4:

| Novel concept | What it demonstrates |
|---|---|
| **Multi-source fan-in** | Two independent source flows (Postgres, Oracle) write to the **same** Kafka topic — Kafka becomes a merge/normalize bus, not just a relay |
| **REST as async processing endpoint** | A Mac-side Ollama service is the destination of Flow 3 (POST records in) and the source of Flow 4 (GET enriched results via cursor) — same REST connector type, opposite roles |
| **AI enrichment as a pipeline stage** | Ollama (`nomic-embed-text`) generates 768-dim embeddings inside the pipeline; the transformer shapes text input, the connector delivers it |
| **Elasticsearch as a destination** | Dense-vector index with `knn` field; Elasticsearch connector type 9, never used before |
| **Four-flow chain** | Longest chain in any case so far; Flows 32 and 33 are independent finite siblings, Flows 34 and 35 are chained infinite/finite streaming consumers |

---

## 2. Business Context

**Company**: Pepperfry (India's largest online furniture/home décor marketplace)
**Problem**: Product catalog is split across two systems that have never been unified:
- **Postgres** (`pf_catalog` DB): new product metadata — title, description, category, price, dimensions, material (managed by the product team, on-prem)
- **Oracle** (`PF_ERP` schema): legacy ERP product attributes — SKU codes, supplier IDs, compliance tags, weight/volume (managed by the supply chain team, cannot be migrated)

Downstream search is keyword-only (Solr). A new **semantic search** initiative requires embedding every product as a 768-dim vector so users can find "sofa that feels like a living room hug" even if no listing uses those exact words.

**Solution**: StreamCraft pipeline that:
1. Polls Postgres for products updated since last run → normalizes to common schema → publishes to `pepperfry.catalog.raw` Kafka topic.
2. Polls Oracle for product attributes updated since last run → normalizes to same common schema → publishes to the **same** `pepperfry.catalog.raw` topic.
3. A streaming pipeline consumes `pepperfry.catalog.raw` → builds an embedding text payload → POSTs batches to a Mac-local Enrich Service (wrapping Ollama).
4. A cursor pipeline polls the Enrich Service for processed results → extracts the embedding vector → bulk-indexes into Elasticsearch with a `dense_vector` field.

**Why Kafka as the fan-in bus?**
- Postgres and Oracle ingestion rates differ; Kafka absorbs the difference without coupling the two flows.
- Future data sources (MongoDB product reviews, S3 image metadata) can publish to the same topic without touching the enrichment or Elasticsearch flows.
- Replay: if the Enrich Service goes down, the Kafka consumer group resumes from the committed offset.

**Why the Mac Enrich Service (not inline Ollama call)?**
- Decouples the pipeline from the model's latency: Ollama embedding can take 50–200 ms per record; batching through a sidecar prevents back-pressure from stalling the Kafka consumer.
- The REST connector type already exists (Case 3); no new connector code needed for the AI integration.
- The service can be replaced by a remote inference endpoint (OpenAI, Vertex AI) by changing one env var.

**Why Elasticsearch (not Cassandra)?**
- `dense_vector` field with `knn` indexing is native in ES 8.x — no plugin required.
- Full-text + vector hybrid search in a single query (BM25 + knn rescoring).
- Pepperfry's search team already owns an ES cluster; no new infrastructure.

---

## 3. Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Collection: Pepperfry Product Catalog Enrichment                            │
│                                                                             │
│  FLOW 32 — finite (Postgres → Kafka)                                        │
│  Postgres ──[T89: normalize]──[T90: validate+route]──► Kafka[catalog.raw]  │
│                                                               │              │
│  FLOW 33 — finite (Oracle → Kafka)                            │ same topic  │
│  Oracle   ──[T91: normalize]──[T92: validate+route]──────────►│             │
│                                                               │              │
│  FLOW 34 — infinite streaming (Kafka → Mac Enrich Service)   │              │
│            Kafka[catalog.raw] ◄───────────────────────────────┘             │
│            ──[T93: build embed text]──[T94: REST route]──► POST /enrich     │
│                                                                │             │
│  FLOW 35 — finite with idle timeout (Mac Enrich → Elasticsearch)            │
│            GET /results?cursor=N ◄─ Mac Enrich Service ◄──────┘             │
│            ──[T95: extract embedding]──[T96: ES doc builder]──► ES index    │
└─────────────────────────────────────────────────────────────────────────────┘

Mac Enrich Service (local, port 8765):
  POST /enrich  ← receives record batches from Flow 34
  GET /results  ← polled with cursor by Flow 35
  Internally calls Ollama POST http://localhost:11434/api/embed
```

---

## 4. Connector Types Used

All enum values from `enum/connector.go` and `enum/source_connector.go`. **Verify before coding.**

| Flow | Role | Connector | `type` int | `captureMethod` int | Notes |
|---|---|---|---|---|---|
| Flow 32 | Source | PostgreSQL | `3` | check `enum/source_connector.go` — use user-defined / FetchRecords constant | Incremental query via `updated_at` |
| Flow 32 | Destination | Kafka | `8` | `0` (dest, N/A) | Publishes to `pepperfry.catalog.raw` |
| Flow 33 | Source | Oracle SQL | `5` | check `enum/source_connector.go` — use user-defined / FetchRecords constant | Incremental query via `UPDATED_AT` |
| Flow 33 | Destination | Kafka | `8` | `0` (dest, N/A) | Same topic as Flow 32 |
| Flow 34 | Source | Kafka | `8` | `1` (subscription) | `UseDBKafkaCaptureTopicSubscriptionMethod` |
| Flow 34 | Destination | REST API | `11` | `0` (dest, N/A) | POST /enrich to Mac service |
| Flow 35 | Source | REST API | `11` | `3` (cursor) | `UseDBRESTAPICaptureByCursorMethod` — GET /results |
| Flow 35 | Destination | Elasticsearch | `9` | `0` (dest, N/A) | Bulk index with dense_vector |

---

## 5. PID Assignments

PIDs must be globally unique across the collection. Previously used PIDs: flows 18–24, 30–31; connectors 29–48, 50–53; entities 109–129, 140–143; transformers 61, 75, 76, 79, 80, 85–88; orchestrators 1, 11, 15, 16; checkpoints/backlogs/terminates 5, 6, 9, 10; destinationwrites 5, 7, 8; auxDB 35.

| Artifact | PID | Notes |
|---|---|---|
| **Flow 32** | `32` | "Pepperfry Postgres Catalog Ingest" |
| **Flow 33** | `33` | "Pepperfry Oracle Catalog Ingest" |
| **Flow 34** | `34` | "Pepperfry AI Enrichment Submit" |
| **Flow 35** | `35` | "Pepperfry AI Enrichment Collect" |
| **Flow 32 source connector** | `54` | Postgres — pf_catalog DB |
| **Flow 32 dest connector** | `55` | Kafka — catalog cluster (publisher) |
| **Flow 33 source connector** | `56` | Oracle — PF_ERP schema |
| **Flow 33 dest connector** | `57` | Kafka — catalog cluster (publisher, same topic) |
| **Flow 34 source connector** | `58` | Kafka — catalog cluster (consumer group) |
| **Flow 34 dest connector** | `59` | REST API — Mac Enrich Service (sink) |
| **Flow 35 source connector** | `60` | REST API — Mac Enrich Service (cursor source) |
| **Flow 35 dest connector** | `61` | Elasticsearch — pepperfry cluster |
| **Flow 32 source entity** | `144` | Postgres incremental query connector |
| **Flow 32 dest entity** | `145` | Kafka publisher (catalog.raw) |
| **Flow 33 source entity** | `146` | Oracle incremental query connector |
| **Flow 33 dest entity** | `147` | Kafka publisher (catalog.raw, same topic) |
| **Flow 34 source entity** | `148` | Kafka consumer (catalog.raw consumer group) |
| **Flow 34 dest entity** | `149` | REST sink — POST /enrich to Mac service |
| **Flow 35 source entity** | `150` | REST cursor source — GET /results from Mac service |
| **Flow 35 dest entity** | `151` | Elasticsearch bulk indexer |
| **Transformer 89** | `89` | Postgres Normalizer (Flow 32) |
| **Transformer 90** | `90` | Catalog Validator + Topic Router (Flow 32) |
| **Transformer 91** | `91` | Oracle Normalizer (Flow 33) |
| **Transformer 92** | `92` | Catalog Validator + Topic Router (Flow 33) |
| **Transformer 93** | `93` | Embed Text Builder (Flow 34) |
| **Transformer 94** | `94` | REST Route Builder (Flow 34) |
| **Transformer 95** | `95` | Embedding Extractor (Flow 35) |
| **Transformer 96** | `96` | ES Document Builder (Flow 35) |
| **Orchestrator 17** | `17` | Flow 32 pipeline orchestrator (Postgres window) |
| **Orchestrator 18** | `18` | Flow 33 pipeline orchestrator (Oracle window) |
| **Orchestrator 19** | `19` | Flow 34 pipeline orchestrator (Kafka consumer group) |
| **Orchestrator 20** | `20` | Flow 35 pipeline orchestrator (results cursor) |
| **Checkpoint 11** | `11` | Flow 32 — last Postgres updated_at |
| **Checkpoint 12** | `12` | Flow 33 — last Oracle UPDATED_AT |
| **Checkpoint 13** | `13` | Flow 34 — Kafka offset |
| **Checkpoint 14** | `14` | Flow 35 — Mac service results cursor |
| **Backlog 11** | `11` | Flow 32 — failed Kafka publishes |
| **Backlog 12** | `12` | Flow 33 — failed Kafka publishes |
| **Backlog 13** | `13` | Flow 34 — failed /enrich POSTs |
| **Backlog 14** | `14` | Flow 35 — failed ES index writes |
| **Terminate 11** | `11` | Flow 32 — page exhausted + idle (finite) |
| **Terminate 12** | `12` | Flow 33 — page exhausted + idle (finite) |
| **Terminate 13** | `13` | Flow 34 — context cancel only (infinite) |
| **Terminate 14** | `14` | Flow 35 — idle timeout 60s (finite) |
| **DestinationWrite 9** | `9` | Flow 32 Kafka batch: 250 msgs |
| **DestinationWrite 10** | `10` | Flow 33 Kafka batch: 250 msgs |
| **DestinationWrite 11** | `11` | Flow 34 REST batch: 50 records per POST |
| **DestinationWrite 12** | `12` | Flow 35 ES batch: 100 docs per bulk |
| **AuxDB** | `35` | PostgreSQL auxiliary hub (reused from Cases 1–4) |

---

## 6. collection.json

Add these four flow definitions to the existing `collection.json` `flowDefinition` array.

```json
[
  {
    "flow": {"name": "Pepperfry Postgres Catalog Ingest", "pid": 32},
    "pipelineOrchestratorDefintion": {"Name": "Postgres Window Orchestrator", "PID": 17},
    "pipelines": [
      {
        "name": "Pipeline 1",
        "entityBaseName": "pf_postgres_ingest",
        "transformers": [
          {"name": "Postgres Normalizer", "pid": 89},
          {"name": "Catalog Validator + Topic Router", "pid": 90}
        ],
        "sourceIsolationEntity":     {"name": "Postgres Catalog Source",   "type": "", "pid": 144},
        "destinatioIsolationEntity": {"name": "Kafka Catalog Publisher",   "type": "", "pid": 145},
        "checkpoint":      {"name": "Postgres Cursor Checkpoint", "pid": 11},
        "backlog":         {"name": "Postgres Publish Backlog",   "pid": 11},
        "terminate":       {"name": "Postgres Page Exhausted",    "pid": 11},
        "destinationWrite":{"name": "Kafka Batch 250",            "pid": 9},
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
      "name": "Pepperfry Postgres Catalog DB",
      "connectionParams": "{\"host\": \"pg-catalog.pepperfry.internal\", \"port\": 5432, \"database\": \"pf_catalog\", \"username\": \"etl_reader\", \"password\": \"\", \"sslMode\": \"require\"}",
      "pid": 54,
      "type": 3,
      "captureMethod": 0
    },
    "destination": {
      "name": "Kafka Catalog Cluster (Postgres Publisher)",
      "connectionParams": "{\"brokers\": [\"kafka-1.pepperfry.internal:9092\", \"kafka-2.pepperfry.internal:9092\"], \"clientID\": \"etlfunnel-pf-postgres-ingest\", \"kafkaVersion\": \"3.6.0\"}",
      "pid": 55,
      "type": 8,
      "captureMethod": 0
    }
  },

  {
    "flow": {"name": "Pepperfry Oracle Catalog Ingest", "pid": 33},
    "pipelineOrchestratorDefintion": {"Name": "Oracle Window Orchestrator", "PID": 18},
    "pipelines": [
      {
        "name": "Pipeline 1",
        "entityBaseName": "pf_oracle_ingest",
        "transformers": [
          {"name": "Oracle Normalizer", "pid": 91},
          {"name": "Catalog Validator + Topic Router", "pid": 92}
        ],
        "sourceIsolationEntity":     {"name": "Oracle Catalog Source",   "type": "", "pid": 146},
        "destinatioIsolationEntity": {"name": "Kafka Catalog Publisher", "type": "", "pid": 147},
        "checkpoint":      {"name": "Oracle Cursor Checkpoint", "pid": 12},
        "backlog":         {"name": "Oracle Publish Backlog",   "pid": 12},
        "terminate":       {"name": "Oracle Page Exhausted",    "pid": 12},
        "destinationWrite":{"name": "Kafka Batch 250",          "pid": 10},
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
      "name": "Pepperfry Oracle ERP DB",
      "connectionParams": "{\"host\": \"oracle-erp.pepperfry.internal\", \"port\": 1521, \"serviceName\": \"PFERP\", \"username\": \"etl_reader\", \"password\": \"\"}",
      "pid": 56,
      "type": 5,
      "captureMethod": 0
    },
    "destination": {
      "name": "Kafka Catalog Cluster (Oracle Publisher)",
      "connectionParams": "{\"brokers\": [\"kafka-1.pepperfry.internal:9092\", \"kafka-2.pepperfry.internal:9092\"], \"clientID\": \"etlfunnel-pf-oracle-ingest\", \"kafkaVersion\": \"3.6.0\"}",
      "pid": 57,
      "type": 8,
      "captureMethod": 0
    }
  },

  {
    "flow": {"name": "Pepperfry AI Enrichment Submit", "pid": 34},
    "pipelineOrchestratorDefintion": {"Name": "Kafka Consumer Orchestrator", "PID": 19},
    "pipelines": [
      {
        "name": "Pipeline 1",
        "entityBaseName": "pf_enrich_submit",
        "transformers": [
          {"name": "Embed Text Builder", "pid": 93},
          {"name": "REST Route Builder",  "pid": 94}
        ],
        "sourceIsolationEntity":     {"name": "Kafka Catalog Consumer",  "type": "", "pid": 148},
        "destinatioIsolationEntity": {"name": "Mac Enrich Service Sink", "type": "", "pid": 149},
        "checkpoint":      {"name": "Kafka Offset Checkpoint",  "pid": 13},
        "backlog":         {"name": "Enrich Submit Backlog",    "pid": 13},
        "terminate":       {"name": "Context Cancel Only",      "pid": 13},
        "destinationWrite":{"name": "REST Batch 50",            "pid": 11},
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
      "name": "Kafka Catalog Cluster (Enrichment Consumer)",
      "connectionParams": "{\"brokers\": [\"kafka-1.pepperfry.internal:9092\", \"kafka-2.pepperfry.internal:9092\"], \"clientID\": \"etlfunnel-pf-enrich-consumer\", \"kafkaVersion\": \"3.6.0\"}",
      "pid": 58,
      "type": 8,
      "captureMethod": 1
    },
    "destination": {
      "name": "Mac Enrich Service (Sink)",
      "connectionParams": "{\"baseURL\": \"http://localhost:8765\", \"authType\": \"\"}",
      "pid": 59,
      "type": 11,
      "captureMethod": 0
    }
  },

  {
    "flow": {"name": "Pepperfry AI Enrichment Collect", "pid": 35},
    "pipelineOrchestratorDefintion": {"Name": "Results Cursor Orchestrator", "PID": 20},
    "pipelines": [
      {
        "name": "Pipeline 1",
        "entityBaseName": "pf_enrich_collect",
        "transformers": [
          {"name": "Embedding Extractor", "pid": 95},
          {"name": "ES Document Builder", "pid": 96}
        ],
        "sourceIsolationEntity":     {"name": "Mac Enrich Service Source", "type": "", "pid": 150},
        "destinatioIsolationEntity": {"name": "Elasticsearch Indexer",     "type": "", "pid": 151},
        "checkpoint":      {"name": "Results Cursor Checkpoint", "pid": 14},
        "backlog":         {"name": "ES Index Backlog",          "pid": 14},
        "terminate":       {"name": "Idle Timeout 60s",          "pid": 14},
        "destinationWrite":{"name": "ES Bulk 100",               "pid": 12},
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
      "name": "Mac Enrich Service (Cursor Source)",
      "connectionParams": "{\"baseURL\": \"http://localhost:8765\", \"authType\": \"\"}",
      "pid": 60,
      "type": 11,
      "captureMethod": 3
    },
    "destination": {
      "name": "Elasticsearch Pepperfry Cluster",
      "connectionParams": "{\"addresses\": [\"http://localhost:9200\"], \"username\": \"elastic\", \"password\": \"\", \"indexName\": \"pf_products\"}",
      "pid": 61,
      "type": 9,
      "captureMethod": 0
    }
  }
]
```

> **Note**: `captureMethod: 0` is used for both Postgres and Oracle sources. Verify the correct user-defined / FetchRecords capture method constant in `enum/source_connector.go` and update these values before running. The `FetchRecords` method on the connector entity does not depend on this value at runtime, but the engine uses it to select the source execution path.

> **Note**: `baseURL` for the Mac Enrich Service defaults to `http://localhost:8765`. Override via env var `ENRICH_SERVICE_URL` if running the pipeline remotely or on a different port.

---

## 7. Mac Enrich Service

This is a standalone Go HTTP server that runs on the local Mac, wrapping Ollama. It is **not part of the StreamCraft engine** — it is a sidecar service started separately before Flows 34 and 35 execute.

### 7.1 Prerequisites

```bash
# Install Ollama (Mac)
brew install ollama

# Pull the embedding model
ollama pull nomic-embed-text

# Start Ollama (runs on localhost:11434)
ollama serve
```

### 7.2 API Contract

```
POST /enrich
Content-Type: application/json
Body:
{
  "batch_id": "uuid-string",       // idempotency key set by entity_149
  "records": [
    {
      "product_id": "PF-12345",
      "embed_text":  "Wooden 3-seater sofa living room fabric upholstery",
      "source":      "postgres"
      // ... any other fields to pass through
    }
  ]
}
Response: 202 Accepted
{ "batch_id": "uuid", "queued": 50 }


GET /results?cursor=0&limit=100
Response: 200 OK
{
  "results": [
    {
      "product_id": "PF-12345",
      "embedding":   [0.023, -0.011, ...],   // 768 float32 values
      "enriched_at": "2026-06-13T10:00:00Z",
      // original passthrough fields included
    }
  ],
  "next_cursor": 100,
  "has_more": true
}
Response when empty: { "results": [], "next_cursor": 0, "has_more": false }


GET /health
Response: 200 OK  { "status": "ok", "pending": 12, "done": 450 }
```

### 7.3 Implementation

Create this file in the case_5 directory:

```go
// cases/case_5/cmd/mac_enrich_service/main.go
package main

// Mac Enrich Service — thin HTTP wrapper around Ollama nomic-embed-text.
//
// POST /enrich  — accept a batch of records, queue for Ollama embedding.
// GET /results  — cursor-paginated access to completed embeddings.
// GET /health   — liveness probe.
//
// Storage is in-memory. Restart clears the queue. For durability across
// restarts, replace the in-memory slices with a SQLite file (modernc.org/sqlite).
//
// Start: ENRICH_PORT=8765 OLLAMA_URL=http://localhost:11434 go run main.go

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

// ── data types ────────────────────────────────────────────────────────────────

type enrichRequest struct {
	BatchID string           `json:"batch_id"`
	Records []map[string]any `json:"records"`
}

type enrichedResult struct {
	ProductID   string    `json:"product_id"`
	Embedding   []float32 `json:"embedding"`
	EnrichedAt  string    `json:"enriched_at"`
	Passthrough map[string]any
}

func (r enrichedResult) MarshalJSON() ([]byte, error) {
	m := make(map[string]any, len(r.Passthrough)+3)
	for k, v := range r.Passthrough {
		m[k] = v
	}
	m["product_id"] = r.ProductID
	m["embedding"] = r.Embedding
	m["enriched_at"] = r.EnrichedAt
	return json.Marshal(m)
}

type ollamaEmbedRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type ollamaEmbedResponse struct {
	Embeddings [][]float32 `json:"embeddings"`
}

// ── service state ─────────────────────────────────────────────────────────────

type service struct {
	mu        sync.Mutex
	pending   []map[string]any  // records waiting for Ollama
	done      []enrichedResult  // completed results (append-only, indexed by position)
	ollamaURL string
}

func (s *service) enqueue(records []map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending = append(s.pending, records...)
}

func (s *service) drain() ([]map[string]any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.pending) == 0 {
		return nil, false
	}
	batch := s.pending
	s.pending = nil
	return batch, true
}

func (s *service) appendDone(results []enrichedResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.done = append(s.done, results...)
}

func (s *service) getResults(cursor, limit int) ([]enrichedResult, int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cursor >= len(s.done) {
		return nil, cursor, false
	}
	end := cursor + limit
	if end > len(s.done) {
		end = len(s.done)
	}
	return s.done[cursor:end], end, end < len(s.done)
}

func (s *service) stats() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending), len(s.done)
}

// ── Ollama caller ─────────────────────────────────────────────────────────────

func (s *service) embed(texts []string) ([][]float32, error) {
	body, _ := json.Marshal(ollamaEmbedRequest{Model: "nomic-embed-text", Input: texts})
	resp, err := http.Post(s.ollamaURL+"/api/embed", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("ollama POST: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("ollama %d: %s", resp.StatusCode, raw)
	}
	var out ollamaEmbedResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("ollama unmarshal: %w", err)
	}
	return out.Embeddings, nil
}

// ── background worker ─────────────────────────────────────────────────────────

func (s *service) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
			batch, ok := s.drain()
			if !ok {
				continue
			}

			texts := make([]string, len(batch))
			for i, rec := range batch {
				texts[i], _ = rec["embed_text"].(string)
			}

			embeddings, err := s.embed(texts)
			if err != nil {
				log.Printf("embed error (requeueing %d records): %v", len(batch), err)
				s.enqueue(batch) // requeue on transient Ollama error
				time.Sleep(2 * time.Second)
				continue
			}

			results := make([]enrichedResult, 0, len(batch))
			for i, rec := range batch {
				if i >= len(embeddings) {
					break
				}
				pid, _ := rec["product_id"].(string)
				results = append(results, enrichedResult{
					ProductID:   pid,
					Embedding:   embeddings[i],
					EnrichedAt:  time.Now().UTC().Format(time.RFC3339),
					Passthrough: rec,
				})
			}
			s.appendDone(results)
			log.Printf("embedded %d records (total done: %d)", len(results), func() int { _, d := s.stats(); return d }())
		}
	}
}

// ── HTTP handlers ─────────────────────────────────────────────────────────────

func (s *service) handleEnrich(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req enrichRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	s.enqueue(req.Records)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{"batch_id": req.BatchID, "queued": len(req.Records)})
}

func (s *service) handleResults(w http.ResponseWriter, r *http.Request) {
	cursorStr := r.URL.Query().Get("cursor")
	limitStr := r.URL.Query().Get("limit")
	cursor, _ := strconv.Atoi(cursorStr)
	limit, _ := strconv.Atoi(limitStr)
	if limit <= 0 || limit > 500 {
		limit = 100
	}

	results, nextCursor, hasMore := s.getResults(cursor, limit)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"results":     results,
		"next_cursor": nextCursor,
		"has_more":    hasMore,
	})
}

func (s *service) handleHealth(w http.ResponseWriter, r *http.Request) {
	pending, done := s.stats()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"status": "ok", "pending": pending, "done": done})
}

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	port := os.Getenv("ENRICH_PORT")
	if port == "" {
		port = "8765"
	}
	ollamaURL := os.Getenv("OLLAMA_URL")
	if ollamaURL == "" {
		ollamaURL = "http://localhost:11434"
	}

	svc := &service{ollamaURL: ollamaURL}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go svc.worker(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/enrich", svc.handleEnrich)
	mux.HandleFunc("/results", svc.handleResults)
	mux.HandleFunc("/health", svc.handleHealth)

	log.Printf("Mac Enrich Service listening on :%s (Ollama: %s)", port, ollamaURL)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal(err)
	}
}
```

---

## 8. Elasticsearch Index Mapping

Create this index before running Flow 35. Run once.

```bash
curl -X PUT "http://localhost:9200/pf_products" \
  -H "Content-Type: application/json" \
  -d '{
    "settings": {
      "number_of_shards": 3,
      "number_of_replicas": 1
    },
    "mappings": {
      "properties": {
        "product_id":   { "type": "keyword" },
        "title":        { "type": "text",    "analyzer": "english" },
        "description":  { "type": "text",    "analyzer": "english" },
        "category":     { "type": "keyword" },
        "price":        { "type": "float" },
        "source":       { "type": "keyword" },
        "updated_at":   { "type": "date" },
        "enriched_at":  { "type": "date" },
        "run_id":       { "type": "keyword" },
        "embedding": {
          "type":       "dense_vector",
          "dims":       768,
          "index":      true,
          "similarity": "cosine"
        }
      }
    }
  }'
```

---

## 9. Data Contracts

### 9.1 Postgres Source Schema (Flow 32 input)

Table: `pf_catalog.products`

```sql
CREATE TABLE products (
  product_id   VARCHAR(50) PRIMARY KEY,
  title        TEXT        NOT NULL,
  description  TEXT,
  category     VARCHAR(100),
  price        NUMERIC(10,2),
  material     VARCHAR(100),
  dimensions   JSONB,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### 9.2 Oracle Source Schema (Flow 33 input)

Table: `PF_ERP.PRODUCT_ATTRS`

```sql
-- Oracle DDL
CREATE TABLE PRODUCT_ATTRS (
  PRODUCT_ID    VARCHAR2(50)   NOT NULL PRIMARY KEY,
  SKU_CODE      VARCHAR2(100),
  SUPPLIER_ID   VARCHAR2(50),
  WEIGHT_KG     NUMBER(8,3),
  CATEGORY      VARCHAR2(100),
  COMPLIANCE    VARCHAR2(500),
  UPDATED_AT    TIMESTAMP      DEFAULT SYSTIMESTAMP
);
```

### 9.3 Normalized common schema (written to Kafka `pepperfry.catalog.raw` by Flows 32 and 33)

`record.Data` after transformers 89/90 (Postgres) or 91/92 (Oracle):

```
product_id    string   — from source (required; records without this are dropped)
title         string   — from source (normalized: trimmed, no double-spaces)
description   string   — from source (may be empty string)
category      string   — from source (lowercased)
price         float64  — Postgres: as-is; Oracle: 0.0 (not available in ERP)
source        string   — "postgres" | "oracle"
updated_at    string   — RFC3339 (both sources normalized to UTC)
run_id        string   — from ReplicaProps["pipeline_run_id"]
_kafka_topic  string   — "pepperfry.catalog.raw" (set by transformers 90/92)
_kafka_key    string   — product_id (for partition co-location)
```

Fields prefixed `_kafka_` are consumed by the Kafka dest connector and not written to Kafka message bodies.

### 9.4 Record shape entering Flow 34 (from Kafka, after transformer 93 and 94)

After the Kafka source engine delivers a record, `_kafka_value` contains the raw message bytes. Transformer 93 (Embed Text Builder) unwraps and adds `embed_text`:

```
product_id    string   — unwrapped from _kafka_value
title         string
description   string
category      string
price         float64
source        string
updated_at    string
run_id        string
embed_text    string   — "{{title}} {{category}} {{description}}" (built by transformer 93)
_rest_path    string   — "/enrich" (set by transformer 94)
_rest_method  string   — "POST"    (set by transformer 94)
```

### 9.5 Mac Enrich Service response → Elasticsearch document (Flow 35)

After transformer 95 (Embedding Extractor) and transformer 96 (ES Document Builder):

```
product_id    string      — from result
title         string      — from result passthrough
description   string
category      string
price         float64
source        string
updated_at    string      — RFC3339
enriched_at   string      — RFC3339 (set by Mac service)
run_id        string
embedding     []float32   — 768 values (extracted by transformer 95)
_es_index     string      — "pf_products" (set by transformer 96)
_es_doc_id    string      — product_id  (set by transformer 96, drives ES document ID)
```

Fields prefixed `_es_` are consumed by the ES dest connector and stripped before indexing.

---

## 10. File Map — What to Create

```
cases/case_5/
  cmd/
    mac_enrich_service/
      main.go                                         ← Section 7.3 above

client/
  orchestrators/
    orchestrator_17/orchestrator.go                   ← Flow 32 (Postgres window)
    orchestrator_18/orchestrator.go                   ← Flow 33 (Oracle window)
    orchestrator_19/orchestrator.go                   ← Flow 34 (Kafka consumer group)
    orchestrator_20/orchestrator.go                   ← Flow 35 (results cursor)
  connectors/
    connector_54/iso_entity_144/connector.go          ← Postgres incremental source
    connector_55/iso_entity_145/connector.go          ← Kafka publisher (catalog.raw)
    connector_56/iso_entity_146/connector.go          ← Oracle incremental source
    connector_57/iso_entity_147/connector.go          ← Kafka publisher (catalog.raw, same topic)
    connector_58/iso_entity_148/connector.go          ← Kafka consumer group (catalog.raw)
    connector_59/iso_entity_149/connector.go          ← REST sink (POST /enrich)
    connector_60/iso_entity_150/connector.go          ← REST cursor source (GET /results)
    connector_61/iso_entity_151/connector.go          ← Elasticsearch bulk dest
  transformers/
    transformer_89/transformer.go                     ← Postgres Normalizer
    transformer_90/transformer.go                     ← Catalog Validator + Topic Router (Postgres)
    transformer_91/transformer.go                     ← Oracle Normalizer
    transformer_92/transformer.go                     ← Catalog Validator + Topic Router (Oracle)
    transformer_93/transformer.go                     ← Embed Text Builder
    transformer_94/transformer.go                     ← REST Route Builder
    transformer_95/transformer.go                     ← Embedding Extractor
    transformer_96/transformer.go                     ← ES Document Builder
  checkpoints/
    checkpoint_11/checkpoint.go                       ← Postgres last updated_at
    checkpoint_12/checkpoint.go                       ← Oracle last UPDATED_AT
    checkpoint_13/checkpoint.go                       ← Kafka offset (Flow 34)
    checkpoint_14/checkpoint.go                       ← Mac service results cursor
  backlogs/
    backlog_11/backlog.go                             ← Flow 32 failed Kafka publishes
    backlog_12/backlog.go                             ← Flow 33 failed Kafka publishes
    backlog_13/backlog.go                             ← Flow 34 failed /enrich POSTs
    backlog_14/backlog.go                             ← Flow 35 failed ES writes
  terminates/
    terminate_11/terminate.go                         ← Postgres page exhausted + idle
    terminate_12/terminate.go                         ← Oracle page exhausted + idle
    terminate_13/terminate.go                         ← Context cancel only (infinite)
    terminate_14/terminate.go                         ← Idle timeout 60s (finite)
  destinationwrites/
    destinationwrite_9/destinationwrite.go            ← Kafka 250 (Flow 32)
    destinationwrite_10/destinationwrite.go           ← Kafka 250 (Flow 33)
    destinationwrite_11/destinationwrite.go           ← REST 50 (Flow 34)
    destinationwrite_12/destinationwrite.go           ← ES 100 (Flow 35)
  pipelines/
    flow_32/pipeline_pipeline_1/bridge.go             ← Flow 32 bridge
    flow_33/pipeline_pipeline_1/bridge.go             ← Flow 33 bridge
    flow_34/pipeline_pipeline_1/bridge.go             ← Flow 34 bridge
    flow_35/pipeline_pipeline_1/bridge.go             ← Flow 35 bridge
```

---

## 11. Client Code — Full Implementations

### 11.1 Orchestrator 17 — Flow 32 Postgres Window

```go
// client/orchestrators/orchestrator_17/orchestrator.go
package client_orchestrator_17

// Flow 32 orchestrator. Reads the last checkpoint timestamp from AuxDB.
// If none exists (first run), starts from epoch (fetches all products).
// Passes the window boundary to the connector via ReplicaProps.

import (
	"context"
	"fmt"
	"os"
	"time"

	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
)

func PipelineOrchestrator(param *models.PipelineOrchestratorProps) ([]models.PipelineOrchestratorTune, error) {
	auxConn, err := ulib.GetAuxPostgresConn(param.AuxilaryDBConnMap)
	if err != nil {
		return nil, fmt.Errorf("orchestrator_17: auxdb connect: %w", err)
	}

	var lastUpdatedAt string
	row := auxConn.QueryRow(context.Background(),
		"SELECT last_updated_at FROM pf_catalog_checkpoints WHERE source = 'postgres' LIMIT 1")
	_ = row.Scan(&lastUpdatedAt)
	if lastUpdatedAt == "" {
		lastUpdatedAt = "1970-01-01T00:00:00Z"
	}

	runID := fmt.Sprintf("pf-postgres-ingest-%s", time.Now().UTC().Format("20060102-150405"))

	pgPassword := os.Getenv("PF_POSTGRES_PASSWORD")

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
				"last_updated_at": lastUpdatedAt,
				"pipeline_run_id": runID,
				"pg_password":     pgPassword,
			},
		})
	}
	return tunes, nil
}
```

### 11.2 Orchestrator 18 — Flow 33 Oracle Window

```go
// client/orchestrators/orchestrator_18/orchestrator.go
package client_orchestrator_18

// Flow 33 orchestrator. Same pattern as orchestrator_17 but for Oracle source.
// Reads last checkpoint from AuxDB (source='oracle').

import (
	"context"
	"fmt"
	"os"
	"time"

	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
)

func PipelineOrchestrator(param *models.PipelineOrchestratorProps) ([]models.PipelineOrchestratorTune, error) {
	auxConn, err := ulib.GetAuxPostgresConn(param.AuxilaryDBConnMap)
	if err != nil {
		return nil, fmt.Errorf("orchestrator_18: auxdb connect: %w", err)
	}

	var lastUpdatedAt string
	row := auxConn.QueryRow(context.Background(),
		"SELECT last_updated_at FROM pf_catalog_checkpoints WHERE source = 'oracle' LIMIT 1")
	_ = row.Scan(&lastUpdatedAt)
	if lastUpdatedAt == "" {
		lastUpdatedAt = "1970-01-01T00:00:00Z"
	}

	runID := fmt.Sprintf("pf-oracle-ingest-%s", time.Now().UTC().Format("20060102-150405"))

	oraclePassword := os.Getenv("PF_ORACLE_PASSWORD")

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
				"last_updated_at": lastUpdatedAt,
				"pipeline_run_id": runID,
				"oracle_password": oraclePassword,
			},
		})
	}
	return tunes, nil
}
```

### 11.3 Orchestrator 19 — Flow 34 Kafka Consumer Group

```go
// client/orchestrators/orchestrator_19/orchestrator.go
package client_orchestrator_19

// Flow 34 orchestrator. Infinite streaming — one replica, stable consumer group.

import (
	"fmt"
	"etlfunnel/execution/models"
)

const (
	kafkaTopic      = "pepperfry.catalog.raw"
	consumerGroupID = "pf-catalog-enrich-submitter"
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
				"consumer_group":   consumerGroupID,
				"pipeline_run_id":  "pf-enrich-submit-streaming",
			},
		})
	}
	return tunes, nil
}
```

### 11.4 Orchestrator 20 — Flow 35 Results Cursor

```go
// client/orchestrators/orchestrator_20/orchestrator.go
package client_orchestrator_20

// Flow 35 orchestrator. Finite cursor-based run — reads last cursor position
// from AuxDB, passes to entity_150. Restarts from 0 only on first ever run.

import (
	"context"
	"fmt"
	"os"
	"time"

	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
)

func PipelineOrchestrator(param *models.PipelineOrchestratorProps) ([]models.PipelineOrchestratorTune, error) {
	auxConn, err := ulib.GetAuxPostgresConn(param.AuxilaryDBConnMap)
	if err != nil {
		return nil, fmt.Errorf("orchestrator_20: auxdb connect: %w", err)
	}

	var lastCursor int
	row := auxConn.QueryRow(context.Background(),
		"SELECT last_cursor FROM pf_enrich_collect_cursors LIMIT 1")
	_ = row.Scan(&lastCursor) // 0 if no row

	enrichURL := os.Getenv("ENRICH_SERVICE_URL")
	if enrichURL == "" {
		enrichURL = "http://localhost:8765"
	}

	runID := fmt.Sprintf("pf-enrich-collect-%s", time.Now().UTC().Format("20060102-150405"))

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
				"start_cursor":    lastCursor,
				"enrich_url":      enrichURL,
				"pipeline_run_id": runID,
			},
		})
	}
	return tunes, nil
}
```

### 11.5 Connector 54 / Entity 144 — Postgres Incremental Source

Implements `IClientDBPostgresSQLSource` or equivalent SQL source interface.
**Verify the exact interface name and method signature against `core/coreinterface/postgres.go` (or equivalent) before coding.**
The `FetchRecords` channel approach is used as the universal user-defined path.

```go
// client/connectors/connector_54/iso_entity_144/connector.go
package client_connector_54_iso_entity_144

// Postgres incremental source for pf_catalog.products.
//
// Queries rows WHERE updated_at > last_updated_at ORDER BY updated_at ASC LIMIT 500.
// Uses FetchRecords (user-defined channel) to avoid depending on the specific
// incremental-query capture method enum value — verify captureMethod in
// enum/source_connector.go and update collection.json accordingly.
//
// Package-level var Instance is used by terminate_11 and checkpoint_11.

import (
	"context"
	"fmt"
	"sync"
	"time"

	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"

	"github.com/jackc/pgx/v5"
)

type IUseConnector struct {
	mu          sync.Mutex
	exhausted   bool
	lastUpdated string
}

// Instance is read by terminate_11 and checkpoint_11.
var Instance = &IUseConnector{}

// Verify this interface name against core/coreinterface/ — it may be
// IClientDBPostgresSQLSource or IClientSQLSource depending on the framework version.
var _ coreinterface.IClientDBPostgresSQLSource = Instance

func (c *IUseConnector) FetchRecords(param *models.PostgresSQLSourceFetch) <-chan map[string]any {
	ch := make(chan map[string]any, 200)

	go func() {
		defer close(ch)

		rp := param.State.GetReplicaProps()
		lastUpdatedAt, _ := rp["last_updated_at"].(string)
		if lastUpdatedAt == "" {
			lastUpdatedAt = "1970-01-01T00:00:00Z"
		}

		// param.SourceDBConn is a *pgx.Conn or pgx.Pool — verify the type.
		// The engine connects using the connectionParams from collection.json.
		conn, ok := param.SourceDBConn.(*pgx.Conn)
		if !ok {
			param.State.GetLogger().Error("entity_144: expected *pgx.Conn")
			c.mu.Lock()
			c.exhausted = true
			c.mu.Unlock()
			return
		}

		const batchSize = 500
		cursor := lastUpdatedAt
		var lastSeen string

		for {
			rows, err := conn.Query(context.Background(), `
				SELECT product_id, title, description, category, price, updated_at
				FROM pf_catalog.products
				WHERE updated_at > $1
				ORDER BY updated_at ASC
				LIMIT $2`,
				cursor, batchSize)
			if err != nil {
				param.State.GetLogger().Error(fmt.Sprintf("entity_144: query: %v", err))
				break
			}

			count := 0
			for rows.Next() {
				var productID, title, description, category string
				var price float64
				var updatedAt time.Time
				if err := rows.Scan(&productID, &title, &description, &category, &price, &updatedAt); err != nil {
					continue
				}
				ts := updatedAt.UTC().Format(time.RFC3339)
				lastSeen = ts
				ch <- map[string]any{
					"product_id":  productID,
					"title":       title,
					"description": description,
					"category":    category,
					"price":       price,
					"updated_at":  ts,
				}
				count++
			}
			rows.Close()

			if count > 0 {
				cursor = lastSeen
			}
			if count < batchSize {
				break // no more rows
			}
		}

		c.mu.Lock()
		c.exhausted = true
		c.lastUpdated = lastSeen
		c.mu.Unlock()
	}()

	return ch
}

func (c *IUseConnector) GetExhausted() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.exhausted
}

func (c *IUseConnector) GetLastUpdatedAt() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastUpdated
}
```

### 11.6 Connector 55 / Entity 145 — Kafka Publisher (Postgres → catalog.raw)

Identical in structure to entity_141 (Case 4). Key field: `_kafka_key = product_id`.

```go
// client/connectors/connector_55/iso_entity_145/connector.go
package client_connector_55_iso_entity_145

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
			topic = "pepperfry.catalog.raw"
		}
		key, _ := rec["_kafka_key"].(string)
		if key == "" {
			key, _ = rec["product_id"].(string)
		}
		source, _ := rec["source"].(string)

		payload := make(map[string]any, len(rec))
		for k, v := range rec {
			if len(k) > 0 && k[0] == '_' {
				continue
			}
			payload[k] = v
		}
		value, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("entity_145: marshal: %w", err)
		}
		tunes = append(tunes, &models.KafkaDestQueryTune{
			Topic:     topic,
			Key:       []byte(key),
			Value:     value,
			Partition: -1,
			Headers: []models.KafkaHeader{
				{Key: "source", Value: []byte(source)},
			},
		})
	}
	return tunes, nil
}
```

### 11.7 Connector 56 / Entity 146 — Oracle Incremental Source

Same pattern as entity_144 but using `github.com/sijms/go-ora/v2` (pure-Go Oracle driver).
**Verify the Oracle source interface in `core/coreinterface/oracle.go`.**

```go
// client/connectors/connector_56/iso_entity_146/connector.go
package client_connector_56_iso_entity_146

// Oracle incremental source for PF_ERP.PRODUCT_ATTRS.
// Uses go-ora/v2 (pure Go, no CGO required).
// Query: WHERE UPDATED_AT > :last_updated ORDER BY UPDATED_AT ASC FETCH FIRST 500 ROWS ONLY
//
// Package-level Instance is used by terminate_12 and checkpoint_12.

import (
	"context"
	"fmt"
	"sync"
	"time"

	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"

	go_ora "github.com/sijms/go-ora/v2"
)

type IUseConnector struct {
	mu          sync.Mutex
	exhausted   bool
	lastUpdated string
}

var Instance = &IUseConnector{}

var _ coreinterface.IClientDBOracleSQLSource = Instance

func (c *IUseConnector) FetchRecords(param *models.OracleSQLSourceFetch) <-chan map[string]any {
	ch := make(chan map[string]any, 200)

	go func() {
		defer close(ch)

		rp := param.State.GetReplicaProps()
		lastUpdatedAt, _ := rp["last_updated_at"].(string)
		if lastUpdatedAt == "" {
			lastUpdatedAt = "1970-01-01T00:00:00Z"
		}

		// param.SourceDBConn should expose the underlying *sql.DB or go-ora connection.
		// Verify the type assertion against the Oracle engine implementation.
		conn, ok := param.SourceDBConn.(*go_ora.Connection)
		if !ok {
			param.State.GetLogger().Error("entity_146: expected *go_ora.Connection")
			c.mu.Lock()
			c.exhausted = true
			c.mu.Unlock()
			return
		}

		lastTime, err := time.Parse(time.RFC3339, lastUpdatedAt)
		if err != nil {
			lastTime = time.Time{}
		}

		const batchSize = 500
		cursor := lastTime
		var lastSeen string

		for {
			rows, err := conn.QueryContext(context.Background(), `
				SELECT PRODUCT_ID, SKU_CODE, SUPPLIER_ID, CATEGORY, WEIGHT_KG, UPDATED_AT
				FROM PF_ERP.PRODUCT_ATTRS
				WHERE UPDATED_AT > :1
				ORDER BY UPDATED_AT ASC
				FETCH FIRST :2 ROWS ONLY`,
				cursor, batchSize)
			if err != nil {
				param.State.GetLogger().Error(fmt.Sprintf("entity_146: query: %v", err))
				break
			}

			count := 0
			for rows.Next() {
				var productID, skuCode, supplierID, category string
				var weightKg float64
				var updatedAt time.Time
				if err := rows.Scan(&productID, &skuCode, &supplierID, &category, &weightKg, &updatedAt); err != nil {
					continue
				}
				ts := updatedAt.UTC().Format(time.RFC3339)
				lastSeen = ts
				ch <- map[string]any{
					"product_id":  productID,
					"sku_code":    skuCode,
					"supplier_id": supplierID,
					"category":    category,
					"weight_kg":   weightKg,
					"updated_at":  ts,
				}
				count++
			}
			rows.Close()

			if count > 0 {
				cursor, _ = time.Parse(time.RFC3339, lastSeen)
			}
			if count < batchSize {
				break
			}
		}

		c.mu.Lock()
		c.exhausted = true
		c.lastUpdated = lastSeen
		c.mu.Unlock()
	}()

	return ch
}

func (c *IUseConnector) GetExhausted() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.exhausted
}

func (c *IUseConnector) GetLastUpdatedAt() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastUpdated
}
```

### 11.8 Connector 57 / Entity 147 — Kafka Publisher (Oracle → catalog.raw)

Identical to entity_145. Copy the file; change the package name to `client_connector_57_iso_entity_147`. The Kafka topic is the same (`pepperfry.catalog.raw`) — this is the fan-in.

### 11.9 Connector 58 / Entity 148 — Kafka Consumer (catalog.raw)

Identical in structure to entity_142 (Case 4). Change consumer group and topic.

```go
// client/connectors/connector_58/iso_entity_148/connector.go
package client_connector_58_iso_entity_148

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
		topic = "pepperfry.catalog.raw"
	}
	if groupID == "" {
		groupID = "pf-catalog-enrich-submitter"
	}
	return &models.KafkaSourceSubscriptionTune{
		Topics:            []string{topic},
		GroupID:           groupID,
		InitialOffset:     sarama.OffsetOldest,
		SessionTimeout:    30 * time.Second,
		HeartbeatInterval: 10 * time.Second,
		MaxWaitTime:       500 * time.Millisecond,
		FetchMaxBytes:     10 * 1024 * 1024,
		AutoCommit:        true,
		CommitInterval:    1 * time.Second,
	}, nil
}

func (c *IUseConnector) GenerateAssignment(_ *models.KafkaSourceAssign) (*models.KafkaSourceAssignmentTune, error) {
	return nil, fmt.Errorf("entity_148: use subscription mode")
}

func (c *IUseConnector) FetchRecords(_ *models.KafkaSourceFetch) <-chan map[string]any {
	ch := make(chan map[string]any)
	close(ch)
	return ch
}
```

### 11.10 Connector 59 / Entity 149 — REST Sink (POST /enrich)

Implements `coreinterface.IClientRESTAPIDest`. Sends a batch of records as a single POST to the Mac service.

```go
// client/connectors/connector_59/iso_entity_149/connector.go
package client_connector_59_iso_entity_149

// REST destination: POSTs batches of records to the Mac Enrich Service.
//
// The engine calls GenerateQuery once per DestinationWrite flush.
// param.Records contains up to 50 records (destinationwrite_11).
// We produce a single RESTAPIDestQueryTune covering the whole batch.
// The Mac service responds 202; we don't wait for embedding completion here —
// entity_150 (Flow 35) polls for results separately.

import (
	"fmt"

	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
)

type IUseConnector struct{}

var _ coreinterface.IClientRESTAPIDest = (*IUseConnector)(nil)

func (d *IUseConnector) GenerateQuery(param *models.RESTAPIDestQuery) ([]*models.RESTAPIDestQueryTune, error) {
	if len(param.Records) == 0 {
		return nil, nil
	}

	// Strip _rest_* routing fields before sending to the service.
	clean := make([]map[string]any, 0, len(param.Records))
	for _, rec := range param.Records {
		row := make(map[string]any, len(rec))
		for k, v := range rec {
			if len(k) > 0 && k[0] == '_' {
				continue
			}
			row[k] = v
		}
		clean = append(clean, row)
	}

	rp := param.State.GetReplicaProps()
	runID, _ := rp["pipeline_run_id"].(string)

	return []*models.RESTAPIDestQueryTune{
		{
			Method: "POST",
			Path:   "/enrich",
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			Body: map[string]any{
				"batch_id": fmt.Sprintf("%s-%d", runID, len(param.Records)),
				"records":  clean,
			},
		},
	}, nil
}
```

### 11.11 Connector 60 / Entity 150 — REST Cursor Source (GET /results)

Implements `coreinterface.IClientRESTAPISource`. Uses `GenerateCursorRequest` to poll the Mac service.

```go
// client/connectors/connector_60/iso_entity_150/connector.go
package client_connector_60_iso_entity_150

// REST cursor source: polls GET /results?cursor=N from the Mac Enrich Service.
//
// Cursor is an integer (position in the done-results slice).
// Advances until has_more=false (terminate_14 then fires after idle timeout).
//
// Package-level Instance is read by terminate_14 and checkpoint_14.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
)

type enrichResultsResponse struct {
	Results    []map[string]any `json:"results"`
	NextCursor int              `json:"next_cursor"`
	HasMore    bool             `json:"has_more"`
}

type IUseConnector struct {
	mu         sync.Mutex
	nextCursor int
	exhausted  bool
}

var Instance = &IUseConnector{}

var _ coreinterface.IClientRESTAPISource = Instance

func (c *IUseConnector) GenerateCursorRequest(param *models.RESTAPISourceFetch) (*models.RESTAPISourceCursorTune, error) {
	rp := param.State.GetReplicaProps()
	startCursor, _ := rp["start_cursor"].(int)

	c.mu.Lock()
	if c.nextCursor == 0 && startCursor > 0 {
		c.nextCursor = startCursor
	}
	currentCursor := c.nextCursor
	c.mu.Unlock()

	return &models.RESTAPISourceCursorTune{
		Path:   "/results",
		Method: "GET",
		Headers: map[string]string{
			"Accept": "application/json",
		},
		QueryParams: map[string]string{
			"cursor": fmt.Sprintf("%d", currentCursor),
			"limit":  "100",
		},
		ParseRecords: func(body []byte, _ http.Header) ([]map[string]any, error) {
			var resp enrichResultsResponse
			if err := json.Unmarshal(body, &resp); err != nil {
				return nil, fmt.Errorf("entity_150: unmarshal: %w", err)
			}
			c.mu.Lock()
			c.nextCursor = resp.NextCursor
			c.exhausted = !resp.HasMore
			c.mu.Unlock()
			return resp.Results, nil
		},
		NextCursorToken: func(body []byte, _ http.Header) (string, bool) {
			c.mu.Lock()
			defer c.mu.Unlock()
			if c.exhausted {
				return "", false
			}
			return fmt.Sprintf("%d", c.nextCursor), true
		},
	}, nil
}

func (c *IUseConnector) GeneratePaginateRequest(_ *models.RESTAPISourceFetch) (*models.RESTAPISourcePaginateTune, error) {
	return nil, fmt.Errorf("entity_150: Mac Enrich Service uses cursor mode")
}

func (c *IUseConnector) GenerateWebhookRequest(_ *models.RESTAPISourceFetch) (*models.RESTAPISourceWebhookTune, error) {
	return nil, fmt.Errorf("entity_150: Mac Enrich Service does not use webhook mode")
}

func (c *IUseConnector) FetchRecords(_ *models.RESTAPISourceFetch) <-chan map[string]any {
	ch := make(chan map[string]any)
	close(ch)
	return ch
}

func (c *IUseConnector) GetExhausted() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.exhausted
}

func (c *IUseConnector) GetNextCursor() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nextCursor
}
```

### 11.12 Connector 61 / Entity 151 — Elasticsearch Bulk Destination

Implements `coreinterface.IClientDBElasticsearchDest`.
**Verify the exact interface name and `ElasticsearchDestQueryTune` struct fields against `core/coreinterface/elasticsearch.go`.**

```go
// client/connectors/connector_61/iso_entity_151/connector.go
package client_connector_61_iso_entity_151

// Elasticsearch bulk destination for pf_products index.
//
// Each record produces one index operation. The document ID is product_id
// (upsert semantics: re-running the pipeline overwrites existing docs).
// _es_* fields are consumed here and stripped from the document body.
//
// Verify IClientDBElasticsearchDest and ElasticsearchDestQueryTune against
// core/coreinterface/elasticsearch.go before coding.

import (
	"fmt"

	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
)

type IUseConnector struct{}

var _ coreinterface.IClientDBElasticsearchDest = (*IUseConnector)(nil)

func (d *IUseConnector) GenerateQuery(param *models.ElasticsearchDestQuery) ([]*models.ElasticsearchDestQueryTune, error) {
	tunes := make([]*models.ElasticsearchDestQueryTune, 0, len(param.Records))

	for _, rec := range param.Records {
		productID, _ := rec["product_id"].(string)
		if productID == "" {
			return nil, fmt.Errorf("entity_151: missing product_id")
		}

		index, _ := rec["_es_index"].(string)
		if index == "" {
			index = "pf_products"
		}
		docID, _ := rec["_es_doc_id"].(string)
		if docID == "" {
			docID = productID
		}

		// Build document: all fields except _* prefixed ones.
		doc := make(map[string]any, len(rec))
		for k, v := range rec {
			if len(k) > 0 && k[0] == '_' {
				continue
			}
			doc[k] = v
		}

		tunes = append(tunes, &models.ElasticsearchDestQueryTune{
			Index:     index,
			DocID:     docID,
			Body:      doc,
			Operation: models.ElasticsearchDestOpIndex, // upsert / index
		})
	}

	return tunes, nil
}
```

---

### 11.13 Transformer 89 — Postgres Normalizer

```go
// client/transformers/transformer_89/transformer.go
package client_transformer_89

// Postgres Normalizer (Flow 32).
// Maps Postgres column names to the common catalog schema.
// Adds source="postgres". Normalizes category to lowercase, trims title/description.
// Drops records without product_id.

import (
	"strings"
	"etlfunnel/execution/models"
)

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	rec := param.Record
	productID, _ := rec["product_id"].(string)
	if productID == "" {
		return nil, nil // drop
	}

	rp := param.State.GetReplicaProps()
	runID, _ := rp["pipeline_run_id"].(string)

	title, _ := rec["title"].(string)
	description, _ := rec["description"].(string)
	category, _ := rec["category"].(string)

	out := make(map[string]any, len(rec)+2)
	for k, v := range rec {
		out[k] = v
	}
	out["title"] = strings.TrimSpace(title)
	out["description"] = strings.TrimSpace(description)
	out["category"] = strings.ToLower(strings.TrimSpace(category))
	out["source"] = "postgres"
	out["run_id"] = runID

	return out, nil
}
```

### 11.14 Transformer 90 — Catalog Validator + Topic Router (Postgres)

```go
// client/transformers/transformer_90/transformer.go
package client_transformer_90

// Catalog Validator + Topic Router (Flow 32).
// Drops records where title is empty (cannot build useful embedding).
// Sets _kafka_topic and _kafka_key consumed by entity_145.

import "etlfunnel/execution/models"

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	rec := param.Record
	title, _ := rec["title"].(string)
	if title == "" {
		return nil, nil // drop — no meaningful embedding possible
	}

	productID, _ := rec["product_id"].(string)

	out := make(map[string]any, len(rec)+2)
	for k, v := range rec {
		out[k] = v
	}
	out["_kafka_topic"] = "pepperfry.catalog.raw"
	out["_kafka_key"] = productID

	return out, nil
}
```

### 11.15 Transformer 91 — Oracle Normalizer

```go
// client/transformers/transformer_91/transformer.go
package client_transformer_91

// Oracle Normalizer (Flow 33).
// Oracle columns are uppercase (PRODUCT_ID, SKU_CODE, etc.).
// Maps to the same common schema as transformer_89.
// Adds source="oracle". Price is not available in ERP → defaults to 0.0.
// Drops records without PRODUCT_ID.

import (
	"strings"
	"etlfunnel/execution/models"
)

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	rec := param.Record

	// Oracle columns may arrive as lowercase via the driver — check both.
	productID := strVal(rec, "product_id", "PRODUCT_ID")
	if productID == "" {
		return nil, nil // drop
	}

	rp := param.State.GetReplicaProps()
	runID, _ := rp["pipeline_run_id"].(string)

	category := strVal(rec, "category", "CATEGORY")
	updatedAt := strVal(rec, "updated_at", "UPDATED_AT")

	// Build a description from available ERP fields.
	sku := strVal(rec, "sku_code", "SKU_CODE")
	supplier := strVal(rec, "supplier_id", "SUPPLIER_ID")
	description := ""
	if sku != "" {
		description += "SKU: " + sku + " "
	}
	if supplier != "" {
		description += "Supplier: " + supplier
	}

	return map[string]any{
		"product_id":  productID,
		"title":       "", // ERP has no title; transformer_92 drops these if empty
		"description": strings.TrimSpace(description),
		"category":    strings.ToLower(strings.TrimSpace(category)),
		"price":       0.0,
		"source":      "oracle",
		"updated_at":  updatedAt,
		"run_id":      runID,
	}, nil
}

func strVal(rec map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := rec[k].(string); ok && v != "" {
			return v
		}
	}
	return ""
}
```

> **NOTE**: Oracle ERP records have no product title. Transformer 92 will drop these records because title is required for embedding. This is intentional — Oracle records that later acquire a title (after a Postgres record is merged) will be re-enriched in the next run when the Postgres flow picks up the updated row. An alternative (deduplication/merge transformer) is listed in the Design Decisions section.

### 11.16 Transformer 92 — Catalog Validator + Topic Router (Oracle)

Identical logic to transformer_90. Copy and change package name to `client_transformer_92`.

### 11.17 Transformer 93 — Embed Text Builder (Flow 34)

```go
// client/transformers/transformer_93/transformer.go
package client_transformer_93

// Embed Text Builder (Flow 34, Kafka → Mac Enrich Service).
//
// The Kafka source engine delivers records with _kafka_value = raw bytes.
// Step 1: Unwrap _kafka_value (JSON) into top-level fields.
// Step 2: Build embed_text = title + " " + category + " " + description.
//         This is the string sent to nomic-embed-text.
// Returns nil (drop) if _kafka_value is absent or unparseable.

import (
	"encoding/json"
	"fmt"
	"etlfunnel/execution/models"
)

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	rec := param.Record

	raw, ok := rec["_kafka_value"].([]byte)
	if !ok {
		return nil, nil // malformed Kafka message
	}

	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, nil
	}

	title, _ := payload["title"].(string)
	category, _ := payload["category"].(string)
	description, _ := payload["description"].(string)

	if title == "" {
		return nil, nil // no useful text to embed
	}

	embedText := fmt.Sprintf("%s %s %s", title, category, description)

	out := make(map[string]any, len(payload)+3)
	for k, v := range payload {
		out[k] = v
	}
	// Preserve Kafka metadata for checkpoint_13.
	out["_kafka_offset"] = rec["_kafka_offset"]
	out["_kafka_partition"] = rec["_kafka_partition"]
	out["_kafka_topic"] = rec["_kafka_topic"]
	out["embed_text"] = embedText

	return out, nil
}
```

### 11.18 Transformer 94 — REST Route Builder (Flow 34)

```go
// client/transformers/transformer_94/transformer.go
package client_transformer_94

// REST Route Builder (Flow 34).
// Sets _rest_path and _rest_method consumed by entity_149.
// These are stripped before the payload is sent to the Mac service.

import "etlfunnel/execution/models"

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	rec := param.Record
	out := make(map[string]any, len(rec)+2)
	for k, v := range rec {
		out[k] = v
	}
	out["_rest_path"] = "/enrich"
	out["_rest_method"] = "POST"
	return out, nil
}
```

### 11.19 Transformer 95 — Embedding Extractor (Flow 35)

```go
// client/transformers/transformer_95/transformer.go
package client_transformer_95

// Embedding Extractor (Flow 35, Mac Enrich Service → Elasticsearch).
//
// The Mac service response is a flat map including an "embedding" key
// containing []interface{} (JSON array decoded by encoding/json).
// This transformer converts it to []float32 for Elasticsearch dense_vector.
// Returns nil (drop) if embedding is absent or wrong length.

import (
	"fmt"
	"etlfunnel/execution/models"
)

const expectedDims = 768

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	rec := param.Record

	rawEmbed, ok := rec["embedding"]
	if !ok {
		return nil, fmt.Errorf("transformer_95: embedding field missing")
	}

	var embedding []float32
	switch v := rawEmbed.(type) {
	case []float32:
		embedding = v
	case []interface{}:
		embedding = make([]float32, 0, len(v))
		for _, e := range v {
			switch f := e.(type) {
			case float64:
				embedding = append(embedding, float32(f))
			case float32:
				embedding = append(embedding, f)
			default:
				return nil, fmt.Errorf("transformer_95: unexpected embedding element type %T", e)
			}
		}
	default:
		return nil, fmt.Errorf("transformer_95: unexpected embedding type %T", rawEmbed)
	}

	if len(embedding) != expectedDims {
		return nil, fmt.Errorf("transformer_95: expected %d dims, got %d", expectedDims, len(embedding))
	}

	out := make(map[string]any, len(rec))
	for k, v := range rec {
		out[k] = v
	}
	out["embedding"] = embedding
	return out, nil
}
```

### 11.20 Transformer 96 — ES Document Builder (Flow 35)

```go
// client/transformers/transformer_96/transformer.go
package client_transformer_96

// ES Document Builder (Flow 35).
// Sets _es_index and _es_doc_id consumed by entity_151.
// These are stripped before Elasticsearch indexing.

import "etlfunnel/execution/models"

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	rec := param.Record
	productID, _ := rec["product_id"].(string)
	if productID == "" {
		return nil, nil // drop
	}

	out := make(map[string]any, len(rec)+2)
	for k, v := range rec {
		out[k] = v
	}
	out["_es_index"] = "pf_products"
	out["_es_doc_id"] = productID
	return out, nil
}
```

---

### 11.21 Checkpoints

#### Checkpoint 11 — Flow 32 (Postgres last updated_at)

```go
// client/checkpoints/checkpoint_11/checkpoint.go
package client_checkpoint_11

// After a successful Kafka flush, upserts the last seen updated_at to AuxDB.
// entity_144.Instance.GetLastUpdatedAt() returns the highest updated_at
// value seen in the current run.
//
// AuxDB table:
//   CREATE TABLE IF NOT EXISTS pf_catalog_checkpoints (
//     source        TEXT PRIMARY KEY,
//     last_updated_at TEXT NOT NULL,
//     updated_at    TIMESTAMPTZ NOT NULL
//   );

import (
	"context"
	"fmt"
	"time"

	entity144 "etlfunnel/execution/client/connectors/connector_54/iso_entity_144"
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
)

func Checkpoint(param *models.CheckpointProps) (*models.CheckpointTune, error) {
	if len(param.Records) == 0 {
		return cont(), nil
	}
	ts := entity144.Instance.GetLastUpdatedAt()
	if ts == "" {
		return cont(), nil
	}
	conn, err := ulib.GetAuxPostgresConn(param.AuxiliaryDBConnMap)
	if err != nil {
		return cont(), fmt.Errorf("checkpoint_11: %w", err)
	}
	_, err = conn.Exec(context.Background(), `
		INSERT INTO pf_catalog_checkpoints (source, last_updated_at, updated_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (source) DO UPDATE SET
			last_updated_at = EXCLUDED.last_updated_at,
			updated_at      = EXCLUDED.updated_at`,
		"postgres", ts, time.Now().UTC())
	if err != nil {
		return cont(), fmt.Errorf("checkpoint_11: upsert: %w", err)
	}
	return cont(), nil
}

func cont() *models.CheckpointTune { return &models.CheckpointTune{Action: models.ActionContinue} }
```

#### Checkpoint 12 — Flow 33 (Oracle last UPDATED_AT)

Identical to checkpoint_11. Import `entity146` instead of `entity144`, use `source = 'oracle'`. Change package to `client_checkpoint_12`.

#### Checkpoint 13 — Flow 34 (Kafka offset)

Identical in structure to checkpoint_10 (Case 4). Change package to `client_checkpoint_13`, table to `pf_enrich_submit_offsets`.

```sql
CREATE TABLE IF NOT EXISTS pf_enrich_submit_offsets (
  topic       TEXT,
  partition   INT,
  last_offset BIGINT NOT NULL,
  updated_at  TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (topic, partition)
);
```

#### Checkpoint 14 — Flow 35 (Mac service results cursor)

```go
// client/checkpoints/checkpoint_14/checkpoint.go
package client_checkpoint_14

// Persists the Mac service results cursor to AuxDB so a Flow 35 restart
// resumes collecting from where it left off without re-indexing already-done records.
//
// AuxDB table:
//   CREATE TABLE IF NOT EXISTS pf_enrich_collect_cursors (
//     id          INT PRIMARY KEY DEFAULT 1,  -- single-row table
//     last_cursor INT NOT NULL,
//     updated_at  TIMESTAMPTZ NOT NULL
//   );

import (
	"context"
	"fmt"
	"time"

	entity150 "etlfunnel/execution/client/connectors/connector_60/iso_entity_150"
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
)

func Checkpoint(param *models.CheckpointProps) (*models.CheckpointTune, error) {
	if len(param.Records) == 0 {
		return cont(), nil
	}
	cursor := entity150.Instance.GetNextCursor()
	conn, err := ulib.GetAuxPostgresConn(param.AuxiliaryDBConnMap)
	if err != nil {
		return cont(), fmt.Errorf("checkpoint_14: %w", err)
	}
	_, err = conn.Exec(context.Background(), `
		INSERT INTO pf_enrich_collect_cursors (id, last_cursor, updated_at)
		VALUES (1, $1, $2)
		ON CONFLICT (id) DO UPDATE SET
			last_cursor = EXCLUDED.last_cursor,
			updated_at  = EXCLUDED.updated_at`,
		cursor, time.Now().UTC())
	if err != nil {
		return cont(), fmt.Errorf("checkpoint_14: upsert: %w", err)
	}
	return cont(), nil
}

func cont() *models.CheckpointTune { return &models.CheckpointTune{Action: models.ActionContinue} }
```

---

### 11.22 Backlogs

All four backlogs follow the identical pattern of backlog_9/backlog_10 (Case 4): write failed records to a per-flow AuxDB table with `ActionContinue`.

| Backlog | Package | Table | Key fields |
|---|---|---|---|
| 11 | `client_backlog_11` | `pf_postgres_ingest_backlog` | `product_id, failure_stage, error_message, record_payload, pipeline_run_id` |
| 12 | `client_backlog_12` | `pf_oracle_ingest_backlog` | same |
| 13 | `client_backlog_13` | `pf_enrich_submit_backlog` | `product_id, kafka_topic, kafka_partition, kafka_offset, ...` |
| 14 | `client_backlog_14` | `pf_es_index_backlog` | `product_id, failure_stage, error_message, record_payload, pipeline_run_id` |

Create the DDL for each table before first run. Model the `INSERT` after backlog_9 in Case 4.

---

### 11.23 Terminates

#### Terminate 11 — Flow 32 (Postgres, finite)

```go
// client/terminates/terminate_11/terminate.go
package client_terminate_11

// Flow 32 is finite. Fires when entity_144 has exhausted the Postgres
// query cursor AND no new records have arrived for 30s (flush buffer).

import (
	"fmt"
	"time"

	entity144 "etlfunnel/execution/client/connectors/connector_54/iso_entity_144"
	"etlfunnel/execution/models"
)

const (
	checkInterval = 5 * time.Second
	idleTimeout   = 30 * time.Second
)

func TerminateRule(_ *models.TerminateRuleProps) (*models.TerminateRuleTune, error) {
	return &models.TerminateRuleTune{
		CheckInterval:        checkInterval,
		UserDefinedCheckFunc: check,
	}, nil
}

func check(props *models.CustomTerminateRuleCheckProps) (*models.TerminateRuleActionTune, error) {
	if !entity144.Instance.GetExhausted() {
		return &models.TerminateRuleActionTune{Action: models.ActionContinue}, nil
	}
	if props.LastMessageAt.IsZero() || time.Since(props.LastMessageAt) < idleTimeout {
		return &models.TerminateRuleActionTune{Action: models.ActionContinue}, nil
	}
	return &models.TerminateRuleActionTune{
		RuleName: models.TerminateRuleIdleTimeout,
		Reason:   fmt.Sprintf("Postgres cursor exhausted and idle for %v", time.Since(props.LastMessageAt).Round(time.Second)),
		Action:   models.ActionStop,
	}, nil
}
```

#### Terminate 12 — Flow 33 (Oracle, finite)

Identical to terminate_11. Import `entity146` instead. Change package to `client_terminate_12`.

#### Terminate 13 — Flow 34 (Kafka consumer, infinite)

Identical to terminate_10 (Case 4). Change package to `client_terminate_13`. Never fires ActionStop.

#### Terminate 14 — Flow 35 (REST cursor source, finite with idle timeout)

```go
// client/terminates/terminate_14/terminate.go
package client_terminate_14

// Flow 35 is finite with idle timeout.
// When entity_150 reports exhausted=true (has_more=false from Mac service)
// AND no new results have arrived for 60s, the flow stops.
// 60s is long enough for the Mac service to process a final in-flight batch.

import (
	"fmt"
	"time"

	entity150 "etlfunnel/execution/client/connectors/connector_60/iso_entity_150"
	"etlfunnel/execution/models"
)

const (
	checkInterval = 10 * time.Second
	idleTimeout   = 60 * time.Second
)

func TerminateRule(_ *models.TerminateRuleProps) (*models.TerminateRuleTune, error) {
	return &models.TerminateRuleTune{
		CheckInterval:        checkInterval,
		UserDefinedCheckFunc: check,
	}, nil
}

func check(props *models.CustomTerminateRuleCheckProps) (*models.TerminateRuleActionTune, error) {
	if !entity150.Instance.GetExhausted() {
		return &models.TerminateRuleActionTune{Action: models.ActionContinue}, nil
	}
	if props.LastMessageAt.IsZero() || time.Since(props.LastMessageAt) < idleTimeout {
		return &models.TerminateRuleActionTune{Action: models.ActionContinue}, nil
	}
	return &models.TerminateRuleActionTune{
		RuleName: models.TerminateRuleIdleTimeout,
		Reason:   fmt.Sprintf("Mac service exhausted and idle for %v — ES bulk complete", time.Since(props.LastMessageAt).Round(time.Second)),
		Action:   models.ActionStop,
	}, nil
}
```

---

### 11.24 DestinationWrites

```go
// client/destinationwrites/destinationwrite_9/destinationwrite.go
package client_destinationwrite_9
// Flow 32: Kafka 250 msgs. Identical to destinationwrite_7 (Case 4). Change package only.

// client/destinationwrites/destinationwrite_10/destinationwrite.go
package client_destinationwrite_10
// Flow 33: Kafka 250 msgs. Identical to destinationwrite_7 (Case 4). Change package only.
```

```go
// client/destinationwrites/destinationwrite_11/destinationwrite.go
package client_destinationwrite_11

// Flow 34: REST POST batch to Mac service.
// 50 records per POST — small enough to keep each HTTP call under 1s at Ollama latency.
// CheckInterval 15s allows terminate_13 to fire promptly.

import (
	"etlfunnel/execution/models"
	"time"
)

func DestinationWriteRule(_ *models.DestinationWriteProps) (*models.DestinationWriteTune, error) {
	return &models.DestinationWriteTune{
		RecordsPerBatch: 50,
		CheckInterval:   15 * time.Second,
	}, nil
}
```

```go
// client/destinationwrites/destinationwrite_12/destinationwrite.go
package client_destinationwrite_12

// Flow 35: ES bulk 100 docs.
// 100 documents per bulk request. Each doc includes a 768-float32 embedding
// (~3KB) so 100 docs ≈ 300KB per bulk call — well within ES defaults.

import (
	"etlfunnel/execution/models"
	"time"
)

func DestinationWriteRule(_ *models.DestinationWriteProps) (*models.DestinationWriteTune, error) {
	return &models.DestinationWriteTune{
		RecordsPerBatch: 100,
		CheckInterval:   20 * time.Second,
	}, nil
}
```

---

### 11.25 Bridge Files

All four bridges are **structural copies** of `client/pipelines/flow_24/pipeline_pipeline_1/bridge.go` (the canonical bridge). Change only the import block. Package name stays `client_pipeline_pipeline_1` in every bridge (each bridge lives in its own directory).

| Bridge | Source entity import | Dest entity import | Transformer imports | Checkpoint | Backlog | Terminate | DestWrite |
|---|---|---|---|---|---|---|---|
| `flow_32/pipeline_pipeline_1/bridge.go` | connector_54/iso_entity_144 | connector_55/iso_entity_145 | transformer_89, transformer_90 | checkpoint_11 | backlog_11 | terminate_11 | destinationwrite_9 |
| `flow_33/pipeline_pipeline_1/bridge.go` | connector_56/iso_entity_146 | connector_57/iso_entity_147 | transformer_91, transformer_92 | checkpoint_12 | backlog_12 | terminate_12 | destinationwrite_10 |
| `flow_34/pipeline_pipeline_1/bridge.go` | connector_58/iso_entity_148 | connector_59/iso_entity_149 | transformer_93, transformer_94 | checkpoint_13 | backlog_13 | terminate_13 | destinationwrite_11 |
| `flow_35/pipeline_pipeline_1/bridge.go` | connector_60/iso_entity_150 | connector_61/iso_entity_151 | transformer_95, transformer_96 | checkpoint_14 | backlog_14 | terminate_14 | destinationwrite_12 |

In each bridge's `applyTransformations`, call the two transformers in the order listed above (left to right).

---

## 12. connectors.go — Required Imports

Add to the root `connectors.go`:

```go
import (
    // existing imports ...

    // Case 5 additions
    _ "etlfunnel/execution/core/source/postgres"       // Flows 32/33 sources (verify package path)
    _ "etlfunnel/execution/core/source/oracle"         // Flow 33 source (verify package path)
    _ "etlfunnel/execution/core/destination/kafka"     // Flows 32/33 destinations (already present if Case 4 active)
    _ "etlfunnel/execution/core/source/kafka"          // Flow 34 source (already present if Case 4 active)
    _ "etlfunnel/execution/core/destination/restapi"   // Flow 34 destination (verify — may need to add)
    _ "etlfunnel/execution/core/source/restapi"        // Flow 35 source (already present if Cases 1/3 active)
    _ "etlfunnel/execution/core/destination/elasticsearch" // Flow 35 destination (new)
)
```

Verify actual package paths against the `core/` directory before coding.

---

## 13. AuxDB Tables — DDL (run once)

```sql
-- Checkpoint tables
CREATE TABLE IF NOT EXISTS pf_catalog_checkpoints (
  source          TEXT PRIMARY KEY,       -- 'postgres' or 'oracle'
  last_updated_at TEXT NOT NULL,
  updated_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS pf_enrich_collect_cursors (
  id          INT PRIMARY KEY DEFAULT 1,
  last_cursor INT NOT NULL DEFAULT 0,
  updated_at  TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS pf_enrich_submit_offsets (
  topic       TEXT,
  partition   INT,
  last_offset BIGINT NOT NULL,
  updated_at  TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (topic, partition)
);

-- Backlog tables
CREATE TABLE IF NOT EXISTS pf_postgres_ingest_backlog (
  id               BIGSERIAL PRIMARY KEY,
  product_id       TEXT,
  failure_stage    TEXT,
  error_message    TEXT,
  record_payload   JSONB,
  pipeline_run_id  TEXT,
  created_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pf_oracle_ingest_backlog (
  id               BIGSERIAL PRIMARY KEY,
  product_id       TEXT,
  failure_stage    TEXT,
  error_message    TEXT,
  record_payload   JSONB,
  pipeline_run_id  TEXT,
  created_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pf_enrich_submit_backlog (
  id               BIGSERIAL PRIMARY KEY,
  product_id       TEXT,
  kafka_topic      TEXT,
  kafka_partition  INT,
  kafka_offset     BIGINT,
  failure_stage    TEXT,
  error_message    TEXT,
  record_payload   JSONB,
  pipeline_run_id  TEXT,
  created_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pf_es_index_backlog (
  id               BIGSERIAL PRIMARY KEY,
  product_id       TEXT,
  failure_stage    TEXT,
  error_message    TEXT,
  record_payload   JSONB,
  pipeline_run_id  TEXT,
  created_at       TIMESTAMPTZ DEFAULT NOW()
);
```

---

## 14. Execution Sequence

1. **Start prerequisites**: Ollama (`ollama serve`), Kafka, Elasticsearch, AuxDB (already running from prior cases).
2. **Pull model**: `ollama pull nomic-embed-text` (one-time, ~274 MB).
3. **Create AuxDB tables** (DDL from section 13 — once only).
4. **Create Elasticsearch index** (curl from section 8 — once only).
5. **Start the Mac Enrich Service**: `cd cases/case_5/cmd/mac_enrich_service && go run main.go`.
   Verify: `curl http://localhost:8765/health` → `{"status":"ok","pending":0,"done":0}`.
6. **Start Flow 34** (`job.json pid: 34`) — must be running before Flow 32/33 publish to Kafka, otherwise records may be missed if Kafka retention is short.
7. **Start Flow 35** (`job.json pid: 35`) — polls Mac service for results; can run concurrently with Flow 34.
8. **Start Flow 32** (`job.json pid: 32`) — Postgres ingest (finite, terminates when cursor exhausted).
9. **Start Flow 33** (`job.json pid: 33`) — Oracle ingest (finite, terminates when cursor exhausted).
10. Flows 32 and 33 publish to `pepperfry.catalog.raw`. Flow 34 consumes and POSTs to Mac Enrich Service. Flow 35 polls and bulk-indexes into Elasticsearch.
11. Verify: query Elasticsearch `GET /pf_products/_count` — should increase as Flow 35 runs.
12. Semantic search test: `POST /pf_products/_search` with `knn` query using a test embedding.

---

## 15. Design Decisions and Trade-offs

| Decision | Choice | Why | Alternative rejected |
|---|---|---|---|
| Fan-in via same Kafka topic | Two separate flows write to `pepperfry.catalog.raw` | Each source has its own cadence, schema, and checkpoint. Forcing them into one flow would require a multi-source connector not supported by the engine's current bridge pattern. | One combined flow with two source pipelines: engine doesn't support per-pipeline source overrides at the flow level |
| Oracle records with no title | Dropped by transformer_92 | An embedding of pure supplier/SKU data has low retrieval value. When Postgres picks up the full product record, it covers the embedding need. | Pass through with description-only embedding: produces low-quality vectors that degrade search quality |
| Ollama as a sidecar (REST), not inline transformer | Sidecar via Mac Enrich Service | Decouples Ollama latency (50–200ms/record) from the Kafka consumer throughput. Flow 34 can run at Kafka speed (thousands/sec); Flow 35 runs at Ollama speed. | Inline Ollama HTTP call inside transformer_93: back-pressure stalls the consumer group, Kafka lag grows, risks rebalance |
| Flow 35 idle-timeout termination | 60s idle → stop | Flow 35 needs to drain all results that Flow 34 submitted before declaring done. 60s > typical Ollama batch latency for 50 records (5–10s). | Infinite: wastes resources polling an empty queue after Flows 32/33 have finished. Finite with max-records: unknown total — can't set a count |
| REST destination (not custom Ollama connector) | Reuse REST connector type 11 | No new connector code needed; the Mac service abstracts Ollama entirely. Future switch to OpenAI embeddings = one env var change in the Mac service. | Custom Ollama connector type: more code, tighter coupling |
| Elasticsearch dense_vector with cosine similarity | Cosine | Text embeddings from nomic-embed-text are not L2-normalized; cosine similarity is the correct metric for this model. | dot_product: requires L2-normalized vectors; l2_norm: less appropriate for semantic similarity |
| In-memory Mac service (no SQLite) | In-memory | Simpler to run locally; for a demo/case-study the state fits in RAM. | SQLite: adds a dependency, more setup, not needed for 100k-product catalog demo |

---

## 16. Testing Notes

### Flow 32/33 — DB Ingestion to Kafka

- **TestPostgresIncrementalQuery**: Insert 20 products into a test Postgres DB with `updated_at` spread across 3 batches. Pre-populate `pf_catalog_checkpoints` with a mid-point timestamp. Assert only products after the checkpoint are published. Assert Kafka receives the correct number of messages.
- **TestOracleNormalizerStripsTitle**: Insert an Oracle row with no `CATEGORY`. Assert transformer_91 produces `category: ""` → transformer_92 passes it through. Insert a row with `PRODUCT_ID` empty → assert drop.
- **TestFanInSameTopic**: Start both Flow 32 and Flow 33 against their respective test DBs. Assert all messages land on `pepperfry.catalog.raw` partitioned by `product_id`.

### Flow 34 — Kafka to Mac Enrich Service

- **TestEnrichSubmitBatch**: Start a mock HTTP server that accepts `POST /enrich` and returns 202. Run Flow 34. Assert each batch has ≤ 50 records (destinationwrite_11). Assert `batch_id` is non-empty.
- **TestEnrichTransformerDropsNoTitle**: Produce a Kafka message with `title: ""`. Assert transformer_93 returns nil (drop). Assert it does not appear in the REST POST.

### Flow 35 — Mac Enrich Service to Elasticsearch

- **TestEmbeddingExtractorConverts**: Pass a record with `embedding: []interface{}{0.1, 0.2, ...}` (768 values). Assert transformer_95 converts to `[]float32`.
- **TestEmbeddingExtractorDropsWrongDims**: Pass embedding with 512 values. Assert drop (nil return).
- **TestESBulkIndex**: Start a local Elasticsearch container. Run Flow 35 against a mock Mac service returning 10 results with embeddings. Assert all 10 docs appear in `pf_products` with the correct `product_id` as `_id`.
- **TestCursorCheckpoint**: After a flush, assert `pf_enrich_collect_cursors` is upserted with the correct cursor position.

### Mac Enrich Service

- **TestEnrichServiceQueues**: POST 5 records. Verify `/health` shows `pending: 5`. After worker runs (mock Ollama), verify `done: 5`.
- **TestResultsCursorPagination**: Generate 250 results. Call `GET /results?cursor=0&limit=100` three times. Assert `next_cursor` advances correctly, `has_more` is false on the final page.

### Environment Variables Required

```powershell
$env:PF_POSTGRES_PASSWORD    = "test-pass"
$env:PF_ORACLE_PASSWORD      = "test-pass"
$env:ENRICH_SERVICE_URL      = "http://localhost:8765"
$env:OLLAMA_URL              = "http://localhost:11434"
$env:ENRICH_PORT             = "8765"
$env:TEST_KAFKA_BROKERS      = "localhost:9092"
$env:TEST_ES_ADDRESS         = "http://localhost:9200"
$env:TEST_POSTGRES_DSN       = "postgres://test:test@localhost:5433/testdb"  # AuxDB
```

---

## 17. Summary

Case 5 introduces **four semantically chained flows with a fan-in + AI enrichment architecture**. The architectural novelty versus Cases 1–4:

- **Flows 32 and 33** (finite, sibling): Postgres and Oracle DB sources both publish to the same Kafka topic — the first multi-source fan-in in any case. Each flow is independently checkpointed, runs on its own schedule, and terminates when its source is exhausted.
- **Flow 34** (infinite, streaming): Kafka consumer batches records and POSTs them to the Mac Enrich Service (Ollama wrapper). The REST connector type is reused as a *sink* — a role it played as a passthrough in Case 3 but now plays as an async AI processing target.
- **Flow 35** (finite with idle timeout): REST cursor source (same connector type, now as a *source*) polls for enriched results and bulk-indexes into Elasticsearch with a `dense_vector` field for knn semantic search.

The five genuinely new concepts introduced that do not exist in Cases 1–4:

1. **Multi-source fan-in**: two source connectors writing to the same Kafka topic across two parallel flows
2. **REST connector as async processing sink**: POST records to an AI service, not to a passthrough API
3. **REST connector as polling source** for AI-processed results (cursor-based, same connector type as Case 1 but against a stateful local service)
4. **AI enrichment as a pipeline stage**: Ollama generates embeddings; the pipeline orchestrates delivery and collection without coupling to the model
5. **Elasticsearch as a destination** with `dense_vector` field — enables knn semantic product search as the end-to-end outcome
