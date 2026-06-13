# Case 4 — Deviations from CASE_4_DESIGN.md

> Recorded during implementation on 2026-06-13.
> Each deviation identifies the design doc section, what it said, what was actually done, and why.

---

## Deviation 1 — Cassandra connector TTL handling (Section 9.6)

**Design doc said:**
```go
cql := `INSERT INTO zepto_events.order_events (...) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?`
tunes = append(tunes, &models.CassandraDestQueryTune{
    ...
    TTL: ttl,
    Parameters: []any{ ..., ttl },  // ttl was the last element
})
```

**What was implemented:**
```go
cql := `INSERT INTO zepto_events.order_events (...) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
// No USING TTL ? in CQL. ttl removed from Parameters.
tunes = append(tunes, &models.CassandraDestQueryTune{
    ...
    TTL: ttl,
    Parameters: []any{ ... },  // 11 values, no trailing ttl
})
```

**Why:**
`core/destination/cassandra/implementation.go::applyModifiers` (line 111) automatically appends
`" USING TTL ?"` and the ttl value to the CQL and parameters when `CassandraDestQueryTune.TTL > 0`.
Including `USING TTL ?` in the CQL string **and** setting the `TTL` field would produce invalid CQL:
`INSERT ... VALUES (...) USING TTL ? USING TTL ?`.
The fix is to use the `TTL` field only, which the engine translates at execution time.

---

## Deviation 2 — Orchestrator 15 AuxDB access (Section 9.1)

**Design doc said:**
```go
auxConn, err := ulib.GetAuxPostgresConn(param.AuxilaryDBConnMap)
```

**What was implemented:**
```go
if len(param.Pipelines) > 0 {
    auxConn, err := ulib.GetAuxPostgresConn(param.Pipelines[0].AuxiliaryDBConnMap)
    ...
}
```

**Why:**
`models.PipelineOrchestratorProps` has no top-level `AuxilaryDBConnMap` field.
The model is:
```go
type PipelineOrchestratorProps struct {
    Pipelines []PipelineOrchestratorItemProps
}
type PipelineOrchestratorItemProps struct {
    AuxiliaryDBConnMap map[string]IDatabaseEngine
    ...
}
```
The AuxDB connection map lives on each pipeline item, not on the orchestrator props directly.
The fix reads from `param.Pipelines[0].AuxiliaryDBConnMap` and guards against an empty slice.
If there are no pipelines (degenerate case), the orchestrator skips the AuxDB lookup and starts
with an empty cursor (i.e., from the beginning), which is the safe default.

---

## Deviation 3 — Flow 30 bridge uses `client_source_entity.Instance` (Section 9.19)

**Design doc said:** "Copy flow_24 bridge verbatim and update imports."

**What was implemented:** `flow_30/pipeline_pipeline_1/bridge.go` initializes the source bridge as:
```go
clientSourceBridge: client_source_entity.Instance,
```
instead of:
```go
clientSourceBridge: &client_source_entity.IUseConnector{},
```

**Why:**
Section 9.3 (connector_50/iso_entity_140) defines a package-level singleton `Instance = &IUseConnector{}`
and states explicitly that `checkpoint_9` and `terminate_9` must call `entity140.Instance.GetLastCursor()`
and `entity140.Instance.GetExhausted()`. For this to work, the bridge, checkpoint, and terminate must
all operate on the **same** struct pointer. Using `&client_source_entity.IUseConnector{}` would create a
separate instance that never has its `nextCursor`/`exhausted` fields mutated, so the terminate rule would
never fire and the checkpoint would never write a cursor.
Using `Instance` in the bridge ensures all three components share one struct.

Flow 31's bridge is not affected — `connector_52/iso_entity_142` (Kafka consumer) has no singleton
requirement and uses `&IUseConnector{}` as the design doc specifies.

---

## Deviation 4 — RESTAPISourceCursorTune fields differ from design (Section 9.3)

**Design doc said:**
```go
&models.RESTAPISourceCursorTune{
    Path:   "/api/v2/order-events",
    Method: "GET",
    Headers: map[string]string{"X-Internal-Token": apiToken, ...},
    QueryParams: func() map[string]string { ... }(),
    ParseRecords: func(...),
    NextCursorToken: func(...),
}
```

**What was implemented:**
```go
&models.RESTAPISourceCursorTune{
    Path:          "/api/v2/order-events?limit=500",
    CursorParam:   "cursor",
    CursorValue:   startCursor,
    ParseRecords:  func(...),
    NextPageToken: func(...),   // renamed from NextCursorToken
}
```

**Why:**
The actual `models.RESTAPISourceCursorTune` struct (in `models/restapi_model.go`) only has:
`Path`, `CursorParam`, `CursorValue`, `ParseRecords`, `NextPageToken`.
There are no `Method`, `Headers`, or `QueryParams` fields.
The engine's `ReadByCursor` in `core/source/restapi/implementation.go` always uses `http.MethodGet`
and passes `nil` for headers — auth is handled by the HTTP client transport set up from `connectionParams`
(authType=apiKey + apiKeyHeader=X-Internal-Token). The `limit=500` param is baked into the `Path`
as a static query param; the engine appends `cursor=<value>` as an additional param alongside it.

**Related orchestrator change:** `orchestrator_15` was simplified to NOT pass `api_token` or `base_url`
via ReplicaProps (since auth is at transport level and base URL is in `connectionParams`). Only
`start_cursor` and `pipeline_run_id` are passed. The ZEPTO_API_TOKEN env var check was also removed —
the token is configured in `collection.json::connectionParams.token` at deployment.

---

## Notes — No Action Required

- **`KafkaSourceSubscriptionTune.FetchMaxBytes` type**: Design doc uses `10 * 1024 * 1024` (untyped
  constant). Model field is `int32`. Go assigns untyped integer constants to any compatible integer
  type at compile time; `10485760 < 2147483647` so this compiles without a cast. No change needed.

- **`_kafka_key` data type**: Section 7.2 data contract lists `_kafka_key` as `[]byte`, but
  `transformer_86` sets it as a `string` (`out["_kafka_key"] = orderID`), and `connector_51/entity_141`
  reads it as `.(string)`. The code is internally consistent; the data contract description was
  aspirational. No change needed.
