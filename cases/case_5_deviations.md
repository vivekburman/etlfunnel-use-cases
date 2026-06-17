# Case 5 Implementation Deviations

This file documents every deviation from `CASE_5_DESIGN.md` found during implementation. All deviations were discovered by reading the actual source code before writing any connector/transformer code.

---

## DEV-01 — Interface name: Postgres source

**Design doc said:** `IClientDBPostgresSQLSource`
**Actual interface:** `IClientDBPostgresSource` (in `core/coreinterface/postgres.go`)
**Impact:** All compile-time assertion lines and function signatures corrected.

---

## DEV-02 — Interface name: Oracle source

**Design doc said:** `IClientDBOracleSQLSource`
**Actual interface:** `IClientDBOracleSource` (in `core/coreinterface/oracle.go`)
**Impact:** All compile-time assertion lines and function signatures corrected.

---

## DEV-03 — Interface name: Elasticsearch destination

**Design doc said:** `IClientDBElasticsearchDest`
**Actual interface:** `IClientDBElasticDest` (in `core/coreinterface/elasticsearch.go`)
**Impact:** Connector 61 / entity_151 compile-time assertion corrected.

---

## DEV-04 — Oracle connection type in FetchRecords

**Design doc said:** `param.SourceDBConn` is `*go_ora.Connection`
**Actual type:** `*sql.DB` from `database/sql` (confirmed in `models/bridge_model.go` and `OracleSourceFetch` struct)
**Impact:** Entity_146 uses `*sql.DB` directly for query execution. No `go_ora` import needed.

---

## DEV-05 — Postgres captureMethod value

**Design doc said:** `captureMethod: 0`
**Actual value:** `4` (`UseDBPostgresCaptureUserDefinedMethod` from `enum/source_connector.go`)
**Impact:** `collection.json` flow 32 source uses `captureMethod: 4`. Entity_144 comment documents this.

---

## DEV-06 — Oracle captureMethod value

**Design doc said:** `captureMethod: 0`
**Actual value:** `3` (`UseDBOracleCaptureUserDefinedMethod` from `enum/source_connector.go`)
**Impact:** `collection.json` flow 33 source uses `captureMethod: 3`. Entity_146 comment documents this.

---

## DEV-07 — Elasticsearch model names

**Design doc said:** `ElasticsearchDestQuery`, `ElasticsearchDestQueryTune`, `Body` field, `ElasticsearchDestOpIndex`
**Actual names:** `ElasticDestQuery`, `ElasticDestQueryTune`, `Document` field, `ElasticWriteIndex` (confirmed in `models/bridge_model.go`)
**Impact:** Entity_151 uses all corrected model names.

---

## DEV-08 — RESTAPISourceCursorTune field names

**Design doc said:** `QueryParams map[string]string`, `ParseRecords func(...)`, `NextCursorToken func(...) string`
**Actual fields:** `CursorParam string`, `CursorValue string`, `ParseFn func(RESTAPIRawResponse) ([]map[string]any, error)`, `NextPageToken func(body []byte, headers http.Header) (string, bool)` (confirmed in `models/bridge_model.go`)
**Impact:** Entity_150 (`GenerateCursorRequest`) rewritten from scratch to use correct fields. `net/http` is imported for `http.Header`.

---

## DEV-09 — KafkaSourceSubscriptionTune missing ParseFn

**Design doc:** Did not include `ParseFn` in `KafkaSourceSubscriptionTune`
**Reality:** `ParseFn func(KafkaRawMessage) (map[string]any, error)` is the mechanism by which raw Kafka bytes become a record map. Without it, transformer_93 would receive an empty map.
**Fix:** Entity_148 sets `ParseFn` to populate `_kafka_value`, `_kafka_key`, `_kafka_topic`, `_kafka_partition`, `_kafka_offset` from `KafkaRawMessage`.

---

## DEV-10 — Orchestrator AuxDB access path

**Design doc said:** `param.AuxilaryDBConnMap["auxdb"]`
**Actual field path:** `param.Pipelines[0].AuxiliaryDBConnMap["auxdb"]` (confirmed in `models/orchestrator_model.go`: `PipelineOrchestratorProps.Pipelines []PipelineOrchestratorItemProps`, each item has `AuxiliaryDBConnMap`)
**Impact:** All four orchestrators (17-20) use `param.Pipelines[0].AuxiliaryDBConnMap`.

---

## DEV-11 — DBElasticsearchConfig connection params

**Design doc said:** `"addresses": ["http://localhost:9200"]` (array)
**Actual model:** `DBElasticsearchConfig.URL string` (single string, confirmed in `models/connection_model.go`)
**Impact:** `collection.json` flow 35 destination uses `{"url": "http://localhost:9200", "username": "elastic", "password": ""}`.

---

## DEV-12 — Checkpoint reference "checkpoint_10" in design doc

**Design doc said:** Kafka offset checkpoint should follow the pattern from `checkpoint_10`
**Reality:** `checkpoint_10` does not exist in the codebase. The highest existing checkpoint before case 5 was `checkpoint_8`.
**Fix:** Checkpoint_13 implemented from scratch, reading `_kafka_topic`, `_kafka_partition`, `_kafka_offset` fields (which entity_148's `ParseFn` populates) and upsetting `pf_enrich_submit_offsets` in AuxDB.

---

## DEV-13 — Elasticsearch destination captureMethod

**Design doc:** Did not specify a captureMethod for Elasticsearch destination.
**Decision:** Used `captureMethod: 1` in `collection.json` flow 35 destination. The destination_connector.go enum file is empty; destination implementations call `SetCaptureMethod` but do not use the value for Elasticsearch. Value 1 is consistent with other non-SQL destination defaults.

---

## DEV-14 — connectors.go missing imports

**Design doc:** Did not mention updating `connectors.go`.
**Reality:** `connectors.go` lacked `core/source/oracle`, `core/destination/elastic`, and `core/destination/restapi` side-effect imports. Without them the registry never registers these engines, causing runtime "unknown connector type" panics.
**Fix:** Three blank imports added to `connectors.go`.
