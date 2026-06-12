# Myntra Digital Analytics Intelligence — ETL Pipeline Case Study Plan
### Google Analytics 4 Data API (REST, Multi-Property) → Microsoft SQL Server | Streamcraft Execution Framework

---

## Overview

This case study models a real-world analytics ingestion problem faced by a large Indian e-commerce platform — modelled after **Myntra** — that uses Google Analytics 4 (GA4) to instrument user behaviour across three surfaces: **Web, Android App, and iOS App**. Each surface is tracked under a separate GA4 property, each with its own event taxonomy, session definition, and conversion goals.

The analytics team needs all three properties' data inside a **Microsoft SQL Server** data warehouse so that their Power BI dashboards, marketing attribution models, and funnel analysis queries can run on-prem without calling the GA4 API every time a report is needed.

The pipeline is built on the Streamcraft Execution framework using the **`IClientRESTAPISource`** interface and covers **three distinct pipeline collections**:

- **Historical Backfill Flow** — Chunked daily pagination over the GA4 Data API (`runReport`) covering 730 days of historical data per property. Respects GA4 sampling thresholds by keeping each request window to exactly one calendar day. Lands deduplicated rows into SQL Server via staging tables + `MERGE`.
- **Incremental Daily Flow** — Scheduled daily pipeline that extracts the prior day's finalised data for all three properties. GA4 data is not fully settled for 48-72 hours, so this flow always fetches `T-2` and upserts, overwriting any preliminary rows from the previous run.
- **Realtime Pulse Flow** — Calls the GA4 Realtime API (`runRealtimeReport`) on a 60-second cadence to populate a `realtime_sessions` table in SQL Server, powering a live ops dashboard showing active users and in-flight conversion events.

The central engineering challenge of this case is **REST API pagination at scale combined with quota exhaustion**. GA4 imposes per-property, per-day quotas on the Data API — a naive full-range fetch will exhaust the quota within hours, corrupting the backfill. The pipeline's `IClientRESTAPISource` implementation must chunk requests, track quota consumption, and back off gracefully.

---

## Part 1 — Source Architecture (Google Analytics 4)

### 1.1 GA4 Properties

Three GA4 properties, one per surface. Each is independently managed and produces divergent event schemas.

| Property | Surface | GA4 Property ID | Primary Conversion Event |
|---|---|---|---|
| `myntra-web` | Desktop + Mobile Web | `properties/123456789` | `purchase` |
| `myntra-android` | Android App | `properties/987654321` | `app_purchase` |
| `myntra-ios` | iOS App | `properties/567891234` | `in_app_purchase` |

### 1.2 GA4 Data API — Technical Mechanics

The GA4 Data API (`analyticsdata.googleapis.com/v1beta`) exposes two endpoints used by this pipeline:

**`POST /v1beta/{property}:runReport`** — Paginated batch reporting
- Request body specifies: `dimensions[]`, `metrics[]`, `dateRanges[]`, `limit`, `offset`
- Response: `{ rows: [...], rowCount: N, nextPageToken: "..." }` — but GA4 does **not** use a cursor token; pagination is purely `offset`-based
- Max rows per response: **100,000** (GA4 hard limit per request)
- Sampling kicks in when a single request covers > ~10M sessions; chunking to 1-day windows avoids this for all but Black Friday-scale traffic

**`POST /v1beta/{property}:runRealtimeReport`** — Live active-user snapshot (last 30 minutes)
- No pagination; returns up to 10,000 rows
- Counts distinct `activeUsers` broken down by dimensions such as `city`, `deviceCategory`, `pagePath`, `eventName`

### 1.3 Quota Model

GA4 API quotas are the dominant operational constraint in this pipeline.

| Quota Type | Limit | Applies To |
|---|---|---|
| Core tokens per property per day | 200,000 | `runReport` |
| Core tokens per property per hour | 40,000 | `runReport` |
| Concurrent requests | 10 | per property |
| Realtime tokens per property per day | 10,000 | `runRealtimeReport` |

Each `runReport` request costs tokens proportional to the number of dimension-metric combinations and date range width. A 1-day window with 6 dimensions + 10 metrics costs roughly 1 token per 1,000 rows returned, with a minimum of 1 token per request.

> **Backfill math**: 730 days × 3 properties × ~5 requests per day (pagination) = ~10,950 requests. At 200K tokens/day/property this is achievable in 1-2 days per property if requests are evenly spread. The pipeline's `QuotaThrottle` transformer tracks token spend per property per hour and inserts synthetic `time.Sleep` when the hourly bucket is 80% consumed.

### 1.4 Event Schema Divergence Across Properties

GA4 properties share a core schema (`session_id`, `user_pseudo_id`, `event_name`, `event_timestamp`) but diverge significantly on custom dimensions and metrics registered per property.

**Core dimensions used across all three properties:**

| Dimension Name | GA4 API Name | Notes |
|---|---|---|
| Date | `date` | `YYYYMMDD` string from GA4 |
| Session ID | `sessionId` | Scoped to property |
| User Pseudo ID | `userPseudoId` | Anonymised device ID |
| Event Name | `eventName` | What happened |
| Device Category | `deviceCategory` | `desktop`, `mobile`, `tablet` |
| City | `city` | IP-derived |
| Country | `country` | ISO-2 |
| Source | `sessionSource` | UTM source |
| Medium | `sessionMedium` | UTM medium |
| Campaign | `sessionCampaignName` | UTM campaign |

**Property-specific custom dimensions:**

| Dimension Concept | `myntra-web` | `myntra-android` | `myntra-ios` |
|---|---|---|---|
| Product category at conversion | `customEvent:product_category` | `customEvent:category_slug` | `customEvent:item_category` |
| Wishlist flag | `customEvent:wishlisted` | `customEvent:is_wishlisted` | *(not tracked)* |
| App version | *(not applicable)* | `appVersion` | `appVersion` |
| OS version | *(not applicable)* | `operatingSystemVersion` | `operatingSystemVersion` |
| Payment method | `customEvent:payment_method` | `customEvent:payment_type` | `customEvent:pay_method` |

> **Why this matters for the ETL:** The `DimensionNormaliser` transformer maps all property-specific custom dimension names to a canonical set of column names before the SQL Server writer sees any record. Without this normalisation the destination schema would fork per property.

### 1.5 Core Metrics

| Metric Concept | GA4 Metric Name | Type |
|---|---|---|
| Sessions | `sessions` | integer |
| Engaged Sessions | `engagedSessions` | integer |
| Total Users | `totalUsers` | integer |
| New Users | `newUsers` | integer |
| Bounce Rate | `bounceRate` | float (0.0–1.0) |
| Average Session Duration | `averageSessionDuration` | float (seconds) |
| Conversions | `conversions` | integer |
| Purchase Revenue | `purchaseRevenue` | float (INR) |
| Event Count | `eventCount` | integer |
| Screen Page Views | `screenPageViews` | integer |

### 1.6 Pagination Shape

GA4 `runReport` uses **offset-based pagination**, not cursor-based. The engine must:

1. Issue the first request with `offset: 0, limit: 100000`
2. Read `rowCount` from the response to know the total
3. Calculate pages: `ceil(rowCount / 100000)`
4. Issue subsequent requests with `offset: 100000, 200000, ...` up to `rowCount`

There is **no `nextPageToken`** in the GA4 response body for `runReport`. The `NextPageToken` method on `IClientRESTAPISource` is adapted here to encode the next offset as a string (`"100000"`, `"200000"`, etc.) and returns `false` when `offset >= rowCount`.

---

## Part 2 — Destination Architecture (Microsoft SQL Server)

### 2.1 Database Layout

**Server**: `analytics-sqlserver.myntra.internal`, port `1433`  
**Database**: `analytics_warehouse`  
**Auth**: SQL Server Authentication — service account `etl_writer`

Two schemas are used to implement a staging-then-merge pattern:

| Schema | Purpose |
|---|---|
| `stage` | Transient landing zone; truncated before each pipeline run |
| `dbo` | Production tables; never truncated; updated via `MERGE` |

### 2.2 Destination Tables

#### `dbo.ga4_sessions` — One row per (property, date, session_id)

| Column | SQL Server Type | Source |
|---|---|---|
| `property_id` | `VARCHAR(50)` NOT NULL | injected by pipeline context |
| `surface` | `VARCHAR(20)` NOT NULL | `web` / `android` / `ios` |
| `report_date` | `DATE` NOT NULL | parsed from GA4 `date` dimension |
| `session_id` | `VARCHAR(100)` NOT NULL | `sessionId` |
| `user_pseudo_id` | `VARCHAR(100)` | `userPseudoId` |
| `device_category` | `VARCHAR(30)` | `deviceCategory` |
| `city` | `VARCHAR(100)` | `city` |
| `country` | `CHAR(2)` | `country` |
| `source` | `VARCHAR(200)` | `sessionSource` |
| `medium` | `VARCHAR(100)` | `sessionMedium` |
| `campaign` | `VARCHAR(500)` | `sessionCampaignName` |
| `product_category` | `VARCHAR(200)` | normalised from per-property custom dim |
| `payment_method` | `VARCHAR(100)` | normalised from per-property custom dim |
| `app_version` | `VARCHAR(50)` | null for web |
| `os_version` | `VARCHAR(50)` | null for web |
| `sessions` | `INT` | `sessions` |
| `engaged_sessions` | `INT` | `engagedSessions` |
| `total_users` | `INT` | `totalUsers` |
| `new_users` | `INT` | `newUsers` |
| `bounce_rate` | `DECIMAL(6,4)` | `bounceRate` |
| `avg_session_duration_secs` | `DECIMAL(10,2)` | `averageSessionDuration` |
| `conversions` | `INT` | `conversions` |
| `purchase_revenue_inr` | `DECIMAL(18,2)` | `purchaseRevenue` |
| `event_count` | `INT` | `eventCount` |
| `screen_page_views` | `INT` | `screenPageViews` |
| `ingested_at` | `DATETIME2` | set by pipeline at write time |
| `pipeline_run_id` | `VARCHAR(100)` | set by pipeline context |

**Primary key**: `(property_id, report_date, session_id)`  
**Clustered index**: `(property_id, report_date)` — supports date-range scans by Power BI  
**Non-clustered index**: `(source, medium, campaign)` — attribution queries  
**Columnstore index**: `(report_date, property_id, conversions, purchase_revenue_inr)` — aggregation performance

#### `stage.ga4_sessions` — Identical columns, no indexes, `HEAP` table

Used as the landing pad. Each pipeline run `TRUNCATE`s this table, bulk-inserts all fetched rows, then runs the `MERGE` against `dbo.ga4_sessions`.

#### `dbo.realtime_sessions` — Rolling 30-minute snapshot

| Column | SQL Server Type | Notes |
|---|---|---|
| `snapshot_at` | `DATETIME2` NOT NULL | when the realtime API was called |
| `property_id` | `VARCHAR(50)` NOT NULL | |
| `surface` | `VARCHAR(20)` NOT NULL | |
| `active_users` | `INT` | |
| `city` | `VARCHAR(100)` | |
| `device_category` | `VARCHAR(30)` | |
| `page_path` | `VARCHAR(1000)` | |
| `event_name` | `VARCHAR(200)` | |

**Retention**: rows older than 2 hours are deleted by a cleanup job run at the end of each Realtime Pulse pipeline execution. This table is effectively a rolling buffer, not a history.

#### `dbo.pipeline_run_log` — Pipeline execution metadata

| Column | SQL Server Type |
|---|---|
| `run_id` | `VARCHAR(100)` PK |
| `pipeline_name` | `VARCHAR(200)` |
| `property_id` | `VARCHAR(50)` |
| `surface` | `VARCHAR(20)` |
| `started_at` | `DATETIME2` |
| `finished_at` | `DATETIME2` |
| `rows_fetched` | `BIGINT` |
| `rows_merged` | `BIGINT` |
| `quota_tokens_spent` | `INT` |
| `status` | `VARCHAR(20)` |
| `error_message` | `NVARCHAR(MAX)` |

### 2.3 MERGE Strategy

After the staging table is populated, the pipeline executes a `MERGE` in a single SQL Server transaction:

```sql
MERGE dbo.ga4_sessions AS target
USING stage.ga4_sessions AS source
ON  target.property_id = source.property_id
AND target.report_date  = source.report_date
AND target.session_id   = source.session_id
WHEN MATCHED THEN
    UPDATE SET
        sessions               = source.sessions,
        engaged_sessions       = source.engaged_sessions,
        conversions            = source.conversions,
        purchase_revenue_inr   = source.purchase_revenue_inr,
        ingested_at            = source.ingested_at,
        pipeline_run_id        = source.pipeline_run_id
        -- (all metric columns)
WHEN NOT MATCHED BY TARGET THEN
    INSERT (property_id, report_date, session_id, ...)
    VALUES (source.property_id, source.report_date, source.session_id, ...);
```

The `WHEN NOT MATCHED BY SOURCE` clause is intentionally **omitted** — the pipeline never deletes rows from the production table (GA4 doesn't retract historical data; it only revises it within the 72-hour settlement window).

---

## Part 3 — Pipeline Collections

### 3.1 Historical Backfill Flow

**Purpose**: One-time (or re-runnable) extraction of 730 days of historical data per property.

**Trigger**: Manual invocation via `make backfill PROPERTY=web DATE_FROM=2024-01-01 DATE_END=2025-12-31`

**Execution model**:
1. The driver generates a list of 730 individual date strings (`2024-01-01` … `2025-12-31`).
2. For each date, the pipeline calls `IClientRESTAPISource.GeneratePaginateRequest` with the date window and requested dimensions/metrics.
3. The engine calls `FetchRecords` per page and aggregates all rows for that date.
4. After all pages for the date are collected, records are written to `stage.ga4_sessions`.
5. `MERGE` is executed.
6. The date is checkpointed in `dbo.pipeline_run_log` so a restart skips completed dates.

**Concurrency**: Maximum 3 dates in parallel per property (respecting the 10 concurrent-request quota). Three properties run sequentially (not in parallel) to avoid cross-property quota collisions on the same GCP service account.

**Backpressure**: The `QuotaThrottle` transformer monitors token spend per property per hour. When spend exceeds 32,000 tokens/hour (80% of the 40K limit), it suspends the date queue for that property until the next hour window opens.

### 3.2 Incremental Daily Flow

**Purpose**: Nightly ingestion of the prior day's finalised GA4 data.

**Trigger**: Scheduled daily at 06:00 IST (UTC+5:30). GA4 data for day `T-2` is fully settled by this time.

**Execution model**:
1. Calculates target date: `today() - 2 days` (to wait for GA4 data finality).
2. Runs paginated `runReport` for each of the three properties for that single date.
3. Lands into `stage.ga4_sessions`, then `MERGE` → `dbo.ga4_sessions`.
4. Also re-fetches `T-1` as a precautionary re-upsert (data may still be settling).

**Expected volume**: 50K–500K rows per property per day for a platform of Myntra's scale.

### 3.3 Realtime Pulse Flow

**Purpose**: Continuous active-user snapshot for the live ops dashboard.

**Trigger**: Runs every 60 seconds via the Streamcraft scheduler.

**Execution model**:
1. Calls `IClientRESTAPISource.StreamRecords` which internally calls `runRealtimeReport` for all three properties concurrently (using goroutines).
2. Records flow into the SQL Server writer as a streaming INSERT (no staging table, no `MERGE` — `realtime_sessions` is append-only with TTL cleanup).
3. After insert, issues `DELETE FROM dbo.realtime_sessions WHERE snapshot_at < DATEADD(HOUR, -2, GETUTCDATE())` to trim stale rows.

**Volume**: ~300 rows per property per snapshot. At 60-second cadence, this produces ~1,500 rows/minute across all three properties — trivial for SQL Server but must stay within the 10,000 realtime tokens/day/property budget (1,440 calls/day per property × 1 token/call = 1,440 tokens — well within budget).

---

## Part 4 — IClientRESTAPISource Implementation

The Go connector package is `client_connector_45_iso_entity_124`. It implements `coreinterface.IClientRESTAPISource` for the GA4 Data API.

### 4.1 Interface Method Mapping

| Interface Method | GA4 Usage | Notes |
|---|---|---|
| `GeneratePaginateRequest` | `runReport` with `offset`/`limit` | Used by Historical Backfill and Incremental Daily flows |
| `GenerateCursorRequest` | Not used for GA4 standard reports | Would be used if GA4 ever adopted cursor tokens |
| `GenerateWebhookRequest` | Not used | GA4 has no push/webhook mechanism |
| `FetchRecords` | Parses `runReport` JSON response body | Extracts `rows[]`, maps dimension/metric headers to key-value maps |
| `NextPageToken` | Encodes next offset as string | Returns `"200000"` after first page if `rowCount > 100000`; returns `("", false)` when exhausted |
| `StreamRecords` | `runRealtimeReport` | Returns a `chan map[string]any` fed by a goroutine calling the Realtime API on a ticker |

### 4.2 `GeneratePaginateRequest` — Parameter Mapping

```
Input (models.RESTAPISourceFetch):
  Property    string   -- "properties/123456789"
  DateFrom    string   -- "2024-06-15"
  DateTo      string   -- "2024-06-15"  (always same day for 1-day chunks)
  PageToken   string   -- "" on first page; "100000", "200000", ... on subsequent

Output (RESTAPISourcePaginateTune):
  Path        = "/v1beta/{property}:runReport"
  Method      = "POST"
  Headers     = {"Content-Type": "application/json", "Authorization": "Bearer {token}"}
  Body        = {
                  "dimensions": [...],
                  "metrics": [...],
                  "dateRanges": [{"startDate": DateFrom, "endDate": DateTo}],
                  "limit": 100000,
                  "offset": parseInt(PageToken) or 0
                }
  RecordsPath = "rows"           -- dot-path into response JSON
  MaxPages    = 0                -- unlimited; engine stops when NextPageToken returns false
```

### 4.3 `FetchRecords` — Response Parsing

GA4's response structure is non-trivial: dimension and metric values are not keyed by name in each row — instead, the response carries a `dimensionHeaders[]` and `metricHeaders[]` array, and each row's `dimensionValues[]` and `metricValues[]` are positionally aligned to those headers.

```json
{
  "dimensionHeaders": [{"name": "date"}, {"name": "sessionId"}, ...],
  "metricHeaders":    [{"name": "sessions", "type": "TYPE_INTEGER"}, ...],
  "rows": [
    {
      "dimensionValues": [{"value": "20240615"}, {"value": "abc123"}, ...],
      "metricValues":    [{"value": "3"}, ...]
    }
  ],
  "rowCount": 87234
}
```

`FetchRecords` re-assembles each row into `map[string]any` by zipping headers with values:

```
{"date": "20240615", "sessionId": "abc123", "sessions": 3, ...}
```

Metric values with `type: TYPE_FLOAT` are parsed as `float64`; all others as `int64` or left as `string` based on the `metricHeaders[i].type` field.

### 4.4 `NextPageToken` — Offset Encoding

```
rowCount extracted from response body and stored in connector state.
currentOffset tracked per call sequence.

NextPageToken(body, headers):
  parse rowCount from body
  nextOffset = currentOffset + 100000
  if nextOffset >= rowCount:
    return ("", false)
  currentOffset = nextOffset
  return (strconv.Itoa(nextOffset), true)
```

The engine passes the returned string back into `GeneratePaginateRequest` as `PageToken` on the next iteration.

### 4.5 `StreamRecords` — Realtime Mode

```
StreamRecords(param):
  out := make(chan map[string]any, 1000)
  go func():
    ticker := time.NewTicker(60 * time.Second)
    for range ticker.C:
      call runRealtimeReport for param.Property
      parse response rows
      for each row: out <- row
    close(out)
  return out
```

The channel buffer of 1,000 prevents the producer goroutine from blocking if the SQL Server writer is momentarily slow.

---

## Part 5 — Transformer Chain

Records flow through the following transformer chain before reaching the SQL Server writer. Each transformer is a stateless function operating on `map[string]any`.

| Transformer | Input | Output | Purpose |
|---|---|---|---|
| `DateParser` | `date: "20240615"` | `report_date: time.Time` | Converts GA4's `YYYYMMDD` string to a typed date |
| `DimensionNormaliser` | property-specific custom dim names | canonical names (`product_category`, `payment_method`, etc.) | Eliminates per-property column name divergence |
| `SurfaceInjector` | pipeline context: `surface=web` | adds `surface: "web"` to record | Injects non-GA4 metadata from pipeline context |
| `PropertyInjector` | pipeline context: `property_id` | adds `property_id: "properties/123456789"` | Needed for the composite PK in SQL Server |
| `NullFiller` | any missing dimensions | sets them to `""` or `0` | SQL Server `NOT NULL` columns cannot accept Go `nil` |
| `MetricTypeCaster` | `"sessions": "3"` (string from GA4) | `"sessions": 3` (int) | GA4 returns all values as strings; SQL Server expects typed data |
| `QuotaThrottle` | token spend counter | passes through or blocks | Enforces hourly quota budget; inserts `time.Sleep` when needed |
| `RunIDStamper` | pipeline run context | adds `pipeline_run_id`, `ingested_at` | Traceability in `dbo.ga4_sessions` |

---

## Part 6 — Schema Design Rationale

### Why staging tables?

SQL Server's `MERGE` statement requires the source to be a table, view, or CTE — it cannot stream from a Go channel. All fetched records are therefore accumulated in `stage.ga4_sessions` (an in-database heap table) before `MERGE` runs. The staging table is truncated at the start of each pipeline run, so it never accumulates stale rows from failed runs.

### Why HEAP for the staging table?

The staging table has no indexes. It is written sequentially (bulk insert) and read once (by `MERGE`). Indexes on the staging table would slow writes and are irrelevant to the `MERGE` since SQL Server will scan it anyway. The clustered and columnstore indexes belong only on `dbo.ga4_sessions`.

### Why `DECIMAL(18,2)` for revenue?

GA4 returns `purchaseRevenue` as a floating-point number in the currency configured for the property (INR for Myntra). `FLOAT` in SQL Server introduces rounding drift across aggregations. `DECIMAL(18,2)` stores up to ₹999,999,999,999,999.99 — sufficient for any realistic revenue value — and arithmetic is exact.

### Why `DATETIME2` over `DATETIME` for timestamps?

`DATETIME2` has 100ns precision and supports the full `0001-01-01` to `9999-12-31` range. `DATETIME` is limited to millisecond precision and a smaller range. GA4 event timestamps are in microseconds — `DATETIME2` preserves sub-millisecond precision for `ingested_at` without lossy rounding.

---

## Part 7 — Rate Limit and Quota Management

GA4 quota is the dominant operational risk. The following strategies are implemented:

| Strategy | Implementation |
|---|---|
| **1-day window chunking** | Each `runReport` covers exactly one calendar day — avoids sampling AND minimises token cost per request |
| **`QuotaThrottle` transformer** | Parses the `X-Quota-Token-Cost` custom response header (injected by connector from GA4's quota feedback) and accumulates spend per property per hour |
| **80% soft limit** | When hourly tokens reach 32,000 (80% of 40K), the pipeline parks the current property's date queue and sleeps until the hour resets |
| **Exponential backoff on 429** | `FetchRecords` detects HTTP 429 / `RESOURCE_EXHAUSTED` and backs off with jitter: `base=5s, multiplier=2, cap=300s` |
| **Sequential property processing** | The three GA4 properties share one GCP service account; running them in parallel would triple token consumption per hour. Properties run sequentially with a 2-minute gap between them |
| **Checkpoint + resume** | `dbo.pipeline_run_log` records the last successfully merged date per property. On restart the engine skips already-completed dates |

---

## Part 8 — Seeder Design

The test seeder generates synthetic GA4 API responses to allow full integration testing without a live GA4 property.

### 8.1 Seeder Architecture

The seeder is a Go HTTP server that mimics the GA4 Data API (`POST :runReport`) and Realtime API (`POST :runRealtimeReport`). It returns deterministic paginated JSON responses from pre-seeded in-memory state.

```
seeder/
  cmd/seeder/main.go       -- starts HTTP server on :11333
  handlers/report.go       -- handles /v1beta/{property}:runReport
  handlers/realtime.go     -- handles /v1beta/{property}:runRealtimeReport
  generators/sessions.go   -- generates synthetic session rows
  generators/realtime.go   -- generates synthetic realtime rows
  state/quota.go           -- tracks and enforces quota limits in test mode
```

### 8.2 Synthetic Data Parameters

| Parameter | Value |
|---|---|
| Properties seeded | 3 (`properties/123456789`, `properties/987654321`, `properties/567891234`) |
| Days of data | 730 (2 years) |
| Sessions per day per property | 10,000–250,000 (random, log-normal distribution) |
| Conversion rate | 2.5% ± 0.5% per day |
| Revenue per conversion (INR) | ₹800–₹12,000 (uniform) |
| Pagination enforced | Hard limit of 100,000 rows per response |
| Quota enforcement | Returns HTTP 429 after 40,000 simulated tokens/hour per property |
| Custom dimensions | Per-property name variations (see §1.4) |

### 8.3 Seeder Startup Sequence

```
1. Reads DATE_FROM / DATE_END from environment.
2. Generates all session rows in memory (or streams from disk for large runs).
3. Starts HTTP server on :11333.
4. Listens for POST requests; routes by endpoint.
5. Returns paginated responses with correct GA4 JSON shape.
6. Shuts down after SIGTERM (triggered by `make seed-stop`).
```

---

## Part 9 — Makefile Targets

```makefile
make up           # Start seeder + SQL Server containers via docker-compose
make down         # Stop and remove containers
make seed         # Run the seeder (populates in-memory GA4 data)
make seed-stop    # Terminate seeder
make migrate      # Apply SQL Server schema (create dbo/stage tables and indexes)
make backfill     # Run Historical Backfill Flow (all 3 properties, 730 days)
make daily        # Run Incremental Daily Flow (T-2 for all 3 properties)
make realtime     # Run Realtime Pulse Flow (single snapshot, for testing)
make verify       # Run SQL queries to assert row counts and no duplicates
make clean        # Drop stage.* tables and truncate dbo.* tables
make test         # Run Go unit tests for connector + transformer chain
```

---

## Part 10 — Open Design Questions

The following are intentional scope boundaries for this case study — deferred to future iterations:

1. **GA4 BigQuery Export vs. Data API**: Large GA4 properties often use the BigQuery export instead of the Data API for bulk access (no quotas, full event-level data). This case deliberately uses the Data API to exercise the `IClientRESTAPISource` pagination and quota machinery. A future case could model the BigQuery export path using a `IClientBigQuerySource`.

2. **SQL Server `BULK INSERT` vs. row-by-row insert**: This case uses the Streamcraft SQL Server writer which batches rows into `INSERT ... VALUES (...), (...), (...)` with a batch size of 1,000 rows. A more advanced implementation would use the `go-mssqldb` bulk copy API for columnar bulk load (≈10× faster for very large staging loads). Deferred as a performance optimisation case.

3. **GA4 property-level data deletion (GDPR)**: If a user exercises their right to erasure, GA4 deletes their events. The pipeline does not currently propagate deletions to SQL Server. A production system would need to periodically reconcile `user_pseudo_id` against a deletions feed and issue `DELETE` statements in `dbo.ga4_sessions`.

4. **Multi-currency support**: Myntra operates in INR only. If the model were extended to an international property (USD, EUR), `purchase_revenue_inr` would need to be either renamed or augmented with `currency_code` + a separate `purchase_revenue_local` column.

5. **Schema evolution**: GA4 custom dimensions are registered per property and can change over time. The `DimensionNormaliser` transformer currently uses a static map. A production-grade implementation would query the GA4 Admin API at pipeline startup to discover current custom dimension definitions and build the normalisation map dynamically.

---

## Part 11 — Step-by-Step Implementation Tasks

### Phase 1 — Infrastructure Setup

- [ ] **STEP-01** — Write `docker-compose.yml` with 2 services: SQL Server (`mcr.microsoft.com/mssql/server:2022-latest`, port `1433`) and the GA4 seeder HTTP server (built from `Dockerfile.seeder`, port `11333`). Pass `SA_PASSWORD`, `ACCEPT_EULA`, and `DATE_FROM`/`DATE_END` env vars. Add a healthcheck on SQL Server (`/opt/mssql-tools18/bin/sqlcmd -Q "SELECT 1"`) so dependent services wait for it to be ready.
- [ ] **STEP-02** — Implement `cmd/sql_schema/main.go` — connects to SQL Server using the `etl_writer` service account and creates all schemas and tables defined in §2: `dbo.ga4_sessions` (with clustered index on `(property_id, report_date)`, non-clustered index on `(source, medium, campaign)`, and columnstore index on `(report_date, property_id, conversions, purchase_revenue_inr)`), `stage.ga4_sessions` (HEAP, no indexes), `dbo.realtime_sessions`, `dbo.pipeline_run_log`, and `dbo.pipeline_backlog` (for failed records). Idempotent: uses `IF NOT EXISTS` guards on all `CREATE TABLE` and `CREATE INDEX` statements.
- [ ] **STEP-03** — Implement `internal/config/config.go` — SQL Server DSN builder (server, port, database, username, password), GA4 seeder base URL (`http://localhost:11333`), and constants for the three property IDs and surface labels (`myntra-web/web`, `myntra-android/android`, `myntra-ios/ios`). Reads values from environment variables with documented fallback defaults for local development.
- [ ] **STEP-04** — Implement `internal/properties/properties.go` — defines the three GA4 property structs including property ID, surface label, primary conversion event name, and the per-property custom dimension name map (§1.4). This is the single source of truth referenced by both the connector and the `DimensionNormaliser` transformer.

### Phase 2 — Data Seeder

- [ ] **STEP-05** — Implement `seeder/cmd/seeder/main.go` — starts an HTTP server on `:11333`. Reads `DATE_FROM` / `DATE_END` from environment, generates all synthetic session data in memory (or streams from disk for runs exceeding 500K rows per property), then begins listening. Handles `SIGTERM` for graceful shutdown.
- [ ] **STEP-06** — Implement `seeder/generators/sessions.go` — generates synthetic session rows for all three properties and all requested dates. Volume: 10,000–250,000 rows/day/property drawn from a log-normal distribution. Each row includes all core dimensions (§1.2), property-specific custom dimensions using the correct per-property names (e.g., `customEvent:product_category` for web, `customEvent:category_slug` for android, `customEvent:item_category` for iOS), and all 10 core metrics (§1.5). Conversion rate: 2.5% ± 0.5%. Revenue per conversion: ₹800–₹12,000 uniform.
- [ ] **STEP-07** — Implement `seeder/handlers/report.go` — handles `POST /v1beta/{property}:runReport`. Parses `offset` and `limit` from the request body, returns a correctly shaped GA4 JSON response (§4.3) with `dimensionHeaders`, `metricHeaders`, `rows` (positional value arrays), and `rowCount`. Enforces the 100,000-row hard limit per response. Returns HTTP 429 with a `RESOURCE_EXHAUSTED` body after 40,000 simulated tokens/hour per property (enforced via `seeder/state/quota.go`).
- [ ] **STEP-08** — Implement `seeder/handlers/realtime.go` — handles `POST /v1beta/{property}:runRealtimeReport`. Returns a snapshot of ~300 active-user rows per property, varying slightly on each call to simulate live fluctuation. No pagination. Respects the 10,000 realtime tokens/day/property budget tracked in `seeder/state/quota.go`.
- [ ] **STEP-09** — Implement `seeder/state/quota.go` — tracks token spend per property per hour (standard) and per day (realtime) in thread-safe maps (`sync.Mutex` protected). Resets hourly buckets on the hour boundary. Exposes `ConsumeTokens(property string, n int) error` — returns an error when the budget is exceeded, causing the respective handler to respond with HTTP 429.
- [ ] **STEP-10** — Inject intentional data quality issues into the seeder: ~2% rows with `sessionId = ""` (empty session ID — tests `NullFiller` and composite PK handling); ~3% rows where `purchaseRevenue` is the string `"(not set)"` instead of a numeric string (tests `MetricTypeCaster` backlog path); ~1% rows per property with an unrecognised custom dimension key (tests `DimensionNormaliser` fallback); ~0.5% rows with `date = "00000000"` (invalid date string — tests `DateParser` error path).

### Phase 3 — GA4 Source Connector

- [ ] **STEP-11** — Scaffold the connector package `client_connector_45_iso_entity_124` and define its internal state struct: `baseURL`, `property`, `surface`, `rowCount` (populated by `FetchRecords`, consumed by `NextPageToken`), `currentOffset`, and `quotaTokensSpent`. Implement bearer token attachment (static token for local seeder; OAuth2 service account for production GA4).
- [ ] **STEP-12** — Implement `GeneratePaginateRequest` (§4.2) — constructs the `POST /v1beta/{property}:runReport` HTTP request with `Content-Type: application/json` and `Authorization: Bearer {token}` headers. JSON body includes all dimensions (§1.2), all metrics (§1.5), `dateRanges` set to the single-day window from `RESTAPISourceFetch`, `limit: 100000`, and `offset` parsed from `PageToken` (defaults to `0` on first call). Sets `RecordsPath = "rows"` and `MaxPages = 0`.
- [ ] **STEP-13** — Implement `FetchRecords` (§4.3) — zips `dimensionHeaders` and `metricHeaders` arrays with the positional `dimensionValues` and `metricValues` arrays in each row to reconstruct `map[string]any` records. Parses metric values to `int64` for `TYPE_INTEGER` and `float64` for `TYPE_FLOAT` based on the `metricHeaders[i].type` field. Stores `rowCount` from the response into connector state for use by `NextPageToken`. Injects the response's quota cost (from `X-Quota-Token-Cost` header or estimated) as a `_quota_cost` field on each record for `QuotaThrottle` to consume.
- [ ] **STEP-14** — Implement `NextPageToken` (§4.4) — uses `rowCount` stored in connector state and `currentOffset` to compute the next offset. Returns `(strconv.Itoa(nextOffset), true)` when more pages remain; returns `("", false)` when `currentOffset + 100000 >= rowCount`. Increments `currentOffset` after each successful call.
- [ ] **STEP-15** — Implement `StreamRecords` (§4.5) — launches a goroutine containing a 60-second `time.Ticker` that calls `runRealtimeReport` for the configured property, parses each response row into `map[string]any`, and sends it to a buffered output channel (capacity 1,000). Returns the channel immediately. Goroutine exits cleanly on context cancellation, closing the channel.
- [ ] **STEP-16** — Stub out unused interface methods: `GenerateCursorRequest` and `GenerateWebhookRequest` both panic with `"GA4 connector does not support cursor/webhook mode"`. Prevents the engine from silently invoking an unimplemented path.

### Phase 4 — SQL Server Destination Connector

- [ ] **STEP-17** — Implement `connector_dest_sqlserver` — opens a `go-mssqldb` connection pool (5 max open, 2 idle). Exposes `TruncateStage() error` and `BulkInsertToStage(rows []map[string]any) error` (batches rows into `INSERT INTO stage.ga4_sessions ... VALUES (...),(...),...` with a batch size of 1,000 rows per statement).
- [ ] **STEP-18** — Implement `ExecuteMerge() error` on the destination connector — issues the full `MERGE dbo.ga4_sessions AS target USING stage.ga4_sessions AS source` statement (§2.3) inside a single SQL Server transaction. Captures and returns rows-affected count (inserted + updated separately) for logging to `dbo.pipeline_run_log`.
- [ ] **STEP-19** — Implement the realtime destination path — `InsertRealtimeRows(rows []map[string]any) error` performs a direct `INSERT INTO dbo.realtime_sessions` (no staging table, no MERGE). After each insert batch, issues `DELETE FROM dbo.realtime_sessions WHERE snapshot_at < DATEADD(HOUR, -2, GETUTCDATE())` to enforce the 2-hour TTL retention policy.
- [ ] **STEP-20** — Implement `WritePipelineRunLog(entry RunLogEntry) error` — upserts a row in `dbo.pipeline_run_log` with run metadata: `run_id`, `pipeline_name`, `property_id`, `surface`, `started_at`, `finished_at`, `rows_fetched`, `rows_merged`, `quota_tokens_spent`, `status`, `error_message`.

### Phase 5 — Transformer Chain

- [ ] **STEP-21** — Implement `DateParser` transformer — converts GA4's `date` dimension value (`"20240615"`) to a `time.Time` using `time.Parse("20060102", v)`. Stores result as `report_date`. Routes records with unparseable date strings (e.g., `"00000000"`) to backlog with error code `INVALID_DATE`.
- [ ] **STEP-22** — Implement `DimensionNormaliser` transformer — reads the per-property custom dimension name map from `internal/properties/properties.go` keyed by `property_id`. Renames source fields to canonical names: `product_category`, `payment_method`, `wishlist_flag`, `app_version`, `os_version`. Drops the original per-property key after renaming. If a property-specific key is absent in the record (e.g., iOS has no `wishlist_flag`), sets the canonical field to `""` rather than routing to backlog.
- [ ] **STEP-23** — Implement `SurfaceInjector` transformer — reads `surface` string from `TransformerProps.State` (set by pipeline context to `web`, `android`, or `ios`) and stamps it on the record. Pure pass-through enrichment with no error path.
- [ ] **STEP-24** — Implement `PropertyInjector` transformer — reads `property_id` from `TransformerProps.State` and stamps it on the record. Required for the composite primary key `(property_id, report_date, session_id)` in `dbo.ga4_sessions`.
- [ ] **STEP-25** — Implement `NullFiller` transformer — iterates over all columns defined in the `dbo.ga4_sessions` schema. For `NOT NULL VARCHAR`/`CHAR` columns: if the field is absent or `nil`, sets it to `""`. For `NOT NULL INT`/`DECIMAL` columns: sets missing values to `0`. Prevents SQL Server insert failures on `NOT NULL` constraint violations.
- [ ] **STEP-26** — Implement `MetricTypeCaster` transformer — GA4 returns all metric values as strings (e.g., `"sessions": "3"`). Parses string → `int64` for integer metrics (`sessions`, `engaged_sessions`, `total_users`, `new_users`, `conversions`, `event_count`, `screen_page_views`) and string → `float64` for float metrics (`bounce_rate`, `avg_session_duration_secs`, `purchase_revenue_inr`). Routes records where parsing fails (e.g., `"(not set)"`) to backlog with error code `METRIC_PARSE_FAILURE`.
- [ ] **STEP-27** — Implement `QuotaThrottle` transformer — maintains an in-memory `map[propertyID]int` of tokens spent in the current hour window. Reads the `_quota_cost` field injected by `FetchRecords` and accumulates it per property. When hourly spend for a property exceeds 32,000 tokens (80% of the 40K hourly limit), calls `time.Sleep` until the next hour boundary before passing the record through. Strips `_quota_cost` from the record before forwarding. Blocks rather than rejects — this is a throttle, not a backlog route.
- [ ] **STEP-28** — Implement `RunIDStamper` transformer — stamps `pipeline_run_id` (from pipeline context) and `ingested_at` (`time.Now().UTC()`) on every record. Both fields are written to `dbo.ga4_sessions` and updated on each upsert via the MERGE statement.

### Phase 6 — Pipeline Collections

- [ ] **STEP-29** — Implement the **Historical Backfill Flow** driver (`cmd/backfill/main.go`) — accepts `--property` and `--date-from`/`--date-to` flags. Generates the list of calendar days in the range and enqueues them as pipeline jobs. Runs up to 3 dates concurrently per property (§3.1) using a worker pool. Before processing each date, checks `dbo.pipeline_run_log` for an existing completed row for that `(property_id, date)` and skips if found. Processes the three properties sequentially with a 2-minute gap between them to avoid cross-property quota collisions on the same GCP service account.
- [ ] **STEP-30** — Wire the Historical Backfill pipeline: source = `client_connector_45_iso_entity_124` in paginate mode, transformer chain = `DateParser` → `DimensionNormaliser` → `SurfaceInjector` → `PropertyInjector` → `NullFiller` → `MetricTypeCaster` → `QuotaThrottle` → `RunIDStamper`, destination = `connector_dest_sqlserver` (truncate stage → bulk insert → MERGE). Write run metadata to `dbo.pipeline_run_log` on start and on completion.
- [ ] **STEP-31** — Implement the **Incremental Daily Flow** driver (`cmd/daily/main.go`) — calculates target dates `T-2` (primary, fully settled) and `T-1` (precautionary re-upsert). Runs the paginated pipeline for all three properties in sequence for both dates using the same pipeline wiring as the backfill flow. Designed to be invoked by a cron scheduler at 06:00 IST daily.
- [ ] **STEP-32** — Implement the **Realtime Pulse Flow** driver (`cmd/realtime/main.go`) — calls `StreamRecords` on `client_connector_45_iso_entity_124` for all three properties concurrently (one goroutine per property). Feeds rows through a trimmed transformer chain (`SurfaceInjector` → `PropertyInjector` → `NullFiller` → `RunIDStamper` only — no date parsing, no dimension normalisation, no quota throttle). Writes rows to `dbo.realtime_sessions` via the realtime destination path. Designed to be invoked every 60 seconds by the Streamcraft scheduler.

### Phase 7 — Pipeline Control Plane

- [ ] **STEP-33** — Implement checkpoint/resume for the Backfill Flow — on each successfully merged date, write a row to `dbo.pipeline_run_log` with `status = 'COMPLETED'` and `report_date` captured in `pipeline_name` metadata. On restart, the backfill driver queries completed dates for the property and excludes them from the work queue.
- [ ] **STEP-34** — Implement backlog handling — records that fail transformer processing (invalid date, metric parse failure) are captured in a per-run `[]map[string]any` slice. At the end of each pipeline run, write accumulated backlog records to `dbo.pipeline_backlog` with columns: `run_id`, `property_id`, `report_date`, `error_code`, `error_message`, `raw_record` (JSON), `created_at`.
- [ ] **STEP-35** — Implement `TerminateRule` evaluation — checked at the start of each date-batch in the backfill/daily flows and each tick in the realtime flow. Rules: `ERROR_RATE_BREACH` (backlog rate > 10% of batch records → stop), `SOURCE_UNREACHABLE` (HTTP 5xx from GA4/seeder after 3 retries → stop), `QUOTA_EXHAUSTED` (HTTP 429 with no retry budget remaining → stop), `IDLE_TIMEOUT` (zero rows returned for a property across 3 consecutive date requests → stop), `MANUAL_KILL` (`FORCE_STOP=true` environment flag set → graceful stop at next checkpoint). Store rules and thresholds in `internal/control/terminate.go`.
- [ ] **STEP-36** — Implement exponential backoff on HTTP 429 in `FetchRecords` — base delay 5s, multiplier 2×, cap 300s, with ±20% jitter. Logs each retry with property ID, current offset, and remaining backoff duration. After 5 consecutive 429 responses with no recovery, returns an error to the engine to trigger `QUOTA_EXHAUSTED` termination.

### Phase 8 — Observability

- [ ] **STEP-37** — Implement `cmd/metrics_watcher/main.go` — polls SQL Server every N seconds and prints a live terminal dashboard. Sections: (1) recent pipeline runs — name, property, status, rows fetched/merged, quota tokens spent; (2) backfill progress — completed dates per property vs total date range; (3) `dbo.ga4_sessions` row count by `(property_id, surface)`; (4) realtime sessions — current row count and oldest/newest `snapshot_at`; (5) backlog summary — count by `error_code`. Configurable via `--sqlserver` and `--interval` flags.
- [ ] **STEP-38** — Add structured logging at key lifecycle events: pagination page fetched (property, date, offset, rows returned, quota tokens spent), MERGE completed (rows inserted, rows updated), `QuotaThrottle` sleep triggered (property, tokens spent, sleep duration), backlog record written (property, error code), realtime snapshot written (property, rows inserted, rows TTL-deleted), TerminateRule fired (rule name, threshold, observed value), checkpoint saved (property, date).

### Phase 9 — End-to-End Test Run

- [ ] **STEP-39** — Run `make up` to start SQL Server and the seeder. Run `make migrate` to apply the schema. Verify all five tables exist in `analytics_warehouse` (`dbo.ga4_sessions`, `stage.ga4_sessions`, `dbo.realtime_sessions`, `dbo.pipeline_run_log`, `dbo.pipeline_backlog`) with correct column definitions and indexes.
- [ ] **STEP-40** — Run seeder smoke test: send a manual `POST /v1beta/properties/123456789:runReport` with a 1-day window and `offset=0`. Verify the response contains `dimensionHeaders`, `metricHeaders`, `rows`, and a valid `rowCount`. Verify pagination by fetching with `offset=100000` for a high-volume day (> 100K rows) and confirming a partial second page is returned.
- [ ] **STEP-41** — Run quota exhaustion test: send 401 consecutive `runReport` requests for the same property in the same simulated hour window. Verify HTTP 429 is returned after 40,000 simulated tokens and that the connector's backoff logic fires correctly.
- [ ] **STEP-42** — Run backfill smoke test: single property (`myntra-web`), 3-day window only. Verify `dbo.pipeline_run_log` shows 3 completed rows, `dbo.ga4_sessions` contains rows for all 3 dates with correct `property_id` and `surface` values, and `stage.ga4_sessions` is empty (truncated post-MERGE).
- [ ] **STEP-43** — Verify intentional bad records from seeder: confirm `dbo.pipeline_backlog` contains rows with `error_code = 'INVALID_DATE'` and `error_code = 'METRIC_PARSE_FAILURE'` at approximately the injected rates from STEP-10.
- [ ] **STEP-44** — Test checkpoint/resume: run the backfill for `myntra-web` for 7 days. Kill the process after 4 dates complete. Restart. Verify the run skips the 4 already-completed dates and processes only the remaining 3 without duplicating rows in `dbo.ga4_sessions`.
- [ ] **STEP-45** — Test MERGE idempotency: run the daily flow for `T-2` twice in succession. Verify that the row count in `dbo.ga4_sessions` does not increase on the second run (rows are updated via MERGE, not duplicated).
- [ ] **STEP-46** — Run realtime smoke test: invoke `make realtime` 3 times in quick succession. Verify `dbo.realtime_sessions` accumulates rows across runs and that the TTL DELETE correctly removes rows older than 2 hours.
- [ ] **STEP-47** — Run full backfill: all 3 properties, 730 days. Monitor via `make watch`. Verify final row counts in `dbo.ga4_sessions` per property against seeder-generated totals (allowing for intentional backlog records). Confirm counts match within ±1%.
