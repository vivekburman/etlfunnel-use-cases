package client_connector_45_iso_entity_124

// GA4 Data API source connector for the Myntra analytics ETL.
// Implements IClientRESTAPISource against analyticsdata.googleapis.com/v1beta
// (or the local seeder at SEEDER_URL).
//
// Pagination: offset-based (no cursor token). NextPageToken encodes the next
// offset as a decimal string; the engine passes it back via PageToken on the
// following GeneratePaginateRequest call.
//
// Quota tracking: after each FetchRecords call the connector increments the
// package-level GlobalQuota (userlibrary_1). The QuotaThrottle transformer
// (transformer_7) reads from the same tracker and sleeps when the hourly
// budget reaches 80%.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/core/coreinterface"
	"etlfunnel/execution/models"
)

const realtimePollInterval = 60 * time.Second

var realtimeDimensions = []string{
	"city", "deviceCategory", "pagePath", "eventName",
}

var realtimeMetrics = []string{
	"activeUsers",
}

func streamRealtimeLoop(
	ctx context.Context,
	out chan<- map[string]any,
	properties []string,
	authToken string,
	baseURL string,
	stopCh <-chan struct{},
) {
	ticker := time.NewTicker(realtimePollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-stopCh:
			return
		case <-ticker.C:
			for _, prop := range properties {
				rows, err := fetchRealtimeRows(ctx, baseURL, prop, authToken)
				if err != nil {
					log.Printf("[realtime] property %s: %v", prop, err)
					continue
				}
				for _, row := range rows {
					row["_property_id"] = prop
					select {
					case out <- row:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}
}

func fetchRealtimeRows(ctx context.Context, baseURL, property, authToken string) ([]map[string]any, error) {
	dims := make([]map[string]string, 0, len(realtimeDimensions))
	for _, d := range realtimeDimensions {
		dims = append(dims, map[string]string{"name": d})
	}
	mets := make([]map[string]string, 0, len(realtimeMetrics))
	for _, m := range realtimeMetrics {
		mets = append(mets, map[string]string{"name": m})
	}

	body := map[string]any{
		"dimensions": dims,
		"metrics":    mets,
	}
	bodyBytes, _ := json.Marshal(body)

	url := fmt.Sprintf("%s/v1beta/%s:runRealtimeReport", baseURL, property)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+authToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
	}

	var ga4Resp ga4Response
	if err := json.NewDecoder(resp.Body).Decode(&ga4Resp); err != nil {
		return nil, fmt.Errorf("decode realtime response: %w", err)
	}

	rows := make([]map[string]any, 0, len(ga4Resp.Rows))
	for _, row := range ga4Resp.Rows {
		rec := make(map[string]any, len(ga4Resp.DimensionHeaders)+len(ga4Resp.MetricHeaders))
		for i, dh := range ga4Resp.DimensionHeaders {
			if i < len(row.DimensionValues) {
				rec[dh.Name] = row.DimensionValues[i].Value
			}
		}
		for i, mh := range ga4Resp.MetricHeaders {
			if i < len(row.MetricValues) {
				rec[mh.Name] = parseMetricValue(row.MetricValues[i].Value, mh.Type)
			}
		}
		rows = append(rows, rec)
	}
	return rows, nil
}

const (
	maxRowsPerRequest = 100_000
	minTokenPerReq    = 1
)

// coreDimensions are requested for every property.
var coreDimensions = []string{
	"date", "sessionId", "userPseudoId", "deviceCategory",
	"city", "country", "sessionSource", "sessionMedium", "sessionCampaignName",
}

// surfaceDimensions are the property-specific custom dimensions, keyed by surface.
var surfaceDimensions = map[string][]string{
	"web": {
		"customEvent:product_category",
		"customEvent:wishlisted",
		"customEvent:payment_method",
	},
	"android": {
		"customEvent:category_slug",
		"customEvent:is_wishlisted",
		"customEvent:payment_type",
		"appVersion",
		"operatingSystemVersion",
	},
	"ios": {
		"customEvent:item_category",
		"customEvent:pay_method",
		"appVersion",
		"operatingSystemVersion",
	},
}

var coreMetrics = []string{
	"sessions", "engagedSessions", "totalUsers", "newUsers",
	"bounceRate", "averageSessionDuration", "conversions",
	"purchaseRevenue", "eventCount", "screenPageViews",
}

// ga4Response is the shape of a runReport or runRealtimeReport response.
type ga4Response struct {
	DimensionHeaders []struct {
		Name string `json:"name"`
	} `json:"dimensionHeaders"`
	MetricHeaders []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"metricHeaders"`
	Rows []struct {
		DimensionValues []struct{ Value string `json:"value"` } `json:"dimensionValues"`
		MetricValues    []struct{ Value string `json:"value"` } `json:"metricValues"`
	} `json:"rows"`
	RowCount int64 `json:"rowCount"`
}

type IUseConnector struct {
	mu              sync.Mutex
	rowCount        int64
	currentOffset   int64
	currentProperty string
	currentSurface  string
	requestBody     map[string]any
}

var _ coreinterface.IClientRESTAPISource = (*IUseConnector)(nil)

func (c *IUseConnector) GeneratePaginateRequest(param *models.RESTAPISourceFetch) (*models.RESTAPISourcePaginateTune, error) {
	rp := param.State.GetReplicaProps()

	property, _ := rp["property_id"].(string)
	surface, _ := rp["surface"].(string)
	dateFrom, _ := rp["date_from"].(string)
	dateTo, _ := rp["date_to"].(string)
	authToken, _ := rp["auth_token"].(string)

	if property == "" {
		return nil, fmt.Errorf("replica prop 'property_id' is required")
	}
	if dateFrom == "" || dateTo == "" {
		return nil, fmt.Errorf("replica props 'date_from' and 'date_to' are required")
	}
	if authToken == "" {
		return nil, fmt.Errorf("replica prop 'auth_token' is required")
	}

	c.mu.Lock()
	c.currentOffset = 0
	c.rowCount = 0
	c.currentProperty = property
	c.currentSurface = surface
	c.mu.Unlock()

	dims := buildDimensions(surface)
	metrics := buildMetrics()

	body := map[string]any{
		"dimensions": dims,
		"metrics":    metrics,
		"dateRanges": []map[string]string{
			{"startDate": dateFrom, "endDate": dateTo},
		},
		"limit":  maxRowsPerRequest,
		"offset": int64(0),
	}

	c.mu.Lock()
	c.requestBody = body
	c.mu.Unlock()

	return &models.RESTAPISourcePaginateTune{
		Path:   fmt.Sprintf("/v1beta/%s:runReport", property),
		Method: "POST",
		Headers: map[string]string{
			"Content-Type":  "application/json",
			"Authorization": "Bearer " + authToken,
		},
		Body:        body,
		RecordsPath: "rows",
		MaxPages:    0,
	}, nil
}

func (c *IUseConnector) FetchRecords(responseBody []byte, headers http.Header) ([]map[string]any, error) {
	var resp ga4Response
	if err := json.Unmarshal(responseBody, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal GA4 response: %w", err)
	}

	c.mu.Lock()
	c.rowCount = resp.RowCount
	property := c.currentProperty
	c.mu.Unlock()

	records := make([]map[string]any, 0, len(resp.Rows))
	for _, row := range resp.Rows {
		rec := make(map[string]any, len(resp.DimensionHeaders)+len(resp.MetricHeaders))

		for i, dh := range resp.DimensionHeaders {
			if i < len(row.DimensionValues) {
				rec[dh.Name] = row.DimensionValues[i].Value
			}
		}

		for i, mh := range resp.MetricHeaders {
			if i < len(row.MetricValues) {
				raw := row.MetricValues[i].Value
				rec[mh.Name] = parseMetricValue(raw, mh.Type)
			}
		}

		records = append(records, rec)
	}

	// Estimate token cost: 1 token per 1,000 rows, minimum 1.
	tokenCost := int64(len(records)/1000) + minTokenPerReq
	ulib.GlobalQuota.Consume(property, tokenCost)

	return records, nil
}

func (c *IUseConnector) NextPageToken(responseBody []byte, _ http.Header) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// rowCount was set in FetchRecords for this same response body.
	// Re-parse here as the engine may call NextPageToken independently.
	var resp struct {
		RowCount int64 `json:"rowCount"`
	}
	if err := json.Unmarshal(responseBody, &resp); err != nil {
		log.Printf("[ga4] NextPageToken: parse rowCount: %v", err)
		return "", false
	}
	if resp.RowCount > 0 {
		c.rowCount = resp.RowCount
	}

	nextOffset := c.currentOffset + maxRowsPerRequest
	if nextOffset >= c.rowCount {
		c.currentOffset = 0
		c.requestBody = nil
		return "", false
	}

	c.currentOffset = nextOffset
	if c.requestBody != nil {
		c.requestBody["offset"] = nextOffset
	}
	return strconv.FormatInt(nextOffset, 10), true
}

func (c *IUseConnector) StreamRecords(param *models.RESTAPISourceFetch) <-chan map[string]any {
	rp := param.State.GetReplicaProps()

	properties, _ := rp["properties"].([]string)
	if len(properties) == 0 {
		ch := make(chan map[string]any)
		close(ch)
		return ch
	}

	authToken, _ := rp["auth_token"].(string)
	baseURL, _ := rp["base_url"].(string)

	if authToken == "" || baseURL == "" {
		ch := make(chan map[string]any)
		close(ch)
		return ch
	}

	out := make(chan map[string]any, 1000)
	stopCh := make(chan struct{})

	go func() {
		defer close(out)
		streamRealtimeLoop(context.Background(), out, properties, authToken, baseURL, stopCh)
	}()

	return out
}

func (c *IUseConnector) GenerateCursorRequest(_ *models.RESTAPISourceFetch) (*models.RESTAPISourceCursorTune, error) {
	// GA4 Data API uses offset-based pagination, not cursor tokens.
	return nil, fmt.Errorf("cursor pagination not supported by GA4 Data API; use GeneratePaginateRequest")
}

func (c *IUseConnector) GenerateWebhookRequest(_ *models.RESTAPISourceFetch) (*models.RESTAPISourceWebhookTune, error) {
	// GA4 has no push/webhook mechanism.
	return nil, fmt.Errorf("webhook mode not supported by GA4 Data API")
}

// ── helpers ────────────────────────────────────────────────────────────────

func buildDimensions(surface string) []map[string]string {
	dims := make([]map[string]string, 0, len(coreDimensions)+5)
	for _, d := range coreDimensions {
		dims = append(dims, map[string]string{"name": d})
	}
	for _, d := range surfaceDimensions[surface] {
		dims = append(dims, map[string]string{"name": d})
	}
	return dims
}

func buildMetrics() []map[string]string {
	metrics := make([]map[string]string, 0, len(coreMetrics))
	for _, m := range coreMetrics {
		metrics = append(metrics, map[string]string{"name": m})
	}
	return metrics
}

func parseMetricValue(raw, metricType string) any {
	switch metricType {
	case "TYPE_FLOAT", "TYPE_CURRENCY", "TYPE_SECONDS", "TYPE_MILLISECONDS":
		if f, err := strconv.ParseFloat(raw, 64); err == nil {
			return f
		}
	case "TYPE_INTEGER":
		if i, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return i
		}
	}
	return raw
}
