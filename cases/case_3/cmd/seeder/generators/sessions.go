package generators

// sessions.go — deterministic synthetic GA4 session row generator.
//
// For a given (property, date) pair, the generator produces a fixed number of
// session rows with realistic-looking dimension and metric values.  The output
// is deterministic: same inputs → same rows, so pagination across multiple
// requests for the same date window returns consistent data.

import (
	"fmt"
	"hash/fnv"
	"math"
	"time"
)

// Session holds one synthetic GA4 session record, pre-flattened to the GA4
// runReport response shape (positional dimension + metric values as strings).
type Session struct {
	DimensionValues []string
	MetricValues    []string
}

// coreDimensions and surfaceDimensions match the connector's request shape
// exactly so the seeder returns headers that the connector can parse.
var coreDimensions = []string{
	"date", "sessionId", "userPseudoId", "deviceCategory",
	"city", "country", "sessionSource", "sessionMedium", "sessionCampaignName",
}

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

// metricTypes matches GA4's metricHeaders[i].type for the coreMetrics slice.
var metricTypes = []string{
	"TYPE_INTEGER", "TYPE_INTEGER", "TYPE_INTEGER", "TYPE_INTEGER",
	"TYPE_FLOAT", "TYPE_FLOAT", "TYPE_INTEGER",
	"TYPE_CURRENCY", "TYPE_INTEGER", "TYPE_INTEGER",
}

var cities = []string{
	"Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Chennai",
	"Kolkata", "Pune", "Ahmedabad", "Jaipur", "Lucknow",
}

var devices = []string{"mobile", "desktop", "tablet"}
var sources = []string{"google", "facebook", "instagram", "direct", "email", "referral"}
var mediums = []string{"organic", "cpc", "social", "email", "(none)"}
var campaigns = []string{"summer_sale", "diwali_fest", "new_year", "(not set)", "app_install", "reactivation"}
var categories = []string{"Clothing", "Footwear", "Accessories", "Beauty", "Sports", "Electronics"}
var paymentMethods = []string{"credit_card", "debit_card", "upi", "net_banking", "wallet", "cod"}
var appVersions = []string{"8.2.1", "8.3.0", "8.4.2", "9.0.0", "9.1.1"}
var osVersions = map[string][]string{
	"android": {"12", "13", "14"},
	"ios":     {"16.0", "16.5", "17.0", "17.2"},
}

// Dimensions returns the full ordered list of dimension names for a given surface,
// matching what the connector requests.
func Dimensions(surface string) []string {
	dims := make([]string, len(coreDimensions))
	copy(dims, coreDimensions)
	return append(dims, surfaceDimensions[surface]...)
}

// Metrics returns the ordered metric name and type slices.
func Metrics() (names, types []string) {
	return coreMetrics, metricTypes
}

// GenerateSessions returns up to rowsPerDay synthetic sessions for (property, surface, date).
// The slice is ordered deterministically so paginated sub-slices are stable.
func GenerateSessions(property, surface, date string, rowsPerDay int) []Session {
	sessions := make([]Session, rowsPerDay)
	for i := 0; i < rowsPerDay; i++ {
		sessions[i] = generateSession(property, surface, date, i)
	}
	return sessions
}

func generateSession(property, surface, date string, idx int) Session {
	seed := hash(fmt.Sprintf("%s:%s:%s:%d", property, surface, date, idx))
	r := newRand(seed)

	// Parse date for the "date" dimension value (GA4 format: YYYYMMDD)
	t, _ := time.Parse("2006-01-02", date)
	ga4Date := t.Format("20060102")

	sessionID := fmt.Sprintf("session_%x", hash(fmt.Sprintf("%s:%s:%s:%d:sid", property, surface, date, idx)))
	userID := fmt.Sprintf("user_%x", hash(fmt.Sprintf("%s:%d", property, r.Intn(500_000))))

	city := cities[r.Intn(len(cities))]
	country := "IN"
	device := devices[r.Intn(len(devices))]
	src := sources[r.Intn(len(sources))]
	medium := mediums[r.Intn(len(mediums))]
	campaign := campaigns[r.Intn(len(campaigns))]

	// Core dimension values
	coreDimVals := []string{
		ga4Date, sessionID, userID, device,
		city, country, src, medium, campaign,
	}

	// Surface-specific dimension values
	var surfaceDimVals []string
	cat := categories[r.Intn(len(categories))]
	pay := paymentMethods[r.Intn(len(paymentMethods))]
	wishlisted := boolStr(r.Intn(10) < 3)

	switch surface {
	case "web":
		surfaceDimVals = []string{cat, wishlisted, pay}
	case "android":
		surfaceDimVals = []string{cat, wishlisted, pay,
			appVersions[r.Intn(len(appVersions))],
			osVersions["android"][r.Intn(len(osVersions["android"]))]}
	case "ios":
		surfaceDimVals = []string{cat, pay,
			appVersions[r.Intn(len(appVersions))],
			osVersions["ios"][r.Intn(len(osVersions["ios"]))]}
	}

	dimVals := append(coreDimVals, surfaceDimVals...)

	// Metrics
	sessions := r.Intn(5) + 1
	engaged := int(math.Max(1, float64(sessions)*0.6))
	total := sessions + r.Intn(3)
	newUsers := r.Intn(total + 1)
	bounceRate := 0.1 + r.Float64()*0.5
	avgDur := 30.0 + r.Float64()*300
	conversions := 0
	revenue := 0.0
	if r.Float64() < 0.025 { // 2.5% conversion rate
		conversions = 1
		revenue = 800.0 + r.Float64()*11200
	}
	events := sessions*3 + r.Intn(20)
	pageViews := sessions*2 + r.Intn(15)

	metricVals := []string{
		fmt.Sprintf("%d", sessions),
		fmt.Sprintf("%d", engaged),
		fmt.Sprintf("%d", total),
		fmt.Sprintf("%d", newUsers),
		fmt.Sprintf("%.6f", bounceRate),
		fmt.Sprintf("%.2f", avgDur),
		fmt.Sprintf("%d", conversions),
		fmt.Sprintf("%.2f", revenue),
		fmt.Sprintf("%d", events),
		fmt.Sprintf("%d", pageViews),
	}

	return Session{
		DimensionValues: dimVals,
		MetricValues:    metricVals,
	}
}

// ── deterministic pseudo-random helpers ───────────────────────────────────

type lcgRand struct{ state uint64 }

func newRand(seed uint64) *lcgRand { return &lcgRand{state: seed | 1} }

func (r *lcgRand) next() uint64 {
	r.state = r.state*6364136223846793005 + 1442695040888963407
	return r.state
}

func (r *lcgRand) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(r.next() % uint64(n))
}

func (r *lcgRand) Float64() float64 {
	return float64(r.next()>>11) / (1 << 53)
}

func hash(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
