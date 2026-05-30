package generators

// realtime.go — generates synthetic active-user rows for runRealtimeReport
// responses.  Each call returns a snapshot reflecting the "last 30 minutes"
// of activity, varied by time-of-day to simulate realistic traffic curves.

import (
	"fmt"
	"time"
)

// RealtimeRow matches dbo.realtime_sessions schema.
type RealtimeRow struct {
	DimensionValues []string // city, deviceCategory, pagePath, eventName
	MetricValues    []string // activeUsers
}

var realtimeDimensions = []string{"city", "deviceCategory", "pagePath", "eventName"}
var realtimeMetricTypes = []string{"TYPE_INTEGER"}

var pagePaths = []string{
	"/", "/men", "/women", "/kids", "/beauty", "/sport",
	"/product/detail", "/cart", "/checkout", "/wishlist",
	"/search", "/account/orders", "/account/profile",
}

var eventNames = []string{
	"page_view", "search", "add_to_cart", "wishlist_add",
	"checkout_start", "purchase", "session_start",
}

// RealtimeDimensions returns the dimension names for realtime reports.
func RealtimeDimensions() []string { return realtimeDimensions }

// RealtimeMetrics returns metric names and types.
func RealtimeMetrics() (names, types []string) {
	return []string{"activeUsers"}, realtimeMetricTypes
}

// GenerateRealtimeRows creates synthetic rows for one property snapshot.
// Row count scales with time-of-day to simulate traffic curves (higher during
// evening hours IST).
func GenerateRealtimeRows(property string) []RealtimeRow {
	hour := time.Now().UTC().Hour() // GA4 uses UTC
	// IST peak: 15:00–18:00 UTC → ~20:30–23:30 IST
	baseActive := trafficMultiplier(hour) * 100

	var rows []RealtimeRow
	seed := hash(fmt.Sprintf("%s:%d", property, time.Now().Unix()/60))
	r := newRand(seed)

	for _, city := range cities {
		for _, device := range devices {
			active := int(float64(baseActive) * deviceShare(device) * cityShare(city))
			active += r.Intn(10)
			if active <= 0 {
				continue
			}
			path := pagePaths[r.Intn(len(pagePaths))]
			event := eventNames[r.Intn(len(eventNames))]
			rows = append(rows, RealtimeRow{
				DimensionValues: []string{city, device, path, event},
				MetricValues:    []string{fmt.Sprintf("%d", active)},
			})
		}
	}
	return rows
}

func trafficMultiplier(utcHour int) float64 {
	// Approximate sine curve with peak at 16:00 UTC (21:30 IST)
	angle := float64(utcHour-4) * (3.14159 / 12)
	return 1.0 + 0.8*((float64(1)+sinApprox(angle))/2.0)
}

func deviceShare(device string) float64 {
	switch device {
	case "mobile":
		return 0.65
	case "desktop":
		return 0.25
	default:
		return 0.10
	}
}

func cityShare(city string) float64 {
	shares := map[string]float64{
		"Mumbai": 0.18, "Delhi": 0.16, "Bengaluru": 0.14,
		"Hyderabad": 0.10, "Chennai": 0.09, "Kolkata": 0.08,
		"Pune": 0.08, "Ahmedabad": 0.07, "Jaipur": 0.05, "Lucknow": 0.05,
	}
	if s, ok := shares[city]; ok {
		return s
	}
	return 0.05
}

// sinApprox is a rough sine approximation to avoid importing math.
func sinApprox(x float64) float64 {
	// Bhaskara I approximation: sin(x) ≈ 16x(π-x) / (5π²-4x(π-x))  for x ∈ [0,π]
	pi := 3.14159265358979
	for x > pi {
		x -= pi
	}
	for x < 0 {
		x += pi
	}
	num := 16 * x * (pi - x)
	den := 5*pi*pi - 4*x*(pi-x)
	if den == 0 {
		return 0
	}
	return num / den
}
