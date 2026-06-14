package generators

// events.go — deterministic Zepto order event generator.
//
// Events are generated once at startup into a fixed pool.
// The cursor index into this pool is stable across seeder restarts,
// so the pipeline can resume from a checkpoint and get the same data.

import (
	"fmt"
	"time"
)

var cities = []string{"bangalore", "delhi", "mumbai", "hyderabad", "chennai", "pune", "kolkata"}

var storesByCity = map[string][]string{
	"bangalore": {"STR-BLR-001", "STR-BLR-012", "STR-BLR-023", "STR-BLR-034", "STR-BLR-042"},
	"delhi":     {"STR-DEL-001", "STR-DEL-007", "STR-DEL-015", "STR-DEL-022"},
	"mumbai":    {"STR-MUM-001", "STR-MUM-008", "STR-MUM-019", "STR-MUM-031"},
	"hyderabad": {"STR-HYD-001", "STR-HYD-005", "STR-HYD-011"},
	"chennai":   {"STR-CHN-001", "STR-CHN-004", "STR-CHN-009"},
	"pune":      {"STR-PNE-001", "STR-PNE-003"},
	"kolkata":   {"STR-KOL-001", "STR-KOL-006"},
}

var eventTypes = []string{
	"ORDER_CREATED",
	"ORDER_CONFIRMED",
	"ORDER_PICKED",
	"ORDER_DISPATCHED",
	"ORDER_DELIVERED",
}

var statusByEventType = map[string]string{
	"ORDER_CREATED":    "created",
	"ORDER_CONFIRMED":  "confirmed",
	"ORDER_PICKED":     "picked",
	"ORDER_DISPATCHED": "dispatched",
	"ORDER_DELIVERED":  "delivered",
}

// OrderEvent matches the Zepto API response shape from the design doc.
type OrderEvent struct {
	EventID    string         `json:"event_id"`
	OrderID    string         `json:"order_id"`
	CustomerID string         `json:"customer_id"`
	StoreID    string         `json:"store_id"`
	City       string         `json:"city"`
	EventType  string         `json:"event_type"`
	Status     string         `json:"status"`
	Amount     float64        `json:"amount"`
	CreatedAt  string         `json:"created_at"`
	Payload    map[string]any `json:"payload"`
}

// Generate produces n deterministic order events.
// Using modular arithmetic instead of math/rand to keep it deterministic
// without relying on a seed.
func Generate(n int) []OrderEvent {
	return GenerateMixed(n, 0)
}

// GenerateMixed produces n events with faultPercent% replaced by fault records.
//
// Three fault types cycle in round-robin so all failure paths fire:
//   A (city="")              → transformer_81 drops the record (records-dropped metric)
//   B (created_at="INVALID") → transformer_84 parse error → zepto_storage_backlog (Flow 2)
//   C (payload._fault_inject) → transformer_81 returns error → zepto_ingestion_backlog (Flow 1)
//
// faultPercent=0 returns a clean pool identical to Generate(n).
func GenerateMixed(n int, faultPercent int) []OrderEvent {
	events := make([]OrderEvent, n)
	// Base time: 25 days ago, advancing 1 minute per event so none are older than 90 days.
	base := time.Now().UTC().Add(-25 * 24 * time.Hour)

	// Compute fault injection interval: inject a fault every `interval` records.
	interval := 0
	if faultPercent > 0 && faultPercent <= 100 {
		interval = 100 / faultPercent
	}
	faultSeq := 0

	for i := 0; i < n; i++ {
		cityIdx := i % len(cities)
		city := cities[cityIdx]

		stores := storesByCity[city]
		storeID := stores[i%len(stores)]

		etIdx := i % len(eventTypes)
		eventType := eventTypes[etIdx]

		orderNum := (i / len(eventTypes)) + 1
		orderID := fmt.Sprintf("ORD-%08d", orderNum)
		customerID := fmt.Sprintf("CUST-%06d", (i%50000)+1)
		eventID := fmt.Sprintf("%08x-%04x-4%03x-%04x-%012x",
			i*0x9e3779b9,
			(i>>16)&0xffff,
			(i>>8)&0x0fff,
			((i>>4)&0x3fff)|0x8000,
			i*0x6c62272e,
		)

		createdAt := base.Add(time.Duration(i) * time.Minute)

		// Vary amount based on city and event index.
		amount := float64(100+(i%900)) + float64(i%100)/100.0

		payload := buildPayload(eventType, i)

		ev := OrderEvent{
			EventID:    eventID,
			OrderID:    orderID,
			CustomerID: customerID,
			StoreID:    storeID,
			City:       city,
			EventType:  eventType,
			Status:     statusByEventType[eventType],
			Amount:     amount,
			CreatedAt:  createdAt.Format(time.RFC3339),
			Payload:    payload,
		}

		if interval > 0 && (i+1)%interval == 0 {
			ev = applyFault(ev, faultSeq)
			faultSeq++
		}

		events[i] = ev
	}
	return events
}

// applyFault mutates an event to exercise a specific failure path in the pipeline.
func applyFault(ev OrderEvent, seq int) OrderEvent {
	switch seq % 3 {
	case 0:
		// Type A: empty city → transformer_81 returns nil (silent drop) → records-dropped metric.
		ev.City = ""
	case 1:
		// Type B: unparseable created_at → transformer_84 returns error → zepto_storage_backlog.
		ev.CreatedAt = "INVALID_TIMESTAMP"
	case 2:
		// Type C: fault-inject marker in payload → transformer_81 returns error → zepto_ingestion_backlog.
		// Copy the map to avoid mutating the shared buildPayload result.
		p := make(map[string]any, len(ev.Payload)+1)
		for k, v := range ev.Payload {
			p[k] = v
		}
		p["_fault_inject"] = "error"
		ev.Payload = p
	}
	return ev
}

func buildPayload(eventType string, seed int) map[string]any {
	switch eventType {
	case "ORDER_CREATED":
		return map[string]any{
			"channel":    pickFrom([]string{"app", "web", "sms"}, seed),
			"promo_code": fmt.Sprintf("PROMO%04d", seed%100),
		}
	case "ORDER_CONFIRMED":
		return map[string]any{
			"estimated_prep_mins": 5 + seed%10,
		}
	case "ORDER_PICKED":
		return map[string]any{
			"picker_id": fmt.Sprintf("PKR-%04d", seed%200),
		}
	case "ORDER_DISPATCHED":
		return map[string]any{
			"driver_id": fmt.Sprintf("DRV-%04d", seed%500),
			"eta_mins":  6 + seed%8,
		}
	case "ORDER_DELIVERED":
		return map[string]any{
			"rating":        1 + seed%5,
			"delivery_mins": 8 + seed%12,
		}
	default:
		return map[string]any{}
	}
}

func pickFrom(options []string, seed int) string {
	return options[seed%len(options)]
}
