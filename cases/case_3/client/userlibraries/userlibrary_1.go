package client_userlibraries

// userlibrary_1.go — shared utilities for the Myntra GA4 analytics ETL.
//
// Exports:
//   GlobalQuota  — package-level quota tracker shared between the GA4 source
//                  connector (which writes token spend) and the QuotaThrottle
//                  transformer (which reads it and sleeps on budget exhaustion).
//   GetAuxPostgresConn — helper to open a single-use pgx connection to AuxDB.

import (
	"fmt"
	"sync"
	"time"

	"etlfunnel/execution/cast"
	"etlfunnel/execution/models"

	"github.com/jackc/pgx/v5"
)

// ── Quota tracker ──────────────────────────────────────────────────────────

const (
	hourlyTokenLimit   = 40_000
	softLimitThreshold = 0.80 // 80% of hourly limit → throttle
)

// quotaBucket tracks token spend for one property within the current hour.
type quotaBucket struct {
	mu       sync.Mutex
	tokens   int64
	windowAt time.Time
}

func (b *quotaBucket) consume(n int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rollIfNeeded()
	b.tokens += n
}

func (b *quotaBucket) spent() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rollIfNeeded()
	return b.tokens
}

// rollIfNeeded resets the counter when a new hour window begins.
// Must be called with b.mu held.
func (b *quotaBucket) rollIfNeeded() {
	now := time.Now()
	if now.Sub(b.windowAt) >= time.Hour {
		b.tokens = 0
		b.windowAt = now.Truncate(time.Hour)
	}
}

// QuotaTracker manages per-property hourly token spend buckets.
type QuotaTracker struct {
	mu      sync.RWMutex
	buckets map[string]*quotaBucket
}

func newQuotaTracker() *QuotaTracker {
	return &QuotaTracker{
		buckets: make(map[string]*quotaBucket),
	}
}

// GlobalQuota is the single shared quota tracker for this process.
// The GA4 connector writes to it; the QuotaThrottle transformer reads from it.
var GlobalQuota = newQuotaTracker()

// Consume records n tokens spent for the given property.
func (qt *QuotaTracker) Consume(property string, n int64) {
	qt.mu.RLock()
	b, ok := qt.buckets[property]
	qt.mu.RUnlock()

	if !ok {
		qt.mu.Lock()
		if b, ok = qt.buckets[property]; !ok {
			b = &quotaBucket{windowAt: time.Now().Truncate(time.Hour)}
			qt.buckets[property] = b
		}
		qt.mu.Unlock()
	}

	b.consume(n)
}

// CheckAndThrottle blocks until the hourly budget recovers if the soft limit
// (80% of 40K = 32K tokens/hour) has been reached for this property.
func (qt *QuotaTracker) CheckAndThrottle(property string) {
	qt.mu.RLock()
	b, ok := qt.buckets[property]
	qt.mu.RUnlock()

	if !ok {
		return
	}

	softLimit := int64(float64(hourlyTokenLimit) * softLimitThreshold)
	if b.spent() < softLimit {
		return
	}

	// Sleep until the next hour window.
	now := time.Now()
	next := now.Truncate(time.Hour).Add(time.Hour)
	time.Sleep(time.Until(next))
}

// SpentThisHour returns the current token spend for the given property.
func (qt *QuotaTracker) SpentThisHour(property string) int64 {
	qt.mu.RLock()
	b, ok := qt.buckets[property]
	qt.mu.RUnlock()
	if !ok {
		return 0
	}
	return b.spent()
}

// ── AuxDB helper ───────────────────────────────────────────────────────────

const AuxDBKey = "Aux DB"

// GetAuxPostgresConn retrieves and casts the auxiliary PostgreSQL connection from the map.
func GetAuxPostgresConn(connMap map[string]models.IDatabaseEngine) (*pgx.Conn, error) {
	engine, ok := connMap[AuxDBKey]
	if !ok {
		return nil, fmt.Errorf("auxiliary connection %q not found", AuxDBKey)
	}
	conn, err := cast.CastAsPostgresDBConnection(engine)
	if err != nil {
		return nil, fmt.Errorf("failed to cast AuxDB connection: %w", err)
	}
	return conn, nil
}
