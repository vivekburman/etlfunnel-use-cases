package state

// quota.go — in-process per-property per-hour token budget enforcer for the
// GA4 mock seeder.  Mirrors the real GA4 quota model:
//
//   Core tokens/property/day  200,000
//   Core tokens/property/hour  40,000
//   Concurrent requests           10   (not enforced here — handled by Go HTTP server)
//   Realtime tokens/property/day 10,000

import (
	"sync"
	"time"
)

const (
	DailyTokenLimit    = 200_000
	HourlyTokenLimit   = 40_000
	RealtimeDailyLimit = 10_000
)

type bucket struct {
	mu           sync.Mutex
	hourlyTokens int64
	dailyTokens  int64
	rtDailyTokens int64
	hourWindow   time.Time
	dayWindow    time.Time
}

func (b *bucket) roll() {
	now := time.Now()
	if now.Sub(b.hourWindow) >= time.Hour {
		b.hourlyTokens = 0
		b.hourWindow = now.Truncate(time.Hour)
	}
	if now.Sub(b.dayWindow) >= 24*time.Hour {
		b.dailyTokens = 0
		b.rtDailyTokens = 0
		b.dayWindow = now.Truncate(24 * time.Hour)
	}
}

// ConsumeReport attempts to consume n core tokens for a runReport request.
// Returns (actualCost, exhausted).  exhausted is true if hourly OR daily
// limits would be breached, in which case no tokens are consumed.
func (b *bucket) ConsumeReport(n int64) (int64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.roll()
	if b.hourlyTokens+n > HourlyTokenLimit || b.dailyTokens+n > DailyTokenLimit {
		return 0, true
	}
	b.hourlyTokens += n
	b.dailyTokens += n
	return n, false
}

// ConsumeRealtime consumes 1 realtime token.  Returns exhausted=true if the
// daily realtime budget is exceeded.
func (b *bucket) ConsumeRealtime() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.roll()
	if b.rtDailyTokens+1 > RealtimeDailyLimit {
		return true
	}
	b.rtDailyTokens++
	return false
}

func (b *bucket) HourlySpent() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.roll()
	return b.hourlyTokens
}

// QuotaStore manages quota buckets for all registered properties.
type QuotaStore struct {
	mu      sync.RWMutex
	buckets map[string]*bucket
}

func NewQuotaStore() *QuotaStore {
	return &QuotaStore{buckets: make(map[string]*bucket)}
}

func (qs *QuotaStore) bucket(property string) *bucket {
	qs.mu.RLock()
	b, ok := qs.buckets[property]
	qs.mu.RUnlock()
	if ok {
		return b
	}
	qs.mu.Lock()
	defer qs.mu.Unlock()
	if b, ok = qs.buckets[property]; ok {
		return b
	}
	b = &bucket{
		hourWindow: time.Now().Truncate(time.Hour),
		dayWindow:  time.Now().Truncate(24 * time.Hour),
	}
	qs.buckets[property] = b
	return b
}

func (qs *QuotaStore) ConsumeReport(property string, n int64) (int64, bool) {
	return qs.bucket(property).ConsumeReport(n)
}

func (qs *QuotaStore) ConsumeRealtime(property string) bool {
	return qs.bucket(property).ConsumeRealtime()
}

func (qs *QuotaStore) HourlySpent(property string) int64 {
	return qs.bucket(property).HourlySpent()
}
