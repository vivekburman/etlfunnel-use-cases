package client_transformer_55

// Zomato Platform Order Intelligence — transformer_9: CityZoneMapper (STEP-22)
//
// Maps city_id (integer) → city_name, zone_label, state, tier using the
// city_mapping reference table seeded in AuxDB.
//
// At startup the transformer loads the full city_mapping table from AuxDB
// into an in-process cache. If AuxDB is unavailable at startup, the
// transformer falls back to the hardcoded built-in table (identical to the
// seeded data) so that pipelines can continue with degraded freshness.
//
// Records with a city_id that has no mapping are routed to backlog with
// error code UNKNOWN_CITY and removed from the downstream batch.
//
// Fields written:
//   city_name   — human-readable city (e.g. "delhi")
//   zone_label  — "north" | "south" | "west" | "east"
//   state       — state slug (e.g. "maharashtra")
//   tier        — "metro" | "tier2"

import (
	"context"
	"etlfunnel/execution/models"
	ulib "etlfunnel/execution/client/userlibraries"
	"fmt"
	"sync"
)

// cityEntry mirrors one row of the city_mapping AuxDB table.
type cityEntry struct {
	name      string
	zoneLabel string
	state     string
	tier      string
}

// builtinCities is the fallback cache — identical to the seeded AuxDB rows.
var builtinCities = map[int]cityEntry{
	1:  {"delhi", "north", "delhi", "metro"},
	2:  {"jaipur", "north", "rajasthan", "tier2"},
	3:  {"lucknow", "north", "up", "tier2"},
	4:  {"bengaluru", "south", "karnataka", "metro"},
	5:  {"chennai", "south", "tamilnadu", "metro"},
	6:  {"hyderabad", "south", "telangana", "metro"},
	7:  {"mumbai", "west", "maharashtra", "metro"},
	8:  {"pune", "west", "maharashtra", "tier2"},
	9:  {"ahmedabad", "west", "gujarat", "tier2"},
	10: {"kolkata", "east", "westbengal", "metro"},
}

var (
	cacheOnce  sync.Once
	cityCache  map[int]cityEntry
)

// loadCache fetches city_mapping from AuxDB; falls back to builtinCities on error.
func loadCache(param *models.TransformerProps) map[int]cityEntry {
	cacheOnce.Do(func() {
		pgConn, err := ulib.GetAuxPostgresConn(param.AuxiliaryDBConnMap)
		if err != nil {
			param.State.GetLogger().Warn(
				fmt.Sprintf("transformer_9: AuxDB unavailable (%v) — using built-in city cache", err),
			)
			cityCache = builtinCities
			return
		}

		rows, qErr := pgConn.Query(
			context.Background(),
			"SELECT city_id, city_name, zone_label, state, tier FROM city_mapping",
		)
		if qErr != nil {
			param.State.GetLogger().Warn(
				fmt.Sprintf("transformer_9: city_mapping query failed (%v) — using built-in cache", qErr),
			)
			cityCache = builtinCities
			return
		}
		defer rows.Close()

		loaded := make(map[int]cityEntry)
		for rows.Next() {
			var id int
			var e cityEntry
			if scanErr := rows.Scan(&id, &e.name, &e.zoneLabel, &e.state, &e.tier); scanErr == nil {
				loaded[id] = e
			}
		}
		if len(loaded) == 0 {
			cityCache = builtinCities
		} else {
			cityCache = loaded
		}
		param.State.GetLogger().Debug(
			fmt.Sprintf("transformer_9: loaded %d city mapping entries from AuxDB", len(cityCache)),
		)
	})
	return cityCache
}

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	cache := loadCache(param)

	cityID, ok := toCityID(param.Record["city_id"])
	if !ok {
		// city_id absent or non-numeric — pass through without city fields
		return param.Record, nil
	}
	entry, mapped := cache[cityID]
	if !mapped {
		return nil, fmt.Errorf("UNKNOWN_CITY: no city mapping for city_id=%d", cityID)
	}

	r := ulib.ShallowClone(param.Record)
	r["city_name"] = entry.name
	r["zone_label"] = entry.zoneLabel
	r["state"] = entry.state
	r["tier"] = entry.tier
	return r, nil
}

func toCityID(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	}
	return 0, false
}

