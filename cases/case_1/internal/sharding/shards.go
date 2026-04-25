// Package sharding defines the zone → state shard map used across all DB setup scripts.
package sharding

// ShardingType controls how a company's tables are named and geo-columns stored.
type ShardingType string

const (
	ZoneState ShardingType = "zone_state" // customers_north_up_1
	ZoneOnly  ShardingType = "zone_only"  // customers_north_1
	StateOnly ShardingType = "state_only" // customers_up_1
)

// SplitRowCap is the maximum rows per shard table before a new split is created.
const SplitRowCap = 1_000_000

// Zone represents a geographic zone.
type Zone struct {
	Name   string
	States []string
}

// Zones is the canonical two-tier shard definition matching the case study plan.
var Zones = []Zone{
	{
		Name:   "north",
		States: []string{"up", "bihar", "jharkhand", "hp", "uttarakhand", "punjab", "haryana", "jk", "delhi"},
	},
	{
		Name:   "south",
		States: []string{"tamilnadu", "kerala", "karnataka", "andhrapradesh", "telangana"},
	},
	{
		Name:   "east",
		States: []string{"westbengal", "odisha", "assam", "meghalaya", "tripura", "sikkim"},
	},
	{
		Name:   "west",
		States: []string{"maharashtra", "gujarat", "rajasthan", "goa"},
	},
	{
		Name:   "central",
		States: []string{"mp", "chhattisgarh"},
	},
}

// Companies is the list of source telecom companies.
// Each company uses a different sharding strategy to simulate real-world divergence
// before the merger — not all operators organised their data the same way.
var Companies = []struct {
	Name         string
	DBName       string
	Port         int
	ShardingType ShardingType
}{
	{Name: "vodafone", DBName: "vodafone_db", Port: 3306, ShardingType: ZoneState},
	{Name: "idea", DBName: "idea_db", Port: 3307, ShardingType: ZoneOnly},
	{Name: "tata_docomo", DBName: "tata_docomo_db", Port: 3308, ShardingType: StateOnly},
	{Name: "aircel", DBName: "aircel_db", Port: 3309, ShardingType: ZoneState},
}

// AllStates returns every state across all zones in definition order.
// Used by state-only sharding to enumerate shard targets.
func AllStates() []string {
	var states []string
	for _, z := range Zones {
		states = append(states, z.States...)
	}
	return states
}
