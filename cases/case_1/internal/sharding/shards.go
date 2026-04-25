// Package sharding defines the zone → state shard map used across all DB setup scripts.
package sharding

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
var Companies = []struct {
	Name   string
	DBName string
	Port   int
}{
	{Name: "vodafone", DBName: "vodafone_db", Port: 3306},
	{Name: "idea", DBName: "idea_db", Port: 3307},
	{Name: "tata_docomo", DBName: "tata_docomo_db", Port: 3308},
	{Name: "aircel", DBName: "aircel_db", Port: 3309},
}

// DefaultSplits is how many table splits to create per state shard by default (simulating 1M-row splits).
const DefaultSplits = 2
