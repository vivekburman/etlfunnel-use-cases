package brands

const SplitRowCap = 1_000_000

type Brand struct {
	Name             string
	DBName           string
	Port             int
	RedisStream      string
	ReplicationSlot  string
}

var Brands = []Brand{
	{Name: "zomato_food", DBName: "zomato_food_db", Port: 5441, RedisStream: "zomato:orders:stream", ReplicationSlot: "zomato_food_slot"},
	{Name: "blinkit", DBName: "blinkit_db", Port: 5442, RedisStream: "blinkit:orders:stream", ReplicationSlot: "blinkit_slot"},
	{Name: "hyperpure", DBName: "hyperpure_db", Port: 5443, RedisStream: "hyperpure:orders:stream", ReplicationSlot: "hyperpure_slot"},
	{Name: "district", DBName: "district_db", Port: 5444, RedisStream: "district:orders:stream", ReplicationSlot: "district_slot"},
}

type City struct {
	Name  string
	Zone  string
	Tier  string
	State string
}

var Cities = []City{
	{Name: "delhi", Zone: "north", Tier: "metro", State: "delhi"},
	{Name: "jaipur", Zone: "north", Tier: "tier2", State: "rajasthan"},
	{Name: "lucknow", Zone: "north", Tier: "tier2", State: "up"},
	{Name: "bengaluru", Zone: "south", Tier: "metro", State: "karnataka"},
	{Name: "chennai", Zone: "south", Tier: "metro", State: "tamilnadu"},
	{Name: "hyderabad", Zone: "south", Tier: "metro", State: "telangana"},
	{Name: "mumbai", Zone: "west", Tier: "metro", State: "maharashtra"},
	{Name: "pune", Zone: "west", Tier: "tier2", State: "maharashtra"},
	{Name: "ahmedabad", Zone: "west", Tier: "tier2", State: "gujarat"},
	{Name: "kolkata", Zone: "east", Tier: "metro", State: "westbengal"},
}

var Entities = []string{"orders", "order_items", "order_status_events", "delivery_assignments"}
