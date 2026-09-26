// Package merchants is the fixed merchant/card-BIN reference data shared by
// cmd/seeder (writes transactions against these merchants) and
// cmd/snowflake_setup (seeds MERCHANT_RISK_PROFILE for these same merchants),
// so Flow 2's MERGE join in the execution engine actually has rows to match
// against instead of falling back to every merchant's UNKNOWN/0 default.
package merchants

type Merchant struct {
	ID             string
	RiskTier       string
	ChargebackRate float64
}

var All = []Merchant{
	{ID: "MER-AMBER-RETAIL", RiskTier: "LOW", ChargebackRate: 0.004},
	{ID: "MER-BLUEDART-LOGISTICS", RiskTier: "LOW", ChargebackRate: 0.006},
	{ID: "MER-CRIMSON-ELECTRONICS", RiskTier: "MEDIUM", ChargebackRate: 0.021},
	{ID: "MER-DAZZLE-FASHION", RiskTier: "MEDIUM", ChargebackRate: 0.034},
	{ID: "MER-EMBER-TRAVEL", RiskTier: "HIGH", ChargebackRate: 0.061},
	{ID: "MER-FOXTROT-GAMING", RiskTier: "HIGH", ChargebackRate: 0.088},
	{ID: "MER-GRANITE-HARDWARE", RiskTier: "LOW", ChargebackRate: 0.002},
	{ID: "MER-HALO-SUBSCRIPTIONS", RiskTier: "MEDIUM", ChargebackRate: 0.029},
}

// CardBINs are deliberately reused across merchants (same card used at
// several merchants) - a real cross-merchant fraud signal the risk-profile
// join alone can't see, left as an obvious follow-on query once the data is
// queryable in Elasticsearch.
var CardBINs = []string{"411111", "451234", "521987", "552266", "601188"}

var Currencies = []string{"INR", "INR", "INR", "USD"}

var Statuses = []string{"AUTHORIZED", "CAPTURED", "CAPTURED", "CAPTURED", "DECLINED", "REFUNDED"}
