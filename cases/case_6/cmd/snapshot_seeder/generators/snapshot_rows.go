package generators

// snapshot_rows.go — deterministic FC inventory snapshot row generator.
//
// Rows are generated via modular arithmetic (no math/rand) so the same
// (n, faultPercent) always produces the same pool, letting a pipeline resume
// from a checkpoint and see exactly the data it would have seen originally.

import "fmt"

var fulfillmentCenters = []string{"FC-BLR-01", "FC-DEL-01", "FC-MUM-01", "FC-HYD-01", "FC-PNE-01", "FC-KOL-01"}
var categories = []string{"Grocery", "Electronics", "Apparel", "Home", "Beauty", "Personal Care"}
var warehouseZones = []string{"A", "B", "C", "D"}

// SnapshotRow matches the Parquet snapshot schema in the case study plan (§1.2).
type SnapshotRow struct {
	FCID             string  `parquet:"fc_id"`
	SellerID         string  `parquet:"seller_id"`
	SKUID            string  `parquet:"sku_id"`
	SKUName          string  `parquet:"sku_name"`
	Category         string  `parquet:"category"`
	WarehouseZone    string  `parquet:"warehouse_zone"`
	QuantityOnHand   int64   `parquet:"quantity_on_hand"`
	QuantityReserved int64   `parquet:"quantity_reserved"`
	UnitCost         float64 `parquet:"unit_cost"`
	SnapshotDate     string  `parquet:"snapshot_date"`
}

// Generate produces n deterministic snapshot rows for the given date.
func Generate(n int, snapshotDate string) []SnapshotRow {
	return GenerateMixed(n, 0, snapshotDate)
}

// GenerateMixed produces n rows with faultPercent% replaced by fault rows,
// cycling round-robin so both fault paths fire every seeder run:
//
//	A (sku_id="")           → transformer_97 drops the row (silent, records-dropped metric)
//	B (quantity_on_hand=-1) → transformer_98 error → fc_snapshot_backlog
//
// faultPercent=0 returns a clean pool identical to Generate(n, snapshotDate).
func GenerateMixed(n int, faultPercent int, snapshotDate string) []SnapshotRow {
	rows := make([]SnapshotRow, n)

	interval := 0
	if faultPercent > 0 && faultPercent <= 100 {
		interval = 100 / faultPercent
	}
	faultCycle := 0

	for i := 0; i < n; i++ {
		fc := fulfillmentCenters[i%len(fulfillmentCenters)]
		category := categories[i%len(categories)]
		zone := warehouseZones[i%len(warehouseZones)]
		sellerN := (i % 500) + 1
		skuN := i + 1

		row := SnapshotRow{
			FCID:             fc,
			SellerID:         fmt.Sprintf("SEL-%06d", sellerN),
			SKUID:            fmt.Sprintf("SKU-%08d", skuN),
			SKUName:          fmt.Sprintf("%s Product %d", category, skuN),
			Category:         category,
			WarehouseZone:    zone,
			QuantityOnHand:   int64(50 + (i % 500)),
			QuantityReserved: int64(i % 20),
			UnitCost:         float64(100+(i%900)) + 0.5,
			SnapshotDate:     snapshotDate,
		}

		if interval > 0 && (i+1)%interval == 0 {
			switch faultCycle % 2 {
			case 0:
				row.SKUID = "" // Fault A: silent drop
			case 1:
				row.QuantityOnHand = -1 // Fault B: backlog
			}
			faultCycle++
		}

		rows[i] = row
	}
	return rows
}
