package generators

// movement_rows.go — deterministic WMS stock movement record generator.
//
// Records are generated via modular arithmetic (no math/rand) so the same
// (n, faultPercent) always produces the same pool. Unlike snapshot_rows.go,
// timestamps advance from "now minus n minutes" so a fresh call always
// produces records after whatever the directory already contains.

import (
	"fmt"
	"time"
)

var fulfillmentCenters = []string{"FC-BLR-01", "FC-DEL-01", "FC-MUM-01", "FC-HYD-01", "FC-PNE-01", "FC-KOL-01"}
var movementTypes = []string{"PUTAWAY", "PICK", "RETURN", "DAMAGE", "ADJUSTMENT"}

// deltaSign is the default sign convention from the case study plan (§1.3.1).
// ADJUSTMENT is intentionally overridden per-record below since its sign
// varies (cycle-count corrections go either direction).
var deltaSign = map[string]int{
	"PUTAWAY":    1,
	"PICK":       -1,
	"RETURN":     1,
	"DAMAGE":     -1,
	"ADJUSTMENT": 1,
}

// MovementRow matches the Avro movement schema in the case study plan (§1.3).
type MovementRow struct {
	MovementID    string `avro:"movement_id"`
	FCID          string `avro:"fc_id"`
	SKUID         string `avro:"sku_id"`
	MovementType  string `avro:"movement_type"`
	QuantityDelta int    `avro:"quantity_delta"`
	ReasonCode    string `avro:"reason_code"`
	OperatorID    string `avro:"operator_id"`
	RecordedAt    string `avro:"recorded_at"`
}

// Generate produces n deterministic movement records.
func Generate(n int) []MovementRow {
	return GenerateMixed(n, 0)
}

// GenerateMixed produces n records with faultPercent% replaced by fault
// records, cycling round-robin so both fault paths fire every seeder run:
//
//	A (fc_id="")                → transformer_100 drops the record (silent)
//	B (movement_type="UNKNOWN") → transformer_101 error → fc_movement_backlog
//
// faultPercent=0 returns a clean pool identical to Generate(n).
func GenerateMixed(n int, faultPercent int) []MovementRow {
	rows := make([]MovementRow, n)
	base := time.Now().UTC().Add(-time.Duration(n) * time.Minute)

	interval := 0
	if faultPercent > 0 && faultPercent <= 100 {
		interval = 100 / faultPercent
	}
	faultCycle := 0

	for i := 0; i < n; i++ {
		fc := fulfillmentCenters[i%len(fulfillmentCenters)]
		skuN := (i % 2000) + 1
		movementType := movementTypes[i%len(movementTypes)]
		sign := deltaSign[movementType]
		if movementType == "ADJUSTMENT" && i%2 == 0 {
			sign = -1
		}
		qty := (1 + i%20) * sign

		reasonCode := ""
		switch movementType {
		case "DAMAGE":
			reasonCode = "DMG-WATER"
		case "ADJUSTMENT":
			reasonCode = "ADJ-CYCLE-COUNT"
		}

		row := MovementRow{
			MovementID:    fmt.Sprintf("%016x", i+1),
			FCID:          fc,
			SKUID:         fmt.Sprintf("SKU-%08d", skuN),
			MovementType:  movementType,
			QuantityDelta: qty,
			ReasonCode:    reasonCode,
			OperatorID:    fmt.Sprintf("OPR-%04d", (i%50)+1),
			RecordedAt:    base.Add(time.Duration(i) * time.Minute).Format(time.RFC3339),
		}

		if interval > 0 && (i+1)%interval == 0 {
			switch faultCycle % 2 {
			case 0:
				row.FCID = "" // Fault A: silent drop
			case 1:
				row.MovementType = "UNKNOWN" // Fault B: backlog
			}
			faultCycle++
		}

		rows[i] = row
	}
	return rows
}
