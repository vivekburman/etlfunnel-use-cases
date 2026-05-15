package client_transformer_8

// Zomato Platform Order Intelligence — transformer_8: TimeBucketer (STEP-21)
//
// Assigns a time bucket to each order for analytics segmentation.
//
// Food brands (zomato_food, blinkit, hyperpure):
//   Reads placed_at timestamp and maps the hour to meal_period:
//     00:00–09:59  → breakfast
//     10:00–13:59  → lunch
//     14:00–17:59  → snack
//     18:00–22:59  → dinner
//     23:00–23:59  → late_night
//
//   meal_period is stored in ES and used for daily demand heatmaps.
//   event_category is left empty for food brands.
//
// District (live events):
//   Does not use meal_period. Instead, copies the source event_category
//   field (concert / comedy / sport / dining_experience / other) directly.
//   meal_period is left unset so TypeCaster omits it from the ES document.

import (
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
)

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	r := ulib.ShallowClone(param.Record)
	bucket(r)
	return r, nil
}

func bucket(r map[string]any) {
	brand, _ := r["sub_brand"].(string)

	if brand == "district" {
		// event_category is already in the record from the source table; nothing to compute.
		// Ensure meal_period is absent so TypeCaster does not write a null field to ES.
		delete(r, "meal_period")
		return
	}

	placedAt, ok := ulib.ToTime(r["placed_at"])
	if !ok {
		r["meal_period"] = "unknown"
		return
	}

	r["meal_period"] = mealPeriod(placedAt.Hour())
}

func mealPeriod(hour int) string {
	switch {
	case hour < 10:
		return "breakfast"
	case hour < 14:
		return "lunch"
	case hour < 18:
		return "snack"
	case hour < 23:
		return "dinner"
	default:
		return "late_night"
	}
}

