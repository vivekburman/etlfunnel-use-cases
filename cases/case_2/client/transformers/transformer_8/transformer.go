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
	"etlfunnel/execution/models"
	"time"
)

func Transform(param *models.TransformerProps) (*models.TransformerTune, error) {
	out := make([]map[string]any, 0, len(param.Records))
	for _, rec := range param.Records {
		r := shallowClone(rec)
		bucket(r)
		out = append(out, r)
	}
	return &models.TransformerTune{Action: models.ActionContinue, Records: out}, nil
}

func bucket(r map[string]any) {
	brand, _ := r["sub_brand"].(string)

	if brand == "district" {
		// event_category is already in the record from the source table; nothing to compute.
		// Ensure meal_period is absent so TypeCaster does not write a null field to ES.
		delete(r, "meal_period")
		return
	}

	placedAt, ok := toTime(r["placed_at"])
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

func toTime(v any) (time.Time, bool) {
	switch t := v.(type) {
	case time.Time:
		if t.IsZero() {
			return time.Time{}, false
		}
		return t, true
	case *time.Time:
		if t == nil || t.IsZero() {
			return time.Time{}, false
		}
		return *t, true
	case string:
		if t == "" {
			return time.Time{}, false
		}
		parsed, err := time.Parse(time.RFC3339, t)
		if err != nil {
			return time.Time{}, false
		}
		return parsed, true
	}
	return time.Time{}, false
}

func shallowClone(src map[string]any) map[string]any {
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
