package client_userlibrary

import "time"

// ToTime attempts to read a time.Time from any value.
// Accepts time.Time, *time.Time, and RFC3339 strings.
// Returns (zero, false) for nil, zero, empty, or unparseable values.
func ToTime(v any) (time.Time, bool) {
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
