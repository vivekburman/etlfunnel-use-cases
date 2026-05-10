package client_userlibrary

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

var mysqlDateFormats = []string{
	"2006-01-02 15:04:05",
	"2006-01-02",
	"2006-01-02T15:04:05Z",
	"2006-01-02T15:04:05",
}

// ParseDecimal coerces MySQL DECIMAL/FLOAT values (commonly []byte from the driver) to float64.
// Handles float64, float32, int, int64, []byte, and string.
func ParseDecimal(raw any) (float64, error) {
	switch v := raw.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case []byte:
		return strconv.ParseFloat(string(v), 64)
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("unexpected decimal type %T", raw)
	}
}

// RoundTo rounds a float64 to the given number of decimal places.
func RoundTo(val float64, places int) float64 {
	shift := math.Pow(10, float64(places))
	return math.Round(val*shift) / shift
}

// ParseBool coerces MySQL TINYINT(1) and related types to bool.
func ParseBool(raw any) (bool, error) {
	switch v := raw.(type) {
	case bool:
		return v, nil
	case int8:
		return v != 0, nil
	case int64:
		return v != 0, nil
	case int:
		return v != 0, nil
	case uint8:
		return v != 0, nil
	case []byte:
		return string(v) != "0", nil
	case string:
		return v != "0" && strings.ToLower(v) != "false", nil
	default:
		return false, fmt.Errorf("unexpected bool type %T", raw)
	}
}

// ParseDate coerces MySQL DATETIME/DATE values to time.Time (UTC).
func ParseDate(raw any) (time.Time, error) {
	var s string
	switch v := raw.(type) {
	case string:
		s = v
	case []byte:
		s = string(v)
	case time.Time:
		return v.UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("unexpected date type %T", raw)
	}
	s = strings.TrimSpace(s)
	if s == "0000-00-00 00:00:00" || s == "0000-00-00" {
		return time.Time{}, nil
	}
	for _, layout := range mysqlDateFormats {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse date %q", s)
}
