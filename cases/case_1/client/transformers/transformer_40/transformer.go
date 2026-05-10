package client_transformer_40

import (
	"crypto/sha256"
	"encoding/hex"
	"etlfunnel/execution/models"
	"fmt"
	"strings"
)

// PIIMasker: SHA-256 hash Aadhaar and PAN fields so raw values never reach the destination.
// Aircel records may arrive with aadhaar/pan already hashed — idempotent: a valid hex-SHA256
// string (64 hex chars) is left unchanged.

const sha256HexLen = 64

func hashField(raw any) (string, error) {
	var s string
	switch v := raw.(type) {
	case string:
		s = v
	case []byte:
		s = string(v)
	default:
		return "", fmt.Errorf("unexpected type %T for PII field", raw)
	}

	s = strings.TrimSpace(s)
	if s == "" {
		return "", nil
	}

	// Already hashed — leave it alone.
	if len(s) == sha256HexLen && isHex(s) {
		return s, nil
	}

	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:]), nil
}

func isHex(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	rec := param.Record

	for _, piiField := range []string{"aadhaar", "pan"} {
		val, ok := rec[piiField]
		if !ok || val == nil {
			continue
		}
		hashed, err := hashField(val)
		if err != nil {
			return nil, fmt.Errorf("PIIMasker: field %q: %w", piiField, err)
		}
		rec[piiField+"_hash"] = hashed
		delete(rec, piiField)
	}

	return rec, nil
}
