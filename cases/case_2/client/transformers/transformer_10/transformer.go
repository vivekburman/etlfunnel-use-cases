package client_transformer_10

// Zomato Platform Order Intelligence — transformer_10: PIIMasker (STEP-23)
//
// Removes Personally Identifiable Information before documents are indexed
// into Elasticsearch:
//
//   1. customer_id  — SHA-256 hashed and stored as customer_id_hash.
//                     The original customer_id field is deleted.
//   2. phone        — stripped entirely (not stored in ES)
//   3. email        — stripped entirely (not stored in ES)
//   4. address      — stripped entirely (not stored in ES)
//
// Idempotency (hot-flow re-entry after a cold-indexed record is updated):
//   If customer_id is already a 64-character hex string (a prior SHA-256
//   hash), it is copied to customer_id_hash without re-hashing.  This
//   prevents double-hashing when a WAL UPDATE event re-runs through the
//   same transformer chain.

import (
	"crypto/sha256"
	"encoding/hex"
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
	"fmt"
)

// piiFields are removed from the record entirely.
var piiFields = []string{"phone", "email", "address"}

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	return mask(param.Record), nil
}

func mask(src map[string]any) map[string]any {
	r := ulib.ShallowClone(src)

	// Hash customer_id → customer_id_hash.
	if raw, ok := r["customer_id"]; ok {
		r["customer_id_hash"] = hashCustomerID(raw)
		delete(r, "customer_id")
	}

	// Strip PII fields.
	for _, field := range piiFields {
		delete(r, field)
	}

	return r
}

// hashCustomerID returns the SHA-256 hex digest of the customer identifier.
// If the value is already a 64-character hex string it is returned as-is
// (idempotent for hot-flow re-entry).
func hashCustomerID(v any) string {
	s := fmt.Sprintf("%v", v)

	// Detect an already-hashed value: 64 lowercase hex chars.
	if len(s) == 64 && isHex(s) {
		return s
	}

	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func isHex(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

