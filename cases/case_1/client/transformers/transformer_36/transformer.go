package client_transformer_36

import (
	"context"
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
	"fmt"
)

// DedupChecker: guard against cross-source MSISDN collisions using the AuxDB dedup_registry.
//
// Logic:
//   1. Look up MSISDN in dedup_registry.
//   2. Not found → register it and continue.
//   3. Found, same company → update canonical_id reference and continue.
//   4. Found, different company → return error so the record goes to backlog for manual review.

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	rec := param.Record

	msisdnVal, ok := rec["msisdn"]
	if !ok || msisdnVal == nil {
		// NullHandler already guards this; defensive check only.
		return nil, fmt.Errorf("DedupChecker: msisdn is nil — cannot check dedup registry")
	}
	msisdn, ok := msisdnVal.(string)
	if !ok {
		return nil, fmt.Errorf("DedupChecker: msisdn is not a string, got %T", msisdnVal)
	}

	currentCompany, _ := rec["source_company"].(string)

	pgConn, err := ulib.GetAuxPostgresConn(param.AuxiliaryDBConnMap)
	if err != nil {
		return nil, fmt.Errorf("DedupChecker: %w", err)
	}

	ctx := context.Background()

	var registeredCompany, canonicalID string
	selectQ := `SELECT canonical_id::text, winning_company FROM dedup_registry WHERE msisdn = $1`
	row := pgConn.QueryRow(ctx, selectQ, msisdn)
	scanErr := row.Scan(&canonicalID, &registeredCompany)

	switch {
	case scanErr != nil && ulib.IsNoRows(scanErr):
		// First time we see this MSISDN — register it; Postgres generates canonical_id via DEFAULT.
		insertQ := `INSERT INTO dedup_registry (msisdn, winning_company, all_sources) VALUES ($1, $2, ARRAY[$3]) RETURNING canonical_id::text`
		if err := pgConn.QueryRow(ctx, insertQ, msisdn, currentCompany, currentCompany).Scan(&canonicalID); err != nil {
			return nil, fmt.Errorf("DedupChecker: failed to register MSISDN %q: %w", msisdn, err)
		}
		rec["canonical_id"] = canonicalID

	case scanErr != nil:
		return nil, fmt.Errorf("DedupChecker: dedup_registry query failed: %w", scanErr)

	case registeredCompany != currentCompany:
		// Cross-source conflict — send to backlog for manual resolution.
		return nil, fmt.Errorf(
			"DedupChecker: MSISDN %q already registered under company %q (current: %q) — dedup conflict",
			msisdn, registeredCompany, currentCompany,
		)

	default:
		// Same company — record is a known subscriber, attach canonical ID.
		rec["canonical_id"] = canonicalID
	}

	return rec, nil
}
