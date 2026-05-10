package client_transformer_31

import (
	"context"
	ulib "etlfunnel/execution/client/userlibraries"
	"etlfunnel/execution/models"
	"fmt"
)

// DedupChecker: guard against cross-source MSISDN collisions using the AuxDB dedup_registry.
//
// AuxDB table schema:
//
//	dedup_registry(
//	  id               BIGSERIAL PRIMARY KEY,
//	  msisdn           VARCHAR(15)  NOT NULL UNIQUE,
//	  canonical_id     UUID         NOT NULL DEFAULT gen_random_uuid(),
//	  winning_company  VARCHAR(30)  NOT NULL,
//	  all_sources      TEXT[]       NOT NULL,
//	  conflict_detected  BOOLEAN    NOT NULL DEFAULT FALSE,
//	  conflict_resolved  BOOLEAN    NOT NULL DEFAULT FALSE,
//	  resolution_method  VARCHAR(50),
//	  created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
//	  updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
//	)
//
// Logic:
//  1. Look up MSISDN in dedup_registry.
//  2. Not found → register it (winning_company = current, all_sources = {current}).
//  3. Found, same company → update all_sources if needed, attach canonical_id and continue.
//  4. Found, different company → mark conflict, send to backlog for manual resolution.

func Transformer(param *models.TransformerProps) (map[string]any, error) {
	rec := param.Record

	msisdnVal, ok := rec["msisdn"]
	if !ok || msisdnVal == nil {
		return nil, fmt.Errorf("DedupChecker: msisdn is nil — cannot check dedup registry")
	}
	msisdn, ok := msisdnVal.(string)
	if !ok {
		return nil, fmt.Errorf("DedupChecker: msisdn is not a string, got %T", msisdnVal)
	}

	companyVal, _ := rec["source_company"]
	currentCompany, _ := companyVal.(string)

	pgConn, err := ulib.GetAuxPostgresConn(param.AuxiliaryDBConnMap)
	if err != nil {
		return nil, fmt.Errorf("DedupChecker: %w", err)
	}

	ctx := context.Background()

	var canonicalID, winningCompany string
	selectQ := `SELECT canonical_id::text, winning_company FROM dedup_registry WHERE msisdn = $1`
	row := pgConn.QueryRow(ctx, selectQ, msisdn)
	scanErr := row.Scan(&canonicalID, &winningCompany)

	switch {
	case scanErr != nil && ulib.IsNoRows(scanErr):
		// First time we see this MSISDN — register it.
		insertQ := `
			INSERT INTO dedup_registry (msisdn, winning_company, all_sources)
			VALUES ($1, $2, ARRAY[$3])
			RETURNING canonical_id::text`
		if err := pgConn.QueryRow(ctx, insertQ, msisdn, currentCompany, currentCompany).Scan(&canonicalID); err != nil {
			return nil, fmt.Errorf("DedupChecker: failed to register MSISDN %q: %w", msisdn, err)
		}
		rec["canonical_id"] = canonicalID

	case scanErr != nil:
		return nil, fmt.Errorf("DedupChecker: dedup_registry query failed: %w", scanErr)

	case winningCompany != currentCompany:
		// Cross-source conflict — flag it and send to backlog for manual resolution.
		updateQ := `
			UPDATE dedup_registry
			SET all_sources      = array_append(all_sources, $2),
			    conflict_detected = TRUE,
			    updated_at        = NOW()
			WHERE msisdn = $1 AND NOT ($2 = ANY(all_sources))`
		if _, execErr := pgConn.Exec(ctx, updateQ, msisdn, currentCompany); execErr != nil {
			return nil, fmt.Errorf("DedupChecker: failed to record conflict for MSISDN %q: %w", msisdn, execErr)
		}
		return nil, fmt.Errorf(
			"DedupChecker: MSISDN %q already registered under company %q (current: %q) — dedup conflict",
			msisdn, winningCompany, currentCompany,
		)

	default:
		// Same company — attach canonical ID and ensure source is tracked.
		updateQ := `
			UPDATE dedup_registry
			SET all_sources = array_append(all_sources, $2),
			    updated_at  = NOW()
			WHERE msisdn = $1 AND NOT ($2 = ANY(all_sources))`
		if _, execErr := pgConn.Exec(ctx, updateQ, msisdn, currentCompany); execErr != nil {
			return nil, fmt.Errorf("DedupChecker: failed to update sources for MSISDN %q: %w", msisdn, execErr)
		}
		rec["canonical_id"] = canonicalID
	}

	return rec, nil
}

