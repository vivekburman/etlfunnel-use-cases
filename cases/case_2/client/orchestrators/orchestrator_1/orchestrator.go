package client_orchestrator_1

// Zomato Platform Order Intelligence — orchestrator_1: Cold Flow (STEP-11)
//
// Discovers all city-split source tables for a given brand + entity and
// returns one PipelineOrchestratorTune per split, ordered by split index.
//
// Table naming convention (set by cmd/pg_schema and cmd/seeder):
//   {entity}_{city}_{n}   e.g.  orders_delhi_1, orders_delhi_2, orders_mumbai_1
//
// Discovery query (runs against each brand's source Postgres):
//   SELECT table_name
//   FROM information_schema.tables
//   WHERE table_schema = 'public'
//     AND table_name ~ '^{entity}_{city}_[0-9]+$'
//   ORDER BY table_name
//
// Resume support:
//   For each discovered split, the orchestrator reads the last checkpoint from
//   AuxDB `pipeline_checkpoints` and sets ConnectorProps.LastPK so the cold
//   source connector starts paginating from where it left off.
//
// The orchestrator runs once per pipeline startup — not on every batch tick.
// After discovery, the framework iterates the returned tunes sequentially
// (or in parallel, controlled by PipelineOrchestratorTune.Concurrency).

import (
	"context"
	"etlfunnel/execution/models"
	ulib "etlfunnel/execution/client/userlibraries"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Orchestrate discovers all city-split tables for the pipeline's entity and
// returns one OrchestratorTune per split with a populated ConnectorProps.
func Orchestrate(ctx context.Context, param *models.OrchestratorProps) ([]*models.OrchestratorTune, error) {
	entityBase := param.EntityBaseName // e.g. "orders"
	brand := param.Brand               // e.g. "zomato_food"

	sourceConn, err := param.SourceDBConn(brand)
	if err != nil {
		return nil, fmt.Errorf("orchestrator_1: source DB connect for %s: %w", brand, err)
	}

	splits, err := discoverSplits(ctx, sourceConn, entityBase)
	if err != nil {
		return nil, fmt.Errorf("orchestrator_1: table discovery %s/%s: %w", brand, entityBase, err)
	}

	if len(splits) == 0 {
		param.State.GetLogger().Warn(fmt.Sprintf(
			"orchestrator_1: no tables found for entity=%s brand=%s — pipeline will be a no-op",
			entityBase, brand,
		))
		return nil, nil
	}

	// Read last checkpoints from AuxDB in one query for all splits.
	checkpoints, err := loadCheckpoints(ctx, param, brand, entityBase)
	if err != nil {
		// Non-fatal: start from 0 if AuxDB is unavailable.
		param.State.GetLogger().Warn(fmt.Sprintf(
			"orchestrator_1: checkpoint load failed (%v) — starting from pk=0 for all splits", err,
		))
		checkpoints = map[splitKey]int64{}
	}

	tunes := make([]*models.OrchestratorTune, 0, len(splits))
	for _, s := range splits {
		ck := splitKey{city: s.city, splitIndex: s.index}
		lastPK := checkpoints[ck]

		tunes = append(tunes, &models.OrchestratorTune{
			ConnectorProps: &models.ConnectorProps{
				TableName:  s.tableName,
				LastPK:     lastPK,
				BatchSize:  param.BatchSize,
				SplitIndex: s.index,
				City:       s.city,
				Brand:      brand,
				Entity:     entityBase,
			},
			// Parallelism is managed at the flow level; each split runs sequentially
			// within its pipeline lane to maintain PK ordering guarantees.
			Concurrency: 1,
		})
	}

	param.State.GetLogger().Debug(fmt.Sprintf(
		"orchestrator_1: discovered %d split(s) for %s/%s (last PKs loaded)",
		len(tunes), brand, entityBase,
	))

	return tunes, nil
}

// tableSplit is a parsed city-split table descriptor.
type tableSplit struct {
	tableName string
	city      string
	index     int
}

// splitKey is used to look up checkpoints.
type splitKey struct {
	city       string
	splitIndex int
}

// discoverSplits queries information_schema for all matching city-split tables.
func discoverSplits(ctx context.Context, conn models.IDBConn, entityBase string) ([]tableSplit, error) {
	// Pattern: ^{entity}_{anything}_{digits}$
	// We match broadly and filter in Go to avoid Postgres regex dialect issues.
	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public'
		  AND table_name LIKE $1
		ORDER BY table_name`

	rows, err := conn.Query(ctx, query, entityBase+"_%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Compile a strict pattern: {entity}_{city}_{n}
	pattern := regexp.MustCompile(`^` + regexp.QuoteMeta(entityBase) + `_([a-z]+)_([0-9]+)$`)

	var splits []tableSplit
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		m := pattern.FindStringSubmatch(name)
		if m == nil {
			continue
		}
		idx, _ := strconv.Atoi(m[2])
		splits = append(splits, tableSplit{
			tableName: name,
			city:      m[1],
			index:     idx,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Sort: city alphabetically, then split index ascending.
	sort.Slice(splits, func(i, j int) bool {
		if splits[i].city != splits[j].city {
			return splits[i].city < splits[j].city
		}
		return splits[i].index < splits[j].index
	})

	return splits, nil
}

// loadCheckpoints fetches last_processed_pk for all cold splits of this
// brand+entity from AuxDB pipeline_checkpoints.
func loadCheckpoints(
	ctx context.Context,
	param *models.OrchestratorProps,
	brand, entity string,
) (map[splitKey]int64, error) {
	pgConn, err := ulib.GetAuxPostgresConn(param.AuxiliaryDBConnMap)
	if err != nil {
		return nil, err
	}

	rows, err := pgConn.Query(ctx, `
		SELECT city, table_split_index, last_processed_pk
		FROM pipeline_checkpoints
		WHERE sub_brand = $1
		  AND entity    = $2
		  AND flow_type = 'cold'`,
		brand, strings.TrimPrefix(entity, "pipeline_"),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[splitKey]int64)
	for rows.Next() {
		var city string
		var splitIdx int
		var lastPK int64
		if scanErr := rows.Scan(&city, &splitIdx, &lastPK); scanErr == nil {
			result[splitKey{city: city, splitIndex: splitIdx}] = lastPK
		}
	}
	return result, rows.Err()
}
