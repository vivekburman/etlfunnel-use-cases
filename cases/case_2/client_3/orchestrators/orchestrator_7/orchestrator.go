package client_orchestrator_7

// Zomato Platform Order Intelligence — orchestrator_1: Cold Flow (STEP-11)
//
// Discovers all city-split source tables for each pipeline's entity and
// returns one PipelineOrchestratorTune per split, ordered by city then index.
//
// Table naming convention (set by cmd/pg_schema and cmd/seeder):
//   {entity}_{city}_{n}   e.g.  orders_delhi_1, orders_delhi_2, orders_mumbai_1
//
// Discovery query (runs against each brand's source Postgres):
//   SELECT table_name
//   FROM information_schema.tables
//   WHERE table_schema = 'public'
//     AND table_name LIKE '{entity}_%'
//   ORDER BY table_name
//
// The strict city-split pattern ({entity}_{city}_{digits}) is enforced in Go
// via regexp after the broad LIKE scan to avoid Postgres regex dialect issues.
//
// ReplicaProps keys consumed downstream by cold connectors:
//   "table"         — full table name (e.g. "orders_delhi_1")
//   "entityBaseName"— base entity name (e.g. "orders")
//   "city"          — city label parsed from table name
//   "splitIndex"    — numeric split index parsed from table name

import (
	"context"
	"etlfunnel/execution/cast"
	"etlfunnel/execution/models"
	"fmt"
	"regexp"
	"sort"
	"strconv"

	"github.com/jackc/pgx/v5"
)

// PipelineOrchestrator discovers all city-split tables for each pipeline and
// returns one PipelineOrchestratorTune per split.
func PipelineOrchestrator(param *models.PipelineOrchestratorProps) ([]models.PipelineOrchestratorTune, error) {
	var tunes []models.PipelineOrchestratorTune

	for _, pipeline := range param.Pipelines {
		entityBase := pipeline.EntityBaseName

		pgConn, err := cast.CastAsPostgresDBConnection(pipeline.SourceDBConn)
		if err != nil {
			return nil, fmt.Errorf("orchestrator_1: postgres cast for %s: %w", pipeline.Name, err)
		}

		splits, err := discoverSplits(pgConn, entityBase)
		if err != nil {
			return nil, fmt.Errorf("orchestrator_1: table discovery %s/%s: %w", pipeline.Name, entityBase, err)
		}

		if len(splits) == 0 {
			return nil, fmt.Errorf("orchestrator_1: no tables found for pipeline %q entity %q", pipeline.Name, entityBase)
		}

		for i, s := range splits {
			tunes = append(tunes, models.PipelineOrchestratorTune{
				ParentName:  pipeline.Name,
				ReplicaName: fmt.Sprintf("%s_%d", pipeline.Name, i+1),
				ReplicaProps: map[string]any{
					"table":          s.tableName,
					"entityBaseName": entityBase,
					"city":           s.city,
					"splitIndex":     s.index,
				},
			})
		}
	}

	return tunes, nil
}

// tableSplit is a parsed city-split table descriptor.
type tableSplit struct {
	tableName string
	city      string
	index     int
}

// discoverSplits queries information_schema for all matching city-split tables.
func discoverSplits(conn *pgx.Conn, entityBase string) ([]tableSplit, error) {
	rows, err := conn.Query(context.Background(), `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public'
		  AND table_name LIKE $1
		ORDER BY table_name`,
		entityBase+"_%",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Strict pattern: {entity}_{city}_{n}
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
