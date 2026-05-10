package client_orchestrator_6

import (
	"fmt"

	"etlfunnel/execution/cast"
	"etlfunnel/execution/models"
)

// PipelineOrchestrator materialises each pipeline template into one tune per
// discovered source table. The real table name is stored in ReplicaProps["table"]
// so connectors can look it up without hardcoding anything. ReplicaName is a
// human-readable label only — it is not used for table resolution.
func PipelineOrchestrator(param *models.PipelineOrchestratorProps) ([]models.PipelineOrchestratorTune, error) {
	var tunes []models.PipelineOrchestratorTune

	for _, pipeline := range param.Pipelines {
		tableBase := pipeline.EntityBaseName
		if tableBase == "" {
			return nil, fmt.Errorf("pipeline %q: entityBaseName not set in flow definition", pipeline.Name)
		}

		tables, err := discoverTableNames(pipeline.SourceDBConn, tableBase)
		if err != nil {
			return nil, fmt.Errorf("pipeline %q: %w", pipeline.Name, err)
		}
		if len(tables) == 0 {
			return nil, fmt.Errorf("pipeline %q: no source tables found for base %q", pipeline.Name, tableBase)
		}

		for i, tableName := range tables {
			tunes = append(tunes, models.PipelineOrchestratorTune{
				ParentName:  pipeline.Name,
				ReplicaName: fmt.Sprintf("%s_%d", pipeline.Name, i+1),
				ReplicaProps: map[string]any{
					"table":          tableName,
					"entityBaseName": tableBase,
				},
			})
		}
	}

	return tunes, nil
}

// discoverTableNames queries the source MySQL DB for all tables whose name
// starts with tableBase followed by an underscore, returning the full table
// names sorted alphabetically. Each returned name becomes one pipeline replica.
func discoverTableNames(sourceConn models.IDatabaseEngine, tableBase string) ([]string, error) {
	if sourceConn == nil {
		return nil, nil
	}

	mysqlConn, err := cast.CastAsMySQLDBConnection(sourceConn)
	if err != nil {
		return nil, fmt.Errorf("cast source conn: %w", err)
	}

	query := fmt.Sprintf(`
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = DATABASE()
		  AND table_name REGEXP '^%s_.+_[0-9]+$'
		ORDER BY table_name`, tableBase)

	result, execErr := mysqlConn.Execute(query)
	if execErr != nil {
		return nil, fmt.Errorf("query tables: %w", execErr)
	}

	tables := make([]string, 0, len(result.Values))
	for _, row := range result.Values {
		if len(row) > 0 {
			tables = append(tables, string(row[0].AsString()))
		}
	}
	return tables, nil
}
