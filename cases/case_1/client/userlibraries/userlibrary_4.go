package client_userlibrary

import (
	"etlfunnel/execution/cast"
	"etlfunnel/execution/models"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// GetAuxPostgresConn retrieves and casts the auxiliary PostgreSQL connection from the map.
// Callers should wrap the returned error with component context.
func GetAuxPostgresConn(connMap map[string]models.IDatabaseEngine) (*pgx.Conn, error) {
	engine, ok := connMap[AuxDBKey]
	if !ok {
		return nil, fmt.Errorf("auxiliary connection %q not found", AuxDBKey)
	}
	conn, err := cast.CastAsPostgresDBConnection(engine)
	if err != nil {
		return nil, fmt.Errorf("failed to cast AuxDB connection: %w", err)
	}
	return conn, nil
}
