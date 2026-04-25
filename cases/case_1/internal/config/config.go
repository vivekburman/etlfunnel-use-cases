// Package config holds shared DB connection config for all setup scripts.
package config

import "fmt"

// MySQLDSN returns a DSN string for a given MySQL container.
// host is typically "127.0.0.1" when running scripts from the host machine.
func MySQLDSN(host string, port int, dbName string) string {
	return fmt.Sprintf("etl_user:etl_pass@tcp(%s:%d)/%s?parseTime=true&multiStatements=true", host, port, dbName)
}

// PostgresDSN returns a DSN string for a Postgres instance.
func PostgresDSN(host string, port int, dbName string) string {
	return fmt.Sprintf("host=%s port=%d user=etl_user password=etl_pass dbname=%s sslmode=disable", host, port, dbName)
}

// Defaults
const (
	DBHost          = "127.0.0.1"
	DestinationPort = 5432
	AuxDBPort       = 5433
	DestinationDB   = "destination_db"
	AuxDB           = "auxdb"
)
