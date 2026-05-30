package config

import (
	"fmt"
	"os"
)

// Config holds runtime connection parameters loaded from environment variables.
type Config struct {
	MSSQLHost string
	MSSQLPort string
	MSSQLDB   string
	MSSQLUser string
	MSSQLPass string
	AuxDBDSN  string
	SeedURL   string
}

// Load reads all connection settings from environment variables.
// Sensible defaults match the docker-compose ports.
func Load() *Config {
	return &Config{
		MSSQLHost: getEnv("MSSQL_HOST", "localhost"),
		MSSQLPort: getEnv("MSSQL_PORT", "1433"),
		MSSQLDB:   getEnv("MSSQL_DB", "analytics_warehouse"),
		MSSQLUser: getEnv("MSSQL_USER", "etl_writer"),
		MSSQLPass: getEnv("MSSQL_PASS", "Etl_Pass_456!"),
		AuxDBDSN:  getEnv("AUXDB_DSN", "host=localhost port=5446 dbname=auxdb user=etl_user password=etl_pass sslmode=disable"),
		SeedURL:   getEnv("SEEDER_URL", "http://localhost:9090"),
	}
}

// MSSQLDSN builds the go-mssqldb connection string.
func (c *Config) MSSQLDSN() string {
	return fmt.Sprintf("sqlserver://%s:%s@%s:%s?database=%s",
		c.MSSQLUser, c.MSSQLPass, c.MSSQLHost, c.MSSQLPort, c.MSSQLDB)
}

// MSSQLSADSN builds the SA (sysadmin) connection string for schema setup.
func MSSQLSADSN(host, port, saPass string) string {
	return fmt.Sprintf("sqlserver://SA:%s@%s:%s", saPass, host, port)
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}
