package config

import "fmt"

const (
	DBHost     = "localhost"
	PGPort     = 5433
	PGDatabase = "payments_db"
	AuxDBPort  = 5435
	AuxDB      = "auxdb"
	DBUser     = "etl_user"
	DBPass     = "etl_pass"
)

func PostgresDSN(host string, port int, database string) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", DBUser, DBPass, host, port, database)
}
