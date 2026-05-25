package config

import "fmt"

const (
	DBHost        = "127.0.0.1"
	AuxDBPort     = 5445
	AuxDB         = "auxdb"
	RedisAddr     = "127.0.0.1:6379"
	RedisPassword = "etl_pass"
	ESAddress     = "http://127.0.0.1:9200"
	ESUser        = "elastic"
	ESPassword    = "etl_pass"
)

func PostgresDSN(host string, port int, dbName string) string {
	return fmt.Sprintf("host=%s port=%d user=etl_user password=etl_pass dbname=%s sslmode=disable", host, port, dbName)
}
