package config

import "os"

// Config holds runtime connection parameters loaded from environment variables.
// Defaults match the docker-compose ports defined in this case.
type Config struct {
	AuxDBDSN        string
	KafkaBrokers    string
	CassandraHosts  string
	SeederPort      string
	SeederURL       string
	ZeptoAPIToken   string
	ZeptoAPIBaseURL string
}

func Load() *Config {
	return &Config{
		AuxDBDSN:        getEnv("AUXDB_DSN", "host=localhost port=5446 dbname=auxdb user=etl_user password=etl_pass sslmode=disable"),
		KafkaBrokers:    getEnv("KAFKA_BROKERS", "localhost:9092"),
		CassandraHosts:  getEnv("CASSANDRA_HOSTS", "localhost"),
		SeederPort:      getEnv("SEEDER_PORT", "11334"),
		SeederURL:       getEnv("SEEDER_URL", "http://localhost:11334"),
		ZeptoAPIToken:   getEnv("ZEPTO_API_TOKEN", "dev-token"),
		ZeptoAPIBaseURL: getEnv("ZEPTO_API_BASE_URL", "http://localhost:11334"),
	}
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}
