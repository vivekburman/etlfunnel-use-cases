// Package config centralizes environment-variable driven settings shared by
// every cmd/ tool in this case study, with local-development defaults.
package config

import "os"

type Config struct {
	AuxDBDSN      string
	MongoURI      string
	MongoDatabase string
	FCSnapshotDir string
	FCMovementDir string
}

func Load() Config {
	return Config{
		AuxDBDSN:      getEnv("AUXDB_DSN", "postgresql://etl_user:etl_pass@localhost:5446/auxdb?sslmode=disable"),
		MongoURI:      getEnv("MONGO_URI", "mongodb://localhost:27017"),
		MongoDatabase: getEnv("MONGO_DATABASE", "fc_inventory"),
		FCSnapshotDir: getEnv("FC_SNAPSHOT_DIR", "/data/fc_inventory_snapshots"),
		FCMovementDir: getEnv("FC_MOVEMENT_DIR", "/data/wms_stock_movements"),
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
