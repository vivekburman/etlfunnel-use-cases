// Package config centralizes environment-variable driven settings shared by
// every cmd/ tool in this case study, with local-development defaults.
package config

import (
	"os"
	"path/filepath"
	"runtime"
)

type Config struct {
	AuxDBDSN      string
	MongoURI      string
	MongoDatabase string
	FCSnapshotDir string
	FCMovementDir string
}

func Load() Config {
	defaultSnapshotDir, defaultMovementDir := defaultDataDirs()
	return Config{
		AuxDBDSN:      getEnv("AUXDB_DSN", "postgresql://etl_user:etl_pass@localhost:5446/auxdb?sslmode=disable"),
		MongoURI:      getEnv("MONGO_URI", "mongodb://etl_user:etl_pass@localhost:27017/?authSource=admin"),
		MongoDatabase: getEnv("MONGO_DATABASE", "fc_inventory"),
		FCSnapshotDir: getEnv("FC_SNAPSHOT_DIR", defaultSnapshotDir),
		FCMovementDir: getEnv("FC_MOVEMENT_DIR", defaultMovementDir),
	}
}

// defaultDataDirs mirrors the Makefile's OS-conditional FC_SNAPSHOT_DIR/
// FC_MOVEMENT_DIR defaults: the Windows path matches streamcraftexecution's
// hardcoded /data/... default, while macOS/Linux fall back to a user-space
// dir under the repo's tmp/ scratch dir since root "/" isn't writable there.
func defaultDataDirs() (snapshotDir, movementDir string) {
	if runtime.GOOS == "windows" {
		return "/data/fc_inventory_snapshots", "/data/wms_stock_movements"
	}
	wd, err := os.Getwd()
	if err != nil {
		wd = "."
	}
	return filepath.Join(wd, "tmp", "data", "fc_inventory_snapshots"),
		filepath.Join(wd, "tmp", "data", "wms_stock_movements")
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
