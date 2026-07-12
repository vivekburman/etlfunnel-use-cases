package main

// mongo_init — creates the fc_inventory database's unique indexes.
//
// Indexes created:
//   inventory_snapshots: unique (fc_id, sku_id, snapshot_date) — matches the
//     "_mongo_id" key transformer_99 stamps, so a re-scan of the same day's
//     directory upserts in place instead of duplicating.
//   stock_movements: unique (movement_id) — already globally unique.
//
// USAGE:
//   go run ./cmd/mongo_init
//   make mongo-init

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/streamcraft/fc-inventory-etl/case6/internal/config"
)

func main() {
	cfg := config.Load()
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(cfg.MongoURI))
	if err != nil {
		log.Fatalf("connect to MongoDB: %v", err)
	}
	defer client.Disconnect(ctx)

	db := client.Database(cfg.MongoDatabase)

	log.Println("=== MongoDB Setup — FC Inventory Ops Pipeline ===")

	_, err = db.Collection("inventory_snapshots").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "fc_id", Value: 1}, {Key: "sku_id", Value: 1}, {Key: "snapshot_date", Value: 1}},
		Options: options.Index().SetUnique(true).SetName("fc_sku_snapshot_date"),
	})
	if err != nil {
		log.Fatalf("create inventory_snapshots index: %v", err)
	}
	log.Println("  done: inventory_snapshots unique (fc_id, sku_id, snapshot_date)")

	_, err = db.Collection("stock_movements").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "movement_id", Value: 1}},
		Options: options.Index().SetUnique(true).SetName("movement_id"),
	})
	if err != nil {
		log.Fatalf("create stock_movements index: %v", err)
	}
	log.Println("  done: stock_movements unique (movement_id)")

	log.Println("=== MongoDB setup complete ===")
}
