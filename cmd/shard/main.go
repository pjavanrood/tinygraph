package main

import (
	"flag"
	"log"

	"github.com/pjavanrood/tinygraph/internal/config"
	"github.com/pjavanrood/tinygraph/pkg/shard"
)

func main() {
	// Parse command-line flags
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	shardNum := flag.Int("id", -1, "The Shard ID to be used")
	flag.Parse()

	log.Println("Starting Shard...")

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load configuratioN: %v", err)
		return
	}

	log.Printf("Loaded configuration with %d shards", len(cfg.Shards))
	log.Printf("Partitioning: %s, Replication: %s", cfg.Partitioning.Algorithm, cfg.Replication.Strategy)

	// try to start up self if within shard limit (if ie. 3 shards, then only shard IDs of 0, 1, 2 are valid)
	if *shardNum == -1 || *shardNum >= len(cfg.Shards) {
		log.Fatalf("Invalid Shard ID, %d, for given configuration (%d shards), ensure proper shard ID is passed with '-id' flag", *shardNum, len(cfg.Shards))
		return
	}

	shard := shard.NewShard(cfg, *shardNum)
	if err := shard.Start(); err != nil {
		log.Fatalf("Failed to start Shard %d: %v", *shardNum, err)
		return
	}
}
