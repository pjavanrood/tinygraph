package main

import (
	"flag"

	"github.com/pjavanrood/tinygraph/internal/config"
	"github.com/pjavanrood/tinygraph/internal/util"
	"github.com/pjavanrood/tinygraph/pkg/shard"
)

var log = util.New("Shard", util.LogLevelInfo)

func main() {
	// Parse command-line flags
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	shardID := flag.Int("shard-id", -1, "The Shard ID to be used")
	replicaID := flag.Int("replica-id", -1, "The Replica ID within the shard")
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Update log level from config
	log.SetLevel(cfg.GetLogLevel())

	log.Println("Starting Shard Replica...")

	log.Printf("Loaded configuration with %d shards", len(cfg.Shards))
	log.Printf("Partitioning: %s, Replication: %s", cfg.Partitioning.Algorithm, cfg.Replication.Strategy)

	// Validate shard ID
	if *shardID == -1 || *shardID >= len(cfg.Shards) {
		log.Fatalf("Invalid Shard ID: %d (must be between 0 and %d), use -shard-id flag", *shardID, len(cfg.Shards)-1)
	}

	// Get shard configuration
	shardConfig, err := cfg.GetShardByID(*shardID)
	if err != nil {
		log.Fatalf("Failed to get shard configuration: %v", err)
	}

	// Validate replica ID
	if *replicaID == -1 || *replicaID >= len(shardConfig.Replicas) {
		log.Fatalf("Invalid Replica ID: %d for shard %d (must be between 0 and %d), use -replica-id flag",
			*replicaID, *shardID, len(shardConfig.Replicas)-1)
	}

	// Get replica configuration
	replicaConfig, err := shardConfig.GetReplicaByID(*replicaID)
	if err != nil {
		log.Fatalf("Failed to get replica configuration: %v", err)
	}

	log.Printf("Starting Shard %d, Replica %d", *shardID, *replicaID)
	log.Printf("RPC Address: %s", replicaConfig.GetRPCAddress())
	log.Printf("Raft Address: %s", replicaConfig.GetRaftAddress())
	log.Printf("Raft Data Directory: %s", replicaConfig.RaftDir)
	log.Printf("Bootstrap: %v", replicaConfig.Bootstrap)

	// Create Shard with Raft consensus
	shardInstance, err := shard.NewShard(cfg, *shardID, *replicaID)
	if err != nil {
		log.Fatalf("Failed to create Shard: %v", err)
	}

	log.Printf("Shard created successfully for Shard %d, Replica %d", *shardID, *replicaID)

	// Start the shard server (blocks)
	if err := shardInstance.Start(replicaConfig.GetRPCAddress()); err != nil {
		log.Fatalf("Failed to start Shard %d Replica %d: %v", *shardID, *replicaID, err)
	}
}
