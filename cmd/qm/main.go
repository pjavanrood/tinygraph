package main

import (
	"flag"

	"github.com/pjavanrood/tinygraph/internal/util"
	"github.com/pjavanrood/tinygraph/internal/config"
	"github.com/pjavanrood/tinygraph/pkg/qm"
)

var log = util.New("QueryManager")

func main() {
	// Parse command-line flags
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	log.Println("Starting Query Manager...")

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	log.Printf("Loaded configuration with %d shards", len(cfg.Shards))
	log.Printf("Partitioning: %s, Replication: %s", cfg.Partitioning.Algorithm, cfg.Replication.Strategy)

	// Create and start the query manager
	queryManager := qm.NewQueryManager(cfg)
	if err := queryManager.Start(); err != nil {
		log.Fatalf("Failed to start Query Manager: %v", err)
	}
}
