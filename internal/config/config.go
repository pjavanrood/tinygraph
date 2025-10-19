package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config represents the complete configuration for the tinygraph system
type Config struct {
	QueryManager QueryManagerConfig `yaml:"query_manager"`
	Shards       []ShardConfig      `yaml:"shards"`
	Partitioning PartitioningConfig `yaml:"partitioning"`
	Replication  ReplicationConfig  `yaml:"replication"`
}

// QueryManagerConfig holds Query Manager specific settings
type QueryManagerConfig struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

// ShardConfig represents a single shard's configuration
type ShardConfig struct {
	ID   int    `yaml:"id"`
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

// PartitioningConfig defines how data is partitioned across shards
type PartitioningConfig struct {
	Algorithm string `yaml:"algorithm"` // e.g., "hash", "range", "consistent_hash"
}

// ReplicationConfig defines the replication strategy
type ReplicationConfig struct {
	Strategy          string `yaml:"strategy"`           // e.g., "none", "master_slave", "multi_master"
	ReplicationFactor int    `yaml:"replication_factor"` // Number of replicas per shard
}

// LoadConfig loads configuration from a YAML file
func LoadConfig(configPath string) (*Config, error) {
	// Read the config file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Parse YAML
	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &config, nil
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	// Validate Query Manager
	if c.QueryManager.Port <= 0 || c.QueryManager.Port > 65535 {
		return fmt.Errorf("invalid query manager port: %d", c.QueryManager.Port)
	}

	// Validate Shards
	if len(c.Shards) == 0 {
		return fmt.Errorf("at least one shard must be configured")
	}

	shardIDs := make(map[int]bool)
	for _, shard := range c.Shards {
		if shard.ID < 0 {
			return fmt.Errorf("invalid shard ID: %d", shard.ID)
		}
		if shardIDs[shard.ID] {
			return fmt.Errorf("duplicate shard ID: %d", shard.ID)
		}
		shardIDs[shard.ID] = true

		if shard.Port <= 0 || shard.Port > 65535 {
			return fmt.Errorf("invalid port for shard %d: %d", shard.ID, shard.Port)
		}
		if shard.Host == "" {
			return fmt.Errorf("host cannot be empty for shard %d", shard.ID)
		}
	}

	// Validate Partitioning
	validAlgorithms := map[string]bool{
		"hash":            true,
	}
	if !validAlgorithms[c.Partitioning.Algorithm] {
		return fmt.Errorf("invalid partitioning algorithm: %s (valid options: hash)", c.Partitioning.Algorithm)
	}

	// Validate Replication
	validStrategies := map[string]bool{
		"none":                       true,
		"read_write_primary":         true,
		"read_replica_write_primary": true,
	}
	if !validStrategies[c.Replication.Strategy] {
		return fmt.Errorf("invalid replication strategy: %s (valid options: none, master_slave, multi_master)", c.Replication.Strategy)
	}

	if c.Replication.ReplicationFactor < 0 {
		return fmt.Errorf("replication factor cannot be negative: %d", c.Replication.ReplicationFactor)
	}

	if c.Replication.Strategy != "none" && c.Replication.ReplicationFactor == 0 {
		return fmt.Errorf("replication factor must be > 0 when replication strategy is not 'none'")
	}

	return nil
}

// GetShardByID returns a shard configuration by its ID
func (c *Config) GetShardByID(id int) (*ShardConfig, error) {
	for _, shard := range c.Shards {
		if shard.ID == id {
			return &shard, nil
		}
	}
	return nil, fmt.Errorf("shard with ID %d not found", id)
}

// GetShardAddress returns the full address (host:port) for a shard
func (s *ShardConfig) GetAddress() string {
	return fmt.Sprintf("%s:%d", s.Host, s.Port)
}
