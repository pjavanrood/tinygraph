package config

import (
	"fmt"
	"os"

	"github.com/pjavanrood/tinygraph/internal/util"
	"gopkg.in/yaml.v3"
)

// Config represents the complete configuration for the tinygraph system
type Config struct {
	QueryManager QueryManagerConfig `yaml:"query_manager"`
	Shards       []ShardConfig      `yaml:"shards"`
	Partitioning PartitioningConfig `yaml:"partitioning"`
	Replication  ReplicationConfig  `yaml:"replication"`
	Logging      LoggingConfig      `yaml:"logging"`
}

// QueryManagerConfig holds Query Manager specific settings
type QueryManagerConfig struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

// ShardConfig represents a single shard's configuration
type ShardConfig struct {
	ID       int             `yaml:"id"`
	Replicas []ReplicaConfig `yaml:"replicas"`
}

// ReplicaConfig represents a single replica's configuration within a shard
type ReplicaConfig struct {
	ID        int    `yaml:"id"`
	RPCHost   string `yaml:"rpc_host"`
	RPCPort   int    `yaml:"rpc_port"`
	RaftHost  string `yaml:"raft_host"`
	RaftPort  int    `yaml:"raft_port"`
	RaftDir   string `yaml:"raft_dir"`
	Bootstrap bool   `yaml:"bootstrap"`
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

// LoggingConfig defines the logging configuration
type LoggingConfig struct {
	Level string `yaml:"level"` // Options: "OFF", "FATAL", "ERROR", "WARN", "INFO", "DEBUG"
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

		// Validate replicas
		if len(shard.Replicas) == 0 {
			return fmt.Errorf("shard %d must have at least one replica", shard.ID)
		}

		replicaIDs := make(map[int]bool)
		bootstrapCount := 0
		for _, replica := range shard.Replicas {
			if replica.ID < 0 {
				return fmt.Errorf("invalid replica ID: %d for shard %d", replica.ID, shard.ID)
			}
			if replicaIDs[replica.ID] {
				return fmt.Errorf("duplicate replica ID: %d for shard %d", replica.ID, shard.ID)
			}
			replicaIDs[replica.ID] = true

			if replica.RPCPort <= 0 || replica.RPCPort > 65535 {
				return fmt.Errorf("invalid RPC port for shard %d replica %d: %d", shard.ID, replica.ID, replica.RPCPort)
			}
			if replica.RaftPort <= 0 || replica.RaftPort > 65535 {
				return fmt.Errorf("invalid Raft port for shard %d replica %d: %d", shard.ID, replica.ID, replica.RaftPort)
			}
			if replica.RPCHost == "" {
				return fmt.Errorf("RPC host cannot be empty for shard %d replica %d", shard.ID, replica.ID)
			}
			if replica.RaftHost == "" {
				return fmt.Errorf("Raft host cannot be empty for shard %d replica %d", shard.ID, replica.ID)
			}
			if replica.RaftDir == "" {
				return fmt.Errorf("Raft directory cannot be empty for shard %d replica %d", shard.ID, replica.ID)
			}
			if replica.Bootstrap {
				bootstrapCount++
			}
		}

		// Ensure exactly one replica is set to bootstrap per shard
		if bootstrapCount == 0 {
			return fmt.Errorf("shard %d must have exactly one replica with bootstrap=true", shard.ID)
		}
		if bootstrapCount > 1 {
			return fmt.Errorf("shard %d has %d replicas with bootstrap=true, but only one is allowed", shard.ID, bootstrapCount)
		}
	}

	// Validate Partitioning
	validAlgorithms := map[string]bool{
		"hash": true,
	}
	if !validAlgorithms[c.Partitioning.Algorithm] {
		return fmt.Errorf("invalid partitioning algorithm: %s (valid options: hash)", c.Partitioning.Algorithm)
	}

	// Validate Replication
	validStrategies := map[string]bool{
		"none": true,
		"raft": true,
	}
	if !validStrategies[c.Replication.Strategy] {
		return fmt.Errorf("invalid replication strategy: %s (valid options: none, raft)", c.Replication.Strategy)
	}

	if c.Replication.ReplicationFactor < 0 {
		return fmt.Errorf("replication factor cannot be negative: %d", c.Replication.ReplicationFactor)
	}

	if c.Replication.Strategy == "raft" && c.Replication.ReplicationFactor == 0 {
		return fmt.Errorf("replication factor must be > 0 when replication strategy is 'raft'")
	}

	// Validate that replication factor matches number of replicas
	if c.Replication.Strategy == "raft" {
		for _, shard := range c.Shards {
			if len(shard.Replicas) != c.Replication.ReplicationFactor {
				return fmt.Errorf("shard %d has %d replicas but replication_factor is %d",
					shard.ID, len(shard.Replicas), c.Replication.ReplicationFactor)
			}
		}
	}

	// Validate Logging
	validLogLevels := map[string]bool{
		"OFF":     true,
		"FATAL":   true,
		"ERROR":   true,
		"WARN":    true,
		"WARNING": true,
		"INFO":    true,
		"DEBUG":   true,
	}
	if c.Logging.Level == "" {
		c.Logging.Level = "INFO" // Default to INFO if not specified
	} else if !validLogLevels[c.Logging.Level] {
		return fmt.Errorf("invalid logging level: %s (valid options: OFF, FATAL, ERROR, WARN, INFO, DEBUG)", c.Logging.Level)
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

// GetReplicaByID returns a replica configuration by its ID
func (s *ShardConfig) GetReplicaByID(replicaID int) (*ReplicaConfig, error) {
	for _, replica := range s.Replicas {
		if replica.ID == replicaID {
			return &replica, nil
		}
	}
	return nil, fmt.Errorf("replica with ID %d not found in shard %d", replicaID, s.ID)
}

// GetAddress returns an RPC address to connect to this shard
// For now, this returns the first replica's address
func (s *ShardConfig) GetReplicaAddress(replicaID int) (string, error) {
	if len(s.Replicas) == 0 {
		return "", fmt.Errorf("no replicas found in shard %d", s.ID)
	}
	for _, replica := range s.Replicas {
		if replica.ID == replicaID {
			return replica.GetRPCAddress(), nil
		}
	}
	return "", fmt.Errorf("replica with ID %d not found in shard %d", replicaID, s.ID)
}

// GetRPCAddress returns the full RPC address (host:port) for a replica
func (r *ReplicaConfig) GetRPCAddress() string {
	return fmt.Sprintf("%s:%d", r.RPCHost, r.RPCPort)
}

// GetRaftAddress returns the full Raft address (host:port) for a replica
func (r *ReplicaConfig) GetRaftAddress() string {
	return fmt.Sprintf("%s:%d", r.RaftHost, r.RaftPort)
}

// GetLogLevel returns the configured log level, defaulting to INFO if not set
func (c *Config) GetLogLevel() util.LogLevel {
	if c.Logging.Level == "" {
		return util.LogLevelInfo
	}
	return util.ParseLogLevel(c.Logging.Level)
}
