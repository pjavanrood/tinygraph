package qm

import (
	"math/rand"

	"github.com/pjavanrood/tinygraph/internal/config"
)

// partition the vertex to a shard: random selection
func RandomPartitioner(config *config.Config) *config.ShardConfig {
	return &config.Shards[rand.Intn(len(config.Shards))]
}

// LDG: choose the shard with the smallest load.
// If shardLoads is invalid, fall back to random.
func LDGPartitioner(cfg *config.Config, shardLoads []int) *config.ShardConfig {
    numShards := len(cfg.Shards)
    if len(shardLoads) != numShards {
        return RandomPartitioner(cfg)
    }

    best := 0
    for i := 1; i < numShards; i++ {
        if shardLoads[i] < shardLoads[best] {
            best = i
        }
    }
    return &cfg.Shards[best]
}
