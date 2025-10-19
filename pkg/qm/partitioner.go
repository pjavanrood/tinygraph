package qm

import (
	"math/rand"

	"github.com/pjavanrood/tinygraph/internal/config"
)

// partition the vertex to a shard: random selection
func RandomPartitioner(config *config.Config) *config.ShardConfig {
	return &config.Shards[rand.Intn(len(config.Shards))]
}
