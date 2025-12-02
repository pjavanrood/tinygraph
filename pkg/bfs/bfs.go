package bfs

import (
	"sync"

	"github.com/pjavanrood/tinygraph/internal/types"
)

// Placeholder for BFS implementation

type BFSInstance struct {
	Mx              sync.RWMutex
	Id              types.BFSId
	Visited         map[types.VertexId]bool
	CallbackAddress string
}
