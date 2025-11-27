package qm

import (
	"testing"

	"github.com/pjavanrood/tinygraph/internal/config"
	"github.com/pjavanrood/tinygraph/internal/types"
)

// localBFS performs BFS on a local graph representation and returns vertex -> level mapping
// This is used as a reference implementation to verify correctness
func localBFS(graph map[types.VertexId][]types.VertexId, startVertexID types.VertexId, radius int) map[types.VertexId]int {
	visited := make(map[types.VertexId]int)
	if radius < 0 {
		return visited
	}

	queue := []types.VertexId{startVertexID}
	visited[startVertexID] = 0

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		currentLevel := visited[current]
		if currentLevel >= radius {
			continue
		}

		neighbors := graph[current]
		for _, neighbor := range neighbors {
			if _, ok := visited[neighbor]; !ok {
				visited[neighbor] = currentLevel + 1
				queue = append(queue, neighbor)
			}
		}
	}

	return visited
}

// compareBFSResults compares two BFS result maps and returns true if they match
func compareBFSResults(expected, actual map[types.VertexId]int) bool {
	if len(expected) != len(actual) {
		return false
	}

	for vertex, level := range expected {
		if actualLevel, ok := actual[vertex]; !ok || actualLevel != level {
			return false
		}
	}

	return true
}

// TestLocalBFSReference tests the local BFS reference implementation
func TestLocalBFSReference(t *testing.T) {
	graph := make(map[types.VertexId][]types.VertexId)
	graph["0-v0"] = []types.VertexId{"0-v1", "1-v4"}
	graph["0-v1"] = []types.VertexId{"1-v2"}
	graph["1-v2"] = []types.VertexId{"1-v3", "1-v4"}
	graph["1-v3"] = []types.VertexId{}
	graph["1-v4"] = []types.VertexId{}

	result := localBFS(graph, "0-v0", 3)

	expected := map[types.VertexId]int{
		"0-v0": 0,
		"0-v1": 1,
		"1-v4": 1,
		"1-v2": 2,
		"1-v3": 3,
	}

	if !compareBFSResults(expected, result) {
		t.Errorf("Local BFS failed. Expected %v, got %v", expected, result)
	}
}

// TestLocalBFSLinearGraph tests BFS on a linear graph
func TestLocalBFSLinearGraph(t *testing.T) {
	graph := make(map[types.VertexId][]types.VertexId)
	graph["0-v0"] = []types.VertexId{"0-v1"}
	graph["0-v1"] = []types.VertexId{"0-v2"}
	graph["0-v2"] = []types.VertexId{"0-v3"}
	graph["0-v3"] = []types.VertexId{}

	result := localBFS(graph, "0-v0", 2)

	expected := map[types.VertexId]int{
		"0-v0": 0,
		"0-v1": 1,
		"0-v2": 2,
	}

	if !compareBFSResults(expected, result) {
		t.Errorf("Linear BFS failed. Expected %v, got %v", expected, result)
	}

	// Test with radius 0 (only start vertex)
	result = localBFS(graph, "0-v0", 0)
	expected = map[types.VertexId]int{"0-v0": 0}

	if !compareBFSResults(expected, result) {
		t.Errorf("Linear BFS radius 0 failed. Expected %v, got %v", expected, result)
	}
}

// TestLocalBFSTreeGraph tests BFS on a tree graph
func TestLocalBFSTreeGraph(t *testing.T) {
	graph := make(map[types.VertexId][]types.VertexId)
	graph["0-v0"] = []types.VertexId{"0-v1", "0-v2"}
	graph["0-v1"] = []types.VertexId{"1-v3", "1-v4"}
	graph["0-v2"] = []types.VertexId{"1-v5"}
	graph["1-v3"] = []types.VertexId{}
	graph["1-v4"] = []types.VertexId{}
	graph["1-v5"] = []types.VertexId{}

	result := localBFS(graph, "0-v0", 2)

	expected := map[types.VertexId]int{
		"0-v0": 0,
		"0-v1": 1,
		"0-v2": 1,
		"1-v3": 2,
		"1-v4": 2,
		"1-v5": 2,
	}

	if !compareBFSResults(expected, result) {
		t.Errorf("Tree BFS failed. Expected %v, got %v", expected, result)
	}
}

// TestLocalBFSCycleGraph tests BFS on a graph with cycles
func TestLocalBFSCycleGraph(t *testing.T) {
	graph := make(map[types.VertexId][]types.VertexId)
	graph["0-v0"] = []types.VertexId{"0-v1"}
	graph["0-v1"] = []types.VertexId{"0-v2"}
	graph["0-v2"] = []types.VertexId{"0-v0"}

	result := localBFS(graph, "0-v0", 2)

	expected := map[types.VertexId]int{
		"0-v0": 0,
		"0-v1": 1,
		"0-v2": 2,
	}

	if !compareBFSResults(expected, result) {
		t.Errorf("Cycle BFS failed. Expected %v, got %v", expected, result)
	}
}

// TestLocalBFSRadiusBoundary tests BFS with different radius values
func TestLocalBFSRadiusBoundary(t *testing.T) {
	graph := make(map[types.VertexId][]types.VertexId)
	graph["0-v0"] = []types.VertexId{"0-v1", "1-v4"}
	graph["0-v1"] = []types.VertexId{"1-v2"}
	graph["1-v2"] = []types.VertexId{"1-v3", "1-v4"}
	graph["1-v3"] = []types.VertexId{}
	graph["1-v4"] = []types.VertexId{}

	startVertex := "0-v0"

	// Test radius 0
	result := localBFS(graph, startVertex, 0)
	expected := map[types.VertexId]int{"0-v0": 0}
	if !compareBFSResults(expected, result) {
		t.Errorf("Radius 0 failed. Expected %v, got %v", expected, result)
	}

	// Test radius 1
	result = localBFS(graph, startVertex, 1)
	expected = map[types.VertexId]int{
		"0-v0": 0,
		"0-v1": 1,
		"1-v4": 1,
	}
	if !compareBFSResults(expected, result) {
		t.Errorf("Radius 1 failed. Expected %v, got %v", expected, result)
	}

	// Test radius -1 (should return empty)
	result = localBFS(graph, startVertex, -1)
	expected = map[types.VertexId]int{}
	if !compareBFSResults(expected, result) {
		t.Errorf("Radius -1 failed. Expected %v, got %v", expected, result)
	}
}

// TestLocalBFSIsolatedVertex tests BFS starting from an isolated vertex
func TestLocalBFSIsolatedVertex(t *testing.T) {
	graph := make(map[types.VertexId][]types.VertexId)
	graph["0-v0"] = []types.VertexId{}

	result := localBFS(graph, "0-v0", 5)
	expected := map[types.VertexId]int{"0-v0": 0}

	if !compareBFSResults(expected, result) {
		t.Errorf("Isolated vertex BFS failed. Expected %v, got %v", expected, result)
	}
}

// TestLocalBFSDisconnectedComponents tests BFS on disconnected components
func TestLocalBFSDisconnectedComponents(t *testing.T) {
	graph := make(map[types.VertexId][]types.VertexId)
	graph["0-v0"] = []types.VertexId{"0-v1"}
	graph["0-v1"] = []types.VertexId{}
	graph["1-v2"] = []types.VertexId{"1-v3"}
	graph["1-v3"] = []types.VertexId{}

	result := localBFS(graph, "0-v0", 5)
	expected := map[types.VertexId]int{
		"0-v0": 0,
		"0-v1": 1,
	}

	if !compareBFSResults(expected, result) {
		t.Errorf("Disconnected components BFS failed. Expected %v, got %v", expected, result)
	}
}

// TestBFSComparisonHelper verifies the comparison function works correctly
func TestBFSComparisonHelper(t *testing.T) {
	expected := map[types.VertexId]int{
		"0-v0": 0,
		"0-v1": 1,
		"1-v2": 2,
	}

	actual := map[types.VertexId]int{
		"0-v0": 0,
		"0-v1": 1,
		"1-v2": 2,
	}

	if !compareBFSResults(expected, actual) {
		t.Error("Comparison function failed for identical maps")
	}

	actual["1-v3"] = 3
	if compareBFSResults(expected, actual) {
		t.Error("Comparison function incorrectly matched different maps")
	}

	actual = map[types.VertexId]int{
		"0-v0": 0,
		"0-v1": 2,
		"1-v2": 2,
	}
	if compareBFSResults(expected, actual) {
		t.Error("Comparison function incorrectly matched maps with different levels")
	}
}

// TestQueryManagerBFSStructure tests the structure and correctness of BFS state management
func TestQueryManagerBFSStructure(t *testing.T) {
	cfg := &config.Config{
		QueryManager: config.QueryManagerConfig{
			Host:    "localhost",
			Port:    9090,
			BFSType: "optimized",
		},
		Shards: []config.ShardConfig{
			{ID: 0},
			{ID: 1},
		},
	}

	qm := NewQueryManager(cfg)

	if qm == nil {
		t.Fatal("QueryManager is nil")
	}

	if qm.config == nil {
		t.Fatal("QueryManager config is nil")
	}

	if qm.activeBFS == nil {
		t.Fatal("QueryManager activeBFS map is nil")
	}

	state := &BFSState{
		visited:       make(map[types.VertexId]int),
		pendingShards: make(map[int]bool),
		done:          make(chan struct{}),
	}

	if state.visited == nil || state.pendingShards == nil || state.done == nil {
		t.Fatal("BFSState not properly initialized")
	}

	state.visited["0-v0"] = 0
	state.visited["0-v1"] = 1

	if state.visited["0-v0"] != 0 || state.visited["0-v1"] != 1 {
		t.Error("BFSState visited map not working correctly")
	}
}


