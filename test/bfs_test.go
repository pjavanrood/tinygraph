package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/pjavanrood/tinygraph/internal/config"
	"github.com/pjavanrood/tinygraph/internal/types"
)

// LocalGraph represents a simple adjacency list for local BFS computation
type LocalGraph struct {
	edges map[types.VertexId][]types.VertexId
}

// NewLocalGraph creates a new local graph
func NewLocalGraph() *LocalGraph {
	return &LocalGraph{
		edges: make(map[types.VertexId][]types.VertexId),
	}
}

// AddEdge adds an edge to the local graph
func (g *LocalGraph) AddEdge(from, to types.VertexId) {
	if g.edges[from] == nil {
		g.edges[from] = make([]types.VertexId, 0)
	}
	g.edges[from] = append(g.edges[from], to)
}

// ComputeBFS performs BFS locally and returns vertices reachable within radius
func (g *LocalGraph) ComputeBFS(start types.VertexId, radius int) []types.VertexId {
	visited := make(map[types.VertexId]bool)
	queue := []types.VertexId{start}
	level := make(map[types.VertexId]int)
	level[start] = 0
	visited[start] = true

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if level[current] >= radius {
			continue
		}

		for _, neighbor := range g.edges[current] {
			if !visited[neighbor] {
				visited[neighbor] = true
				level[neighbor] = level[current] + 1
				queue = append(queue, neighbor)
			}
		}
	}

	result := make([]types.VertexId, 0, len(visited))
	for vertex := range visited {
		result = append(result, vertex)
	}
	return result
}

// Helper to compare BFS results
func compareBFSResults(t *testing.T, testName string, expected, actual []types.VertexId) bool {
	if !sameElements(expected, actual) {
		t.Errorf("%s: Expected %v, got %v", testName, expected, actual)
		return false
	}
	return true
}

// TestBFSLinearChain tests BFS on a linear chain graph
func TestBFSLinearChain(t *testing.T) {
	cfg, err := config.LoadConfig("../3_shard_3_replica_config.yaml")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	client := NewTestClient(t, cfg)
	defer client.Close()

	// Create linear chain: v1 -> v2 -> v3 -> v4 -> v5
	v1, _ := client.AddVertex(map[string]string{"name": "v1"})
	v2, _ := client.AddVertex(map[string]string{"name": "v2"})
	v3, _ := client.AddVertex(map[string]string{"name": "v3"})
	v4, _ := client.AddVertex(map[string]string{"name": "v4"})
	v5, _ := client.AddVertex(map[string]string{"name": "v5"})

	time.Sleep(1 * time.Second)
	client.AddEdge(v1, v2, nil)
	client.AddEdge(v2, v3, nil)
	client.AddEdge(v3, v4, nil)
	client.AddEdge(v4, v5, nil)

	// Build local graph for verification
	localGraph := NewLocalGraph()
	localGraph.AddEdge(v1, v2)
	localGraph.AddEdge(v2, v3)
	localGraph.AddEdge(v3, v4)
	localGraph.AddEdge(v4, v5)

	time.Sleep(1 * time.Second)
	timestamp := types.Timestamp(time.Now().Unix())

	// Test different radii
	testCases := []struct {
		radius   int
		expected []types.VertexId
	}{
		{0, []types.VertexId{v1}},
		{1, []types.VertexId{v1, v2}},
		{2, []types.VertexId{v1, v2, v3}},
		{3, []types.VertexId{v1, v2, v3, v4}},
		{4, []types.VertexId{v1, v2, v3, v4, v5}},
		{10, []types.VertexId{v1, v2, v3, v4, v5}}, // Beyond graph depth
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("radius=%d", tc.radius), func(t *testing.T) {
			// Compute expected result locally
			expected := localGraph.ComputeBFS(v1, tc.radius)

			// Compute result from distributed system
			actual := client.BFS(v1, tc.radius, timestamp)

			// Compare results
			if !compareBFSResults(t, fmt.Sprintf("Linear chain radius=%d", tc.radius), expected, actual) {
				t.Logf("Expected (local): %v", expected)
				t.Logf("Actual (distributed): %v", actual)
			}
		})
	}

	t.Log("Linear chain BFS test passed")
}

// TestBFSStarTopology tests BFS on a star-shaped graph
func TestBFSStarTopology(t *testing.T) {
	cfg, err := config.LoadConfig("../3_shard_3_replica_config.yaml")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	client := NewTestClient(t, cfg)
	defer client.Close()

	// Create star topology: center -> leaf1, leaf2, leaf3, leaf4
	center, _ := client.AddVertex(map[string]string{"name": "center"})
	leaf1, _ := client.AddVertex(map[string]string{"name": "leaf1"})
	leaf2, _ := client.AddVertex(map[string]string{"name": "leaf2"})
	leaf3, _ := client.AddVertex(map[string]string{"name": "leaf3"})
	leaf4, _ := client.AddVertex(map[string]string{"name": "leaf4"})

	time.Sleep(1 * time.Second)
	client.AddEdge(center, leaf1, nil)
	client.AddEdge(center, leaf2, nil)
	client.AddEdge(center, leaf3, nil)
	client.AddEdge(center, leaf4, nil)

	// Build local graph
	localGraph := NewLocalGraph()
	localGraph.AddEdge(center, leaf1)
	localGraph.AddEdge(center, leaf2)
	localGraph.AddEdge(center, leaf3)
	localGraph.AddEdge(center, leaf4)

	time.Sleep(1 * time.Second)
	timestamp := types.Timestamp(time.Now().Unix())

	// Test different radii
	testCases := []struct {
		radius int
		count  int // Expected number of vertices
	}{
		{0, 1}, // Just center
		{1, 5}, // Center + 4 leaves
		{2, 5}, // Same, no more vertices
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("radius=%d", tc.radius), func(t *testing.T) {
			expected := localGraph.ComputeBFS(center, tc.radius)
			actual := client.BFS(center, tc.radius, timestamp)

			if !compareBFSResults(t, fmt.Sprintf("Star topology radius=%d", tc.radius), expected, actual) {
				t.Logf("Expected count: %d, actual count: %d", len(expected), len(actual))
			}

			if len(actual) != tc.count {
				t.Errorf("Star topology radius=%d: expected %d vertices, got %d", tc.radius, tc.count, len(actual))
			}
		})
	}

	t.Log("Star topology BFS test passed")
}

// TestBFSCycle tests BFS on a cyclic graph
func TestBFSCycle(t *testing.T) {
	cfg, err := config.LoadConfig("../3_shard_3_replica_config.yaml")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	client := NewTestClient(t, cfg)
	defer client.Close()

	// Create cycle: v1 -> v2 -> v3 -> v4 -> v1
	v1, _ := client.AddVertex(map[string]string{"name": "v1"})
	v2, _ := client.AddVertex(map[string]string{"name": "v2"})
	v3, _ := client.AddVertex(map[string]string{"name": "v3"})
	v4, _ := client.AddVertex(map[string]string{"name": "v4"})

	time.Sleep(1 * time.Second)
	client.AddEdge(v1, v2, nil)
	client.AddEdge(v2, v3, nil)
	client.AddEdge(v3, v4, nil)
	client.AddEdge(v4, v1, nil) // Cycle back

	// Build local graph
	localGraph := NewLocalGraph()
	localGraph.AddEdge(v1, v2)
	localGraph.AddEdge(v2, v3)
	localGraph.AddEdge(v3, v4)
	localGraph.AddEdge(v4, v1)

	time.Sleep(1 * time.Second)
	timestamp := types.Timestamp(time.Now().Unix())

	// Test different radii - should visit all vertices regardless of radius >= 3
	testCases := []int{0, 1, 2, 3, 4, 10}

	for _, radius := range testCases {
		t.Run(fmt.Sprintf("radius=%d", radius), func(t *testing.T) {
			expected := localGraph.ComputeBFS(v1, radius)
			actual := client.BFS(v1, radius, timestamp)

			if !compareBFSResults(t, fmt.Sprintf("Cycle radius=%d", radius), expected, actual) {
				t.Logf("Expected: %v", expected)
				t.Logf("Actual: %v", actual)
			}
		})
	}

	t.Log("Cycle BFS test passed")
}

// TestBFSDisconnectedGraph tests BFS on disconnected components
func TestBFSDisconnectedGraph(t *testing.T) {
	cfg, err := config.LoadConfig("../3_shard_3_replica_config.yaml")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	client := NewTestClient(t, cfg)
	defer client.Close()

	// Create two disconnected components:
	// Component 1: v1 -> v2 -> v3
	// Component 2: v4 -> v5
	v1, _ := client.AddVertex(map[string]string{"name": "v1"})
	v2, _ := client.AddVertex(map[string]string{"name": "v2"})
	v3, _ := client.AddVertex(map[string]string{"name": "v3"})
	v4, _ := client.AddVertex(map[string]string{"name": "v4"})
	v5, _ := client.AddVertex(map[string]string{"name": "v5"})

	time.Sleep(1 * time.Second)
	client.AddEdge(v1, v2, nil)
	client.AddEdge(v2, v3, nil)
	client.AddEdge(v4, v5, nil)

	// Build local graph
	localGraph := NewLocalGraph()
	localGraph.AddEdge(v1, v2)
	localGraph.AddEdge(v2, v3)
	localGraph.AddEdge(v4, v5)

	time.Sleep(1 * time.Second)
	timestamp := types.Timestamp(time.Now().Unix())

	// BFS from v1 should only reach component 1
	t.Run("component1", func(t *testing.T) {
		expected := localGraph.ComputeBFS(v1, 10)
		actual := client.BFS(v1, 10, timestamp)

		if !compareBFSResults(t, "Disconnected graph component 1", expected, actual) {
			t.Logf("Expected: %v", expected)
			t.Logf("Actual: %v", actual)
		}

		// Verify v4 and v5 are NOT in the result
		for _, v := range actual {
			if v == v4 || v == v5 {
				t.Errorf("BFS from component 1 should not reach component 2 (found %s)", v)
			}
		}
	})

	// BFS from v4 should only reach component 2
	t.Run("component2", func(t *testing.T) {
		expected := localGraph.ComputeBFS(v4, 10)
		actual := client.BFS(v4, 10, timestamp)

		if !compareBFSResults(t, "Disconnected graph component 2", expected, actual) {
			t.Logf("Expected: %v", expected)
			t.Logf("Actual: %v", actual)
		}

		// Verify v1, v2, v3 are NOT in the result
		for _, v := range actual {
			if v == v1 || v == v2 || v == v3 {
				t.Errorf("BFS from component 2 should not reach component 1 (found %s)", v)
			}
		}
	})

	t.Log("Disconnected graph BFS test passed")
}

// TestBFSComplexGraph tests BFS on a more complex graph with multiple paths
func TestBFSComplexGraph(t *testing.T) {
	cfg, err := config.LoadConfig("../3_shard_3_replica_config.yaml")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	client := NewTestClient(t, cfg)
	defer client.Close()

	// Create a diamond-shaped graph with multiple paths:
	//     v1
	//    /  \
	//   v2  v3
	//    \  /
	//     v4
	//      |
	//     v5
	v1, _ := client.AddVertex(map[string]string{"name": "v1"})
	v2, _ := client.AddVertex(map[string]string{"name": "v2"})
	v3, _ := client.AddVertex(map[string]string{"name": "v3"})
	v4, _ := client.AddVertex(map[string]string{"name": "v4"})
	v5, _ := client.AddVertex(map[string]string{"name": "v5"})

	time.Sleep(1 * time.Second)
	client.AddEdge(v1, v2, nil)
	client.AddEdge(v1, v3, nil)
	client.AddEdge(v2, v4, nil)
	client.AddEdge(v3, v4, nil)
	client.AddEdge(v4, v5, nil)

	// Build local graph
	localGraph := NewLocalGraph()
	localGraph.AddEdge(v1, v2)
	localGraph.AddEdge(v1, v3)
	localGraph.AddEdge(v2, v4)
	localGraph.AddEdge(v3, v4)
	localGraph.AddEdge(v4, v5)

	time.Sleep(1 * time.Second)
	timestamp := types.Timestamp(time.Now().Unix())

	// Test different radii
	testCases := []struct {
		radius   int
		expected []types.VertexId
	}{
		{0, []types.VertexId{v1}},
		{1, []types.VertexId{v1, v2, v3}},
		{2, []types.VertexId{v1, v2, v3, v4}},
		{3, []types.VertexId{v1, v2, v3, v4, v5}},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("radius=%d", tc.radius), func(t *testing.T) {
			expected := localGraph.ComputeBFS(v1, tc.radius)
			actual := client.BFS(v1, tc.radius, timestamp)

			if !compareBFSResults(t, fmt.Sprintf("Complex graph radius=%d", tc.radius), expected, actual) {
				t.Logf("Expected: %v", expected)
				t.Logf("Actual: %v", actual)
			}
		})
	}

	t.Log("Complex graph BFS test passed")
}

// TestBFSSingleVertex tests BFS on a graph with just one vertex
func TestBFSSingleVertex(t *testing.T) {
	cfg, err := config.LoadConfig("../3_shard_3_replica_config.yaml")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	client := NewTestClient(t, cfg)
	defer client.Close()

	// Create single vertex with no edges
	v1, _ := client.AddVertex(map[string]string{"name": "v1"})

	time.Sleep(1 * time.Second)
	timestamp := types.Timestamp(time.Now().Unix())

	// BFS from v1 should only return v1 regardless of radius
	for radius := 0; radius < 5; radius++ {
		t.Run(fmt.Sprintf("radius=%d", radius), func(t *testing.T) {
			result := client.BFS(v1, radius, timestamp)
			expected := []types.VertexId{v1}

			if !compareBFSResults(t, fmt.Sprintf("Single vertex radius=%d", radius), expected, result) {
				t.Logf("Expected: %v", expected)
				t.Logf("Actual: %v", result)
			}
		})
	}

	t.Log("Single vertex BFS test passed")
}

// TestBFSBinaryTree tests BFS on a binary tree structure
func TestBFSBinaryTree(t *testing.T) {
	cfg, err := config.LoadConfig("../3_shard_3_replica_config.yaml")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	client := NewTestClient(t, cfg)
	defer client.Close()

	// Create binary tree:
	//        v1
	//       /  \
	//      v2   v3
	//     / \   / \
	//    v4 v5 v6 v7
	v1, _ := client.AddVertex(map[string]string{"name": "v1"})
	v2, _ := client.AddVertex(map[string]string{"name": "v2"})
	v3, _ := client.AddVertex(map[string]string{"name": "v3"})
	v4, _ := client.AddVertex(map[string]string{"name": "v4"})
	v5, _ := client.AddVertex(map[string]string{"name": "v5"})
	v6, _ := client.AddVertex(map[string]string{"name": "v6"})
	v7, _ := client.AddVertex(map[string]string{"name": "v7"})

	time.Sleep(1 * time.Second)
	client.AddEdge(v1, v2, nil)
	client.AddEdge(v1, v3, nil)
	client.AddEdge(v2, v4, nil)
	client.AddEdge(v2, v5, nil)
	client.AddEdge(v3, v6, nil)
	client.AddEdge(v3, v7, nil)

	// Build local graph
	localGraph := NewLocalGraph()
	localGraph.AddEdge(v1, v2)
	localGraph.AddEdge(v1, v3)
	localGraph.AddEdge(v2, v4)
	localGraph.AddEdge(v2, v5)
	localGraph.AddEdge(v3, v6)
	localGraph.AddEdge(v3, v7)

	time.Sleep(1 * time.Second)
	timestamp := types.Timestamp(time.Now().Unix())

	// Test different radii
	testCases := []struct {
		radius int
		count  int
	}{
		{0, 1}, // Just root
		{1, 3}, // Root + 2 children
		{2, 7}, // All vertices
		{3, 7}, // Same (no more vertices)
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("radius=%d", tc.radius), func(t *testing.T) {
			expected := localGraph.ComputeBFS(v1, tc.radius)
			actual := client.BFS(v1, tc.radius, timestamp)

			if !compareBFSResults(t, fmt.Sprintf("Binary tree radius=%d", tc.radius), expected, actual) {
				t.Logf("Expected count: %d, actual count: %d", len(expected), len(actual))
			}

			if len(actual) != tc.count {
				t.Errorf("Binary tree radius=%d: expected %d vertices, got %d", tc.radius, tc.count, len(actual))
			}
		})
	}

	t.Log("Binary tree BFS test passed")
}

// TestBFSMultipleStartingPoints tests BFS from different starting vertices in the same graph
func TestBFSMultipleStartingPoints(t *testing.T) {
	cfg, err := config.LoadConfig("../3_shard_3_replica_config.yaml")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	client := NewTestClient(t, cfg)
	defer client.Close()

	// Create graph: v1 -> v2 -> v3 -> v4
	v1, _ := client.AddVertex(map[string]string{"name": "v1"})
	v2, _ := client.AddVertex(map[string]string{"name": "v2"})
	v3, _ := client.AddVertex(map[string]string{"name": "v3"})
	v4, _ := client.AddVertex(map[string]string{"name": "v4"})

	time.Sleep(1 * time.Second)
	client.AddEdge(v1, v2, nil)
	client.AddEdge(v2, v3, nil)
	client.AddEdge(v3, v4, nil)

	// Build local graph
	localGraph := NewLocalGraph()
	localGraph.AddEdge(v1, v2)
	localGraph.AddEdge(v2, v3)
	localGraph.AddEdge(v3, v4)

	time.Sleep(1 * time.Second)
	timestamp := types.Timestamp(time.Now().Unix())

	// Test BFS from each vertex
	testCases := []struct {
		start    types.VertexId
		radius   int
		expected []types.VertexId
	}{
		{v1, 2, []types.VertexId{v1, v2, v3}},
		{v2, 2, []types.VertexId{v2, v3, v4}},
		{v3, 1, []types.VertexId{v3, v4}},
		{v4, 1, []types.VertexId{v4}}, // No outgoing edges
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("case_%d_start=%s_radius=%d", i, tc.start, tc.radius), func(t *testing.T) {
			expected := localGraph.ComputeBFS(tc.start, tc.radius)
			actual := client.BFS(tc.start, tc.radius, timestamp)

			if !compareBFSResults(t, fmt.Sprintf("Start=%s radius=%d", tc.start, tc.radius), expected, actual) {
				t.Logf("Expected: %v", expected)
				t.Logf("Actual: %v", actual)
			}
		})
	}

	t.Log("Multiple starting points BFS test passed")
}

// TestBFSLargeRadius tests BFS with radius larger than graph diameter
func TestBFSLargeRadius(t *testing.T) {
	cfg, err := config.LoadConfig("../3_shard_3_replica_config.yaml")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	client := NewTestClient(t, cfg)
	defer client.Close()

	// Create simple chain: v1 -> v2 -> v3
	v1, _ := client.AddVertex(map[string]string{"name": "v1"})
	v2, _ := client.AddVertex(map[string]string{"name": "v2"})
	v3, _ := client.AddVertex(map[string]string{"name": "v3"})

	time.Sleep(1 * time.Second)
	client.AddEdge(v1, v2, nil)
	client.AddEdge(v2, v3, nil)

	// Build local graph
	localGraph := NewLocalGraph()
	localGraph.AddEdge(v1, v2)
	localGraph.AddEdge(v2, v3)

	time.Sleep(1 * time.Second)
	timestamp := types.Timestamp(time.Now().Unix())

	// Test with very large radii - should return all vertices
	largeRadii := []int{10, 100, 1000}

	for _, radius := range largeRadii {
		t.Run(fmt.Sprintf("radius=%d", radius), func(t *testing.T) {
			expected := localGraph.ComputeBFS(v1, radius)
			actual := client.BFS(v1, radius, timestamp)

			if !compareBFSResults(t, fmt.Sprintf("Large radius=%d", radius), expected, actual) {
				t.Logf("Expected: %v (count=%d)", expected, len(expected))
				t.Logf("Actual: %v (count=%d)", actual, len(actual))
			}

			// Should return all 3 vertices regardless of large radius
			if len(actual) != 3 {
				t.Errorf("Large radius=%d: expected 3 vertices, got %d", radius, len(actual))
			}
		})
	}

	t.Log("Large radius BFS test passed")
}
