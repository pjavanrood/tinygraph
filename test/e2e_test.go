package test

import (
	"testing"
	"time"

	"github.com/pjavanrood/tinygraph/internal/config"
	"github.com/pjavanrood/tinygraph/internal/types"
	"github.com/pjavanrood/tinygraph/test/utils"
)

// TestBasicOperations tests basic add vertex, add edge, and BFS operations
func TestBasicOperations(t *testing.T) {
	cfg, err := config.LoadConfig("../config.yaml")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	client := utils.NewTestClient(t, cfg)
	defer client.Close()
	defer func() {
		client.DumpTimestampedState(t)
	}()

	// Add vertices
	v1, ts1 := client.AddVertex(map[string]string{"name": "v1"})
	v2, ts2 := client.AddVertex(map[string]string{"name": "v2"})
	v3, ts3 := client.AddVertex(map[string]string{"name": "v3"})

	t.Logf("Created vertices: v1=%s (ts=%.0f), v2=%s (ts=%.0f), v3=%s (ts=%.0f)",
		v1, ts1, v2, ts2, v3, ts3)

	// Add edges: v1 -> v2 -> v3
	ts4 := client.AddEdge(v1, v2, map[string]string{"weight": "1"})
	ts5 := client.AddEdge(v2, v3, map[string]string{"weight": "2"})

	t.Logf("Created edges: v1->v2 (ts=%.0f), v2->v3 (ts=%.0f)", ts4, ts5)

	// Sleep to ensure timestamp difference
	time.Sleep(2 * time.Second)

	// Query BFS from v1 with radius 0 (should only return v1)
	currentTime := types.Timestamp(time.Now().Unix())
	result := client.BFS(v1, 0, currentTime)
	if !utils.SameElements(result, []types.VertexId{v1}) {
		t.Errorf("BFS(v1, radius=0) expected [%s], got %v", v1, result)
	}

	// Query BFS from v1 with radius 1 (should return v1 and v2)
	result = client.BFS(v1, 1, currentTime)
	if !utils.SameElements(result, []types.VertexId{v1, v2}) {
		t.Errorf("BFS(v1, radius=1) expected [%s, %s], got %v", v1, v2, result)
	}

	// Query BFS from v1 with radius 2 (should return v1, v2, v3)
	result = client.BFS(v1, 2, currentTime)
	if !utils.SameElements(result, []types.VertexId{v1, v2, v3}) {
		t.Errorf("BFS(v1, radius=2) expected [%s, %s, %s], got %v", v1, v2, v3, result)
	}

	t.Log("Basic operations test passed")
}

// TestMVCCSimpleGraph tests MVCC correctness with a simple graph evolution
func TestMVCCSimpleGraph(t *testing.T) {
	cfg, err := config.LoadConfig("../config.yaml")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	client := utils.NewTestClient(t, cfg)
	defer client.Close()
	defer func() {
		client.DumpTimestampedState(t)
	}()

	// Create initial graph: v1 -> v2
	v1, ts1 := client.AddVertex(map[string]string{"name": "v1"})
	v2, ts2 := client.AddVertex(map[string]string{"name": "v2"})

	time.Sleep(1 * time.Second)
	ts3 := client.AddEdge(v1, v2, map[string]string{"weight": "1"})

	t.Logf("State 1 - Created v1 (ts=%.0f), v2 (ts=%.0f), v1->v2 (ts=%.0f)", ts1, ts2, ts3)

	// Query at ts3: should see v1 -> v2
	time.Sleep(1 * time.Second)
	queryTs1 := types.Timestamp(time.Now().Unix())
	result := client.BFS(v1, 1, queryTs1)
	if !utils.SameElements(result, []types.VertexId{v1, v2}) {
		t.Errorf("At timestamp %.0f, BFS(v1, 1) expected [%s, %s], got %v", queryTs1, v1, v2, result)
	}

	// Add another vertex v3 and edge v2 -> v3
	time.Sleep(1 * time.Second)
	v3, ts4 := client.AddVertex(map[string]string{"name": "v3"})
	time.Sleep(1 * time.Second)
	ts5 := client.AddEdge(v2, v3, map[string]string{"weight": "2"})

	t.Logf("State 2 - Created v3 (ts=%.0f), v2->v3 (ts=%.0f)", ts4, ts5)

	// Query at current time: should see v1 -> v2 -> v3
	time.Sleep(1 * time.Second)
	queryTs2 := types.Timestamp(time.Now().Unix())
	result = client.BFS(v1, 2, queryTs2)
	if !utils.SameElements(result, []types.VertexId{v1, v2, v3}) {
		t.Errorf("At timestamp %.0f, BFS(v1, 2) expected [%s, %s, %s], got %v", queryTs2, v1, v2, v3, result)
	}

	// Query at old timestamp (queryTs1): should still see only v1 -> v2 (no v3)
	result = client.BFS(v1, 2, queryTs1)
	if !utils.SameElements(result, []types.VertexId{v1, v2}) {
		t.Errorf("At old timestamp %.0f, BFS(v1, 2) expected [%s, %s], got %v", queryTs1, v1, v2, result)
	}

	t.Log("MVCC simple graph test passed")
}

// TestMVCCEdgeDeletion tests MVCC correctness with edge deletion
func TestMVCCEdgeDeletion(t *testing.T) {
	cfg, err := config.LoadConfig("../config.yaml")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	client := utils.NewTestClient(t, cfg)
	defer client.Close()
	defer func() {
		client.DumpTimestampedState(t)
	}()

	// Create graph: v1 -> v2 -> v3
	v1, _ := client.AddVertex(map[string]string{"name": "v1"})
	v2, _ := client.AddVertex(map[string]string{"name": "v2"})
	v3, _ := client.AddVertex(map[string]string{"name": "v3"})

	time.Sleep(1 * time.Second)
	ts4 := client.AddEdge(v1, v2, map[string]string{"weight": "1"})
	ts5 := client.AddEdge(v2, v3, map[string]string{"weight": "2"})

	t.Logf("Created graph: v1=%s, v2=%s, v3=%s", v1, v2, v3)
	t.Logf("Edges: v1->v2 (ts=%.0f), v2->v3 (ts=%.0f)", ts4, ts5)

	// Query before deletion
	time.Sleep(1 * time.Second)
	beforeDeletionTs := types.Timestamp(time.Now().Unix())
	result := client.BFS(v1, 2, beforeDeletionTs)
	if !utils.SameElements(result, []types.VertexId{v1, v2, v3}) {
		t.Errorf("Before deletion, BFS(v1, 2) expected [%s, %s, %s], got %v", v1, v2, v3, result)
	}

	// Delete edge v1 -> v2
	time.Sleep(1 * time.Second)
	ts6 := client.DeleteEdge(v1, v2)
	t.Logf("Deleted edge v1->v2 (ts=%.0f)", ts6)

	// Query after deletion: should only see v1 (v2 and v3 are disconnected)
	time.Sleep(1 * time.Second)
	afterDeletionTs := types.Timestamp(time.Now().Unix())
	result = client.BFS(v1, 2, afterDeletionTs)
	if !utils.SameElements(result, []types.VertexId{v1}) {
		t.Errorf("After deletion, BFS(v1, 2) expected [%s], got %v", v1, result)
	}

	// Query at timestamp before deletion: should still see all three vertices
	result = client.BFS(v1, 2, beforeDeletionTs)
	if !utils.SameElements(result, []types.VertexId{v1, v2, v3}) {
		t.Errorf("At old timestamp %.0f (before deletion), BFS(v1, 2) expected [%s, %s, %s], got %v",
			beforeDeletionTs, v1, v2, v3, result)
	}

	t.Log("MVCC edge deletion test passed")
}

// TestMVCCComplexEvolution tests MVCC with multiple additions and deletions
func TestMVCCComplexEvolution(t *testing.T) {
	cfg, err := config.LoadConfig("../config.yaml")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	client := utils.NewTestClient(t, cfg)
	defer client.Close()
	defer func() {
		client.DumpTimestampedState(t)
	}()

	// Create initial vertices
	v1, _ := client.AddVertex(map[string]string{"name": "v1"})
	v2, _ := client.AddVertex(map[string]string{"name": "v2"})
	v3, _ := client.AddVertex(map[string]string{"name": "v3"})
	v4, _ := client.AddVertex(map[string]string{"name": "v4"})

	// State 1: Linear chain v1 -> v2 -> v3
	time.Sleep(1 * time.Second)
	client.AddEdge(v1, v2, map[string]string{"weight": "1"})
	client.AddEdge(v2, v3, map[string]string{"weight": "2"})

	time.Sleep(1 * time.Second)
	ts_state1 := types.Timestamp(time.Now().Unix())
	result := client.BFS(v1, 2, ts_state1)
	if !utils.SameElements(result, []types.VertexId{v1, v2, v3}) {
		t.Errorf("State 1: BFS(v1, 2) expected [%s, %s, %s], got %v", v1, v2, v3, result)
	}
	t.Logf("State 1 (ts=%.0f): v1->v2->v3 verified", ts_state1)

	// State 2: Add branch v1 -> v4
	time.Sleep(1 * time.Second)
	client.AddEdge(v1, v4, map[string]string{"weight": "3"})

	time.Sleep(1 * time.Second)
	ts_state2 := types.Timestamp(time.Now().Unix())
	result = client.BFS(v1, 1, ts_state2)
	if !utils.SameElements(result, []types.VertexId{v1, v2, v4}) {
		t.Errorf("State 2: BFS(v1, 1) expected [%s, %s, %s], got %v", v1, v2, v4, result)
	}
	t.Logf("State 2 (ts=%.0f): Added v1->v4 branch verified", ts_state2)

	// State 3: Delete v1 -> v2
	time.Sleep(1 * time.Second)
	client.DeleteEdge(v1, v2)

	time.Sleep(1 * time.Second)
	ts_state3 := types.Timestamp(time.Now().Unix())
	result = client.BFS(v1, 2, ts_state3)
	if !utils.SameElements(result, []types.VertexId{v1, v4}) {
		t.Errorf("State 3: BFS(v1, 2) expected [%s, %s], got %v", v1, v4, result)
	}
	t.Logf("State 3 (ts=%.0f): Deleted v1->v2, only v1->v4 remains", ts_state3)

	// State 4: Add v3 -> v4
	time.Sleep(1 * time.Second)
	client.AddEdge(v3, v4, map[string]string{"weight": "4"})

	time.Sleep(1 * time.Second)
	ts_state4 := types.Timestamp(time.Now().Unix())
	// BFS from v1 should still only reach v4
	result = client.BFS(v1, 2, ts_state4)
	if !utils.SameElements(result, []types.VertexId{v1, v4}) {
		t.Errorf("State 4: BFS(v1, 2) expected [%s, %s], got %v", v1, v4, result)
	}
	t.Logf("State 4 (ts=%.0f): Added v3->v4, BFS from v1 still reaches [v1, v4]", ts_state4)

	// Verify historical queries work correctly
	result = client.BFS(v1, 2, ts_state1)
	if !utils.SameElements(result, []types.VertexId{v1, v2, v3}) {
		t.Errorf("Historical query at ts_state1: expected [%s, %s, %s], got %v", v1, v2, v3, result)
	}

	result = client.BFS(v1, 1, ts_state2)
	if !utils.SameElements(result, []types.VertexId{v1, v2, v4}) {
		t.Errorf("Historical query at ts_state2: expected [%s, %s, %s], got %v", v1, v2, v4, result)
	}

	t.Log("MVCC complex evolution test passed")
}

// TestMVCCMultipleEdgeUpdates tests adding the same edge multiple times
func TestMVCCMultipleEdgeUpdates(t *testing.T) {
	cfg, err := config.LoadConfig("../config.yaml")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	client := utils.NewTestClient(t, cfg)
	defer client.Close()
	defer func() {
		client.DumpTimestampedState(t)
	}()

	// Create vertices
	v1, _ := client.AddVertex(map[string]string{"name": "v1"})
	v2, _ := client.AddVertex(map[string]string{"name": "v2"})

	// Add edge v1 -> v2 multiple times with different properties
	time.Sleep(1 * time.Second)
	_ = client.AddEdge(v1, v2, map[string]string{"weight": "1", "version": "1"})

	time.Sleep(1 * time.Second)
	query_ts1 := types.Timestamp(time.Now().Unix())

	time.Sleep(1 * time.Second)
	_ = client.AddEdge(v1, v2, map[string]string{"weight": "2", "version": "2"})

	time.Sleep(1 * time.Second)
	query_ts2 := types.Timestamp(time.Now().Unix())

	time.Sleep(1 * time.Second)
	_ = client.AddEdge(v1, v2, map[string]string{"weight": "3", "version": "3"})

	time.Sleep(1 * time.Second)
	query_ts3 := types.Timestamp(time.Now().Unix())

	// All queries should see the edge exists (MVCC should maintain all versions)
	result1 := client.BFS(v1, 1, query_ts1)
	if !utils.SameElements(result1, []types.VertexId{v1, v2}) {
		t.Errorf("At ts1, BFS(v1, 1) expected [%s, %s], got %v", v1, v2, result1)
	}

	result2 := client.BFS(v1, 1, query_ts2)
	if !utils.SameElements(result2, []types.VertexId{v1, v2}) {
		t.Errorf("At ts2, BFS(v1, 1) expected [%s, %s], got %v", v1, v2, result2)
	}

	result3 := client.BFS(v1, 1, query_ts3)
	if !utils.SameElements(result3, []types.VertexId{v1, v2}) {
		t.Errorf("At ts3, BFS(v1, 1) expected [%s, %s], got %v", v1, v2, result3)
	}

	t.Log("MVCC multiple edge updates test passed")
}

// TestMVCCDeleteAndReadd tests deleting and re-adding edges
func TestMVCCDeleteAndReadd(t *testing.T) {
	cfg, err := config.LoadConfig("../config.yaml")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	client := utils.NewTestClient(t, cfg)
	defer client.Close()
	defer func() {
		client.DumpTimestampedState(t)
	}()

	// Create vertices
	v1, _ := client.AddVertex(map[string]string{"name": "v1"})
	v2, _ := client.AddVertex(map[string]string{"name": "v2"})

	// Add edge
	time.Sleep(1 * time.Second)
	client.AddEdge(v1, v2, map[string]string{"weight": "1"})

	time.Sleep(1 * time.Second)
	ts_after_add := types.Timestamp(time.Now().Unix())

	// Delete edge
	time.Sleep(1 * time.Second)
	client.DeleteEdge(v1, v2)

	time.Sleep(1 * time.Second)
	ts_after_delete := types.Timestamp(time.Now().Unix())

	// Re-add edge
	time.Sleep(1 * time.Second)
	client.AddEdge(v1, v2, map[string]string{"weight": "2"})

	time.Sleep(1 * time.Second)
	ts_after_readd := types.Timestamp(time.Now().Unix())

	// Query at each timestamp
	result_add := client.BFS(v1, 1, ts_after_add)
	if !utils.SameElements(result_add, []types.VertexId{v1, v2}) {
		t.Errorf("After initial add, BFS(v1, 1) expected [%s, %s], got %v", v1, v2, result_add)
	}

	result_delete := client.BFS(v1, 1, ts_after_delete)
	if !utils.SameElements(result_delete, []types.VertexId{v1}) {
		t.Errorf("After delete, BFS(v1, 1) expected [%s], got %v", v1, result_delete)
	}

	result_readd := client.BFS(v1, 1, ts_after_readd)
	if !utils.SameElements(result_readd, []types.VertexId{v1, v2}) {
		t.Errorf("After re-add, BFS(v1, 1) expected [%s, %s], got %v", v1, v2, result_readd)
	}

	// Historical query should still work
	result_historical := client.BFS(v1, 1, ts_after_add)
	if !utils.SameElements(result_historical, []types.VertexId{v1, v2}) {
		t.Errorf("Historical query at first add timestamp, BFS(v1, 1) expected [%s, %s], got %v",
			v1, v2, result_historical)
	}

	t.Log("MVCC delete and re-add test passed")
}

func TestMain(m *testing.M) {
	utils.TestMain(m)
}
