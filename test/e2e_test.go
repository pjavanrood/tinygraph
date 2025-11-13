package test

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"os/exec"
	"sort"
	"syscall"
	"testing"
	"time"

	"github.com/pjavanrood/tinygraph/internal/config"
	"github.com/pjavanrood/tinygraph/internal/types"
	rpcTypes "github.com/pjavanrood/tinygraph/pkg/rpc"
)

// TestClient wraps the RPC client with helper methods
type TestClient struct {
	conn         *rpc.Client
	t            *testing.T
	timestampLog []types.Timestamp
	knownVerts   []types.VertexId
}

// NewTestClient creates a new test client
func NewTestClient(t *testing.T, cfg *config.Config) *TestClient {
	// Retry connection a few times as services may be starting up
	var conn *rpc.Client
	var err error
	for i := 0; i < 10; i++ {
		conn, err = rpc.Dial("tcp", fmt.Sprintf("%s:%d", cfg.QueryManager.Host, cfg.QueryManager.Port))
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("Failed to connect to query manager after retries: %v", err)
	}
	return &TestClient{
		conn: conn,
		t:    t,
		// defaults to 0, which is what we want for our initial timestamp
		timestampLog: make([]types.Timestamp, 1),
	}
}

// Close closes the client connection
func (c *TestClient) Close() {
	c.conn.Close()
}

// Dumps the shared state at every timestamp logged
func (c *TestClient) dumpTimestampedState(t *testing.T) {
	t.Logf("========== BEGIN DUMP ==========")
	// for each shared state, dump what the server stores at that timestamp, beginning at 0
	for _, timestamp := range c.timestampLog {
		t.Logf("~~~~State at timestamp %f~~~~", timestamp)
		for shard, vertexSlice := range c.FetchAll() {
			// get the values at the prescribed timestamp
			t.Logf("On shard %d:", shard)
			for _, vinfo := range vertexSlice {
				t.Logf("|-Vertex %s at %f:", vinfo.VertexID, timestamp)
				exists, props, ts := c.GetVertexAt(vinfo.VertexID, timestamp)
				if exists {
					t.Logf("  |-Exists (latest update %f)!", ts)
					for k, v := range props {
						t.Logf("  |-%v: %v", k, v)
					}
				} else {
					t.Logf("  |-Does not exist")
				}

				t.Logf("  |-Edges: %d", len(vinfo.EdgesTo))
				for _, vid := range vinfo.EdgesTo {
					t.Logf("    |-Child Vertex %s at %f:", vid, timestamp)
					exists, props, ts := c.GetEdgeAt(vinfo.VertexID, vid, timestamp)
					if exists {
						t.Logf("    |-Exists (latest update %f)!", ts)
						for k, v := range props {
							t.Logf("    |-%v: %v", k, v)
						}
					} else {
						t.Logf("    |-Does not exist")
					}
				}
			}
		}
	}

	t.Logf("========== END DUMP ==========")
}

// AddVertex adds a vertex and returns the vertex ID and timestamp
func (c *TestClient) AddVertex(properties map[string]string) (types.VertexId, types.Timestamp) {
	var resp rpcTypes.AddVertexResponse
	err := c.conn.Call("QueryManager.AddVertex", &rpcTypes.AddVertexRequest{
		Properties: properties,
	}, &resp)
	if err != nil {
		c.t.Fatalf("AddVertex failed: %v", err)
	}
	if !resp.Success {
		c.t.Fatalf("AddVertex returned success=false")
	}
	c.timestampLog = append(c.timestampLog, resp.Timestamp)
	return resp.VertexID, resp.Timestamp
}

func (c *TestClient) GetVertexAt(vertexId types.VertexId, timestamp types.Timestamp) (bool, types.Properties, types.Timestamp) {
	var resp rpcTypes.GetVertexResponse
	err := c.conn.Call("QueryManager.GetVertexAt", &rpcTypes.GetVertexAtRequest{
		Vertex:    vertexId,
		Timestamp: timestamp,
	}, &resp)
	if err != nil {
		c.t.Fatalf("GetVertex failed: %v", err)
	}
	return resp.Exists, resp.Properties, resp.Timestamp
}
func (c *TestClient) GetVertex(vertexId types.VertexId) (bool, types.Properties, types.Timestamp) {
	return c.GetVertexAt(vertexId, types.Timestamp(time.Now().Unix()))
}

func (c *TestClient) GetEdgeAt(vertexFrom types.VertexId, vertexTo types.VertexId, timestamp types.Timestamp) (bool, types.Properties, types.Timestamp) {
	var resp rpcTypes.GetEdgeResponse
	err := c.conn.Call("QueryManager.GetVertexAt", &rpcTypes.GetEdgeAtRequest{
		FromVertex: vertexFrom,
		ToVertex:   vertexTo,
		Timestamp:  timestamp,
	}, &resp)
	if err != nil {
		c.t.Fatalf("GetEdge failed: %v", err)
	}
	return resp.Exists, resp.Properties, resp.Timestamp
}
func (c *TestClient) GetEdge(vertexFrom, vertexTo types.VertexId) (bool, types.Properties, types.Timestamp) {
	return c.GetEdgeAt(vertexFrom, vertexTo, types.Timestamp(time.Now().Unix()))
}

// FetchAll calls the FetchAll debug command to get all vertices
func (c *TestClient) FetchAll() map[int][]rpcTypes.VertexInfo {
	var resp rpcTypes.FetchAllResponse
	err := c.conn.Call("QueryManager.FetchAll", &rpcTypes.FetchAllRequest{}, &resp)
	if err != nil {
		c.t.Fatalf("FetchAll failed: %v", err)
	}
	return resp.ShardVertices
}

// AddEdge adds an edge and returns the timestamp
func (c *TestClient) AddEdge(fromVertexID, toVertexID types.VertexId, properties map[string]string) types.Timestamp {
	var resp rpcTypes.AddEdgeResponse
	err := c.conn.Call("QueryManager.AddEdge", &rpcTypes.AddEdgeRequest{
		FromVertexID: fromVertexID,
		ToVertexID:   toVertexID,
		Properties:   properties,
	}, &resp)
	if err != nil {
		c.t.Fatalf("AddEdge failed: %v", err)
	}
	if !resp.Success {
		c.t.Fatalf("AddEdge returned success=false")
	}
	c.timestampLog = append(c.timestampLog, resp.Timestamp)
	return resp.Timestamp
}

// DeleteEdge deletes an edge and returns the timestamp
func (c *TestClient) DeleteEdge(fromVertexID, toVertexID types.VertexId) types.Timestamp {
	var resp rpcTypes.DeleteEdgeResponse
	err := c.conn.Call("QueryManager.DeleteEdge", &rpcTypes.DeleteEdgeRequest{
		FromVertexID: fromVertexID,
		ToVertexID:   toVertexID,
	}, &resp)
	if err != nil {
		c.t.Fatalf("DeleteEdge failed: %v", err)
	}
	if !resp.Success {
		c.t.Fatalf("DeleteEdge returned success=false")
	}
	c.timestampLog = append(c.timestampLog, resp.Timestamp)
	return resp.Timestamp
}

// BFS performs a BFS query at a given timestamp
func (c *TestClient) BFS(startVertexID types.VertexId, radius int, timestamp types.Timestamp) []types.VertexId {
	var resp rpcTypes.BFSResponse
	err := c.conn.Call("QueryManager.BFS", &rpcTypes.BFSRequest{
		StartVertexID: startVertexID,
		Radius:        radius,
		Timestamp:     timestamp,
	}, &resp)
	if err != nil {
		c.t.Fatalf("BFS failed: %v", err)
	}
	return resp.Vertices
}

// Helper function to check if two slices contain the same elements (ignoring order)
func sameElements(a, b []types.VertexId) bool {
	if len(a) != len(b) {
		return false
	}
	sortedA := make([]types.VertexId, len(a))
	sortedB := make([]types.VertexId, len(b))
	copy(sortedA, a)
	copy(sortedB, b)
	sort.Slice(sortedA, func(i, j int) bool {
		return sortedA[i] < sortedA[j]
	})
	sort.Slice(sortedB, func(i, j int) bool {
		return sortedB[i] < sortedB[j]
	})
	for i := range sortedA {
		if sortedA[i] != sortedB[i] {
			return false
		}
	}
	return true
}

// TestBasicOperations tests basic add vertex, add edge, and BFS operations
func TestBasicOperations(t *testing.T) {
	cfg, err := config.LoadConfig("../config.yaml")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	client := NewTestClient(t, cfg)
	defer client.Close()
	defer func() {
		client.dumpTimestampedState(t)
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
	if !sameElements(result, []types.VertexId{v1}) {
		t.Errorf("BFS(v1, radius=0) expected [%s], got %v", v1, result)
	}

	// Query BFS from v1 with radius 1 (should return v1 and v2)
	result = client.BFS(v1, 1, currentTime)
	if !sameElements(result, []types.VertexId{v1, v2}) {
		t.Errorf("BFS(v1, radius=1) expected [%s, %s], got %v", v1, v2, result)
	}

	// Query BFS from v1 with radius 2 (should return v1, v2, v3)
	result = client.BFS(v1, 2, currentTime)
	if !sameElements(result, []types.VertexId{v1, v2, v3}) {
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

	client := NewTestClient(t, cfg)
	defer client.Close()
	defer func() {
		client.dumpTimestampedState(t)
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
	if !sameElements(result, []types.VertexId{v1, v2}) {
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
	if !sameElements(result, []types.VertexId{v1, v2, v3}) {
		t.Errorf("At timestamp %.0f, BFS(v1, 2) expected [%s, %s, %s], got %v", queryTs2, v1, v2, v3, result)
	}

	// Query at old timestamp (queryTs1): should still see only v1 -> v2 (no v3)
	result = client.BFS(v1, 2, queryTs1)
	if !sameElements(result, []types.VertexId{v1, v2}) {
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

	client := NewTestClient(t, cfg)
	defer client.Close()
	defer func() {
		client.dumpTimestampedState(t)
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
	if !sameElements(result, []types.VertexId{v1, v2, v3}) {
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
	if !sameElements(result, []types.VertexId{v1}) {
		t.Errorf("After deletion, BFS(v1, 2) expected [%s], got %v", v1, result)
	}

	// Query at timestamp before deletion: should still see all three vertices
	result = client.BFS(v1, 2, beforeDeletionTs)
	if !sameElements(result, []types.VertexId{v1, v2, v3}) {
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

	client := NewTestClient(t, cfg)
	defer client.Close()
	defer func() {
		client.dumpTimestampedState(t)
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
	if !sameElements(result, []types.VertexId{v1, v2, v3}) {
		t.Errorf("State 1: BFS(v1, 2) expected [%s, %s, %s], got %v", v1, v2, v3, result)
	}
	t.Logf("State 1 (ts=%.0f): v1->v2->v3 verified", ts_state1)

	// State 2: Add branch v1 -> v4
	time.Sleep(1 * time.Second)
	client.AddEdge(v1, v4, map[string]string{"weight": "3"})

	time.Sleep(1 * time.Second)
	ts_state2 := types.Timestamp(time.Now().Unix())
	result = client.BFS(v1, 1, ts_state2)
	if !sameElements(result, []types.VertexId{v1, v2, v4}) {
		t.Errorf("State 2: BFS(v1, 1) expected [%s, %s, %s], got %v", v1, v2, v4, result)
	}
	t.Logf("State 2 (ts=%.0f): Added v1->v4 branch verified", ts_state2)

	// State 3: Delete v1 -> v2
	time.Sleep(1 * time.Second)
	client.DeleteEdge(v1, v2)

	time.Sleep(1 * time.Second)
	ts_state3 := types.Timestamp(time.Now().Unix())
	result = client.BFS(v1, 2, ts_state3)
	if !sameElements(result, []types.VertexId{v1, v4}) {
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
	if !sameElements(result, []types.VertexId{v1, v4}) {
		t.Errorf("State 4: BFS(v1, 2) expected [%s, %s], got %v", v1, v4, result)
	}
	t.Logf("State 4 (ts=%.0f): Added v3->v4, BFS from v1 still reaches [v1, v4]", ts_state4)

	// Verify historical queries work correctly
	result = client.BFS(v1, 2, ts_state1)
	if !sameElements(result, []types.VertexId{v1, v2, v3}) {
		t.Errorf("Historical query at ts_state1: expected [%s, %s, %s], got %v", v1, v2, v3, result)
	}

	result = client.BFS(v1, 1, ts_state2)
	if !sameElements(result, []types.VertexId{v1, v2, v4}) {
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

	client := NewTestClient(t, cfg)
	defer client.Close()
	defer func() {
		client.dumpTimestampedState(t)
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
	if !sameElements(result1, []types.VertexId{v1, v2}) {
		t.Errorf("At ts1, BFS(v1, 1) expected [%s, %s], got %v", v1, v2, result1)
	}

	result2 := client.BFS(v1, 1, query_ts2)
	if !sameElements(result2, []types.VertexId{v1, v2}) {
		t.Errorf("At ts2, BFS(v1, 1) expected [%s, %s], got %v", v1, v2, result2)
	}

	result3 := client.BFS(v1, 1, query_ts3)
	if !sameElements(result3, []types.VertexId{v1, v2}) {
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

	client := NewTestClient(t, cfg)
	defer client.Close()
	defer func() {
		client.dumpTimestampedState(t)
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
	if !sameElements(result_add, []types.VertexId{v1, v2}) {
		t.Errorf("After initial add, BFS(v1, 1) expected [%s, %s], got %v", v1, v2, result_add)
	}

	result_delete := client.BFS(v1, 1, ts_after_delete)
	if !sameElements(result_delete, []types.VertexId{v1}) {
		t.Errorf("After delete, BFS(v1, 1) expected [%s], got %v", v1, result_delete)
	}

	result_readd := client.BFS(v1, 1, ts_after_readd)
	if !sameElements(result_readd, []types.VertexId{v1, v2}) {
		t.Errorf("After re-add, BFS(v1, 1) expected [%s, %s], got %v", v1, v2, result_readd)
	}

	// Historical query should still work
	result_historical := client.BFS(v1, 1, ts_after_add)
	if !sameElements(result_historical, []types.VertexId{v1, v2}) {
		t.Errorf("Historical query at first add timestamp, BFS(v1, 1) expected [%s, %s], got %v",
			v1, v2, result_historical)
	}

	t.Log("MVCC delete and re-add test passed")
}

// Helper function to start a background process
func startProcess(t *testing.T, name string, args ...string) *exec.Cmd {
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		t.Fatalf("Failed to start %s: %v", name, err)
	}
	return cmd
}

// TestMain sets up and tears down the test environment
func TestMain(m *testing.M) {
	log.Println("Setting up end-to-end test environment...")

	// Load config
	cfg, err := config.LoadConfig("../config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Start shard replica processes
	var shardCmds []*exec.Cmd
	for _, shard := range cfg.Shards {
		for _, replica := range shard.Replicas {
			log.Printf("Starting shard %d replica %d...", shard.ID, replica.ID)
			cmd := exec.Command("go", "run", "../cmd/shard/main.go",
				"-config", "../config.yaml",
				"-shard-id", fmt.Sprintf("%d", shard.ID),
				"-replica-id", fmt.Sprintf("%d", replica.ID))
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			// Set process group so we can kill the entire group including child processes
			cmd.SysProcAttr = &syscall.SysProcAttr{
				Setpgid: true,
			}
			err := cmd.Start()
			if err != nil {
				log.Fatalf("Failed to start shard %d replica %d: %v", shard.ID, replica.ID, err)
			}
			shardCmds = append(shardCmds, cmd)
		}
	}

	// Start query manager
	log.Println("Starting query manager...")
	qmCmd := exec.Command("go", "run", "../cmd/qm/main.go", "-config", "../config.yaml")
	qmCmd.Stdout = os.Stdout
	qmCmd.Stderr = os.Stderr
	// Set process group so we can kill the entire group including child processes
	qmCmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}
	err = qmCmd.Start()
	if err != nil {
		log.Fatalf("Failed to start query manager: %v", err)
	}

	// Wait for services to be ready
	log.Println("Waiting for services to start...")
	time.Sleep(5 * time.Second)

	// Run tests
	exitCode := m.Run()

	// Cleanup
	log.Println("Cleaning up test environment...")
	// Kill the entire process group (negative PID) to ensure child processes are terminated
	if qmCmd.Process != nil {
		pgid, err := syscall.Getpgid(qmCmd.Process.Pid)
		if err == nil {
			// Kill entire process group
			syscall.Kill(-pgid, syscall.SIGINT)
		}
		qmCmd.Wait()
	}
	for _, cmd := range shardCmds {
		if cmd.Process != nil {
			pgid, err := syscall.Getpgid(cmd.Process.Pid)
			if err == nil {
				// Kill entire process group
				syscall.Kill(-pgid, syscall.SIGINT)
			}
			cmd.Wait()
		}
	}

	os.Exit(exitCode)
}
