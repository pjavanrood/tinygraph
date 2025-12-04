package utils

import (
	"fmt"
	"net/rpc"
	"os"
	"os/exec"
	"sort"
	"testing"
	"time"

	"github.com/pjavanrood/tinygraph/internal/config"
	"github.com/pjavanrood/tinygraph/internal/types"
	rpcTypes "github.com/pjavanrood/tinygraph/pkg/rpc"
)

// Helper function to check if two slices contain the same elements (ignoring order)
func SameElements(a, b []types.VertexId) bool {
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
func (c *TestClient) DumpTimestampedState(t *testing.T) {
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

// Helper function to start a background process
func StartProcess(t *testing.T, name string, args ...string) *exec.Cmd {
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		t.Fatalf("Failed to start %s: %v", name, err)
	}
	return cmd
}
