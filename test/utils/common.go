package utils

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
	err := c.conn.Call("QueryManager.ShardedBFS", &rpcTypes.BFSRequest{
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
