package main

import (
	"flag"
	"fmt"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dominikbraun/graph"
	"github.com/pjavanrood/tinygraph/internal/config"
	"github.com/pjavanrood/tinygraph/internal/types"
	"github.com/pjavanrood/tinygraph/internal/util"
	rpcTypes "github.com/pjavanrood/tinygraph/pkg/rpc"
)

var log = util.New("Client")

type GraphClient struct {
	cfg         *config.Config
	conn        *rpc.Client
	vertexIDMap map[string]types.VertexId   // external vertex ID -> internal vertex ID
	localGraph  graph.Graph[string, string] // local graph for verification
}

func NewGraphClient(cfg *config.Config) *GraphClient {
	conn, err := rpc.Dial("tcp", fmt.Sprintf("%s:%d", cfg.QueryManager.Host, cfg.QueryManager.Port))
	if err != nil {
		log.Fatalf("Failed to connect to query manager: %v", err)
	}

	// Create a directed graph for local verification
	g := graph.New(graph.StringHash, graph.Directed())

	return &GraphClient{
		cfg:         cfg,
		conn:        conn,
		vertexIDMap: make(map[string]types.VertexId),
		localGraph:  g,
	}
}

func (gc *GraphClient) getExternalVertexID(internalVertexID types.VertexId) (string, error) {
	for externalVertexID, internalVertexID_ := range gc.vertexIDMap {
		if internalVertexID_ == internalVertexID {
			return externalVertexID, nil
		}
	}
	return "", fmt.Errorf("vertex %s not found", internalVertexID)
}

func (gc *GraphClient) sendAddVertexRPC(properties types.Properties) (types.VertexId, error) {
	var resp rpcTypes.AddVertexResponse
	err := gc.conn.Call("QueryManager.AddVertex", &rpcTypes.AddVertexRequest{
		Properties: properties,
	}, &resp)
	return resp.VertexID, err
}

func (gc *GraphClient) sendAddEdgeRPC(fromVertexID types.VertexId, toVertexID types.VertexId, properties types.Properties) (bool, error) {
	var resp rpcTypes.AddEdgeResponse
	err := gc.conn.Call("QueryManager.AddEdge", &rpcTypes.AddEdgeRequest{
		FromVertexID: fromVertexID,
		ToVertexID:   toVertexID,
		Properties:   properties,
	}, &resp)
	return resp.Success, err
}

func (gc *GraphClient) sendDeleteEdgeRPC(fromVertexID types.VertexId, toVertexID types.VertexId) error {
	var resp rpcTypes.DeleteEdgeResponse
	err := gc.conn.Call("QueryManager.DeleteEdge", &rpcTypes.DeleteEdgeRequest{
		FromVertexID: fromVertexID,
		ToVertexID:   toVertexID,
	}, &resp)
	return err
}

func (gc *GraphClient) sendBFSRPC(startVertexID types.VertexId, radius int) ([]types.VertexId, error) {
	var resp rpcTypes.BFSResponse
	err := gc.conn.Call("QueryManager.BFS", &rpcTypes.BFSRequest{
		StartVertexID: startVertexID,
		Radius:        radius,
		Timestamp:     types.Timestamp(float64(time.Now().Unix())),
	}, &resp)
	if err != nil {
		return nil, err
	}
	return resp.Vertices, nil
}

func (gc *GraphClient) sendFetchAllRPC() (map[int][]rpcTypes.VertexInfo, error) {
	var resp rpcTypes.FetchAllResponse
	err := gc.conn.Call("QueryManager.FetchAll", &rpcTypes.FetchAllRequest{}, &resp)
	if err != nil {
		return nil, err
	}
	return resp.ShardVertices, nil
}

func (gc *GraphClient) sendDeleteAllRPC() (bool, error) {
	var resp rpcTypes.DeleteAllResponse
	err := gc.conn.Call("QueryManager.DeleteAll", &rpcTypes.DeleteAllRequest{}, &resp)
	if err != nil {
		return false, err
	}
	return resp.Success, nil
}

func (gc *GraphClient) AddVertex(vertexID string) types.VertexId {
	properties := types.Properties{
		"external_id": vertexID,
	}

	out, err := gc.sendAddVertexRPC(properties)
	if err != nil {
		log.Fatalf("Failed to add vertex: %v", err)
	}
	return out
}

func (gc *GraphClient) AddEdge(fromVertexID string, toVertexID string, weight int) {
	fromVertexID_internal, ok := gc.vertexIDMap[fromVertexID]
	if !ok {
		fromVertexID_internal = gc.AddVertex(fromVertexID)
		gc.vertexIDMap[fromVertexID] = fromVertexID_internal
	}
	toVertexID_internal, ok := gc.vertexIDMap[toVertexID]
	if !ok {
		toVertexID_internal = gc.AddVertex(toVertexID)
		gc.vertexIDMap[toVertexID] = toVertexID_internal
	}
	properties := map[string]string{
		"weight": strconv.Itoa(weight),
	}
	success, err := gc.sendAddEdgeRPC(fromVertexID_internal, toVertexID_internal, properties)
	if err != nil {
		log.Fatalf("Failed to add edge: %v", err)
	}
	if !success {
		log.Fatalf("Failed to add edge")
	}

	// Add to local graph for verification
	_ = gc.localGraph.AddVertex(fromVertexID)
	_ = gc.localGraph.AddVertex(toVertexID)
	err = gc.localGraph.AddEdge(fromVertexID, toVertexID)
	if err != nil {
		log.Printf("Warning: Failed to add edge to local graph: %v", err)
	}
}

func (gc *GraphClient) DeleteEdge(fromVertexID string, toVertexID string) {
	fromVertexID_internal, ok := gc.vertexIDMap[fromVertexID]
	if !ok {
		log.Fatalf("From vertex %s not found", fromVertexID)
	}
	toVertexID_internal, ok := gc.vertexIDMap[toVertexID]
	if !ok {
		log.Fatalf("To vertex %s not found", toVertexID)
	}
	err := gc.sendDeleteEdgeRPC(fromVertexID_internal, toVertexID_internal)
	if err != nil {
		log.Fatalf("Failed to delete edge: %v", err)
	}

	// Remove from local graph for verification
	err = gc.localGraph.RemoveEdge(fromVertexID, toVertexID)
	if err != nil {
		log.Printf("Warning: Failed to remove edge from local graph: %v", err)
	}
}

// performLocalBFS performs BFS on the local graph and returns vertices within the given radius
func (gc *GraphClient) performLocalBFS(startVertexID string, radius int) ([]string, error) {
	if radius < 0 {
		return nil, fmt.Errorf("radius must be non-negative")
	}

	visited := make(map[string]bool)
	result := []string{}

	// BFS using a queue with distance tracking
	type queueItem struct {
		vertex string
		dist   int
	}
	queue := []queueItem{{vertex: startVertexID, dist: 0}}
	visited[startVertexID] = true
	result = append(result, startVertexID)

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		// Don't explore beyond the radius
		if current.dist >= radius {
			continue
		}

		// Get adjacency map for the current vertex
		adjacencyMap, err := gc.localGraph.AdjacencyMap()
		if err != nil {
			return nil, fmt.Errorf("failed to get adjacency map: %v", err)
		}

		neighbors, ok := adjacencyMap[current.vertex]
		if !ok {
			continue
		}

		// Visit all neighbors
		for neighbor := range neighbors {
			if !visited[neighbor] {
				visited[neighbor] = true
				result = append(result, neighbor)
				queue = append(queue, queueItem{vertex: neighbor, dist: current.dist + 1})
			}
		}
	}

	return result, nil
}

func (gc *GraphClient) BFS(startVertexID string, radius int) {
	startVertexID_internal, ok := gc.vertexIDMap[startVertexID]
	if !ok {
		log.Fatalf("Start vertex %s not found", startVertexID)
	}
	verticesInternalIDs, err := gc.sendBFSRPC(startVertexID_internal, radius)
	if err != nil {
		log.Fatalf("Failed to perform BFS: %v", err)
	}

	// Convert internal IDs to external IDs
	verticesExternalIDs := []string{}
	fmt.Println("BFS result:")
	for _, vertexInternalID := range verticesInternalIDs {
		vertex, err := gc.getExternalVertexID(vertexInternalID)
		if err != nil {
			log.Fatalf("Vertex %s not found", vertexInternalID)
		}
		verticesExternalIDs = append(verticesExternalIDs, vertex)
		fmt.Print(vertex + " ")
	}
	fmt.Println()

	// Perform local BFS for verification
	localBFSResult, err := gc.performLocalBFS(startVertexID, radius)
	if err != nil {
		log.Printf("Warning: Failed to perform local BFS verification: %v", err)
		return
	}

	// Compare results
	if gc.verifyBFSResults(verticesExternalIDs, localBFSResult) {
		fmt.Println("✓ BFS verification PASSED: Results match local graph")
	} else {
		fmt.Println("✗ BFS verification FAILED: Results do not match local graph")
		fmt.Printf("  Expected (local): %v\n", localBFSResult)
		fmt.Printf("  Got (remote):     %v\n", verticesExternalIDs)
	}
}

// verifyBFSResults compares two BFS results (order-independent)
func (gc *GraphClient) verifyBFSResults(remoteResult, localResult []string) bool {
	if len(remoteResult) != len(localResult) {
		return false
	}

	// Convert to sets for comparison
	remoteSet := make(map[string]bool)
	for _, v := range remoteResult {
		remoteSet[v] = true
	}

	localSet := make(map[string]bool)
	for _, v := range localResult {
		localSet[v] = true
	}

	// Check if sets are equal
	if len(remoteSet) != len(localSet) {
		return false
	}

	for v := range remoteSet {
		if !localSet[v] {
			return false
		}
	}

	return true
}

func (gc *GraphClient) FetchAll() {
	shardVertices, err := gc.sendFetchAllRPC()
	if err != nil {
		log.Fatalf("Failed to fetch all vertices: %v", err)
	}
	fmt.Println("FetchAll result:")
	for shardID, vertices := range shardVertices {
		fmt.Printf("Shard %d:\n", shardID)
		for _, vertexInfo := range vertices {
			externalID := "unknown"
			if extID, ok := vertexInfo.Properties["external_id"]; ok {
				externalID = extID
			}
			fmt.Printf("  Vertex %s (internal: %s, timestamp: %v)\n",
				externalID, vertexInfo.VertexID, vertexInfo.Timestamp)
		}
	}
}

func (gc *GraphClient) DeleteAll() {
	success, err := gc.sendDeleteAllRPC()
	if err != nil {
		log.Fatalf("Failed to delete all: %v", err)
	}
	if !success {
		log.Fatalf("Delete all operation failed")
	}

	// Clear local state
	gc.vertexIDMap = make(map[string]types.VertexId)
	gc.localGraph = graph.New(graph.StringHash, graph.Directed())

	fmt.Println("Successfully deleted all vertices and edges from the graph")
}

func runWorkload(workloadPath string, cfg *config.Config) {
	workload, err := os.ReadFile(workloadPath)
	if err != nil {
		log.Fatalf("Failed to read workload file: %v", err)
	}

	client := NewGraphClient(cfg)
	defer client.conn.Close()

	for _, line := range strings.Split(string(workload), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Split(line, " ")
		if len(fields) < 1 {
			log.Fatalf("Invalid workload line: %s", line)
		}

		switch fields[0] {
		case "A":
			if len(fields) < 3 {
				log.Fatalf("Invalid Add Edge command, expected at least 3 fields: %s", line)
			}
			weight := 0
			if len(fields) == 4 {
				weight, err = strconv.Atoi(fields[3])
				if err != nil {
					log.Fatalf("Invalid weight: %v", err)
				}
			}
			client.AddEdge(fields[1], fields[2], weight)
		case "D":
			if len(fields) != 3 {
				log.Fatalf("Invalid Delete Edge command, expected 3 fields: %s", line)
			}
			client.DeleteEdge(fields[1], fields[2])
		case "Q":
			if len(fields) == 1 {
				// Q without parameters - fetch all vertices
				client.FetchAll()
			} else {
				// Q with parameters - perform BFS
				if len(fields) < 2 {
					log.Fatalf("Invalid Query command: %s", line)
				}
				radius := 1
				if len(fields) >= 3 {
					radius, err = strconv.Atoi(fields[2])
					if err != nil {
						log.Fatalf("Invalid radius: %v", err)
					}
				}
				client.BFS(fields[1], radius)
			}
		default:
			log.Fatalf("Unknown command: %s", fields[0])
		}
	}

	// After processing all commands, delete all vertices and edges
	log.Println("Workload completed. Deleting all vertices and edges...")
	client.DeleteAll()
}

func main() {
	// Parse command-line flags
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	workloadPath := flag.String("workload", "workloads/simple_graph.txt", "Path to workload file")
	flag.Parse()

	log.Println("Starting Client...")

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	runWorkload(*workloadPath, cfg)
}
