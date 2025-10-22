package main

import (
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pjavanrood/tinygraph/internal/config"
	"github.com/pjavanrood/tinygraph/internal/types"
	rpcTypes "github.com/pjavanrood/tinygraph/pkg/rpc"
)

type GraphClient struct {
	cfg         *config.Config
	conn        *rpc.Client
	vertexIDMap map[string]types.VertexId // external vertex ID -> internal vertex ID
}

func NewGraphClient(cfg *config.Config) *GraphClient {
	conn, err := rpc.Dial("tcp", fmt.Sprintf("%s:%d", cfg.QueryManager.Host, cfg.QueryManager.Port))
	if err != nil {
		log.Fatalf("Failed to connect to query manager: %v", err)
	}
	return &GraphClient{
		cfg:         cfg,
		conn:        conn,
		vertexIDMap: make(map[string]types.VertexId),
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
	fmt.Println("BFS result:")
	for _, vertexInternalID := range verticesInternalIDs {
		vertex, err := gc.getExternalVertexID(vertexInternalID)
		if err != nil {
			log.Fatalf("Vertex %s not found", vertexInternalID)
		}
		fmt.Print(vertex + " ")
	}
	fmt.Println()
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
		if len(fields) != 3 {
			log.Fatalf("Invalid workload line: %s", line)
		}

		switch fields[0] {
		case "A":
			weight := 0
			if len(fields) == 4 {
				weight, err = strconv.Atoi(fields[3])
				if err != nil {
					log.Fatalf("Invalid weight: %v", err)
				}
			}
			client.AddEdge(fields[1], fields[2], weight)
		case "D":
			client.DeleteEdge(fields[1], fields[2])
		case "Q":
			radius := 1
			if len(fields) == 3 {
				radius, err = strconv.Atoi(fields[2])
				if err != nil {
					log.Fatalf("Invalid radius: %v", err)
				}
			}
			client.BFS(fields[1], radius)
		}
	}
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
