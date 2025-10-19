package main

import (
	"flag"
	"log"
	"os"
	"strings"
	"net/rpc"
	"fmt"
	"strconv"

	"github.com/pjavanrood/tinygraph/internal/config"
	rpcTypes "github.com/pjavanrood/tinygraph/pkg/rpc"
)

type GraphClient struct {
	cfg *config.Config
	conn *rpc.Client
	vertexIDMap map[string]string
	edgeIDMap map[string]string
}

func NewGraphClient(cfg *config.Config) *GraphClient {
	conn, err := rpc.Dial("tcp", fmt.Sprintf("%s:%d", cfg.QueryManager.Host, cfg.QueryManager.Port))
	if err != nil {
		log.Fatalf("Failed to connect to query manager: %v", err)
	}
	return &GraphClient{
		cfg: cfg,
		conn: conn,
		vertexIDMap: make(map[string]string),
		edgeIDMap: make(map[string]string),
	}
}

func (gc *GraphClient) sendAddVertexRPC(properties map[string]string) (string, error) {
	var resp rpcTypes.AddVertexResponse
	err := gc.conn.Call("QueryManager.AddVertex", &rpcTypes.AddVertexRequest{
		Properties: properties,
	}, &resp)
	return resp.VertexID, err
}

func (gc *GraphClient) sendAddEdgeRPC(fromVertexID string, toVertexID string, properties map[string]string) (string, error) {
	var resp rpcTypes.AddEdgeResponse
	err := gc.conn.Call("QueryManager.AddEdge", &rpcTypes.AddEdgeRequest{
		FromVertexID: fromVertexID,
		ToVertexID: toVertexID,
		Properties: properties,
	}, &resp)
	return resp.EdgeID, err
}

func (gc *GraphClient) sendDeleteEdgeRPC(edgeID string) error {
	var resp rpcTypes.DeleteEdgeResponse
	err := gc.conn.Call("QueryManager.DeleteEdge", &rpcTypes.DeleteEdgeRequest{
		EdgeID: edgeID,
	}, &resp)
	return err
}

func (gc *GraphClient) AddVertex(vertexID string) string {
	properties := map[string]string{
		"external_id": vertexID,
	}

	vertexID, err := gc.sendAddVertexRPC(properties)
	if err != nil {
		log.Fatalf("Failed to add vertex: %v", err)
	}
	return vertexID
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
	edgeID, err := gc.sendAddEdgeRPC(fromVertexID_internal, toVertexID_internal, properties)
	if err != nil {
		log.Fatalf("Failed to add edge: %v", err)
	}
	gc.edgeIDMap[fmt.Sprintf("%s-%s", fromVertexID_internal, toVertexID_internal)] = edgeID
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
	edgeID, ok := gc.edgeIDMap[fmt.Sprintf("%s-%s", fromVertexID_internal, toVertexID_internal)]
	if !ok {
		log.Fatalf("Edge %s-%s not found", fromVertexID, toVertexID)
	}
	err := gc.sendDeleteEdgeRPC(edgeID)
	if err != nil {
		log.Fatalf("Failed to delete edge: %v", err)
	}
	delete(gc.edgeIDMap, fmt.Sprintf("%s-%s", fromVertexID_internal, toVertexID_internal))
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
