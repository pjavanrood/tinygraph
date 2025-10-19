package qm

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"strconv"
	"strings"

	"github.com/pjavanrood/tinygraph/internal/config"
	rpcTypes "github.com/pjavanrood/tinygraph/pkg/rpc"
)

// QueryManager handles client queries and coordinates shards
type QueryManager struct {
	config *config.Config
}

// NewQueryManager creates a new query manager instance
func NewQueryManager(cfg *config.Config) *QueryManager {
	return &QueryManager{
		config: cfg,
	}
}

// generate a vertex ID for a shard: VertexID = "%shardID-%randomHex"
func (qm *QueryManager) generateVertexID(shardConfig *config.ShardConfig) string {
	// Compact encoding: shardID-randomHex
	// Example: "0-a3f2b8c1d4e5f6a7" or "42-1234567890abcdef"
	randomPart := fmt.Sprintf("%016x", rand.Int63())
	return fmt.Sprintf("%d-%s", shardConfig.ID, randomPart)
}

func (qm *QueryManager) getShardIDFromVertexID(vertexID string) (int, error) {
	// Parse the vertex ID format: shardID-randomHex
	parts := strings.Split(vertexID, "-")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid vertex ID format: %s", vertexID)
	}

	shardID, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, fmt.Errorf("invalid shard ID in vertex ID: %s", vertexID)
	}

	return shardID, nil
}

func (qm *QueryManager) addVertexToShard(shardConfig *config.ShardConfig, req *rpcTypes.AddVertexToShardRequest) error {
	// Connect to the shard
	addr := shardConfig.GetAddress()
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to connect to shard %d at %s: %w", shardConfig.ID, addr, err)
	}
	defer client.Close()

	// Make the RPC call to add the vertex
	var resp rpcTypes.AddVertexToShardResponse
	err = client.Call("Shard.AddVertex", req, &resp)
	if err != nil {
		return fmt.Errorf("RPC call to shard %d failed: %w", shardConfig.ID, err)
	}

	if !resp.Success {
		return fmt.Errorf("shard %d failed to add vertex", shardConfig.ID)
	}

	return nil
}

func (qm *QueryManager) addEdgeToShard(shardConfig *config.ShardConfig, req *rpcTypes.AddEdgeToShardRequest) (string, error) {
	// Connect to the shard
	addr := shardConfig.GetAddress()
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return "", fmt.Errorf("failed to connect to shard %d at %s: %w", shardConfig.ID, addr, err)
	}
	defer client.Close()

	// Make the RPC call to add the edge
	var resp rpcTypes.AddEdgeToShardResponse
	err = client.Call("Shard.AddEdge", req, &resp)
	if err != nil {
		return "", fmt.Errorf("RPC call to shard %d failed: %w", shardConfig.ID, err)
	}

	if !resp.Success {
		return "", fmt.Errorf("shard %d failed to add edge", shardConfig.ID)
	}

	return resp.EdgeID, nil
}

func (qm *QueryManager) deleteEdgeToShard(shardConfig *config.ShardConfig, req *rpcTypes.DeleteEdgeToShardRequest) error {
	// Connect to the shard
	addr := shardConfig.GetAddress()
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to connect to shard %d at %s: %w", shardConfig.ID, addr, err)
	}
	defer client.Close()

	// Make the RPC call to delete the edge
	var resp rpcTypes.DeleteEdgeToShardResponse
	err = client.Call("Shard.DeleteEdge", req, &resp)
	if err != nil {
		return fmt.Errorf("RPC call to shard %d failed: %w", shardConfig.ID, err)
	}

	if !resp.Success {
		return fmt.Errorf("shard %d failed to delete edge", shardConfig.ID)
	}

	return nil
}

// AddVertex is the RPC handler for adding a vertex
func (qm *QueryManager) AddVertex(req *rpcTypes.AddVertexRequest, resp *rpcTypes.AddVertexResponse) error {
	log.Printf("Received AddVertex request")

	// Select a shard and generate a vertex ID
	shardConfig := RandomPartitioner(qm.config)
	vertexID := qm.generateVertexID(shardConfig)

	// Add the vertex to the shard
	err := qm.addVertexToShard(shardConfig, &rpcTypes.AddVertexToShardRequest{
		VertexID:   vertexID,
		Properties: req.Properties,
	})
	if err != nil {
		log.Printf("Failed to add vertex: %v", err)
		resp.Success = false
		return err
	}

	resp.Success = true
	resp.VertexID = vertexID
	log.Printf("Successfully added vertex with ID: %s", vertexID)
	return nil
}

// AddEdge is the RPC handler for adding an edge
func (qm *QueryManager) AddEdge(req *rpcTypes.AddEdgeRequest, resp *rpcTypes.AddEdgeResponse) error {
	log.Printf("Received AddEdge request")

	shardID, err := qm.getShardIDFromVertexID(req.FromVertexID)
	if err != nil {
		log.Printf("Failed to get shard ID from vertex ID: %v", err)
		resp.Success = false
		return err
	}

	shardConfig, err := qm.config.GetShardByID(shardID)
	if err != nil {
		log.Printf("Failed to get shard by ID: %v", err)
		resp.Success = false
		return err
	}

	edgeID, err := qm.addEdgeToShard(shardConfig, &rpcTypes.AddEdgeToShardRequest{
		FromVertexID: req.FromVertexID,
		ToVertexID: req.ToVertexID,
		Properties: req.Properties,
	})
	if err != nil {
		log.Printf("Failed to add edge: %v", err)
		resp.Success = false
		return err
	}

	resp.Success = true
	resp.EdgeID = edgeID
	return nil
}

// DeleteEdge is the RPC handler for deleting an edge
func (qm *QueryManager) DeleteEdge(req *rpcTypes.DeleteEdgeRequest, resp *rpcTypes.DeleteEdgeResponse) error {
	log.Printf("Received DeleteEdge request")

	shardID, err := qm.getShardIDFromVertexID(req.EdgeID)
	if err != nil {
		log.Printf("Failed to get shard ID from edge ID: %v", err)
		resp.Success = false
		return err
	}

	shardConfig, err := qm.config.GetShardByID(shardID)
	if err != nil {
		log.Printf("Failed to get shard by ID: %v", err)
		resp.Success = false
		return err
	}

	err = qm.deleteEdgeToShard(shardConfig, &rpcTypes.DeleteEdgeToShardRequest{
		EdgeID: req.EdgeID,
	})
	if err != nil {
		log.Printf("Failed to delete edge: %v", err)
		resp.Success = false
		return err
	}

	resp.Success = true
	return nil
}

// Start begins the RPC server
func (qm *QueryManager) Start() error {
	// Register the RPC service
	server := rpc.NewServer()
	err := server.Register(qm)
	if err != nil {
		return fmt.Errorf("failed to register RPC service: %v", err)
	}

	// Listen on the specified port from config
	addr := fmt.Sprintf("%s:%d", qm.config.QueryManager.Host, qm.config.QueryManager.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", addr, err)
	}

	log.Printf("Query Manager RPC server listening on %s", addr)
	log.Printf("Connected to %d shards", len(qm.config.Shards))

	// Accept and serve connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		go server.ServeConn(conn)
	}
}
