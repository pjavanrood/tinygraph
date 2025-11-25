package qm

import (
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"strconv"
	"strings"
	"time"

	"github.com/pjavanrood/tinygraph/internal/config"
	"github.com/pjavanrood/tinygraph/internal/types"
	"github.com/pjavanrood/tinygraph/internal/util"
	rpcTypes "github.com/pjavanrood/tinygraph/pkg/rpc"
)

var log = util.New("QueryManager")

// QueryManager handles client queries and coordinates shards
type QueryManager struct {
	config         *config.Config
	replicaManager ReplicaManager
}

// NewQueryManager creates a new query manager instance
func NewQueryManager(cfg *config.Config) *QueryManager {
	return &QueryManager{
		config:         cfg,
		replicaManager: NewPushBasedReplicaManager(cfg),
	}
}

func (qm *QueryManager) generateTimestamp() types.Timestamp {
	return types.Timestamp(time.Now().Unix())
}

// generate a vertex ID for a shard: VertexID = "%shardID-%randomHex"
func (qm *QueryManager) generateVertexID(shardConfig *config.ShardConfig) types.VertexId {
	// Compact encoding: shardID-randomHex
	// Example: "0-a3f2b8c1d4e5f6a7" or "42-1234567890abcdef"
	randomPart := fmt.Sprintf("%016x", rand.Int63())
	return types.VertexId(fmt.Sprintf("%d-%s", shardConfig.ID, randomPart))
}

func (qm *QueryManager) getShardIDFromVertexID(vertexID types.VertexId) (int, error) {
	// Parse the vertex ID format: shardID-randomHex
	parts := strings.Split(string(vertexID), "-")
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
	log.Printf("Adding vertex to shard %d", shardConfig.ID)

	// Connect to the shard
	leaderID, err := qm.replicaManager.GetLeaderID(shardConfig.ID)
	if err != nil {
		return fmt.Errorf("failed to get leader ID for shard %d: %w", shardConfig.ID, err)
	}
	addr, err := shardConfig.GetReplicaAddress(leaderID)
	if err != nil {
		return fmt.Errorf("failed to get replica address for shard %d: %w", shardConfig.ID, err)
	}
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

func (qm *QueryManager) addEdgeToShard(shardConfig *config.ShardConfig, req *rpcTypes.AddEdgeToShardRequest) error {
	log.Printf("Adding edge to shard %d", shardConfig.ID)

	// Connect to the shard
	leaderID, err := qm.replicaManager.GetLeaderID(shardConfig.ID)
	if err != nil {
		return fmt.Errorf("failed to get leader ID for shard %d: %w", shardConfig.ID, err)
	}
	addr, err := shardConfig.GetReplicaAddress(leaderID)
	if err != nil {
		return fmt.Errorf("failed to get replica address for shard %d: %w", shardConfig.ID, err)
	}
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to connect to shard %d at %s: %w", shardConfig.ID, addr, err)
	}
	defer client.Close()

	// Make the RPC call to add the edge
	var resp rpcTypes.AddEdgeToShardResponse
	err = client.Call("Shard.AddEdge", req, &resp)
	if err != nil {
		return fmt.Errorf("RPC call to shard %d failed: %w", shardConfig.ID, err)
	}

	if !resp.Success {
		return fmt.Errorf("shard %d failed to add edge", shardConfig.ID)
	}

	return nil
}

func (qm *QueryManager) deleteEdgeToShard(shardConfig *config.ShardConfig, req *rpcTypes.DeleteEdgeToShardRequest) error {
	log.Printf("Deleting edge to shard %d", shardConfig.ID)

	// Connect to the shard
	leaderID, err := qm.replicaManager.GetLeaderID(shardConfig.ID)
	if err != nil {
		return fmt.Errorf("failed to get leader ID for shard %d: %w", shardConfig.ID, err)
	}
	addr, err := shardConfig.GetReplicaAddress(leaderID)
	if err != nil {
		return fmt.Errorf("failed to get replica address for shard %d: %w", shardConfig.ID, err)
	}
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

func (qm *QueryManager) getVertexAtToShard(shardConfig *config.ShardConfig, req *rpcTypes.GetVertexAtShardRequest, resp *rpcTypes.GetVertexAtShardResponse) error {
	log.Printf("GetVertexAt to shard %d", shardConfig.ID)

	// Connect to the shard
	leaderID, err := qm.replicaManager.GetLeaderID(shardConfig.ID)
	if err != nil {
		return fmt.Errorf("failed to get leader ID for shard %d: %w", shardConfig.ID, err)
	}
	addr, err := shardConfig.GetReplicaAddress(leaderID)
	if err != nil {
		return fmt.Errorf("failed to get replica address for shard %d: %w", shardConfig.ID, err)
	}
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to connect to shard %d at %s: %w", shardConfig.ID, addr, err)
	}
	defer client.Close()

	// Make the RPC call
	err = client.Call("Shard.GetVertexAt", req, resp)
	if err != nil {
		return fmt.Errorf("RPC call to shard %d failed: %w", shardConfig.ID, err)
	}

	return nil
}

func (qm *QueryManager) getEdgeAtToShard(shardConfig *config.ShardConfig, req *rpcTypes.GetEdgeAtShardRequest, resp *rpcTypes.GetEdgeAtShardResponse) error {
	log.Printf("GetEdgeAt to shard %d", shardConfig.ID)

	// Connect to the shard
	leaderID, err := qm.replicaManager.GetLeaderID(shardConfig.ID)
	if err != nil {
		return fmt.Errorf("failed to get leader ID for shard %d: %w", shardConfig.ID, err)
	}
	addr, err := shardConfig.GetReplicaAddress(leaderID)
	if err != nil {
		return fmt.Errorf("failed to get replica address for shard %d: %w", shardConfig.ID, err)
	}
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to connect to shard %d at %s: %w", shardConfig.ID, addr, err)
	}
	defer client.Close()

	// Make the RPC call
	err = client.Call("Shard.GetEdgeAt", req, resp)
	if err != nil {
		return fmt.Errorf("RPC call to shard %d failed: %w", shardConfig.ID, err)
	}

	return nil
}

func (qm *QueryManager) getNeighborsToShard(shardConfig *config.ShardConfig, req *rpcTypes.GetNeighborsToShardRequest) ([]types.VertexId, error) {
	log.Printf("Getting neighbors to shard %d", shardConfig.ID)

	// Connect to the shard
	leaderID, err := qm.replicaManager.GetLeaderID(shardConfig.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get leader ID for shard %d: %w", shardConfig.ID, err)
	}
	addr, err := shardConfig.GetReplicaAddress(leaderID)
	if err != nil {
		return nil, fmt.Errorf("failed to get replica address for shard %d: %w", shardConfig.ID, err)
	}
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to shard %d at %s: %w", shardConfig.ID, addr, err)
	}
	defer client.Close()

	// Make the RPC call to get the neighbors
	var resp rpcTypes.GetNeighborsToShardResponse
	err = client.Call("Shard.GetNeighbors", req, &resp)
	if err != nil {
		return nil, fmt.Errorf("RPC call to shard %d failed: %w", shardConfig.ID, err)
	}

	return resp.Neighbors, nil
}

func (qm *QueryManager) fetchAllFromShard(shardConfig *config.ShardConfig, req *rpcTypes.FetchAllToShardRequest) ([]rpcTypes.VertexInfo, error) {
	log.Printf("Fetching all vertices from shard %d", shardConfig.ID)

	// Connect to the shard
	leaderID, err := qm.replicaManager.GetLeaderID(shardConfig.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get leader ID for shard %d: %w", shardConfig.ID, err)
	}
	addr, err := shardConfig.GetReplicaAddress(leaderID)
	if err != nil {
		return nil, fmt.Errorf("failed to get replica address for shard %d: %w", shardConfig.ID, err)
	}
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to shard %d at %s: %w", shardConfig.ID, addr, err)
	}
	defer client.Close()

	// Make the RPC call to fetch all vertices
	var resp rpcTypes.FetchAllToShardResponse
	err = client.Call("Shard.FetchAll", req, &resp)
	if err != nil {
		return nil, fmt.Errorf("RPC call to shard %d failed: %w", shardConfig.ID, err)
	}

	return resp.Vertices, nil
}

func (qm *QueryManager) deleteAllFromShard(shardConfig *config.ShardConfig, req *rpcTypes.DeleteAllToShardRequest) error {
	log.Printf("Deleting all vertices and edges from shard %d", shardConfig.ID)

	// Connect to the shard
	leaderID, err := qm.replicaManager.GetLeaderID(shardConfig.ID)
	if err != nil {
		return fmt.Errorf("failed to get leader ID for shard %d: %w", shardConfig.ID, err)
	}
	addr, err := shardConfig.GetReplicaAddress(leaderID)
	if err != nil {
		return fmt.Errorf("failed to get replica address for shard %d: %w", shardConfig.ID, err)
	}
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to connect to shard %d at %s: %w", shardConfig.ID, addr, err)
	}
	defer client.Close()

	// Make the RPC call to delete all
	var resp rpcTypes.DeleteAllToShardResponse
	err = client.Call("Shard.DeleteAll", req, &resp)
	if err != nil {
		return fmt.Errorf("RPC call to shard %d failed: %w", shardConfig.ID, err)
	}

	if !resp.Success {
		return fmt.Errorf("shard %d failed to delete all", shardConfig.ID)
	}

	return nil
}

// AddVertex is the RPC handler for adding a vertex
func (qm *QueryManager) AddVertex(req *rpcTypes.AddVertexRequest, resp *rpcTypes.AddVertexResponse) error {
	log.Printf("Received AddVertex request")

	// Select a shard and generate a vertex ID
	shardConfig := RandomPartitioner(qm.config)
	vertexID := qm.generateVertexID(shardConfig)
	timestamp := qm.generateTimestamp()

	// Add the vertex to the shard
	err := qm.addVertexToShard(shardConfig, &rpcTypes.AddVertexToShardRequest{
		VertexID:   vertexID,
		Timestamp:  timestamp,
		Properties: req.Properties,
	})
	if err != nil {
		log.Printf("Failed to add vertex: %v", err)
		resp.Success = false
		return err
	}

	resp.Success = true
	resp.VertexID = vertexID
	resp.Timestamp = timestamp
	log.Printf("Successfully added vertex with ID: %s", vertexID)
	return nil
}

// GetVertex is the RPC handler for getting the latest vertex
func (qm *QueryManager) GetVertexAt(req *rpcTypes.GetVertexAtRequest, resp *rpcTypes.GetVertexResponse) error {
	log.Printf("Received GetVertex request")

	shardConfig := RandomPartitioner(qm.config)
	// Add the vertex to the shard
	var respShard rpcTypes.GetVertexAtShardResponse
	err := qm.getVertexAtToShard(shardConfig, &rpcTypes.GetVertexAtShardRequest{
		Vertex:    req.Vertex,
		Timestamp: req.Timestamp,
	}, &respShard)
	if err != nil {
		log.Printf("Failed to add vertex: %v", err)
		resp.Exists = false
		return err
	}

	resp.Exists = respShard.Exists
	resp.Properties = respShard.Properties
	resp.Timestamp = respShard.Timestamp
	return nil
}

func (qm *QueryManager) GetVertex(req *rpcTypes.GetVertexRequest, resp *rpcTypes.GetVertexResponse) error {
	return qm.GetVertexAt(&rpcTypes.GetVertexAtRequest{
		Vertex:    req.Vertex,
		Timestamp: qm.generateTimestamp(),
	}, resp)
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

	timestamp := qm.generateTimestamp()

	err = qm.addEdgeToShard(shardConfig, &rpcTypes.AddEdgeToShardRequest{
		FromVertexID: req.FromVertexID,
		ToVertexID:   req.ToVertexID,
		Properties:   req.Properties,
		Timestamp:    timestamp,
	})
	if err != nil {
		log.Printf("Failed to add edge: %v", err)
		resp.Success = false
		return err
	}

	resp.Success = true
	resp.Timestamp = timestamp
	return nil
}

// GetEdge is the RPC handler for getting the latest Edge
func (qm *QueryManager) GetEdgeAt(req *rpcTypes.GetEdgeAtRequest, resp *rpcTypes.GetEdgeResponse) error {
	log.Printf("Received GetEdge request")

	shardConfig := RandomPartitioner(qm.config)

	// Add the vertex to the shard
	var respShard rpcTypes.GetEdgeAtShardResponse
	err := qm.getEdgeAtToShard(shardConfig, &rpcTypes.GetEdgeAtShardRequest{
		FromVertex: req.FromVertex,
		ToVertex:   req.ToVertex,
		Timestamp:  req.Timestamp,
	}, &respShard)
	if err != nil {
		log.Printf("Failed to add vertex: %v", err)
		resp.Exists = false
		return err
	}

	resp.Exists = respShard.Exists
	resp.Properties = respShard.Properties
	resp.Timestamp = respShard.Timestamp
	return nil
}

func (qm *QueryManager) GetEdge(req *rpcTypes.GetEdgeRequest, resp *rpcTypes.GetEdgeResponse) error {
	return qm.GetEdgeAt(&rpcTypes.GetEdgeAtRequest{
		FromVertex: req.FromVertex,
		ToVertex:   req.ToVertex,
		Timestamp:  qm.generateTimestamp(),
	}, resp)
}

// DeleteEdge is the RPC handler for deleting an edge
func (qm *QueryManager) DeleteEdge(req *rpcTypes.DeleteEdgeRequest, resp *rpcTypes.DeleteEdgeResponse) error {
	log.Printf("Received DeleteEdge request")

	shardID, err := qm.getShardIDFromVertexID(req.FromVertexID)
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

	timestamp := qm.generateTimestamp()

	err = qm.deleteEdgeToShard(shardConfig, &rpcTypes.DeleteEdgeToShardRequest{
		FromVertexID: req.FromVertexID,
		ToVertexID:   req.ToVertexID,
		Timestamp:    timestamp,
	})
	if err != nil {
		log.Printf("Failed to delete edge: %v", err)
		resp.Success = false
		return err
	}

	resp.Success = true
	resp.Timestamp = timestamp
	return nil
}

// BFS is the RPC handler for performing a BFS
func (qm *QueryManager) BFS(req *rpcTypes.BFSRequest, resp *rpcTypes.BFSResponse) error {
	log.Printf("Received BFS request")

	timestamp := req.Timestamp
	radius := req.Radius

	q := make([]types.VertexId, 0)
	q = append(q, req.StartVertexID)
	visited := make(map[types.VertexId]int)
	visited[req.StartVertexID] = 0

	for len(q) > 0 {
		current := q[0]
		q = q[1:]

		shardID, err := qm.getShardIDFromVertexID(current)
		if err != nil {
			log.Printf("Failed to get shard ID from vertex ID: %v", err)
			return err
		}

		shardConfig, err := qm.config.GetShardByID(shardID)
		if err != nil {
			log.Printf("Failed to get shard by ID: %v", err)
			return err
		}

		if visited[current] == radius {
			continue
		}

		neighbors, err := qm.getNeighborsToShard(
			shardConfig, &rpcTypes.GetNeighborsToShardRequest{
				VertexID:  current,
				Timestamp: timestamp,
			},
		)
		if err != nil {
			log.Printf("Failed to get neighbors to shard: %v", err)
			return err
		}

		for _, neighbor := range neighbors {
			if _, ok := visited[neighbor]; !ok {
				visited[neighbor] = visited[current] + 1
				q = append(q, neighbor)
			}
		}
	}

	resp.Vertices = make([]types.VertexId, len(visited))
	i := 0
	for vertex, _ := range visited {
		resp.Vertices[i] = vertex
		i++
	}

	return nil
}

// FetchAll is the RPC handler for fetching all vertices from all shards
func (qm *QueryManager) FetchAll(req *rpcTypes.FetchAllRequest, resp *rpcTypes.FetchAllResponse) error {
	log.Printf("Received FetchAll request")

	// Initialize the response map
	resp.ShardVertices = make(map[int][]rpcTypes.VertexInfo)

	// Iterate through all shards and fetch vertices from each
	for _, shardConfig := range qm.config.Shards {
		vertices, err := qm.fetchAllFromShard(&shardConfig, &rpcTypes.FetchAllToShardRequest{})
		if err != nil {
			log.Printf("Failed to fetch vertices from shard %d: %v", shardConfig.ID, err)
			return fmt.Errorf("failed to fetch vertices from shard %d: %w", shardConfig.ID, err)
		}
		resp.ShardVertices[shardConfig.ID] = vertices
		log.Printf("Fetched %d vertices from shard %d", len(vertices), shardConfig.ID)
	}

	log.Printf("Successfully fetched vertices from all %d shards", len(qm.config.Shards))
	return nil
}

// DeleteAll is the RPC handler for deleting all vertices and edges from all shards
func (qm *QueryManager) DeleteAll(req *rpcTypes.DeleteAllRequest, resp *rpcTypes.DeleteAllResponse) error {
	log.Printf("Received DeleteAll request")

	timestamp := qm.generateTimestamp()

	// Iterate through all shards and delete all vertices and edges from each
	for _, shardConfig := range qm.config.Shards {
		err := qm.deleteAllFromShard(&shardConfig, &rpcTypes.DeleteAllToShardRequest{
			Timestamp: timestamp,
		})
		if err != nil {
			log.Printf("Failed to delete all from shard %d: %v", shardConfig.ID, err)
			resp.Success = false
			return fmt.Errorf("failed to delete all from shard %d: %w", shardConfig.ID, err)
		}
		log.Printf("Successfully deleted all from shard %d", shardConfig.ID)
	}

	resp.Success = true
	log.Printf("Successfully deleted all vertices and edges from all %d shards", len(qm.config.Shards))
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
	err = server.Register(qm.replicaManager)
	if err != nil {
		return fmt.Errorf("failed to register replica manager RPC service: %v", err)
	}

	// Listen on the specified port from config
	addr := fmt.Sprintf("%s:%d", qm.config.QueryManager.Host, qm.config.QueryManager.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", addr, err)
	}

	log.Printf("Query Manager RPC server listening on %s", addr)
	log.Printf("Connected to %d shards", len(qm.config.Shards))

	// Start the replica manager
	qm.replicaManager.Start()

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
