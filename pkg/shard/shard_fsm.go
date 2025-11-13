package shard

import (
	"fmt"
	"maps"
	"slices"

	"github.com/pjavanrood/tinygraph/internal/config"
	internalTypes "github.com/pjavanrood/tinygraph/internal/types"
	mvccTypes "github.com/pjavanrood/tinygraph/pkg/mvcc"
	rpcTypes "github.com/pjavanrood/tinygraph/pkg/rpc"
)

// Type aliases for cleaner code
type ShardId internalTypes.ShardId
type VertexId internalTypes.VertexId
type Timestamp internalTypes.Timestamp
type Props internalTypes.Properties

// TIMEOUT_SECONDS is the timeout for Raft operations
const TIMEOUT_SECONDS = 5

// ShardFSM represents the Finite State Machine for a shard partition
// It stores a subset of vertices and their edges using MVCC
// This is the internal state that gets replicated through Raft
type ShardFSM struct {
	vertices map[VertexId]*mvccTypes.Vertex
	Id       ShardId
	config   *config.Config
}

// addVertex adds a new vertex to the shard
// This is an internal method called by Shard through Raft consensus
func (s *ShardFSM) addVertex(req rpcTypes.AddVertexToShardRequest, resp *rpcTypes.AddVertexToShardResponse) error {
	success := true
	defer func() {
		resp.Success = success
	}()

	// check if vertex with given ID already exists, and is not deleted
	if _, ok := s.vertices[VertexId(req.VertexID)]; ok {
		success = false
		return fmt.Errorf("Vertex with ID \"%s\" already exists and is not deleted", req.VertexID)
	}

	prop := mvccTypes.VertexProp(req.Properties)

	// we add this new vertex to our map
	vertex := mvccTypes.NewVertex(req.VertexID, &prop, req.Timestamp)

	s.vertices[VertexId(req.VertexID)] = vertex
	return nil
}

// getVertexAt gets the vertex information at a specific timestamp
// This is an internal method called by Shard through Raft consensus
func (s *ShardFSM) getVertexAt(req rpcTypes.GetVertexAtShardRequest, resp *rpcTypes.GetVertexAtShardResponse) error {
	exists := true
	defer func() {
		resp.Exists = exists
	}()

	// check if vertex with given ID already exists, and is not deleted
	vertex, vertexExists := s.vertices[VertexId(req.Vertex)]
	if !vertexExists {
		exists = false
		return nil
	}

	timestampedVertex := vertex.GetAt(req.Timestamp)
	if timestampedVertex == nil {
		exists = false
		return nil
	}

	resp.Properties = internalTypes.Properties(*timestampedVertex.Prop)
	resp.Timestamp = timestampedVertex.TS

	return nil
}

// getEdgeAt gets the Edge information at a specific timestamp
// This is an internal method called by Shard through Raft consensus
func (s *ShardFSM) getEdgeAt(req rpcTypes.GetEdgeAtShardRequest, resp *rpcTypes.GetEdgeAtShardResponse) error {
	exists := true
	defer func() {
		resp.Exists = exists
	}()

	// check if vertex with given ID already exists, and is not deleted
	vertex, vertexExists := s.vertices[VertexId(req.FromVertex)]
	if !vertexExists {
		exists = false
		return nil
	}

	timestampedVertex := vertex.GetAt(req.Timestamp)
	if edge, ok := timestampedVertex.Edges[req.ToVertex]; ok {
		if !edge.AliveAt(req.Timestamp) {
			exists = false
			return nil
		}

		timestampedEdge := edge.GetAt(req.Timestamp)
		resp.Properties = internalTypes.Properties(*timestampedEdge.Prop)
		resp.Timestamp = timestampedEdge.TS
		return nil
	}

	exists = false
	return nil
}

// addEdge adds a new edge from one vertex to another
// This is an internal method called by Shard through Raft consensus
func (s *ShardFSM) addEdge(req rpcTypes.AddEdgeToShardRequest, resp *rpcTypes.AddEdgeToShardResponse) error {
	success := true
	defer func() {
		resp.Success = success
	}()

	// check if source Vertex exists
	fromVertex, ok := s.vertices[VertexId(req.FromVertexID)]
	if !ok {
		success = false
		log.Printf("Source vertex with ID \"%s\" does not exist", req.FromVertexID)
		return fmt.Errorf("source vertex with ID \"%s\" does not exist", req.FromVertexID)
	}

	err := fromVertex.AddEdge(req.ToVertexID, req.Timestamp)
	if err != nil {
		success = false
		return err
	}

	return nil
}

// deleteEdge marks an edge as deleted at the given timestamp
// This is an internal method called by Shard through Raft consensus
func (s *ShardFSM) deleteEdge(req rpcTypes.DeleteEdgeToShardRequest, resp *rpcTypes.DeleteEdgeToShardResponse) error {
	success := true
	defer func() {
		resp.Success = success
	}()

	// check if source Vertex exists
	fromVertex, ok := s.vertices[VertexId(req.FromVertexID)]
	if !ok {
		success = false
		return fmt.Errorf("source vertex with ID \"%s\" does not exist", req.FromVertexID)
	}

	fromVertex.DeleteEdge(req.ToVertexID, req.Timestamp)

	return nil
}

// getNeighbors retrieves all neighbors of a vertex at a given timestamp
// This is an internal method called by Shard
func (s *ShardFSM) getNeighbors(req rpcTypes.GetNeighborsToShardRequest, resp *rpcTypes.GetNeighborsToShardResponse) error {
	vertex, ok := s.vertices[VertexId(req.VertexID)]
	if !ok {
		return fmt.Errorf("Vertex with ID \"%s\" does not exist", req.VertexID)
	}

	neighbors := vertex.GetAllEdges(req.Timestamp)
	resp.Neighbors = make([]internalTypes.VertexId, len(neighbors))
	for i, edge := range neighbors {
		resp.Neighbors[i] = edge.ToID
	}

	return nil
}

// fetchAll retrieves all vertex IDs and properties in the shard
// This is an internal method called by Shard
func (s *ShardFSM) fetchAll(req rpcTypes.FetchAllToShardRequest, resp *rpcTypes.FetchAllToShardResponse) error {
	resp.Vertices = make([]rpcTypes.VertexInfo, 0, len(s.vertices))
	for vertexID, vertex := range s.vertices {
		vertexInfo := rpcTypes.VertexInfo{
			VertexID:  internalTypes.VertexId(vertexID),
			EdgesTo:   slices.Collect(maps.Keys(vertex.Edges)),
			Timestamp: vertex.TS,
		}
		// Convert VertexProp to Properties if not nil
		if vertex.Prop != nil {
			vertexInfo.Properties = internalTypes.Properties(*vertex.Prop)
		}
		resp.Vertices = append(resp.Vertices, vertexInfo)
	}
	return nil
}

// newShardFSM creates a new ShardFSM instance
func newShardFSM(cfg *config.Config, id int) *ShardFSM {
	return &ShardFSM{
		vertices: make(map[VertexId]*mvccTypes.Vertex, 0),
		Id:       ShardId(id),
		config:   cfg,
	}
}
