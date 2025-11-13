package rpc

import "github.com/pjavanrood/tinygraph/internal/types"

// ------------------------------------------------------------
// Service interfaces
// ------------------------------------------------------------

// QueryManagerService defines the RPC service interface
type QueryManagerService struct{}

// ------------------------------------------------------------
// Request and response types
// ------------------------------------------------------------

// AddVertex request and response
type AddVertexRequest struct {
	Properties types.Properties // Key-value properties for the vertex
}

type AddVertexResponse struct {
	Success   bool            // Whether the operation succeeded
	VertexID  types.VertexId  // The ID of the vertex that was added
	Timestamp types.Timestamp // The timestamp of the vertex
}

type AddVertexToShardRequest struct {
	VertexID   types.VertexId   // The ID of the vertex that was added
	Timestamp  types.Timestamp  // The timestamp of the vertex
	Properties types.Properties // Key-value properties for the vertex
}

type AddVertexToShardResponse struct {
	Success bool // Whether the operation succeeded
}

// ------------------------------------------------------------

// AddEdge request and response
type AddEdgeRequest struct {
	FromVertexID types.VertexId   // The ID of the from vertex
	ToVertexID   types.VertexId   // The ID of the to vertex
	Properties   types.Properties // Key-value properties for the edge
}

type AddEdgeResponse struct {
	Success   bool            // Whether the operation succeeded
	Timestamp types.Timestamp // The timestamp of the edge
}

type AddEdgeToShardRequest struct {
	FromVertexID types.VertexId   // The ID of the from vertex
	ToVertexID   types.VertexId   // The ID of the to vertex
	Properties   types.Properties // Key-value properties for the edge
	Timestamp    types.Timestamp  // The timestamp of the edge
}

type AddEdgeToShardResponse struct {
	Success bool // Whether the operation succeeded
}

// ------------------------------------------------------------

// DeleteEdge request and response
type DeleteEdgeRequest struct {
	FromVertexID types.VertexId // The ID of the source vertex
	ToVertexID   types.VertexId // The ID of the to vertex
}

type DeleteEdgeResponse struct {
	Success   bool            // Whether the operation succeeded
	Timestamp types.Timestamp // The timestamp of the edge
}

type DeleteEdgeToShardRequest struct {
	FromVertexID types.VertexId  // The ID of the source vertex
	ToVertexID   types.VertexId  // The ID of the to vertex
	Timestamp    types.Timestamp // The timestamp of the edge
}

type DeleteEdgeToShardResponse struct {
	Success bool // Whether the operation succeeded
}

// ------------------------------------------------------------

// GetNeighborsToShard request and response
type GetNeighborsToShardRequest struct {
	VertexID  types.VertexId  // The ID of the vertex
	Timestamp types.Timestamp // The timestamp
}

type GetNeighborsToShardResponse struct {
	Neighbors []types.VertexId // The IDs of the neighbors
}

// ------------------------------------------------------------

// BFS request and response
type BFSRequest struct {
	StartVertexID types.VertexId  // The ID of the start vertex
	Radius        int             // The radius of the BFS
	Timestamp     types.Timestamp // The timestamp of the BFS
}

type BFSResponse struct {
	Vertices []types.VertexId // The IDs of the vertices in the BFS
}

// ------------------------------------------------------------

// FetchAll request and response
type VertexInfo struct {
	VertexID   types.VertexId   // The vertex ID
	Properties types.Properties // The vertex properties
	Timestamp  types.Timestamp  // The timestamp of the vertex
}

type FetchAllRequest struct {
}

type FetchAllResponse struct {
	ShardVertices map[int][]VertexInfo // Map from shard ID to list of vertex info
}

type FetchAllToShardRequest struct {
}

type FetchAllToShardResponse struct {
	Vertices []VertexInfo // The vertex information for all vertices in the shard
}

// ------------------------------------------------------------

// Raft leadership request(none) and response
type RaftLeadershipRequest struct{}

type RaftLeadershipResponse string

type NotifyLeaderIDUpdateRequest struct {
	ShardID  int
	LeaderID string
}

type NotifyLeaderIDUpdateResponse struct{}

// ------------------------------------------------------------

// DeleteAll request and response
type DeleteAllRequest struct {
}

type DeleteAllResponse struct {
	Success bool // Whether the operation succeeded
}

type DeleteAllToShardRequest struct {
	Timestamp types.Timestamp // The timestamp of the operation
}

type DeleteAllToShardResponse struct {
	Success bool // Whether the operation succeeded
}
