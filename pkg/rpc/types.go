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

// GetVertex request and response
type GetVertexRequest struct {
	Vertex types.VertexId // The ID of the vertex
}

type GetVertexAtRequest struct {
	Vertex    types.VertexId  // The ID of the vertex
	Timestamp types.Timestamp // The timestamp at which we wish to observe the vertex
}

type GetVertexResponse struct {
	Exists     bool             // Whether the vertex exists at the timestamp
	Properties types.Properties // Key-value properties for the vertex
	Timestamp  types.Timestamp  // The latest timestamp of the Vertex at the time of query
}

type GetVertexAtShardRequest struct {
	Vertex    types.VertexId
	Timestamp types.Timestamp // The timestamp at which we wish to observe the vertex
}

type GetVertexAtShardResponse struct {
	Exists     bool             // Whether the vertex exists at the timestamp
	Properties types.Properties // Key-value properties for the vertex
	Timestamp  types.Timestamp  // The latest timestamp of the Vertex at the time of query
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

// GetVertex request and response
type GetEdgeRequest struct {
	FromVertex types.VertexId // The ID of the from vertex
	ToVertex   types.VertexId // The ID of the to vertex
}

type GetEdgeAtRequest struct {
	FromVertex types.VertexId  // The ID of the from vertex
	ToVertex   types.VertexId  // The ID of the to vertex
	Timestamp  types.Timestamp // The timestamp at which we wish to observe the Edge
}

type GetEdgeResponse struct {
	Exists     bool             // Whether the Edge exists at the timestamp
	Properties types.Properties // Key-value properties for the Edge
	Timestamp  types.Timestamp  // The latest timestamp of the Edge at the time of query
}

type GetEdgeAtShardRequest struct {
	FromVertex types.VertexId  // The ID of the from vertex
	ToVertex   types.VertexId  // The ID of the to vertex
	Timestamp  types.Timestamp // The timestamp at which we wish to observe the Edge
}

type GetEdgeAtShardResponse struct {
	Exists     bool             // Whether the Edge exists at the timestamp
	Properties types.Properties // Key-value properties for the Edge
	Timestamp  types.Timestamp  // The latest timestamp of the Edge at the time of query
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
	EdgesTo    []types.VertexId // The outgoing edges of this vertex
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

// ------------------------------------------------------------

// Perform BFS on given vertex V for N steps
type ShardedBFSRequest struct {
	Root      types.VertexId
	N         int
	Timestamp types.Timestamp
}

type ShardedBFSResponse struct {
	Vertices []types.VertexId
	Success  bool // Whether the operation succeeded
}

type BFSToShardRequest struct {
	Root         types.VertexId
	N            int
	Timestamp    types.Timestamp // The timestamp of the operation
	Id           types.BFSId
	CallbackAddr string
}

type BFSToShardResponse struct {
	Success bool // Whether the operation has been dispatched
}

// sends the BFS visited information to the QM
type BFSFromShardRequest struct {
	Id                 types.BFSId      // the request ID
	Shard              types.ShardId    // the shard id
	Vertices           []types.VertexId // can be empty
	DispatchedRequests map[int]int      // this is the additional requests dispatched by this shard
}

type BFSFromShardResponse struct {
}

// ------------------------------------------------------------

type GetLeaderIdRequest struct {
	ShardId int
}

type GetLeaderIdResponse struct {
	LeaderId int
}
