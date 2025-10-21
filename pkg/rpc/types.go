package rpc

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
	Properties map[string]string // Key-value properties for the vertex
}

type AddVertexResponse struct {
	Success  bool   // Whether the operation succeeded
	VertexID string // The ID of the vertex that was added
	Timestamp float64 // The timestamp of the vertex
}

type AddVertexToShardRequest struct {
	VertexID   string            // The ID of the vertex that was added
	Timestamp  float64           // The timestamp of the vertex
	Properties map[string]string // Key-value properties for the vertex
}

type AddVertexToShardResponse struct {
	Success bool // Whether the operation succeeded
}

// ------------------------------------------------------------

// AddEdge request and response
type AddEdgeRequest struct {
	FromVertexID string            // The ID of the from vertex
	ToVertexID   string            // The ID of the to vertex
	Properties   map[string]string // Key-value properties for the edge
}

type AddEdgeResponse struct {
	Success bool // Whether the operation succeeded
	Timestamp float64 // The timestamp of the edge
}

type AddEdgeToShardRequest struct {
	FromVertexID string            // The ID of the from vertex
	ToVertexID   string            // The ID of the to vertex
	Properties   map[string]string // Key-value properties for the edge
	Timestamp    float64           // The timestamp of the edge
}

type AddEdgeToShardResponse struct {
	Success bool // Whether the operation succeeded
}

// ------------------------------------------------------------

// DeleteEdge request and response
type DeleteEdgeRequest struct {
	FromVertexID string // The ID of the source vertex
	ToVertexID   string // The ID of the to vertex
}

type DeleteEdgeResponse struct {
	Success bool // Whether the operation succeeded
	Timestamp float64 // The timestamp of the edge
}

type DeleteEdgeToShardRequest struct {
	FromVertexID string // The ID of the source vertex
	ToVertexID   string // The ID of the to vertex
	Timestamp    float64 // The timestamp of the edge
}

type DeleteEdgeToShardResponse struct {
	Success bool // Whether the operation succeeded
}

// ------------------------------------------------------------

// GetNeighborsToShard request and response
type GetNeighborsToShardRequest struct {
	VertexID string // The ID of the vertex
	Timestamp float64 // The timestamp
}

type GetNeighborsToShardResponse struct {
	Neighbors []string // The IDs of the neighbors
}

// ------------------------------------------------------------

// BFS request and response
type BFSRequest struct {
	StartVertexID string // The ID of the start vertex
	Radius        int    // The radius of the BFS
	Timestamp     float64 // The timestamp of the BFS
}

type BFSResponse struct {
	Vertices []string // The IDs of the vertices in the BFS
}