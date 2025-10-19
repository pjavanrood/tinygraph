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
}

type AddVertexToShardRequest struct {
	VertexID string            // The ID of the vertex that was added
	Properties map[string]string // Key-value properties for the vertex
}

type AddVertexToShardResponse struct {
	Success bool // Whether the operation succeeded
}

// ------------------------------------------------------------

// AddEdge request and response
type AddEdgeRequest struct {
	FromVertexID string            // The ID of the from vertex
	ToVertexID string            // The ID of the to vertex
	Properties map[string]string // Key-value properties for the edge
}

type AddEdgeResponse struct {
	Success bool // Whether the operation succeeded
	EdgeID string // The ID of the edge that was added
}

type AddEdgeToShardRequest struct {
	FromVertexID string            // The ID of the from vertex
	ToVertexID string            // The ID of the to vertex
	Properties map[string]string // Key-value properties for the edge
}

type AddEdgeToShardResponse struct {
	EdgeID string // The ID of the edge that was added
	Success bool // Whether the operation succeeded
}

// ------------------------------------------------------------

// DeleteEdge request and response
type DeleteEdgeRequest struct {
	EdgeID string // The ID of the edge to delete
}

type DeleteEdgeResponse struct {
	Success bool // Whether the operation succeeded
}

type DeleteEdgeToShardRequest struct {
	EdgeID string // The ID of the edge to delete
}

type DeleteEdgeToShardResponse struct {
	Success bool // Whether the operation succeeded
}
