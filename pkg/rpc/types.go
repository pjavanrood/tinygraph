package rpc

// AddVertexRequest represents a request to add a vertex
type AddVertexRequest struct {
	Properties map[string]string // Key-value properties for the vertex
}

// AddVertexResponse represents the response from adding a vertex
type AddVertexResponse struct {
	Success  bool   // Whether the operation succeeded
	VertexID string // The ID of the vertex that was added
}

// QueryManagerService defines the RPC service interface
type QueryManagerService struct{}

// Additional common RPC types can go here
type AddVertexToShardRequest struct {
	VertexID string            // The ID of the vertex that was added
	Properties map[string]string // Key-value properties for the vertex
}

type AddVertexToShardResponse struct {
	Success bool // Whether the operation succeeded
}
