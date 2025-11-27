package shard

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/pjavanrood/tinygraph/internal/config"
	"github.com/pjavanrood/tinygraph/internal/types"
	"github.com/pjavanrood/tinygraph/internal/util"
	mvccTypes "github.com/pjavanrood/tinygraph/pkg/mvcc"
	"github.com/pjavanrood/tinygraph/pkg/replica"
	rpcTypes "github.com/pjavanrood/tinygraph/pkg/rpc"
)

var log = util.New("Shard", util.LogLevelInfo)

// ShardOp represents the type of operation to be applied through Raft
type ShardOp int

const (
	AddVertex ShardOp = iota
	AddEdge
	DeleteEdge
	DeleteAll
)

// BFSRequestState tracks active BFS requests for deduplication
type BFSRequestState struct {
	visited       map[string]int // vertexID -> level
	pendingShards map[int]int    // shardID -> number of pending responses
	mu            sync.Mutex
}

// connectionPool manages a pool of RPC connections for a specific shard
type connectionPool struct {
	connections chan *rpc.Client
	address     string
	mu          sync.Mutex
	maxSize     int
	closed      bool
}

// newConnectionPool creates a new connection pool for a shard
func newConnectionPool(address string, maxSize int) *connectionPool {
	return &connectionPool{
		connections: make(chan *rpc.Client, maxSize),
		address:     address,
		maxSize:     maxSize,
	}
}

// getConnection gets a connection from the pool or creates a new one
func (cp *connectionPool) getConnection() (*rpc.Client, error) {
	cp.mu.Lock()
	closed := cp.closed
	cp.mu.Unlock()

	if closed {
		// Pool is closed, create new connection directly
		return rpc.Dial("tcp", cp.address)
	}

	select {
	case client := <-cp.connections:
		// Check if connection is still valid
		if client != nil {
			return client, nil
		}
	default:
		// No connection available, create new one
	}

	// Create new connection
	client, err := rpc.Dial("tcp", cp.address)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// returnConnection returns a connection to the pool or closes it if pool is full
func (cp *connectionPool) returnConnection(client *rpc.Client) {
	if client == nil {
		return
	}

	cp.mu.Lock()
	closed := cp.closed
	cp.mu.Unlock()

	if closed {
		// Pool is closed, close the connection
		client.Close()
		return
	}

	select {
	case cp.connections <- client:
		// Successfully returned to pool
	default:
		// Pool is full, close the connection
		client.Close()
	}
}

// closeAll closes all connections in the pool
func (cp *connectionPool) closeAll() {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.closed {
		return // Already closed
	}

	cp.closed = true

	// Drain and close all connections
	for {
		select {
		case client := <-cp.connections:
			if client != nil {
				client.Close()
			}
		default:
			// No more connections in pool
			return
		}
	}
}

// Shard wraps a ShardFSM with Raft consensus functionality
// It implements the raft.FSM interface and handles all RPC calls
type Shard struct {
	raft           *raft.Raft
	shardFSM       *ShardFSM
	mu             sync.Mutex
	requestQueue   chan net.Conn // Queue of incoming connection requests
	config         *config.Config
	shardID        int
	replicaManager replica.ReplicaManager
	// Track active BFS requests by requestID for deduplication
	activeBFSRequests map[string]*BFSRequestState
	bfsMu             sync.Mutex
	// Connection pools per shard (keyed by shardID)
	connectionPools map[int]*connectionPool
	poolMu          sync.RWMutex
}

// ShardLogOp represents an operation to be logged and replicated via Raft
type ShardLogOp struct {
	Op  ShardOp
	Req []byte
}

// ShardApplyResponse wraps the response from applying a Raft log entry
type ShardApplyResponse struct {
	Response interface{}
	Error    error
}

// ShardSnapshot represents a point-in-time snapshot of the shard state
// It implements the raft.FSMSnapshot interface
type ShardSnapshot struct {
	vertices map[VertexId]*mvccTypes.Vertex
}

// Apply applies a Raft log entry to the FSM (Finite State Machine)
// This is called by Raft after a log entry has been committed
// This method implements the raft.FSM interface
func (s *Shard) Apply(logEntry *raft.Log) interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	var logOp ShardLogOp
	err := gob.NewDecoder(bytes.NewReader(logEntry.Data)).Decode(&logOp)
	if err != nil {
		return ShardApplyResponse{Response: nil, Error: err}
	}

	switch logOp.Op {
	case AddVertex:
		var req rpcTypes.AddVertexToShardRequest
		err = gob.NewDecoder(bytes.NewReader(logOp.Req)).Decode(&req)
		if err != nil {
			return ShardApplyResponse{Response: nil, Error: err}
		}
		var resp rpcTypes.AddVertexToShardResponse
		err = s.shardFSM.addVertex(req, &resp)
		if err != nil {
			return ShardApplyResponse{Response: nil, Error: err}
		}
		return ShardApplyResponse{Response: resp, Error: nil}
	case AddEdge:
		var req rpcTypes.AddEdgeToShardRequest
		err = gob.NewDecoder(bytes.NewReader(logOp.Req)).Decode(&req)
		if err != nil {
			return ShardApplyResponse{Response: nil, Error: err}
		}
		var resp rpcTypes.AddEdgeToShardResponse
		err = s.shardFSM.addEdge(req, &resp)
		if err != nil {
			return ShardApplyResponse{Response: nil, Error: err}
		}
		return ShardApplyResponse{Response: resp, Error: nil}
	case DeleteEdge:
		var req rpcTypes.DeleteEdgeToShardRequest
		err = gob.NewDecoder(bytes.NewReader(logOp.Req)).Decode(&req)
		if err != nil {
			return ShardApplyResponse{Response: nil, Error: err}
		}
		var resp rpcTypes.DeleteEdgeToShardResponse
		err = s.shardFSM.deleteEdge(req, &resp)
		if err != nil {
			return ShardApplyResponse{Response: nil, Error: err}
		}
		return ShardApplyResponse{Response: resp, Error: nil}
	case DeleteAll:
		var req rpcTypes.DeleteAllToShardRequest
		err = gob.NewDecoder(bytes.NewReader(logOp.Req)).Decode(&req)
		if err != nil {
			return ShardApplyResponse{Response: nil, Error: err}
		}
		var resp rpcTypes.DeleteAllToShardResponse
		err = s.shardFSM.deleteAll(req, &resp)
		if err != nil {
			return ShardApplyResponse{Response: nil, Error: err}
		}
		return ShardApplyResponse{Response: resp, Error: nil}
	default:
		return ShardApplyResponse{Response: nil, Error: fmt.Errorf("unknown operation: %d", logOp.Op)}
	}
}

// Snapshot returns an FSMSnapshot used to: support log compaction, to
// restore the FSM to a previous state, or to bring out-of-date followers up
// to a recent log index.
// This method implements the raft.FSM interface
func (s *Shard) Snapshot() (raft.FSMSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Create a deep copy of the shard state
	verticesCopy := make(map[VertexId]*mvccTypes.Vertex, len(s.shardFSM.vertices))
	for k, v := range s.shardFSM.vertices {
		verticesCopy[k] = v
	}

	return &ShardSnapshot{vertices: verticesCopy}, nil
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous state.
// This method implements the raft.FSM interface
func (s *Shard) Restore(snapshot io.ReadCloser) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer snapshot.Close()

	var vertices map[VertexId]*mvccTypes.Vertex
	decoder := gob.NewDecoder(snapshot)
	if err := decoder.Decode(&vertices); err != nil {
		return err
	}

	s.shardFSM.vertices = vertices
	return nil
}

// Persist writes the snapshot to the given sink
// This method implements the raft.FSMSnapshot interface
func (s *ShardSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode the vertices map
		encoder := gob.NewEncoder(sink)
		if err := encoder.Encode(s.vertices); err != nil {
			return err
		}
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

// Release is called when we are finished with the snapshot
// This method implements the raft.FSMSnapshot interface
func (s *ShardSnapshot) Release() {
	// Nothing to do here since we don't hold any resources
}

// AddVertex is the RPC handler for adding a vertex through Raft consensus
func (s *Shard) AddVertex(req rpcTypes.AddVertexToShardRequest, resp *rpcTypes.AddVertexToShardResponse) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	// Encode the request
	var reqBuf bytes.Buffer
	if err := gob.NewEncoder(&reqBuf).Encode(req); err != nil {
		return err
	}

	// Create the log operation
	logOp := ShardLogOp{
		Op:  AddVertex,
		Req: reqBuf.Bytes(),
	}

	// Encode the log operation
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(logOp); err != nil {
		return err
	}

	f := s.raft.Apply(buf.Bytes(), time.Second*TIMEOUT_SECONDS)
	if f.Error() != nil {
		return f.Error()
	}
	shardApplyResponse := f.Response().(ShardApplyResponse)
	if shardApplyResponse.Error != nil {
		return shardApplyResponse.Error
	}
	*resp = shardApplyResponse.Response.(rpcTypes.AddVertexToShardResponse)
	return nil
}

// AddEdge is the RPC handler for adding an edge through Raft consensus
func (s *Shard) AddEdge(req rpcTypes.AddEdgeToShardRequest, resp *rpcTypes.AddEdgeToShardResponse) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	// Encode the request
	var reqBuf bytes.Buffer
	if err := gob.NewEncoder(&reqBuf).Encode(req); err != nil {
		return err
	}

	// Create the log operation
	logOp := ShardLogOp{
		Op:  AddEdge,
		Req: reqBuf.Bytes(),
	}

	// Encode the log operation
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(logOp); err != nil {
		return err
	}

	f := s.raft.Apply(buf.Bytes(), time.Second*TIMEOUT_SECONDS)
	if f.Error() != nil {
		return f.Error()
	}
	shardApplyResponse := f.Response().(ShardApplyResponse)
	if shardApplyResponse.Error != nil {
		return shardApplyResponse.Error
	}
	*resp = shardApplyResponse.Response.(rpcTypes.AddEdgeToShardResponse)
	return nil
}

// DeleteEdge is the RPC handler for deleting an edge through Raft consensus
func (s *Shard) DeleteEdge(req rpcTypes.DeleteEdgeToShardRequest, resp *rpcTypes.DeleteEdgeToShardResponse) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	// Encode the request
	var reqBuf bytes.Buffer
	if err := gob.NewEncoder(&reqBuf).Encode(req); err != nil {
		return err
	}

	// Create the log operation
	logOp := ShardLogOp{
		Op:  DeleteEdge,
		Req: reqBuf.Bytes(),
	}

	// Encode the log operation
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(logOp); err != nil {
		return err
	}

	f := s.raft.Apply(buf.Bytes(), time.Second*TIMEOUT_SECONDS)
	if f.Error() != nil {
		return f.Error()
	}
	shardApplyResponse := f.Response().(ShardApplyResponse)
	if shardApplyResponse.Error != nil {
		return shardApplyResponse.Error
	}
	*resp = shardApplyResponse.Response.(rpcTypes.DeleteEdgeToShardResponse)
	return nil
}

// GetVertexAt is the RPC handler for getting a vertex with linearizable read semantics
func (s *Shard) GetVertexAt(req rpcTypes.GetVertexAtShardRequest, resp *rpcTypes.GetVertexAtShardResponse) error {
	// Use VerifyLeader to ensure we have up-to-date state and are still the leader
	// This provides linearizable read semantics without going through the Raft log
	if err := s.raft.VerifyLeader().Error(); err != nil {
		return fmt.Errorf("not leader or unable to verify leadership: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.shardFSM.getVertexAt(req, resp)
}

// GetEdgeAt is the RPC handler for getting a vertex with linearizable read semantics
func (s *Shard) GetEdgeAt(req rpcTypes.GetEdgeAtShardRequest, resp *rpcTypes.GetEdgeAtShardResponse) error {
	// Use VerifyLeader to ensure we have up-to-date state and are still the leader
	// This provides linearizable read semantics without going through the Raft log
	if err := s.raft.VerifyLeader().Error(); err != nil {
		return fmt.Errorf("not leader or unable to verify leadership: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.shardFSM.getEdgeAt(req, resp)
}

// GetNeighbors is the RPC handler for getting vertex neighbors with linearizable read semantics
func (s *Shard) GetNeighbors(req rpcTypes.GetNeighborsToShardRequest, resp *rpcTypes.GetNeighborsToShardResponse) error {
	// Use VerifyLeader to ensure we have up-to-date state and are still the leader
	// This provides linearizable read semantics without going through the Raft log
	if err := s.raft.VerifyLeader().Error(); err != nil {
		return fmt.Errorf("not leader or unable to verify leadership: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.shardFSM.getNeighbors(req, resp)
}

// BatchGetNeighbors is the RPC handler for getting neighbors of multiple vertices with linearizable read semantics
func (s *Shard) BatchGetNeighbors(req rpcTypes.BatchGetNeighborsToShardRequest, resp *rpcTypes.BatchGetNeighborsToShardResponse) error {
	// Use VerifyLeader to ensure we have up-to-date state and are still the leader
	// This provides linearizable read semantics without going through the Raft log
	if err := s.raft.VerifyLeader().Error(); err != nil {
		return fmt.Errorf("not leader or unable to verify leadership: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.shardFSM.batchGetNeighbors(req, resp)
}

// getShardIDFromVertexID extracts shard ID from vertex ID format: "shardID-randomHex"
func (s *Shard) getShardIDFromVertexID(vertexID string) (int, error) {
	parts := strings.Split(vertexID, "-")
	if len(parts) < 2 {
		return 0, fmt.Errorf("invalid vertex ID format: %s", vertexID)
	}
	shardID, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, fmt.Errorf("invalid shard ID in vertex ID: %s", vertexID)
	}
	return shardID, nil
}

// getOrCreateConnectionPool gets or creates a connection pool for a shard
// It checks the current leader and invalidates the pool if the leader has changed
func (s *Shard) getOrCreateConnectionPool(targetShardID int) (*connectionPool, error) {
	// Get target shard config
	targetShardConfig, err := s.config.GetShardByID(targetShardID)
	if err != nil {
		return nil, fmt.Errorf("failed to get shard config for shard %d: %w", targetShardID, err)
	}

	// Use replica manager to get the current leader ID
	currentLeaderID, err := s.replicaManager.GetLeaderID(targetShardID)
	if err != nil {
		return nil, fmt.Errorf("failed to get leader ID for shard %d: %w", targetShardID, err)
	}

	currentAddr, err := targetShardConfig.GetReplicaAddress(currentLeaderID)
	if err != nil {
		return nil, fmt.Errorf("failed to get replica address for shard %d: %w", targetShardID, err)
	}

	// Acquire write lock to check and create pool atomically
	s.poolMu.Lock()
	defer s.poolMu.Unlock()

	// Check if we have an existing pool
	if pool, exists := s.connectionPools[targetShardID]; exists {
		// Check if the pool's address matches the current leader address
		pool.mu.Lock()
		poolAddr := pool.address
		poolClosed := pool.closed
		pool.mu.Unlock()

		if !poolClosed && poolAddr == currentAddr {
			// Pool is still valid, reuse it
			return pool, nil
		}
		// Leader has changed or pool is closed, invalidate it
		pool.closeAll()
		delete(s.connectionPools, targetShardID)
	}

	// Create new connection pool (max 10 connections per shard)
	pool := newConnectionPool(currentAddr, 10)
	s.connectionPools[targetShardID] = pool

	return pool, nil
}

// invalidateConnectionPool removes the connection pool for a shard (e.g., when leader changes)
func (s *Shard) invalidateConnectionPool(targetShardID int) {
	s.poolMu.Lock()
	defer s.poolMu.Unlock()

	if pool, exists := s.connectionPools[targetShardID]; exists {
		pool.closeAll()
		delete(s.connectionPools, targetShardID)
	}
}

// callShardBFS calls the BFS handler on another shard using connection pooling (async, no response needed)
func (s *Shard) callShardBFS(targetShardID int, req *rpcTypes.ShardToShardBFSRequest) error {
	// Get or create connection pool for this shard (checks current leader)
	pool, err := s.getOrCreateConnectionPool(targetShardID)
	if err != nil {
		return err
	}

	// Get connection from pool
	client, err := pool.getConnection()
	if err != nil {
		return fmt.Errorf("failed to get connection to shard %d: %w", targetShardID, err)
	}

	// Make async call - return connection to pool after use (or close if error)
	go func() {
		var callErr error
		defer func() {
			if callErr != nil {
				// On error, close the connection (it might be broken)
				client.Close()
				// Check if error indicates leader change
				errStr := callErr.Error()
				if strings.Contains(errStr, "not leader") || strings.Contains(errStr, "leader") {
					// Leader has changed, invalidate the pool
					s.invalidateConnectionPool(targetShardID)
				}
			} else {
				// On success, return to pool
				pool.returnConnection(client)
			}
		}()

		// Make RPC call (async, we don't wait for response)
		var dummyResp rpcTypes.ShardToQMBFSResponse
		callErr = client.Call("Shard.DistributedBFSFromShard", req, &dummyResp)
		if callErr != nil {
			log.Printf("Failed to call shard %d for BFS: %v", targetShardID, callErr)
		}
	}()

	return nil
}

// performBFS performs the actual BFS logic (shared between QM and shard requests)
// Returns: vertices found, map of shardID -> list of vertices to send to that shard, map of shardID -> expected response count
func (s *Shard) performBFS(reqStartVertices []rpcTypes.StartVertex, radius int, timestamp types.Timestamp, requestID string) ([]rpcTypes.VertexLevel, map[int][]rpcTypes.StartVertex, map[int]int) {
	log.Printf("[Shard %d] performBFS: RequestID=%s, StartVertices=%d, Radius=%d", s.shardID, requestID, len(reqStartVertices), radius)

	// Get or create request state for deduplication
	s.bfsMu.Lock()
	requestState, exists := s.activeBFSRequests[requestID]
	if !exists {
		requestState = &BFSRequestState{
			visited:       make(map[string]int),
			pendingShards: make(map[int]int),
		}
		s.activeBFSRequests[requestID] = requestState
		log.Printf("[Shard %d] Created new BFS request state for RequestID=%s", s.shardID, requestID)
	} else {
		log.Printf("[Shard %d] Reusing existing BFS request state for RequestID=%s (visited: %d)", s.shardID, requestID, len(requestState.visited))
	}
	s.bfsMu.Unlock()

	// Perform local BFS
	requestState.mu.Lock()
	defer requestState.mu.Unlock()

	// Local BFS queue: (vertexID, level)
	type bfsItem struct {
		vertexID types.VertexId
		level    int
	}
	localQueue := make([]bfsItem, 0, len(reqStartVertices))
	// Process each start vertex with its level
	for _, startVertex := range reqStartVertices {
		// Check if already visited in this request
		if _, visited := requestState.visited[string(startVertex.VertexID)]; visited {
			// Already visited, skip
			continue
		}
		// Use the level from the start vertex
		absoluteLevel := startVertex.Level
		if absoluteLevel <= radius {
			localQueue = append(localQueue, bfsItem{vertexID: startVertex.VertexID, level: absoluteLevel})
		}
	}

	// Group cross-shard requests by target shard only
	// Map structure: shardID -> []struct{vertexID, level}
	type vertexLevelPair struct {
		vertexID types.VertexId
		level    int
	}
	crossShardRequests := make(map[int][]vertexLevelPair)
	vertices := make([]rpcTypes.VertexLevel, 0)

	// Perform local BFS
	for len(localQueue) > 0 {
		current := localQueue[0]
		localQueue = localQueue[1:]

		// Skip if already visited
		if existingLevel, visited := requestState.visited[string(current.vertexID)]; visited {
			if existingLevel <= current.level {
				continue // Already visited at same or lower level
			}
		}

		// Mark as visited
		requestState.visited[string(current.vertexID)] = current.level
		vertices = append(vertices, rpcTypes.VertexLevel{
			VertexID: current.vertexID,
			Level:    current.level,
		})

		// Check if we've reached max radius
		if current.level >= radius {
			continue
		}

		// Get neighbors
		s.mu.Lock()
		neighbors, err := s.shardFSM.getNeighborsForBFS(current.vertexID, timestamp)
		s.mu.Unlock()
		if err != nil {
			// Vertex might not exist, continue
			continue
		}

		// Process neighbors
		for _, neighbor := range neighbors {
			// Check if already visited
			if _, visited := requestState.visited[string(neighbor)]; visited {
				continue
			}

			// Determine neighbor's shard
			neighborShardID, err := s.getShardIDFromVertexID(string(neighbor))
			if err != nil {
				log.Printf("Failed to get shard ID for vertex %s: %v", neighbor, err)
				continue
			}

			if neighborShardID == s.shardID {
				// Local neighbor, continue BFS
				nextLevel := current.level + 1
				if nextLevel <= radius {
					localQueue = append(localQueue, bfsItem{vertexID: neighbor, level: nextLevel})
				}
			} else {
				// Cross-shard edge, add to cross-shard requests with correct level
				nextLevel := current.level + 1
				if nextLevel <= radius {
					crossShardRequests[neighborShardID] = append(crossShardRequests[neighborShardID], vertexLevelPair{
						vertexID: neighbor,
						level:    nextLevel,
					})
				}
			}
		}
	}

	// Group vertices by target shard for cross-shard requests
	shardVertices := make(map[int][]rpcTypes.StartVertex)
	expectedResponses := make(map[int]int)
	for targetShardID, vertexLevelPairs := range crossShardRequests {
		// Convert to StartVertex format
		startVertices := make([]rpcTypes.StartVertex, 0, len(vertexLevelPairs))
		for _, pair := range vertexLevelPairs {
			startVertices = append(startVertices, rpcTypes.StartVertex{
				VertexID: pair.vertexID,
				Level:    pair.level,
			})
		}
		shardVertices[targetShardID] = startVertices
		expectedResponses[targetShardID] = 1 // One request per shard
		log.Printf("[Shard %d] Will send %d vertices to shard %d", s.shardID, len(startVertices), targetShardID)
	}

	log.Printf("[Shard %d] performBFS complete: Found %d local vertices, sending to %d shards", s.shardID, len(vertices), len(shardVertices))
	return vertices, shardVertices, expectedResponses
}

// DistributedBFS is the RPC handler for distributed BFS traversal from QM
func (s *Shard) DistributedBFS(req rpcTypes.QMToShardBFSRequest, resp *rpcTypes.ShardToQMBFSResponse) error {
	log.Printf("[Shard %d] DistributedBFS called from QM: RequestID=%s, StartVertices=%d, Radius=%d", s.shardID, req.RequestID, len(req.StartVertices), req.Radius)

	// Use VerifyLeader to ensure we have up-to-date state
	if err := s.raft.VerifyLeader().Error(); err != nil {
		return fmt.Errorf("not leader or unable to verify leadership: %w", err)
	}

	// Initialize response
	resp.RequestID = req.RequestID
	resp.ShardID = s.shardID
	resp.Vertices = make([]rpcTypes.VertexLevel, 0)
	resp.ExpectedResponses = make([]rpcTypes.ExpectedResponse, 0)

	// Perform BFS
	vertices, shardVertices, expectedResponses := s.performBFS(req.StartVertices, req.Radius, req.Timestamp, req.RequestID)

	log.Printf("[Shard %d] BFS complete: Found %d vertices, sending to %d shards", s.shardID, len(vertices), len(shardVertices))

	// Set vertices in response
	resp.Vertices = vertices

	// Convert expected responses to response format
	for shardID, count := range expectedResponses {
		resp.ExpectedResponses = append(resp.ExpectedResponses, rpcTypes.ExpectedResponse{
			ShardID: shardID,
			Count:   count,
		})
		log.Printf("[Shard %d] Expecting %d response(s) from shard %d", s.shardID, count, shardID)
	}

	// Send cross-shard requests asynchronously
	for targetShardID, startVertices := range shardVertices {
		if len(startVertices) > 0 {
			go func(shardID int, vertices []rpcTypes.StartVertex) {
				log.Printf("[Shard %d] Sending BFS request to shard %d with %d vertices", s.shardID, shardID, len(vertices))
				// Create request for target shard
				targetReq := rpcTypes.ShardToShardBFSRequest{
					StartVertices: vertices,
					Radius:        req.Radius,
					Timestamp:     req.Timestamp,
					RequestID:     req.RequestID,
					RequesterAddr: req.RequesterAddr,
				}

				// Call target shard (async, no response needed)
				err := s.callShardBFS(shardID, &targetReq)
				if err != nil {
					log.Printf("[Shard %d] Failed to call shard %d for BFS: %v", s.shardID, shardID, err)
				}
			}(targetShardID, startVertices)
		}
	}

	// NOTE: We return the response synchronously to QM, so we don't send async response here
	// The synchronous response is sufficient for QM-initiated requests
	log.Printf("[Shard %d] Returning synchronous response to QM: %d vertices, %d expected responses", s.shardID, len(resp.Vertices), len(resp.ExpectedResponses))

	return nil
}

// DistributedBFSFromShard is the RPC handler for distributed BFS traversal from another shard
func (s *Shard) DistributedBFSFromShard(req rpcTypes.ShardToShardBFSRequest, resp *rpcTypes.ShardToQMBFSResponse) error {
	log.Printf("[Shard %d] DistributedBFSFromShard called: RequestID=%s, StartVertices=%d, Radius=%d", s.shardID, req.RequestID, len(req.StartVertices), req.Radius)

	// Use VerifyLeader to ensure we have up-to-date state
	if err := s.raft.VerifyLeader().Error(); err != nil {
		return fmt.Errorf("not leader or unable to verify leadership: %w", err)
	}

	// Perform BFS
	vertices, shardVertices, expectedResponses := s.performBFS(req.StartVertices, req.Radius, req.Timestamp, req.RequestID)

	log.Printf("[Shard %d] BFS complete: Found %d vertices, sending to %d shards", s.shardID, len(vertices), len(shardVertices))

	// Initialize response
	resp.RequestID = req.RequestID
	resp.ShardID = s.shardID
	resp.Vertices = vertices
	resp.ExpectedResponses = make([]rpcTypes.ExpectedResponse, 0)

	// Convert expected responses to response format
	for shardID, count := range expectedResponses {
		resp.ExpectedResponses = append(resp.ExpectedResponses, rpcTypes.ExpectedResponse{
			ShardID: shardID,
			Count:   count,
		})
		log.Printf("[Shard %d] Expecting %d response(s) from shard %d", s.shardID, count, shardID)
	}

	// Send cross-shard requests asynchronously
	for targetShardID, startVertices := range shardVertices {
		if len(startVertices) > 0 {
			go func(shardID int, vertices []rpcTypes.StartVertex) {
				log.Printf("[Shard %d] Sending BFS request to shard %d with %d vertices", s.shardID, shardID, len(vertices))
				// Create request for target shard
				targetReq := rpcTypes.ShardToShardBFSRequest{
					StartVertices: vertices,
					Radius:        req.Radius,
					Timestamp:     req.Timestamp,
					RequestID:     req.RequestID,
					RequesterAddr: req.RequesterAddr,
				}

				// Call target shard (async, no response needed)
				err := s.callShardBFS(shardID, &targetReq)
				if err != nil {
					log.Printf("[Shard %d] Failed to call shard %d for BFS: %v", s.shardID, shardID, err)
				}
			}(targetShardID, startVertices)
		}
	}

	// Send results back to QM asynchronously (this is called from another shard, so we must send async)
	go func() {
		qmResp := rpcTypes.ShardToQMBFSResponse{
			RequestID:         req.RequestID,
			ShardID:           s.shardID,
			Vertices:          resp.Vertices,
			ExpectedResponses: resp.ExpectedResponses,
		}

		log.Printf("[Shard %d] Sending async response to QM at %s: %d vertices, %d expected responses", s.shardID, req.RequesterAddr, len(qmResp.Vertices), len(qmResp.ExpectedResponses))

		client, err := rpc.Dial("tcp", req.RequesterAddr)
		if err != nil {
			log.Printf("[Shard %d] Failed to connect to QM at %s: %v", s.shardID, req.RequesterAddr, err)
			return
		}
		defer client.Close()

		var dummyResp rpcTypes.ShardToQMBFSResponse
		err = client.Call("QueryManager.ReceiveBFSResult", &qmResp, &dummyResp)
		if err != nil {
			log.Printf("[Shard %d] Failed to send BFS results to QM: %v", s.shardID, err)
		} else {
			log.Printf("[Shard %d] Successfully sent BFS results to QM", s.shardID)
		}
	}()

	return nil
}

// FetchAll is the RPC handler for fetching all vertex IDs in the shard with linearizable read semantics
func (s *Shard) FetchAll(req rpcTypes.FetchAllToShardRequest, resp *rpcTypes.FetchAllToShardResponse) error {
	// Use VerifyLeader to ensure we have up-to-date state and are still the leader
	// This provides linearizable read semantics without going through the Raft log
	if err := s.raft.VerifyLeader().Error(); err != nil {
		return fmt.Errorf("not leader or unable to verify leadership: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.shardFSM.fetchAll(req, resp)
}

// DeleteAll is the RPC handler for deleting all vertices and edges through Raft consensus
func (s *Shard) DeleteAll(req rpcTypes.DeleteAllToShardRequest, resp *rpcTypes.DeleteAllToShardResponse) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	// Encode the request
	var reqBuf bytes.Buffer
	if err := gob.NewEncoder(&reqBuf).Encode(req); err != nil {
		return err
	}

	// Create the log operation
	logOp := ShardLogOp{
		Op:  DeleteAll,
		Req: reqBuf.Bytes(),
	}

	// Encode the log operation
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(logOp); err != nil {
		return err
	}

	f := s.raft.Apply(buf.Bytes(), time.Second*TIMEOUT_SECONDS)
	if f.Error() != nil {
		return f.Error()
	}
	shardApplyResponse := f.Response().(ShardApplyResponse)
	if shardApplyResponse.Error != nil {
		return shardApplyResponse.Error
	}
	*resp = shardApplyResponse.Response.(rpcTypes.DeleteAllToShardResponse)
	return nil
}

func (s *Shard) GetLeaderID(req rpcTypes.RaftLeadershipRequest, resp *rpcTypes.RaftLeadershipResponse) error {
	_, leaderID := s.raft.LeaderWithID()
	*resp = rpcTypes.RaftLeadershipResponse(leaderID)
	return nil
}

// NewShard creates a new Shard with Raft consensus
func NewShard(cfg *config.Config, shardID int, replicaID int) (*Shard, error) {
	// Update log level from config
	log.SetLevel(cfg.GetLogLevel())

	// Get shard configuration
	shardConfig, err := cfg.GetShardByID(shardID)
	if err != nil {
		return nil, fmt.Errorf("failed to get shard config: %w", err)
	}

	// Get replica configuration
	replicaConfig, err := shardConfig.GetReplicaByID(replicaID)
	if err != nil {
		return nil, fmt.Errorf("failed to get replica config: %w", err)
	}

	// Create the underlying FSM
	shardFSM := newShardFSM(cfg, shardID)

	// Create a request queue for incoming connections
	// Buffer size allows some queuing before blocking
	requestQueue := make(chan net.Conn, 100)

	s := &Shard{
		shardFSM:          shardFSM,
		requestQueue:      requestQueue,
		config:            cfg,
		shardID:           shardID,
		replicaManager:    replica.NewPushBasedReplicaManager(cfg),
		activeBFSRequests: make(map[string]*BFSRequestState),
		connectionPools:   make(map[int]*connectionPool),
	}

	// Setup Raft configuration
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(fmt.Sprintf("shard-%d-replica-%d", shardID, replicaID))

	// Create in-memory log store and stable store
	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()

	// Create in-memory snapshot store
	snapshotStore := raft.NewInmemSnapshotStore()

	// Setup Raft transport
	raftBindAddr := replicaConfig.GetRaftAddress()
	addr, err := net.ResolveTCPAddr("tcp", raftBindAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve raft bind address: %w", err)
	}

	transport, err := raft.NewTCPTransport(raftBindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft transport: %w", err)
	}

	// Create the Raft instance
	raftNode, err := raft.NewRaft(raftConfig, s, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft node: %w", err)
	}

	// Setup Leader Election Observer
	// The observer will watch for leader changes and notify the query manager
	qmAddress := fmt.Sprintf("%s:%d", cfg.QueryManager.Host, cfg.QueryManager.Port)
	SetupLeaderElectionObserver(raftNode, shardID, raftConfig.LocalID, qmAddress)

	s.raft = raftNode

	// Start the replica manager
	s.replicaManager.Start()

	// Bootstrap the cluster if this is the bootstrap replica
	if replicaConfig.Bootstrap {
		// Build configuration with all replicas in the shard
		servers := make([]raft.Server, 0, len(shardConfig.Replicas))
		for _, replica := range shardConfig.Replicas {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(fmt.Sprintf("shard-%d-replica-%d", shardID, replica.ID)),
				Address: raft.ServerAddress(replica.GetRaftAddress()),
			})
		}

		configuration := raft.Configuration{
			Servers: servers,
		}

		future := raftNode.BootstrapCluster(configuration)
		if err := future.Error(); err != nil {
			log.Printf("Warning: Bootstrap failed (this is okay if cluster already exists): %v", err)
		} else {
			log.Printf("Successfully bootstrapped Raft cluster for shard %d", shardID)
		}
	}

	return s, nil
}

// Start begins the RPC server for this Shard
func (s *Shard) Start(rpcAddress string) error {
	serv := rpc.NewServer()
	err := serv.Register(s)
	if err != nil {
		return err
	}
	err = serv.Register(s.replicaManager)
	if err != nil {
		return fmt.Errorf("failed to register replica manager RPC service: %w", err)
	}

	var listener net.Listener
	listener, err = net.Listen("tcp", rpcAddress)
	if err != nil {
		return err
	}

	log.Printf("Shard %d listening for RPC connections on %s", s.shardFSM.Id, rpcAddress)

	// Start worker pool (50 workers)
	numWorkers := 50
	for i := 0; i < numWorkers; i++ {
		go func() {
			for conn := range s.requestQueue {
				serv.ServeConn(conn)
				conn.Close()
			}
		}()
	}

	// Accept connections and enqueue them
	for {
		var conn net.Conn
		conn, err = listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v\n", err)
			continue
		}

		// Enqueue connection for worker to process
		s.requestQueue <- conn
	}
}
