package shard

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/pjavanrood/tinygraph/internal/config"
	"github.com/pjavanrood/tinygraph/internal/util"
	mvccTypes "github.com/pjavanrood/tinygraph/pkg/mvcc"
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

// Shard wraps a ShardFSM with Raft consensus functionality
// It implements the raft.FSM interface and handles all RPC calls
type Shard struct {
	raft          *raft.Raft
	shardFSM      *ShardFSM
	mu            sync.Mutex
	connSemaphore chan struct{} // Limits concurrent connections
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

func (s *Shard) BFS(req rpcTypes.BFSToShardRequest, resp *rpcTypes.BFSToShardResponse) error {
	// read requests do not need to go through the leader
	/*
		if err := s.raft.VerifyLeader().Error; err != nil {
			return fmt.Errorf("not leader or unable to verify leadership: %w", err)
		}
	*/

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.shardFSM.BFS(req, resp)
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

	// Create a semaphore to limit concurrent connections (max 50)
	maxConcurrentConns := 50
	connSemaphore := make(chan struct{}, maxConcurrentConns)

	s := &Shard{
		shardFSM:      shardFSM,
		connSemaphore: connSemaphore,
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

	var listener net.Listener
	listener, err = net.Listen("tcp", rpcAddress)
	if err != nil {
		return err
	}

	log.Printf("Shard %d listening for RPC connections on %s", s.shardFSM.Id, rpcAddress)

	for {
		var conn net.Conn
		conn, err = listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v\n", err)
			continue
		}

		// Acquire semaphore before spawning goroutine
		s.connSemaphore <- struct{}{}

		go func(c net.Conn) {
			defer func() {
				// Release semaphore when done
				<-s.connSemaphore
			}()
			serv.ServeConn(c)
		}(conn)
	}
}
