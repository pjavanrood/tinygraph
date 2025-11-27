package shard

import (
	"container/list"
	"fmt"
	"maps"
	"net"
	"net/rpc"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/pjavanrood/tinygraph/internal/config"
	"github.com/pjavanrood/tinygraph/internal/types"
	internalTypes "github.com/pjavanrood/tinygraph/internal/types"
	"github.com/pjavanrood/tinygraph/pkg/bfs"
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
	vertices     map[VertexId]*mvccTypes.Vertex
	Id           ShardId
	config       *config.Config
	bfsInstances map[types.BFSId]*bfs.BFSInstance
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

// actual background BFS call
func (s *ShardFSM) bfs(req rpcTypes.BFSToShardRequest) {
	instance := s.bfsInstances[req.Id]
	instance.Mx.Lock()
	defer instance.Mx.Unlock()

	var wg sync.WaitGroup
	var wgmx sync.Mutex

	type BFSEntry struct {
		Id VertexId
		N  int
	}

	q := list.New()
	q.PushBack(&BFSEntry{
		Id: VertexId(req.Root),
		N:  req.N,
	})

	localVisited := make([]internalTypes.VertexId, 0)
	dispatchedRequests := make(map[int]int)

	for q.Len() > 0 {
		curr := q.Front().Value.(*BFSEntry)
		q.Remove(q.Front())

		if _, vis := instance.Visited[internalTypes.VertexId(curr.Id)]; vis {
			continue
		}

		if vert, exists := s.vertices[curr.Id]; exists {
			stamped := vert.GetAt(req.Timestamp)
			if stamped != nil {
				instance.Visited[internalTypes.VertexId(curr.Id)] = true
				localVisited = append(localVisited, internalTypes.VertexId(curr.Id))
				if curr.N == 0 {
					continue
				}
				for _, edge := range vert.GetAllEdges(req.Timestamp) {
					if _, has := s.vertices[VertexId(edge.ToID)]; has {
						// edge to local vertex
						q.PushBack(&BFSEntry{
							Id: VertexId(edge.ToID),
							N:  curr.N - 1,
						})
					} else {
						// edge to remote vertex

						// figure out WHERE the vertex is
						// copied from pkg/qm/query_manager.go
						// TODO: make this shared maybe? If it's static in this way
						parts := strings.Split(string(edge.ToID), "-")
						shardId, _ := strconv.Atoi(parts[0])
						shardConfig, err := s.config.GetShardByID(shardId)
						if err != nil {
							log.Printf("Failed to get shard by ID: %v", err)
							continue
						}

						// dispatch the request to the other shard
						wg.Add(1)
						go func() {
							defer wg.Done()
							wgmx.Lock()
							defer wgmx.Unlock()

							// connect to the shard
							leaderHostname := func() string {
								leaderData := &rpcTypes.GetLeaderIdRequest{
									ShardId: shardId,
								}
								var leaderResponse rpcTypes.GetLeaderIdResponse

								client, err := rpc.Dial("tcp", net.JoinHostPort(s.config.QueryManager.Host, strconv.Itoa(s.config.QueryManager.Port)))
								if err != nil {
									log.Printf("Failed to connect to QueryManager")
									return ""
								}

								err = client.Call("QueryManager.GetLeaderId", leaderData, &leaderResponse)
								if err != nil {
									log.Printf("RPC call to shard %d failed: %w", shardConfig.ID, err)
									return ""
								}
								defer client.Close()

								var addr string
								addr, err = shardConfig.GetReplicaAddress(leaderResponse.LeaderId)
								if err != nil {
									log.Printf("failed to get replica address for shard %d: %w", shardConfig.ID, err)
									return ""
								}

								return addr
							}()

							if leaderHostname == "" {
								log.Printf("Failed getting leader hostname")
								return
							}

							client, err := rpc.Dial("tcp", leaderHostname)
							if err != nil {
								fmt.Printf("Failed to dial shard leader")
								return
							}
							defer client.Close()

							bfsReq := &rpcTypes.BFSToShardRequest{
								Root:         edge.ToID,
								N:            curr.N - 1,
								Timestamp:    req.Timestamp,
								Id:           req.Id,
								CallbackAddr: req.CallbackAddr,
							}
							var bfsResp rpcTypes.BFSToShardResponse
							err = client.Call("Shard.BFS", bfsReq, &bfsResp)
							if err != nil {
								log.Printf("RPC call to shard %d failed: %w", shardConfig.ID, err)
								return
							}
							if !bfsResp.Success {
								log.Println("Success is false")
								return
							}
							dispatchedRequests[shardConfig.ID]++
						}()
					}
				}
			}
		}
	}

	// send the results to the callback addr

	client, err := rpc.Dial("tcp", req.CallbackAddr)
	if err != nil {
		fmt.Printf("Unable to connect to client %s", req.CallbackAddr)
		return
	}
	defer client.Close()
	toClientReq := &rpcTypes.BFSFromShardRequest{
		Id:                 req.Id,
		Shard:              types.ShardId(s.Id),
		Vertices:           localVisited,
		DispatchedRequests: dispatchedRequests,
	}
	var toClientResp rpcTypes.BFSFromShardResponse
	err = client.Call("QueryManager.BFSResponse", toClientReq, &toClientResp)
	if err != nil {
		fmt.Printf("RPC to BFSResponse failed: %w", err)
		return
	}
}

func (s *ShardFSM) BFS(req rpcTypes.BFSToShardRequest, resp *rpcTypes.BFSToShardResponse) error {
	success := true
	defer func() {
		resp.Success = success
	}()
	// search for the vertex at the timestamp first
	vertex, ok := s.vertices[VertexId(req.Root)]
	if !ok {
		success = false
		return fmt.Errorf("Vertex with ID \"%s\" has never existed", req.Root)
	}

	if vertex.GetAt(req.Timestamp) == nil {
		success = false
		return fmt.Errorf("Vertex with ID \"%s\" does not exist at timestamp %f", req.Root, req.Timestamp)
	}

	// find or create bfsInstances entry
	if _, exists := s.bfsInstances[req.Id]; !exists {
		s.bfsInstances[req.Id] = &bfs.BFSInstance{
			Id:              req.Id,
			Visited:         make(map[internalTypes.VertexId]bool),
			CallbackAddress: req.CallbackAddr,
		}
	}

	go s.bfs(req)

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

// deleteAll removes all vertices and edges from the shard
// This is an internal method called by Shard through Raft consensus
func (s *ShardFSM) deleteAll(req rpcTypes.DeleteAllToShardRequest, resp *rpcTypes.DeleteAllToShardResponse) error {
	// Clear all vertices (which also clears all edges since edges are stored in vertices)
	s.vertices = make(map[VertexId]*mvccTypes.Vertex, 0)
	resp.Success = true
	return nil
}

// newShardFSM creates a new ShardFSM instance
func newShardFSM(cfg *config.Config, id int) *ShardFSM {
	return &ShardFSM{
		vertices:     make(map[VertexId]*mvccTypes.Vertex, 0),
		Id:           ShardId(id),
		config:       cfg,
		bfsInstances: make(map[types.BFSId]*bfs.BFSInstance),
	}
}
