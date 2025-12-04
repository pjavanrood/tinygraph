package shard

import (
	"container/list"
	"fmt"
	"maps"
	"math/rand/v2"
	"net/rpc"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

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
	bfsMu        sync.RWMutex
	// Connection pools per shard (keyed by ShardID)
	connections   map[string]*rpc.Client
	connectionsMu sync.Mutex
}

func (s *ShardFSM) Call(hostname string, callLambda func(*rpc.Client) error) {
	for {
		s.connectionsMu.Lock()
		if s.connections[hostname] == nil {
			client, err := rpc.Dial("tcp", hostname)
			for err != nil {
				log.Printf("Retrying dial to shard leader")
				s.connectionsMu.Unlock()
				time.Sleep(50 * time.Millisecond)
				s.connectionsMu.Lock()
				client, err = rpc.Dial("tcp", hostname)
			}
			s.connections[hostname] = client
		}
		s.connectionsMu.Unlock()

		//err := s.connections[hostname].Call(rpcCall, req, resp)
		err := callLambda(s.connections[hostname])
		if err == nil {
			return
		}
	}
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
	s.bfsMu.RLock()
	instance := s.bfsInstances[req.Id]
	s.bfsMu.RUnlock()

	q := list.New()
	for _, vertexLevelPair := range req.Vertices {
		q.PushBack(&vertexLevelPair)
	}

	localVisited := make([]internalTypes.VertexId, 0)
	frontierVertices := make(map[internalTypes.ShardId]([]rpcTypes.BFSVertexLevelPair))

	for q.Len() > 0 {
		curr := q.Front().Value.(*rpcTypes.BFSVertexLevelPair)
		q.Remove(q.Front())

		instance.Mx.RLock()
		if _, exists := instance.Visited[curr.V]; exists {
			instance.Mx.RUnlock()
			continue
		}
		instance.Mx.RUnlock()

		if vert, exists := s.vertices[VertexId(curr.V)]; exists {
			stamped := vert.GetAt(req.Timestamp)
			if stamped != nil {
				instance.Mx.Lock()
				instance.Visited[curr.V] = true
				instance.Mx.Unlock()
				localVisited = append(localVisited, curr.V)
				if curr.N == 0 {
					continue
				}
				for _, edge := range vert.GetAllEdges(req.Timestamp) {
					// early out if already visited
					instance.Mx.RLock()
					if _, exists := instance.Visited[internalTypes.VertexId(edge.ToID)]; exists {
						instance.Mx.RUnlock()
						continue
					}
					instance.Mx.RUnlock()

					if _, has := s.vertices[VertexId(edge.ToID)]; has {
						// edge to local vertex
						q.PushBack(&rpcTypes.BFSVertexLevelPair{
							V: edge.ToID,
							N: curr.N - 1,
						})
					} else {
						// edge to remote vertex

						// figure out WHERE the vertex is
						// copied from pkg/qm/query_manager.go
						parts := strings.Split(string(edge.ToID), "-")
						shardId, _ := strconv.Atoi(parts[0])
						shardConfig, err := s.config.GetShardByID(shardId)
						if err != nil {
							log.Printf("Failed to get shard by ID: %v", err)
							continue
						}

						// add the vertex and level to the list
						if _, has := frontierVertices[internalTypes.ShardId(shardConfig.ID)]; !has {
							frontierVertices[internalTypes.ShardId(shardConfig.ID)] = make([]rpcTypes.BFSVertexLevelPair, 0)
						}

						frontierVertices[internalTypes.ShardId(shardConfig.ID)] = append(frontierVertices[internalTypes.ShardId(shardConfig.ID)], rpcTypes.BFSVertexLevelPair{V: edge.ToID, N: curr.N - 1})
						instance.Mx.Lock()
						instance.Visited[internalTypes.VertexId(edge.ToID)] = true
						instance.Mx.Unlock()

						// TODO: make this shared maybe? If it's static in this way
						/*
							parts := strings.Split(string(edge.ToID), "-")
							shardId, _ := strconv.Atoi(parts[0])
							shardConfig, err := s.config.GetShardByID(shardId)
							if err != nil {
								log.Printf("Failed to get shard by ID: %v", err)
								continue
							}

							// dispatch the request to the other shard
							dispatchedRequests[shardConfig.ID]++
							instance.Mx.Lock()
							instance.Visited[internalTypes.VertexId(edge.ToID)] = true
							instance.Mx.Unlock()
							go func() {
								log.Printf("Dispatching for vertex %s", edge.ToID)

								// connect to the shard
								replicas := shardConfig.Replicas
								leaderHostname := replicas[rand.IntN(len(shardConfig.Replicas))].GetRPCAddress()

								if leaderHostname == "" {
									log.Printf("Failed getting leader hostname")
									return
								}

								bfsReq := &rpcTypes.BFSToShardRequest{
									Root:         edge.ToID,
									N:            curr.N - 1,
									Timestamp:    req.Timestamp,
									Id:           req.Id,
									CallbackAddr: req.CallbackAddr,
									FirstReq:     false,
								}
								var bfsResp rpcTypes.BFSToShardResponse
								bfsLambda := func(client *rpc.Client) error {
									return client.Call("Shard.BFS", bfsReq, &bfsResp)
								}
								s.Call(leaderHostname, bfsLambda)
								if !bfsResp.Success {
									log.Println("Success is false")
									return
								}
							}()
						*/
					}
				}
			}
		}
	}

	// dispatch shard requests
	for shardId, frontierVerts := range frontierVertices {
		go func() {
			log.Printf("Dispatching for shard %d", shardId)

			shardConfig, err := s.config.GetShardByID(int(shardId))
			if err != nil {
				log.Printf("Failed to get shard by ID: %v", err)
				return
			}

			// connect to the shard
			replicas := shardConfig.Replicas
			leaderHostname := replicas[rand.IntN(len(shardConfig.Replicas))].GetRPCAddress()

			if leaderHostname == "" {
				log.Printf("Failed getting leader hostname")
				return
			}

			bfsReq := &rpcTypes.BFSToShardRequest{
				Vertices:     frontierVerts,
				Timestamp:    req.Timestamp,
				Id:           req.Id,
				CallbackAddr: req.CallbackAddr,
				FirstReq:     false,
			}
			var bfsResp rpcTypes.BFSToShardResponse
			bfsLambda := func(client *rpc.Client) error {
				return client.Call("Shard.BFS", bfsReq, &bfsResp)
			}
			s.Call(leaderHostname, bfsLambda)
			if !bfsResp.Success {
				log.Println("Success is false")
				return
			}
		}()
	}

	// send the results to the callback addr

	toClientReq := &rpcTypes.BFSFromShardRequest{
		Id:                 req.Id,
		Shard:              types.ShardId(s.Id),
		Vertices:           localVisited,
		DispatchedRequests: slices.Collect(maps.Keys(frontierVertices)),
		FirstResp:          req.FirstReq,
	}
	var toClientResp rpcTypes.BFSFromShardResponse
	responseLambda := func(client *rpc.Client) error {
		return client.Call("QueryManager.BFSResponse", toClientReq, &toClientResp)
	}
	s.Call(req.CallbackAddr, responseLambda)
}

func (s *ShardFSM) BFS(req rpcTypes.BFSToShardRequest, resp *rpcTypes.BFSToShardResponse) error {
	success := true
	defer func() {
		resp.Success = success
	}()
	// search for the vertices at the timestamp first
	for _, vertexLevelPair := range req.Vertices {
		vertex, ok := s.vertices[VertexId(vertexLevelPair.V)]
		if !ok {
			success = false
			return fmt.Errorf("Vertex with ID \"%s\" has never existed", vertex)
		}

		if vertex.GetAt(req.Timestamp) == nil {
			success = false
			return fmt.Errorf("Vertex with ID \"%s\" does not exist at timestamp %f", vertex, req.Timestamp)
		}
	}

	s.bfsMu.Lock()
	// find or create bfsInstances entry
	if _, exists := s.bfsInstances[req.Id]; !exists {
		s.bfsInstances[req.Id] = &bfs.BFSInstance{
			Id:              req.Id,
			Visited:         make(map[internalTypes.VertexId]bool),
			CallbackAddress: req.CallbackAddr,
		}
	}
	s.bfsMu.Unlock()

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
		connections:  make(map[string]*rpc.Client),
	}
}
