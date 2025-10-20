package shard

import (
	"fmt"
	"log"
	"net"
	"net/rpc"

	"github.com/pjavanrood/tinygraph/internal/config"
	rpcTypes "github.com/pjavanrood/tinygraph/pkg/rpc"
)

type ShardId uint32
type VertexId string
type VertexProps map[string]string
type EdgeProps map[string]string

// Placeholder for Vertex impl
type Vertex struct {
	id    VertexId
	edges map[VertexId]*Edge
	props VertexProps
}

// Placeholder for Edge impl
type Edge struct {
	v1, v2 VertexId
	props  EdgeProps
}

type Shard struct {
	vertices map[VertexId]*Vertex
	Id       ShardId
	config   *config.Config
}

func (s *Shard) AddVertex(req rpcTypes.AddVertexToShardRequest, resp *rpcTypes.AddVertexToShardResponse) error {
	success := true
	defer func() {
		resp.Success = success
	}()

	// check if vertex with given ID already exists, and is not deleted
	// TODO: change this logic when MVCC is implemented -- so far, if exists then throw err
	if _, ok := s.vertices[VertexId(req.VertexID)]; ok {
		success = false
		return fmt.Errorf("Vertex with ID \"%s\" already exists and is not deleted", req.VertexID)
	}

	// we add this new vertex to our map
	vertex := &Vertex{
		id:    VertexId(req.VertexID),
		edges: make(map[VertexId]*Edge),
		props: VertexProps(req.Properties),
	}

	// TODO: if this is a previously deleted vertex, then update it's history

	s.vertices[VertexId(req.VertexID)] = vertex
	return nil
}

func (s *Shard) AddEdge(req rpcTypes.AddEdgeToShardRequest, resp *rpcTypes.AddEdgeToShardResponse) error {
	success := true
	defer func() {
		resp.Success = success
	}()

	// TODO: update with MVCC check
	// check if source Vertex exists
	fromVertex, ok := s.vertices[VertexId(req.FromVertexID)]
	if !ok {
		success = false
		return fmt.Errorf("Source vertex with ID \"%s\" does not exist", req.FromVertexID)
	}

	// TODO: update with MVCC check
	// check if destination vertex exists
	var toVertex *Vertex
	toVertex, ok = s.vertices[VertexId(req.ToVertexID)]
	if !ok {
		success = false
		return fmt.Errorf("Destination vertex with ID \"%s\" does not exist", req.ToVertexID)
	}

	// check if edge already exists
	// TODO: update with MVCC check
	if _, ok = s.vertices[fromVertex.id].edges[toVertex.id]; ok {
		success = false
		return fmt.Errorf("Edge with vertices (\"%s\", \"%s\") already exists", req.FromVertexID, req.ToVertexID)
	}

	// create the edge
	newEdge := &Edge{
		v1:    fromVertex.id,
		v2:    toVertex.id,
		props: EdgeProps(req.Properties),
	}

	fromVertex.edges[toVertex.id] = newEdge

	return nil
}

func (s *Shard) DeleteEdge(req rpcTypes.DeleteEdgeToShardRequest, resp *rpcTypes.DeleteEdgeToShardResponse) error {
	success := true
	defer func() {
		resp.Success = success
	}()

	// TODO: update with MVCC check
	// check if source Vertex exists
	fromVertex, ok := s.vertices[VertexId(req.FromVertexID)]
	if !ok {
		success = false
		return fmt.Errorf("Source vertex with ID \"%s\" does not exist", req.FromVertexID)
	}

	// TODO: update with MVCC check
	// check if destination vertex exists
	var toVertex *Vertex
	toVertex, ok = s.vertices[VertexId(req.ToVertexID)]
	if !ok {
		success = false
		return fmt.Errorf("Destination vertex with ID \"%s\" does not exist", req.ToVertexID)
	}

	// check if edge already exists
	// TODO: update with MVCC check
	if _, ok = s.vertices[fromVertex.id].edges[toVertex.id]; !ok {
		success = false
		return fmt.Errorf("Edge with vertices (\"%s\", \"%s\") does not exist", req.FromVertexID, req.ToVertexID)
	}

	// delete edge
	// TODO: update with MVCC check

	delete(fromVertex.edges, toVertex.id)

	return nil
}

func NewShard(cfg *config.Config, id int) *Shard {
	return &Shard{
		vertices: make(map[VertexId]*Vertex, 0),
		Id:       ShardId(id),
		config:   cfg,
	}
}

func (s *Shard) Start() error {
	err := rpc.Register(s)
	if err != nil {
		return err
	}

	serv := rpc.NewServer()

	var shardConfig *config.ShardConfig
	shardConfig, err = s.config.GetShardByID(int(s.Id))
	if err != nil {
		return err
	}
	var listener net.Listener
	listener, err = net.Listen("tcp", shardConfig.GetAddress())
	if err != nil {
		return err
	}

	for {
		var conn net.Conn
		conn, err = listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v\n", err)
			continue
		}
		go serv.ServeConn(conn)
	}
}
