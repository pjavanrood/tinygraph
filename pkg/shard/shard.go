package shard

import (
	"fmt"
	"log"
	"net"
	"net/rpc"

	"github.com/pjavanrood/tinygraph/internal/config"
	internalTypes "github.com/pjavanrood/tinygraph/internal/types"
	mvccTypes "github.com/pjavanrood/tinygraph/pkg/mvcc"
	rpcTypes "github.com/pjavanrood/tinygraph/pkg/rpc"
)

type ShardId internalTypes.ShardId
type VertexId internalTypes.VertexId
type Timestamp internalTypes.Timestamp
type Props internalTypes.Properties

type Shard struct {
	vertices map[VertexId]*mvccTypes.Vertex
	Id       ShardId
	config   *config.Config
}

func (s *Shard) AddVertex(req rpcTypes.AddVertexToShardRequest, resp *rpcTypes.AddVertexToShardResponse) error {
	success := true
	defer func() {
		resp.Success = success
	}()

	// check if vertex with given ID already exists, and is not deleted
	if _, ok := s.vertices[VertexId(req.VertexID)]; ok {
		success = false
		return fmt.Errorf("Vertex with ID \"%s\" already exists and is not deleted", req.VertexID)
	}

	// we add this new vertex to our map
	vertex := mvccTypes.NewVertex(req.VertexID, req.Timestamp)

	s.vertices[VertexId(req.VertexID)] = vertex
	return nil
}

func (s *Shard) AddEdge(req rpcTypes.AddEdgeToShardRequest, resp *rpcTypes.AddEdgeToShardResponse) error {
	success := true
	defer func() {
		resp.Success = success
	}()

	// check if source Vertex exists
	fromVertex, _ := s.vertices[VertexId(req.FromVertexID)]

	err := fromVertex.AddEdge(req.ToVertexID, req.Timestamp)
	if err != nil {
		success = false
		return err
	}

	return nil
}

func (s *Shard) DeleteEdge(req rpcTypes.DeleteEdgeToShardRequest, resp *rpcTypes.DeleteEdgeToShardResponse) error {
	success := true
	defer func() {
		resp.Success = success
	}()

	// check if source Vertex exists
	fromVertex, _ := s.vertices[VertexId(req.FromVertexID)]

	fromVertex.DeleteEdge(req.ToVertexID, req.Timestamp)

	return nil
}

func (s *Shard) GetNeighbors(req rpcTypes.GetNeighborsToShardRequest, resp *rpcTypes.GetNeighborsToShardResponse) error {
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

func NewShard(cfg *config.Config, id int) *Shard {
	return &Shard{
		vertices: make(map[VertexId]*mvccTypes.Vertex, 0),
		Id:       ShardId(id),
		config:   cfg,
	}
}

func (s *Shard) Start() error {
	serv := rpc.NewServer()
	err := serv.Register(s)
	if err != nil {
		return err
	}

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
