package mvcc

import (
	"slices"

	"github.com/pjavanrood/tinygraph/internal/types"
)

// Vertex structure for MVCC

// VertexProp placeholder
type VertexProp types.Properties

// Vertex represents a graph vertex that maintains outgoing edges.
type Vertex struct {
	ID    types.VertexId           // Vertex ID
	Edges map[types.VertexId]*Edge // Outgoing edges keyed by "from->to"
	TS    types.Timestamp          // Timestamp of latest modification
	Prop  *VertexProp              // The vertex properties at this timestamp
	Prev  []*Vertex                // All previous versions (nil or empty for first)
}

// NewVertex creates the first version of a vertex.
func NewVertex(id types.VertexId, ts types.Timestamp) *Vertex {
	return &Vertex{
		ID:    id,
		Edges: make(map[types.VertexId]*Edge),
		TS:    ts,
		Prop:  nil,
		Prev:  nil, // no previous versions
	}
}

func (v *Vertex) UpdateVertex(ts types.Timestamp, prop *VertexProp) *Vertex {
	temp := v.Prev
	v.Prev = nil
	out := &Vertex{
		ID:    v.ID,
		Edges: v.Edges,
		TS:    ts,
		Prop:  prop,
		Prev:  nil,
	}

	if temp == nil {
		out.Prev = make([]*Vertex, 1)
		out.Prev[0] = v
	} else {
		out.Prev = slices.Insert(temp, 0, v)
	}

	return v
}

// AddEdge creates or updates an outgoing edge.
func (v *Vertex) AddEdge(to types.VertexId, ts types.Timestamp) error {
	if cur, ok := v.Edges[to]; ok {
		v.Edges[to] = cur.UpdateEdge(ts, cur.Prop)
	} else {
		v.Edges[to] = NewEdge(v.ID, to, ts)
	}
	return nil
}

// DeleteEdge logically deletes an outgoing edge by creating a deleted version.
func (v *Vertex) DeleteEdge(to types.VertexId, ts types.Timestamp) {
	if cur, ok := v.Edges[to]; ok && cur.AliveAt(ts) {
		v.Edges[to] = cur.MarkDeleted(ts)
	}
}

// GetEdge returns the version of an edge at timestamp ts.
func (v *Vertex) GetEdge(to types.VertexId, ts types.Timestamp) *Edge {
	if head, ok := v.Edges[to]; ok {
		return head.GetAt(ts)
	}
	return nil
}

// HasEdge checks whether an edge to `to` is alive at timestamp ts.
func (v *Vertex) HasEdge(to types.VertexId, ts types.Timestamp) bool {
	e := v.GetEdge(to, ts)
	return e != nil && e.AliveAt(ts)
}

// GetAllEdges returns all edges for a vertex at timestamp ts.
func (v *Vertex) GetAllEdges(ts types.Timestamp) []*Edge {
	edges := make([]*Edge, 0)
	for _, edge := range v.Edges {
		if edge.AliveAt(ts) {
			edges = append(edges, edge)
		}
	}
	return edges
}

func (v *Vertex) GetAt(ts types.Timestamp) *Vertex {
	if v.TS <= ts {
		return v
	}

	for _, vp := range v.Prev {
		if vp.TS <= ts {
			return vp
		}
	}

	return nil
}
