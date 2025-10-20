package mvcc

import "fmt"

// Vertex structure for MVCC

// VertexProp placeholder 
type VertexProp struct{}

// Vertex represents a graph vertex that maintains outgoing edges.
type Vertex struct {
	ID    string           // Vertex ID
	Edges map[string]*Edge // Outgoing edges keyed by "from->to"
	TS    float64          // Timestamp of latest modification
	Prev  []*Vertex        // All previous versions (nil or empty for first)
}

// NewVertex creates the first version of a vertex.
func NewVertex(id string, ts float64) *Vertex {
	return &Vertex{
		ID:    id,
		Edges: make(map[string]*Edge),
		TS:    ts,
		Prev:  nil, // no previous versions
	}
}

// AddEdge creates or updates an outgoing edge.
func (v *Vertex) AddEdge(to string, ts float64) error {
	edgeID := v.ID + "->" + to
	if cur, ok := v.Edges[edgeID]; ok {
		v.Edges[edgeID] = cur.UpdateEdge(ts, cur.Prop)
	} else {
		v.Edges[edgeID] = NewEdge(v.ID, to, ts)
	}
	return nil
}

// DeleteEdge logically deletes an outgoing edge by creating a deleted version.
func (v *Vertex) DeleteEdge(to string, ts float64) {
	edgeID := v.ID + "->" + to
	if cur, ok := v.Edges[edgeID]; ok && cur.AliveAt(ts) {
		v.Edges[edgeID] = cur.MarkDeleted(ts)
	}
}

// GetEdge returns the latest alive version of an edge at timestamp ts.
// Traverses backwards through Prev versions if necessary.
func (v *Vertex) GetEdge(to string, ts float64) *Edge {
	edgeID := v.ID + "->" + to
	if head, ok := v.Edges[edgeID]; ok {
		for queue := []*Edge{head}; len(queue) > 0; {
			e := queue[0]
			queue = queue[1:]
			if e.AliveAt(ts) {
				return e
			}
			queue = append(queue, e.Prev...)
		}
	}
	return nil
}

// HasEdge checks whether an edge to `to` is alive at timestamp ts.
func (v *Vertex) HasEdge(to string, ts float64) bool {
	return v.GetEdge(to, ts) != nil
}
