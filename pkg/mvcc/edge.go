package mvcc

import (
	"slices"

	"github.com/pjavanrood/tinygraph/internal/types"
)

// Edge structure for MVCC

// EdgeProp placeholder
type EdgeProp types.Properties

// Edge represents one version of a directed edge between two vertices.
type Edge struct {
	FromID    types.VertexId  // Source vertex ID
	ToID      types.VertexId  // Destination vertex ID
	Prop      *EdgeProp       // Optional edge properties
	TS        types.Timestamp // Version timestamp
	Destroyed bool            // True if this version marks logical deletion
	Prev      []*Edge         // All previous versions (nil or empty for first version)
}

// NewEdge creates the first version of an edge between two vertices.
func NewEdge(from, to types.VertexId, ts types.Timestamp) *Edge {
	return &Edge{
		FromID:    from,
		ToID:      to,
		Prop:      nil,
		TS:        ts,
		Destroyed: false,
		Prev:      nil, // no previous versions
	}
}

// UpdateEdge creates a new (updated) version of the same edge.
func (e *Edge) UpdateEdge(ts types.Timestamp, prop *EdgeProp) *Edge {
	temp := e.Prev
	e.Prev = nil
	out := &Edge{
		FromID:    e.FromID,
		ToID:      e.ToID,
		Prop:      prop,
		TS:        ts,
		Destroyed: false,
		Prev:      nil,
	}

	if temp == nil {
		out.Prev = make([]*Edge, 1)
		out.Prev[0] = e
	} else {
		out.Prev = slices.Insert(temp, 0, e)
	}

	return e
}

// MarkDeleted creates a deleted version of this edge (logical deletion).
func (e *Edge) MarkDeleted(ts types.Timestamp) *Edge {
	out := e.UpdateEdge(ts, e.Prop)
	out.Destroyed = true
	return out
}

func (e *Edge) GetAt(ts types.Timestamp) *Edge {
	if e.TS <= ts {
		return e
	}

	for _, ep := range e.Prev {
		if e.TS <= ts {
			return ep
		}
	}

	return nil
}

// AliveAt checks if this version is alive (visible) at the given timestamp.
func (e *Edge) AliveAt(ts types.Timestamp) bool {
	timestamped := e.GetAt(ts)
	return timestamped != nil && !timestamped.Destroyed
}
