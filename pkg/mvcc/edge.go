package mvcc
// Edge structure for MVCC

// EdgeProp placeholder
type EdgeProp struct{}

// Edge represents one version of a directed edge between two vertices.
type Edge struct {
	ID        string     // Unique edge ID ("from->to")
	FromID    string     // Source vertex ID
	ToID      string     // Destination vertex ID
	Prop      *EdgeProp  // Optional edge properties
	TS        float64    // Version timestamp
	Destroyed bool       // True if this version marks logical deletion
	Prev      []*Edge    // All previous versions (nil or empty for first version)
}

// NewEdge creates the first version of an edge between two vertices.
func NewEdge(from, to string, ts float64) *Edge {
	return &Edge{
		ID:        from + "->" + to,
		FromID:    from,
		ToID:      to,
		Prop:      nil,
		TS:        ts,
		Destroyed: false,
		Prev:      nil, // no previous versions
	}
}

// UpdateEdge creates a new (updated) version of the same edge.
func (e *Edge) UpdateEdge(ts float64, prop *EdgeProp) *Edge {
	e.Destroyed = true // mark old version as superseded
	return &Edge{
		ID:        e.ID,
		FromID:    e.FromID,
		ToID:      e.ToID,
		Prop:      prop,
		TS:        ts,
		Destroyed: false,
		Prev:      []*Edge{e}, // link to previous version
	}
}

// MarkDeleted creates a deleted version of this edge (logical deletion).
func (e *Edge) MarkDeleted(ts float64) *Edge {
	e.Destroyed = true // current version becomes inactive
	return &Edge{
		ID:        e.ID,
		FromID:    e.FromID,
		ToID:      e.ToID,
		Prop:      e.Prop,
		TS:        ts,
		Destroyed: true,
		Prev:      []*Edge{e}, // link back to previous version(s)
	}
}

// AliveAt checks if this version is alive (visible) at the given timestamp.
func (e *Edge) AliveAt(ts float64) bool {
	return !e.Destroyed && e.TS <= ts
}
