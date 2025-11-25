package main

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/dcadenas/pagerank"
	"github.com/dominikbraun/graph"
)

// LocalGraphBenchmark handles local graph benchmarking
type LocalGraphBenchmark struct {
	localGraph       graph.Graph[string, string]
	measurements     []Measurement
	measurementsLock sync.Mutex
	bfsVertices      []string
	bfsRadius        int
}

func NewLocalGraphBenchmark(bfsVertices []string, bfsRadius int) *LocalGraphBenchmark {
	return &LocalGraphBenchmark{
		localGraph:   graph.New(graph.StringHash, graph.Directed()),
		measurements: make([]Measurement, 0),
		bfsVertices:  bfsVertices,
		bfsRadius:    bfsRadius,
	}
}

func (lgb *LocalGraphBenchmark) recordMeasurement(m Measurement) {
	lgb.measurementsLock.Lock()
	defer lgb.measurementsLock.Unlock()
	lgb.measurements = append(lgb.measurements, m)
}

func (lgb *LocalGraphBenchmark) performLocalBFS(startVertexID string, radius int) ([]string, error) {
	if radius < 0 {
		return nil, fmt.Errorf("radius must be non-negative")
	}

	visited := make(map[string]bool)
	result := []string{}

	type queueItem struct {
		vertex string
		dist   int
	}
	queue := []queueItem{{vertex: startVertexID, dist: 0}}
	visited[startVertexID] = true
	result = append(result, startVertexID)

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if current.dist >= radius {
			continue
		}

		adjacencyMap, err := lgb.localGraph.AdjacencyMap()
		if err != nil {
			return nil, fmt.Errorf("failed to get adjacency map: %v", err)
		}

		neighbors, ok := adjacencyMap[current.vertex]
		if !ok {
			continue
		}

		for neighbor := range neighbors {
			if !visited[neighbor] {
				visited[neighbor] = true
				result = append(result, neighbor)
				queue = append(queue, queueItem{vertex: neighbor, dist: current.dist + 1})
			}
		}
	}

	return result, nil
}

func (lgb *LocalGraphBenchmark) Run(workload []string, checkpointPositions []int) BenchmarkResults {
	startTime := time.Now()

	// Parse workload
	operations := parseWorkload(workload)
	totalOps := len(operations)
	currentOp := 0
	checkpointIdx := 0

	// Ingest graph and run BFS at checkpoints
	for _, op := range operations {
		// Add vertices and edge to local graph
		_ = lgb.localGraph.AddVertex(op.From)
		_ = lgb.localGraph.AddVertex(op.To)
		_ = lgb.localGraph.AddEdge(op.From, op.To)

		currentOp++

		// Check if we've reached a checkpoint
		if checkpointIdx < len(checkpointPositions) && currentOp >= checkpointPositions[checkpointIdx] {
			// Run BFS queries
			for _, vertexID := range lgb.bfsVertices {
				// Check if vertex exists in graph
				_, err := lgb.localGraph.Vertex(vertexID)
				if err != nil {
					continue // Vertex doesn't exist yet
				}

				start := time.Now()
				_, err = lgb.performLocalBFS(vertexID, lgb.bfsRadius)
				rtt := time.Since(start)

				if err != nil {
					log.Printf("Error running local BFS for vertex %s: %v", vertexID, err)
					continue
				}

				// Record measurement
				lgb.recordMeasurement(Measurement{
					Operation:  "bfs",
					RTT:        rtt,
					RTTMs:      float64(rtt) / float64(time.Millisecond),
					Checkpoint: checkpointIdx + 1,
					BFSStart:   vertexID,
					BFSRadius:  lgb.bfsRadius,
					Timestamp:  time.Now(),
				})
			}
			checkpointIdx++
		}
	}

	totalDuration := time.Since(startTime)

	// Calculate summary
	results := BenchmarkResults{
		Config: struct {
			NumGoroutines  int      `json:"num_goroutines"`
			NumCheckpoints int      `json:"num_checkpoints"`
			BFSVertices    []string `json:"bfs_vertices"`
			BFSRadius      int      `json:"bfs_radius"`
		}{
			NumGoroutines:  1, // Local graph is single-threaded
			NumCheckpoints: len(checkpointPositions),
			BFSVertices:    lgb.bfsVertices,
			BFSRadius:      lgb.bfsRadius,
		},
		Measurements: lgb.measurements,
	}

	// Calculate statistics
	var totalRTT time.Duration
	var minRTT, maxRTT time.Duration
	bfsCount := len(lgb.measurements)

	if len(lgb.measurements) > 0 {
		minRTT = lgb.measurements[0].RTT
		maxRTT = lgb.measurements[0].RTT
	}

	for _, m := range lgb.measurements {
		totalRTT += m.RTT
		if m.RTT < minRTT {
			minRTT = m.RTT
		}
		if m.RTT > maxRTT {
			maxRTT = m.RTT
		}
	}

	avgRTT := time.Duration(0)
	if len(lgb.measurements) > 0 {
		avgRTT = totalRTT / time.Duration(len(lgb.measurements))
	}

	results.Summary = struct {
		TotalOperations  int           `json:"total_operations"`
		TotalAddVertices int           `json:"total_add_vertices"`
		TotalAddEdges    int           `json:"total_add_edges"`
		TotalBFSQueries  int           `json:"total_bfs_queries"`
		AvgRTTMs         float64       `json:"avg_rtt_ms"`
		MinRTTMs         float64       `json:"min_rtt_ms"`
		MaxRTTMs         float64       `json:"max_rtt_ms"`
		TotalDuration    time.Duration `json:"total_duration_ns"`
		TotalDurationMs  float64       `json:"total_duration_ms"`
	}{
		TotalOperations:  totalOps,
		TotalAddVertices: 0,
		TotalAddEdges:    totalOps,
		TotalBFSQueries:  bfsCount,
		AvgRTTMs:         float64(avgRTT) / float64(time.Millisecond),
		MinRTTMs:         float64(minRTT) / float64(time.Millisecond),
		MaxRTTMs:         float64(maxRTT) / float64(time.Millisecond),
		TotalDuration:    totalDuration,
		TotalDurationMs:  float64(totalDuration) / float64(time.Millisecond),
	}

	return results
}

// buildLocalGraphFromWorkload builds a local graph from the workload file
func buildLocalGraphFromWorkload(workload []string) (graph.Graph[string, string], error) {
	g := graph.New(graph.StringHash, graph.Directed())

	operations := parseWorkload(workload)
	for _, op := range operations {
		// Add vertices if they don't exist
		_ = g.AddVertex(op.From)
		_ = g.AddVertex(op.To)

		// Add edge
		_ = g.AddEdge(op.From, op.To)
	}

	return g, nil
}

// transposeGraph creates a transposed version of the graph (reverses all edges)
func transposeGraph(g graph.Graph[string, string]) (graph.Graph[string, string], error) {
	transposed := graph.New(graph.StringHash, graph.Directed())

	// Get all vertices
	adjMap, err := g.AdjacencyMap()
	if err != nil {
		return nil, fmt.Errorf("failed to get adjacency map: %w", err)
	}

	// Add all vertices to transposed graph
	for vertex := range adjMap {
		_ = transposed.AddVertex(vertex)
	}

	// Add reversed edges
	for from, neighbors := range adjMap {
		for to := range neighbors {
			_ = transposed.AddEdge(to, from) // Reverse: to -> from
		}
	}

	return transposed, nil
}

// getTopVerticesByPageRank computes PageRank on the transposed graph and returns top V vertices
func getTopVerticesByPageRank(g graph.Graph[string, string], topV int) ([]string, error) {
	// Build adjacency map for PageRank
	adjMap, err := g.AdjacencyMap()
	if err != nil {
		return nil, fmt.Errorf("failed to get adjacency map: %w", err)
	}

	// Create a bidirectional map between string vertex IDs and integer IDs
	// The pagerank package requires integer identifiers
	vertices := make([]string, 0, len(adjMap))
	vertexToInt := make(map[string]int)
	intToVertex := make(map[int]string)
	intID := 0
	for vertex := range adjMap {
		vertices = append(vertices, vertex)
		vertexToInt[vertex] = intID
		intToVertex[intID] = vertex
		intID++
	}

	// Build the PageRank graph using integer IDs
	prGraph := pagerank.New()
	for from, neighbors := range adjMap {
		fromInt := vertexToInt[from]
		for to := range neighbors {
			toInt := vertexToInt[to]
			prGraph.Link(fromInt, toInt)
		}
	}

	// Collect PageRank scores
	type vertexScore struct {
		vertex string
		score  float64
	}
	vertexScores := make([]vertexScore, 0, len(vertices))
	var scoresLock sync.Mutex

	// Run PageRank with callback to collect scores
	// Using standard damping factor (0.85) and tolerance (0.0001)
	probability := 0.85
	tolerance := 0.0001
	prGraph.Rank(probability, tolerance, func(identifier int, rank float64) {
		scoresLock.Lock()
		defer scoresLock.Unlock()
		vertex := intToVertex[identifier]
		vertexScores = append(vertexScores, vertexScore{
			vertex: vertex,
			score:  rank,
		})
	})

	// Sort by score (descending)
	sort.Slice(vertexScores, func(i, j int) bool {
		return vertexScores[i].score > vertexScores[j].score
	})

	// Get top V vertices
	if topV > len(vertexScores) {
		topV = len(vertexScores)
	}
	topVertices := make([]string, topV)
	for i := 0; i < topV; i++ {
		topVertices[i] = vertexScores[i].vertex
		log.Printf("Top vertex %d: %s (PageRank: %.6f)", i+1, vertexScores[i].vertex, vertexScores[i].score)
	}

	return topVertices, nil
}
