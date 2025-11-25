package main

import (
	"context"
	"fmt"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pjavanrood/tinygraph/internal/config"
	"github.com/pjavanrood/tinygraph/internal/types"
	rpcTypes "github.com/pjavanrood/tinygraph/pkg/rpc"
	"golang.org/x/time/rate"
)

// ParallelBenchmarkClient handles parallel ingestion
type ParallelBenchmarkClient struct {
	cfg              *config.Config
	conn             *rpc.Client
	vertexIDMap      sync.Map // external vertex ID -> internal vertex ID
	measurements     []Measurement
	measurementsLock sync.Mutex
	numGoroutines    int
	numCheckpoints   int
	bfsVertices      []string
	bfsRadius        int
	totalOps         int64
	checkpointOps    []int64       // Operations completed at each checkpoint
	rateLimiter      *rate.Limiter // Rate limiter for operations
}

func NewParallelBenchmarkClient(cfg *config.Config, numGoroutines, numCheckpoints int, bfsVertices []string, bfsRadius int, rateLimit float64) *ParallelBenchmarkClient {
	conn, err := rpc.Dial("tcp", fmt.Sprintf("%s:%d", cfg.QueryManager.Host, cfg.QueryManager.Port))
	if err != nil {
		log.Fatalf("Failed to connect to query manager: %v", err)
	}

	// Create rate limiter if rate limit is specified
	var limiter *rate.Limiter
	if rateLimit > 0 {
		// Allow some burst capacity (e.g., 2x the per-second rate)
		limiter = rate.NewLimiter(rate.Limit(rateLimit), int(rateLimit*2))
	}

	return &ParallelBenchmarkClient{
		cfg:            cfg,
		conn:           conn,
		measurements:   make([]Measurement, 0),
		numGoroutines:  numGoroutines,
		numCheckpoints: numCheckpoints,
		bfsVertices:    bfsVertices,
		bfsRadius:      bfsRadius,
		checkpointOps:  make([]int64, numCheckpoints),
		rateLimiter:    limiter,
	}
}

func (pbc *ParallelBenchmarkClient) recordMeasurement(m Measurement) {
	pbc.measurementsLock.Lock()
	defer pbc.measurementsLock.Unlock()
	pbc.measurements = append(pbc.measurements, m)
}

func (pbc *ParallelBenchmarkClient) getOrAddVertex(externalID string) (types.VertexId, error) {
	// Check if vertex already exists
	if val, ok := pbc.vertexIDMap.Load(externalID); ok {
		return val.(types.VertexId), nil
	}

	// Try to add vertex
	start := time.Now()
	properties := types.Properties{
		"external_id": externalID,
	}
	var resp rpcTypes.AddVertexResponse
	err := pbc.conn.Call("QueryManager.AddVertex", &rpcTypes.AddVertexRequest{
		Properties: properties,
	}, &resp)
	rtt := time.Since(start)

	if err != nil {
		return "", err
	}

	// Store the vertex ID (use LoadOrStore to handle race condition)
	actual, _ := pbc.vertexIDMap.LoadOrStore(externalID, resp.VertexID)

	// Record measurement
	pbc.recordMeasurement(Measurement{
		Operation:  "add_vertex",
		RTT:        rtt,
		RTTMs:      float64(rtt) / float64(time.Millisecond),
		Checkpoint: 0,
		VertexFrom: externalID,
		Timestamp:  time.Now(),
	})

	return actual.(types.VertexId), nil
}

func (pbc *ParallelBenchmarkClient) addEdge(fromVertexID, toVertexID string, weight int) error {
	// Get or add vertices
	fromInternal, err := pbc.getOrAddVertex(fromVertexID)
	if err != nil {
		return fmt.Errorf("failed to get/add from vertex: %w", err)
	}

	toInternal, err := pbc.getOrAddVertex(toVertexID)
	if err != nil {
		return fmt.Errorf("failed to get/add to vertex: %w", err)
	}

	// Add edge
	start := time.Now()
	properties := map[string]string{
		"weight": strconv.Itoa(weight),
	}
	var resp rpcTypes.AddEdgeResponse
	err = pbc.conn.Call("QueryManager.AddEdge", &rpcTypes.AddEdgeRequest{
		FromVertexID: fromInternal,
		ToVertexID:   toInternal,
		Properties:   properties,
	}, &resp)
	rtt := time.Since(start)

	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("add edge failed")
	}

	// Record measurement
	pbc.recordMeasurement(Measurement{
		Operation:  "add_edge",
		RTT:        rtt,
		RTTMs:      float64(rtt) / float64(time.Millisecond),
		Checkpoint: 0,
		VertexFrom: fromVertexID,
		VertexTo:   toVertexID,
		Timestamp:  time.Now(),
	})

	atomic.AddInt64(&pbc.totalOps, 1)
	return nil
}

func (pbc *ParallelBenchmarkClient) runBFSQueries(checkpointNum int) {
	log.Printf("Running BFS queries at checkpoint %d", checkpointNum)

	for _, vertexID := range pbc.bfsVertices {
		// Get internal vertex ID
		val, ok := pbc.vertexIDMap.Load(vertexID)
		if !ok {
			log.Printf("Warning: Vertex %s not found, skipping BFS", vertexID)
			continue
		}

		startVertexID := val.(types.VertexId)

		// Run BFS
		start := time.Now()
		var resp rpcTypes.BFSResponse
		err := pbc.conn.Call("QueryManager.BFS", &rpcTypes.BFSRequest{
			StartVertexID: startVertexID,
			Radius:        pbc.bfsRadius,
			Timestamp:     types.Timestamp(float64(time.Now().Unix())),
		}, &resp)
		rtt := time.Since(start)

		if err != nil {
			log.Printf("Error running BFS for vertex %s: %v", vertexID, err)
			continue
		}

		// Record measurement
		pbc.recordMeasurement(Measurement{
			Operation:  "bfs",
			RTT:        rtt,
			RTTMs:      float64(rtt) / float64(time.Millisecond),
			Checkpoint: checkpointNum,
			BFSStart:   vertexID,
			BFSRadius:  pbc.bfsRadius,
			Timestamp:  time.Now(),
		})
	}
}

func (pbc *ParallelBenchmarkClient) Run(workload []string) BenchmarkResults {
	startTime := time.Now()

	// Parse workload into operations
	type Op struct {
		From   string
		To     string
		Weight int
	}
	operations := make([]Op, 0)

	for _, line := range workload {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Split(line, " ")
		if len(fields) < 2 || fields[0] == "#" {
			continue // Skip comments
		}

		weight := 0
		if len(fields) == 3 {
			var err error
			weight, err = strconv.Atoi(fields[2])
			if err != nil {
				log.Printf("Warning: Invalid weight in line: %s", line)
				weight = 0
			}
		}
		operations = append(operations, Op{
			From:   fields[0],
			To:     fields[1],
			Weight: weight,
		})
	}

	totalOps := len(operations)
	log.Printf("Total operations to process: %d", totalOps)

	// Calculate checkpoint positions
	checkpointPositions := make([]int, pbc.numCheckpoints)
	for i := 0; i < pbc.numCheckpoints; i++ {
		checkpointPositions[i] = (i + 1) * totalOps / (pbc.numCheckpoints + 1)
	}
	log.Printf("Checkpoint positions: %v", checkpointPositions)

	// Channel for operations
	opChan := make(chan Op, pbc.numGoroutines*10)
	var wg sync.WaitGroup
	var opsCompleted int64

	// Start worker goroutines
	for i := 0; i < pbc.numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for op := range opChan {
				err := pbc.addEdge(op.From, op.To, op.Weight)
				if err != nil {
					log.Printf("Error adding edge %s -> %s: %v", op.From, op.To, err)
				}
				atomic.AddInt64(&opsCompleted, 1)
			}
		}()
	}

	// Send operations and handle checkpoints
	currentOp := 0
	checkpointIdx := 0

	// Send operations in batches, pausing at checkpoints
	for _, op := range operations {
		// Rate limiting: wait for token from rate limiter
		if pbc.rateLimiter != nil {
			err := pbc.rateLimiter.Wait(context.Background())
			if err != nil {
				log.Printf("Rate limiter error: %v", err)
			}
		}

		opChan <- op
		currentOp++

		// Check if we've reached a checkpoint
		if checkpointIdx < len(checkpointPositions) && currentOp >= checkpointPositions[checkpointIdx] {
			// Pause sending new operations
			log.Printf("Checkpoint %d reached at operation %d. Pausing to run BFS queries...", checkpointIdx+1, currentOp)

			// Wait for all pending operations to complete
			// We wait until the number of completed operations matches what we've sent
			for atomic.LoadInt64(&opsCompleted) < int64(currentOp) {
				time.Sleep(50 * time.Millisecond)
			}
			// Give a bit more time for any in-flight operations
			time.Sleep(200 * time.Millisecond)

			// Run BFS queries
			pbc.runBFSQueries(checkpointIdx + 1)
			pbc.checkpointOps[checkpointIdx] = int64(currentOp)
			checkpointIdx++

			log.Printf("Checkpoint %d completed. Resuming operations...", checkpointIdx)
		}
	}

	// Close channel and wait for remaining workers to finish
	close(opChan)
	wg.Wait()

	totalDuration := time.Since(startTime)

	// Calculate summary
	results := BenchmarkResults{
		Config: struct {
			NumGoroutines  int      `json:"num_goroutines"`
			NumCheckpoints int      `json:"num_checkpoints"`
			BFSVertices    []string `json:"bfs_vertices"`
			BFSRadius      int      `json:"bfs_radius"`
		}{
			NumGoroutines:  pbc.numGoroutines,
			NumCheckpoints: pbc.numCheckpoints,
			BFSVertices:    pbc.bfsVertices,
			BFSRadius:      pbc.bfsRadius,
		},
		Measurements: pbc.measurements,
	}

	// Calculate statistics
	var totalRTT time.Duration
	var minRTT, maxRTT time.Duration
	var addVertexCount, addEdgeCount, bfsCount int

	if len(pbc.measurements) > 0 {
		minRTT = pbc.measurements[0].RTT
		maxRTT = pbc.measurements[0].RTT
	}

	for _, m := range pbc.measurements {
		totalRTT += m.RTT
		if m.RTT < minRTT {
			minRTT = m.RTT
		}
		if m.RTT > maxRTT {
			maxRTT = m.RTT
		}
		switch m.Operation {
		case "add_vertex":
			addVertexCount++
		case "add_edge":
			addEdgeCount++
		case "bfs":
			bfsCount++
		}
	}

	avgRTT := totalRTT / time.Duration(len(pbc.measurements))

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
		TotalOperations:  len(pbc.measurements),
		TotalAddVertices: addVertexCount,
		TotalAddEdges:    addEdgeCount,
		TotalBFSQueries:  bfsCount,
		AvgRTTMs:         float64(avgRTT) / float64(time.Millisecond),
		MinRTTMs:         float64(minRTT) / float64(time.Millisecond),
		MaxRTTMs:         float64(maxRTT) / float64(time.Millisecond),
		TotalDuration:    totalDuration,
		TotalDurationMs:  float64(totalDuration) / float64(time.Millisecond),
	}

	return results
}
