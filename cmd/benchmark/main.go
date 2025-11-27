package main

import (
	"encoding/json"
	"flag"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pjavanrood/tinygraph/internal/config"
	"github.com/pjavanrood/tinygraph/internal/util"
)

var log = util.New("Benchmark", util.LogLevelInfo)

// Measurement represents a single RTT measurement
type Measurement struct {
	Operation  string        `json:"operation"`  // "add_vertex", "add_edge", "bfs"
	RTT        time.Duration `json:"rtt_ns"`     // RTT in nanoseconds
	RTTMs      float64       `json:"rtt_ms"`     // RTT in milliseconds (for readability)
	Checkpoint int           `json:"checkpoint"` // Checkpoint number (0 = no checkpoint, 1..K = checkpoint number)
	VertexFrom string        `json:"vertex_from,omitempty"`
	VertexTo   string        `json:"vertex_to,omitempty"`
	BFSStart   string        `json:"bfs_start,omitempty"`
	BFSRadius  int           `json:"bfs_radius,omitempty"`
	BFSResult  []string      `json:"bfs_result,omitempty"` // Vertices found by BFS
	Timestamp  time.Time     `json:"timestamp"`
}

// BenchmarkResults contains all measurements
type BenchmarkResults struct {
	Config struct {
		NumGoroutines  int      `json:"num_goroutines"`
		NumCheckpoints int      `json:"num_checkpoints"`
		BFSVertices    []string `json:"bfs_vertices"`
		BFSRadius      int      `json:"bfs_radius"`
	} `json:"config"`
	Measurements []Measurement `json:"measurements"`
	Summary      struct {
		TotalOperations  int           `json:"total_operations"`
		TotalAddVertices int           `json:"total_add_vertices"`
		TotalAddEdges    int           `json:"total_add_edges"`
		TotalBFSQueries  int           `json:"total_bfs_queries"`
		AvgRTTMs         float64       `json:"avg_rtt_ms"`
		MinRTTMs         float64       `json:"min_rtt_ms"`
		MaxRTTMs         float64       `json:"max_rtt_ms"`
		TotalDuration    time.Duration `json:"total_duration_ns"`
		TotalDurationMs  float64       `json:"total_duration_ms"`
	} `json:"summary"`
}

// Operation represents a single graph operation from the workload
type Operation struct {
	From   string
	To     string
	Weight int
}

// parseWorkload parses the workload file content into a slice of operations
func parseWorkload(workload []string) []Operation {
	operations := make([]Operation, 0)

	for _, line := range workload {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
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
		operations = append(operations, Operation{
			From:   fields[0],
			To:     fields[1],
			Weight: weight,
		})
	}

	return operations
}

func main() {
	// Parse command-line flags
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	workloadPath := flag.String("workload", "data/simple_graph.txt", "Path to workload file")
	numGoroutines := flag.Int("goroutines", 4, "Number of parallel goroutines")
	numCheckpoints := flag.Int("checkpoints", 3, "Number of checkpoints")
	topVertices := flag.Int("top-vertices", 5, "Number of top vertices to select by PageRank for BFS queries")
	bfsRadius := flag.Int("bfs-radius", 1, "Radius for BFS queries")
	rateLimit := flag.Float64("rate-limit", 0, "Operations per second rate limit (0 = unlimited)")
	outputParallel := flag.String("output-parallel", "benchmark_parallel.json", "Output file for parallel benchmark results")
	outputLocal := flag.String("output-local", "benchmark_local.json", "Output file for local graph benchmark results")
	runMode := flag.String("mode", "both", "Benchmark mode: parallel, local, or both")
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	cfg.Logging.Level = "INFO" // Force INFO level for benchmark

	// Update log level from config
	log.SetLevel(cfg.GetLogLevel())

	// Read workload
	workloadData, err := os.ReadFile(*workloadPath)
	if err != nil {
		log.Fatalf("Failed to read workload file: %v", err)
	}
	workload := strings.Split(string(workloadData), "\n")

	// Build local graph from workload
	log.Printf("Building local graph from workload...")
	localGraph, err := buildLocalGraphFromWorkload(workload)
	if err != nil {
		log.Fatalf("Failed to build local graph: %v", err)
	}

	// Transpose the graph
	log.Printf("Transposing graph...")
	transposedGraph, err := transposeGraph(localGraph)
	if err != nil {
		log.Fatalf("Failed to transpose graph: %v", err)
	}

	// Compute PageRank and get top V vertices
	log.Printf("Computing PageRank on transposed graph to find top %d vertices...", *topVertices)
	bfsVertices, err := getTopVerticesByPageRank(transposedGraph, *topVertices)
	if err != nil {
		log.Fatalf("Failed to compute PageRank: %v", err)
	}

	if len(bfsVertices) == 0 {
		log.Fatalf("No vertices found for BFS queries")
	}

	mode := strings.ToLower(strings.TrimSpace(*runMode))
	runParallel := false
	runLocal := false
	switch mode {
	case "parallel":
		runParallel = true
	case "local":
		runLocal = true
	case "both", "":
		runParallel = true
		runLocal = true
	default:
		log.Fatalf("Invalid mode %q. Must be one of: parallel, local, both", *runMode)
	}
	log.Printf("Benchmark mode: %s (parallel=%t, local=%t)", mode, runParallel, runLocal)

	if runParallel {
		log.Printf("Starting parallel benchmark with %d goroutines, %d checkpoints", *numGoroutines, *numCheckpoints)
		log.Printf("BFS radius: %d", *bfsRadius)
		if *rateLimit > 0 {
			log.Printf("Rate limit: %.2f ops/sec", *rateLimit)
		} else {
			log.Printf("Rate limit: unlimited")
		}

		parallelClient := NewParallelBenchmarkClient(cfg, *numGoroutines, *numCheckpoints, bfsVertices, *bfsRadius, *rateLimit)
		defer parallelClient.Close()

		parallelResults := parallelClient.Run(workload)

		parallelJSON, err := json.MarshalIndent(parallelResults, "", "  ")
		if err != nil {
			log.Fatalf("Failed to marshal parallel results: %v", err)
		}
		if err := os.WriteFile(*outputParallel, parallelJSON, 0644); err != nil {
			log.Fatalf("Failed to write parallel results: %v", err)
		}
		log.Printf("Parallel benchmark results written to %s", *outputParallel)
	}

	if runLocal {
		// Calculate checkpoint positions for local graph (same as parallel)
		operations := parseWorkload(workload)
		totalOps := len(operations)

		checkpointPositions := make([]int, *numCheckpoints)
		for i := 0; i < *numCheckpoints; i++ {
			checkpointPositions[i] = (i + 1) * totalOps / (*numCheckpoints + 1)
		}
		log.Printf("Checkpoint positions: %v (total operations: %d)", checkpointPositions, totalOps)

		log.Printf("Starting local graph benchmark")
		localBenchmark := NewLocalGraphBenchmark(bfsVertices, *bfsRadius)
		localResults := localBenchmark.Run(workload, checkpointPositions)

		localJSON, err := json.MarshalIndent(localResults, "", "  ")
		if err != nil {
			log.Fatalf("Failed to marshal local results: %v", err)
		}
		if err := os.WriteFile(*outputLocal, localJSON, 0644); err != nil {
			log.Fatalf("Failed to write local results: %v", err)
		}
		log.Printf("Local graph benchmark results written to %s", *outputLocal)
	}

	if !runParallel && !runLocal {
		log.Printf("Nothing to run. Exiting.")
	}

	log.Printf("Benchmark completed!")
}
