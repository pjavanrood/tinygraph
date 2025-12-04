# Milestone 3

This milestone focused on two main deliverables: (1) an optimized distributed BFS implementation that uses shard-to-shard communication to reduce latency and improve scalability, and (2) a comprehensive benchmarking client that evaluates BFS performance by comparing the distributed implementation against a local baseline across multiple checkpoints.

## Additional Internal Updates (Unrelated to BFS)

### 1. MVCC Write-Path Safety Fix
**Files:**  
- `pkg/mvcc/vertex.go`  
- `pkg/mvcc/edge.go`

**Description:**  
- Fixed an issue where `UpdateVertex` returned the previous version instead of the newly created one.  
- Added a mutex *only around write operations* (`UpdateEdge`, `UpdateVertex`, `AddEdge`, `DeleteEdge`) to avoid unsafe concurrent modifications.  
  Go slices/maps are not safe under concurrent writes, and the proposal’s CAS-based design does not directly translate to Go.  
- **Read operations remain lock-free**, since they only traverse immutable historical versions.  
- If this assumption is incorrect or if single-writer behavior is enforced elsewhere, the mutex can be removed.

### 2. LDG-Style Partitioner
**File:**  
- `pkg/qm/partitioner.go`

**Description:**  
- Added a lightweight LDG-style partitioner that selects the shard with the lowest current load (based on vertex count).  
- Provides more balanced shard assignment compared to the previous random partitioning strategy.


## Overview

The optimized BFS uses a distributed, shard-to-shard communication pattern where shards coordinate directly with each other, reducing the Query Manager's (QM) role to initialization and result aggregation.

## Naive BFS Implementation

Before implementing the optimized version, the system used a simple sequential BFS approach (`pkg/qm/query_manager.go:naiveBFS`). This implementation:

**How it works**:
1. Maintains a queue of vertices to process, starting with the start vertex
2. Processes vertices **sequentially** one at a time
3. For each vertex:
   - Extracts the shard ID from the vertex ID
   - Makes a **synchronous RPC call** to the shard to get neighbors
   - Adds unvisited neighbors to the queue with incremented level
4. Continues until the queue is empty or the radius limit is reached

**Limitations**:
- **Sequential processing**: Each vertex is processed one at a time, blocking on each RPC call
- **No parallelism**: Cannot explore multiple vertices simultaneously across different shards
- **High latency**: Network round-trips accumulate linearly, resulting in poor performance for large graphs
- **Query Manager bottleneck**: All coordination happens through the QM, which must wait for each shard response before proceeding

The naive implementation serves as a baseline for comparison, demonstrating the performance improvements achieved by the optimized distributed approach.

## Key Components

### Query Manager (QM)
- **Role**: Initiates BFS, tracks completion, aggregates results
- **Key Functions**:
  - `pkg/qm/query_manager.go:ShardedBFS` - Initiaties the BFS request
  - `pkg/qm/query_manager.go:BFSResponse` - Updates the local BFS request state with information from shards
  - `pkg/qm/query_manager.go:BFS` - RPC entry point

### Shards
- **Role**: Perform local BFS traversal, dispatch BFS calls to other shards
- **Key Functions**:
  - `pkg/shard/shard_fsm.go:BFS` - Manages BFS call state and dispatches internal BFS resolution
  - `pkg/shard/shard.go:bfs` - Performs BFS on local nodes, and dispatches BFS to other shards when edges to other shards are found. Calls into the QueryManager to report with the results and which calls it made that the QM must await.

## BFS Request Flow

### Step 1: Client Request → QM
**Function**: `pkg/qm/query_manager.go:BFS`

- Client sends BFS request with `StartVertexID`, `Radius`, and `Timestamp`
- QM checks config: if `BFSType == "optimized"`, calls `distributedBFS`

### Step 2: QM Initializes BFS State
**Function**: `pkg/qm/query_manager.go:ShardedBFS`

**QM Actions**:
1. Generates unique `BFSId` (stored as a monotonically increasing number, but can be arbitrary as long as it is unique across the system)
2. Creates `BFSManager` to track:
   - `Vertices`: map of discovered vertices
   - `DispatchedRequests`: map tracking the number of requests in flight for a given shard
   - `FirstRecvd`: ensures that this value is set to true only when the initial request is returned (if we do not explicitly wait for the first request, there is a race condition where we early-out because we receive a response from the same shard, but not of the original request, ie qm->shard1->shard2->shard1, where the second request in the chain returns before the first does)
   - `Done`: the channel on which we signal the end of the distributed BFS algorithm 
3. Extracts shard ID from `StartVertexID` (format: `{shardID}-{randomHex}`)
4. Registers state in `managers` map
5. Initializes pending state for starting shard: `DispatchedRequests[shardId] = 1`

### Step 3: QM → Starting Shard (Synchronous)
**Function**: `pkg/shard/shard_fsm.go:BFS`

**QM sends**:
- `BFSToShardRequest` with start vertex, radius, timestamp, BFSId, QM address, and the `FirstReq` value to true.

**Shard receives and**:
1. Checks if the vertex exists in the graph on (a) this machine, and (b) at the given timestamp
2. Finds or creates a new BFSInstance corresponding to the BFSId
3. asynchronously calls `ShardFSM.bfs`, and returns from the RPC call

### Step 4: Shard Performs Local BFS & Dispatches BFS Across Shards
**Function**: `pkg/shard/shard_fsm.go:bfs`

**Shard Actions**:
1. **Deduplication and Race Conditions**: Fetches the corresponding BFSInstance and locks it so that it has exclusive RW access.
2. **Local BFS Traversal**:
   - Processes vertices in queue using BFS
   - For each vertex:
     - Skips if already visited in the BFSInstance
     - Checks if it exists at the given timestamp, and fetches it's data
     - Marks as "locally visited", and adds to BFSInstance's Visited map
     - If the "current level" is 0, continues.
     - Otherwise, iterates over all it's neighbours
     - If the neighbour is found locally, it is added to the queue with "level" N-1, s.t. N is the "level" of the current vertex
     - If the neighbour is not found locally, an asynchronous function is called that will 
       1. Find which shard it is located on
       2. Send a BFS request with the radius being set to "level" N-1, and root being the remote vertex.
       3. If the RPC call returns with no error, we add the vertex to the BFSInstance's Visited map (so we don't send out another RPC for this same vertex)
     - Once the BFS has finished running, we wait for all the async functions to finish sending out their requests so we can tally up how many requests we sent to which shards (`dispatchedRequests`). This helps us inform the QM on what information it needs to wait for before it can proceed (it expects to receive all the messages this shard sent out). An RPC to the QM is made with `QueryManager.BFSResponse` to notify that it has completed it's BFS traversal.

### Step 5: QM Processes Responses & Collects Results
**Function**: `pkg/qm/query_manager.go:BFSResponse`

**QM Actions**:
1. **Up-called**: Gets up-called into by a shard, specifying which BFSId it is providing information for.
2. **Updates pending counts**: Decrements the calling shard's entry in the BFSManager's `DispatchedRequests` by 1 to symbolize acknowledging and expected BFS traversal by the calling shard. Then, Receives information about calls dispatched by the calling shard, and updates the BFSManager's `DispatchedRequests` to reflect any additional RPCs it must wait on based on the calling shard's BFS traversal.
3. **Updates visited map**: Inserts every vertex reached by the calling shard's BFS to the BFSManager's `Vertices` field.
4. **Checks for algorithm termination**: If every shard of the BFSManager's `DispatchedRequests` reaches 0, and the first request has been responded to, we send a message on the `Done` channel to signal termination.

### Step 9: QM Completes and Returns
**Function**: `pkg/qm/query_manager.go:ShardedBFS` (lines 701-728)

**QM Actions**:
1. Waits on `Done` channel
2. When complete, creates a slice of vertices from the `BFSManager.Vertex` map, and puts it in the response
3. Cleans up the corresponding `BFSManager`
4. Returns result to client via `BFS` handler

## Key Design Features

### Deduplication
- Each shard maintains `BFSInstance` per `BFSId` to avoid running BFS on already-visited vertices
- QM tracks visited vertices globally to prevent duplicates in final result

### Asynchronous Communication
- Shard-to-shard requests are async (fire-and-forget)
- Shard-to-QM responses are async
- Reduces latency by not blocking on network calls
- QM awaits responses from all asynchronous Shard calls before retuning results

### Connection Pooling
- Shards maintain connection pools per target shard
- Reduces connection overhead
- Automatically re-opens closed connections until a connection is established

### Completion Tracking
- QM tracks the total number of dispatched BFS calls based on responses from shards
- BFS completes when all shards have 0 in-flight BFS calls, verified by explicitly awaiting the reception of the initial BFS call to ensure all calls are accounted for

### Level Tracking
- Decrements the radius for BFS calls on each step (local or remote) ensuring we only visit the nodes within the radius of the original call
- Ensures radius limits are correctly enforced

## Example Flow Diagram

```
Client → QM.BFS
         ↓
    QM.ShardedBFS (creates BFSId, BFSManager)
         ↓
    QM → Shard 0: BFS(v, N, ts, BFSId) [async]
         ↓
    Shard 0: BFS
         ├─→ Finds local vertices
         ├─→ Dispatches BFS call on remote vertices
         └─→ (asynchronously) Returns: vertices + DispatchedRequests
         ↓
    Shard 0 → QM: BFSResponse (async response)
         ↓
    QM: BFSResponse (updates state)
         ↓
    Shard 0 → Shard 1: BFS(v', N-1, ts, BFSId) [async]
    Shard 0 → Shard 2: BFS(v'', N-1, ts, BFSId) [async]
         ↓
    Shard 1: BFS → QM: BFSResponse (async)
    Shard 2: BFS → QM: BFSResponse (async)
         ↓
    QM: BFSResponse (multiple times, updates state)
         ↓
    When first received and all Received == Expected: Done <- nil
         ↓
    QM.ShardedBFS returns result
         ↓
    QM.BFS returns to client
```

## Benchmarking

### Benchmark Architecture

The benchmark suite (`cmd/benchmark/`) evaluates BFS performance by comparing the optimized distributed implementation against a local single-threaded baseline. The benchmark uses a realistic workload pattern with parallel ingestion and periodic query checkpoints.

#### Key Components

**Functions**:
- `cmd/benchmark/main.go:main` - Entry point, orchestrates benchmark execution
- `cmd/benchmark/local.go:buildLocalGraphFromWorkload` - Builds local graph representation
- `cmd/benchmark/local.go:transposeGraph` - Creates transposed graph for PageRank
- `cmd/benchmark/local.go:getTopVerticesByPageRank` - Computes PageRank to select BFS start vertices
- `cmd/benchmark/remote.go:ParallelBenchmarkClient` - Handles parallel distributed benchmark
- `cmd/benchmark/local.go:LocalGraphBenchmark` - Handles local single-threaded benchmark

#### Benchmark Process

1. **Graph Construction** (`cmd/benchmark/local.go:buildLocalGraphFromWorkload`):
   - Parses workload file (edge list format: `from to [weight]`)
   - Builds local in-memory graph representation

2. **BFS Start Vertex Selection** (`cmd/benchmark/local.go:getTopVerticesByPageRank`):
   - Transposes the graph (reverses all edges)
   - Computes PageRank on the transposed graph
   - Selects top V vertices by PageRank score as BFS start vertices
   - **Rationale**: High PageRank in transposed graph indicates vertices with many incoming edges, making them good candidates for exploring graph connectivity

3. **Parallel Ingestion** (`cmd/benchmark/remote.go:ParallelBenchmarkClient.Run`):
   - Uses multiple goroutines (configurable via `-goroutines` flag) to ingest edges in parallel
   - Each goroutine maintains its own RPC connection to the Query Manager
   - Operations are distributed across shards based on vertex ID partitioning
   - Optional rate limiting via token bucket algorithm

4. **Checkpoint-Based Querying** (`cmd/benchmark/remote.go:ParallelBenchmarkClient.runBFSQueries`):
   - Divides workload into N+1 segments (N = number of checkpoints)
   - At each checkpoint position `(i+1) * totalOps / (N+1)`:
     - Pauses new edge ingestion
     - Waits for all pending operations to complete
     - Runs BFS queries from all selected start vertices
     - Records latency measurements for each query
   - Resumes ingestion after queries complete

5. **Local Baseline** (`cmd/benchmark/local.go:LocalGraphBenchmark.Run`):
   - Single-threaded sequential edge ingestion
   - Same checkpoint positions as parallel benchmark
   - Performs local BFS at each checkpoint for comparison

#### Benchmark Command

```bash
go run cmd/benchmark/*.go \
  -config 3_shard_3_replica_config.yaml \
  -workload cmd/client/workloads/large_rmat_copy.txt \
  -goroutines 10 \
  -checkpoints 5 \
  -bfs-radius 10 \
  > logs/benchmark.log 2>&1
```

**Parameters**:
- `-config`: Cluster configuration file
- `-workload`: Graph workload file (edge list)
- `-goroutines`: Number of parallel ingestion threads
- `-checkpoints`: Number of checkpoint intervals
- `-bfs-radius`: Maximum BFS traversal depth
- `-top-vertices`: Number of top PageRank vertices to use as BFS starts (default: 5)

**Output Files**:
- `benchmark_parallel_optimized.json`: Results from distributed optimized BFS
- `benchmark_parallel_naive.json`: Results from distributed naive BFS (if run)
- `benchmark_local.json`: Results from local single-threaded baseline

### Benchmark Results

The benchmark results are analyzed in `notebooks/bfs_performance_comparison.ipynb`, which provides:

#### Correctness Validation

- **Result Matching**: The notebook verifies that BFS results match between local and parallel implementations for all `(checkpoint, bfs_start)` pairs
- **Result Sizes**: Tracks the number of vertices discovered by each BFS query
- **Consistency**: Ensures distributed implementation produces identical results to local baseline

#### Performance Metrics

**Latency Measurements**:
- **RTT (Round-Trip Time)**: End-to-end latency for each BFS query in milliseconds
- **Checkpoint Progression**: Latency trends as graph grows over checkpoints
- **Start Vertex Variation**: Latency differences across different start vertices

**Statistical Summary**:
- Mean, median, min, max latencies per dataset
- Comparison between local, parallel optimized, and parallel naive implementations
- Percentage difference calculations between implementations

#### Visualizations

The notebook (`notebooks/bfs_performance_comparison.ipynb`) generates and saves the following visualizations:

1. **BFS Latency vs Checkpointnotebooks/** (`bfs_latency_vs_checkpoint.png`):
   - Line plot showing BFS latency progression across checkpoints
   - Log scale Y-axis to handle wide latency ranges
   - Separate lines for each dataset (local, parallel optimized, parallel naive)
   - **Insight**: Shows how latency scales as graph size increases
   - ![BFS Latency vs Checkpoint](notebooks/bfs_latency_vs_checkpoint.png)

2. **BFS Latency by Start Vertex** (`notebooks/bfs_latency_by_start_vertex.png`):
   - Scatter plot of latency vs start vertex ID
   - Log scale Y-axis
   - **Insight**: Reveals which vertices produce faster/slower BFS queries (may correlate with vertex degree, shard distribution, etc.)
   - ![BFS Latency by Start Vertex](notebooks/bfs_latency_by_start_vertex.png)

3. **Checkpoint Comparison Table** (`notebooks/checkpoint_comparison.csv`):
   <!-- - Pivot table comparing latencies across checkpoints
   - Percentage difference calculations (parallel vs local)
   - **Insight**: Quantifies performance overhead of distributed implementation
   - Saved as CSV for detailed analysis -->

   | Checkpoint | Local (ms) | Parallel Optimized (ms) | Parallel Naive (ms) | Parallel vs Local (%) |
   |------------|------------|-------------------------|---------------------|----------------------|
   | 1          | 0.199      | 4.0                     | 298.0               | 1911.5               |
   | 2          | 0.305      | 5.0                     | 366.0               | 1539.0               |
   | 3          | 0.385      | 5.2                     | 394.2               | 1252.2               |
   | 4          | 0.654      | 6.4                     | 419.0               | 878.6                |
   | 5          | 0.724      | 8.2                     | 431.6               | 1032.6               |

   **Observations**:
   - Optimized parallel implementation shows ~4-8ms latency vs ~0.2-0.7ms for local
   - Parallel naive implementation is significantly slower (~300-430ms)
   - Optimized implementation provides ~50-70x speedup over naive parallel approach
   - Latency increases with checkpoint number as graph grows

#### Key Findings

From the benchmark analysis:

- **Correctness**: Distributed BFS produces identical results to local baseline, validating the implementation
- **Latency Characteristics**: 
  - Parallel implementation shows higher latency due to network overhead and coordination
  - Latency increases with checkpoint number (larger graph = more vertices to traverse)
  - Some start vertices produce consistently faster queries (likely due to local shard distribution)
- **Scalability**: The optimized implementation maintains reasonable latency even as graph size grows across checkpoints

