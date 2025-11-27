# Distributed BFS Algorithm Design

## Problem
Current approach requires R communication rounds (one per BFS level), which is inefficient when there are many cross-shard edges.

## Key Architectural Insight
**Shards can communicate directly with each other!** They don't need QM as a middleman for cross-shard operations because:
- ShardID is encoded in vertexID format (`shardID-randomHex`)
- Shards have access to config and can discover other shards
- This allows shards to handle cross-shard coordination directly, dramatically reducing QM involvement

## Proposed Algorithm: Fully Distributed BFS with Direct Shard Communication

### Key Insight
**Shards handle cross-shard coordination directly**, eliminating QM as a bottleneck:
- QM only initiates BFS and collects final results
- Shards perform local BFS and directly contact other shards for cross-shard edges
- Shards pass original requestID and QM address, so results flow back to QM
- This reduces QM-Shard communication from O(R) rounds to O(1) (initiation + result collection)

### Algorithm Pseudo-code (QM Perspective)

```
// QM initiates BFS and waits for results
QM_BFS(startVertex, radius, timestamp):
    requestID = GENERATE_UNIQUE_ID()
    
    // Find starting shard
    startShardID = GET_SHARD_ID(startVertex)
    startShard = GET_SHARD_CONFIG(startShardID)
    
    // Send BFS request to starting shard
    // Pass QM address as requester so results come back to QM
    SEND_TO_SHARD(startShard, BFSRequest(
        startVertex: startVertex,
        radius: radius,
        timestamp: timestamp,
        requestID: requestID,
        requesterAddress: QM_ADDRESS
    ))
    
    // Track which shards we're waiting for
    waitingFor = {startShardID}
    collectedResults = {}  // vertex -> level
    
    // Wait for all shards to respond
    WHILE waitingFor is not empty:
        response = RECEIVE_FROM_SHARD()
        shardID = response.shardID
        vertices = response.vertices  // vertices found by this shard
        
        // Add results
        FOR EACH (vertex, level) IN vertices:
            collectedResults[vertex] = level
        
        // Update waiting set
        waitingFor.remove(shardID)
        waitingFor.addAll(response.pendingShards)  // shards this shard contacted
    
    RETURN collectedResults
```

### Algorithm Pseudo-code (Shard Perspective)

```
// Shard receives BFS request and handles it locally + cross-shard
SHARD_BFS(request):
    requestID = request.requestID
    requesterAddress = request.requesterAddress  // QM address
    startVertex = request.startVertex
    radius = request.radius
    timestamp = request.timestamp
    
    // Check if we've seen this request before (deduplication)
    IF requestID IN activeRequests:
        sharedVisited = activeRequests[requestID].visited
    ELSE:
        sharedVisited = {}
        activeRequests[requestID] = {visited: sharedVisited, pendingShards: set()}
    
    // Perform local BFS from startVertex
    localVisited = {}
    localQueue = [(startVertex, 0)]  // (vertex, level)
    crossShardRequests = {}  // targetShardID -> [(vertex, level), ...]
    
    WHILE localQueue is not empty:
        (currentVertex, level) = localQueue.dequeue()
        
        IF currentVertex IN sharedVisited:
            CONTINUE  // Already visited in this BFS
        
        IF level > radius:
            CONTINUE
        
        // Mark as visited
        sharedVisited[currentVertex] = level
        localVisited[currentVertex] = level
        
        // Get neighbors
        neighbors = GET_NEIGHBORS(currentVertex, timestamp)
        
        FOR EACH neighbor IN neighbors:
            neighborShardID = GET_SHARD_ID(neighbor)
            
            IF neighborShardID == CURRENT_SHARD_ID:
                // Local neighbor, continue BFS
                IF neighbor NOT IN sharedVisited:
                    localQueue.append((neighbor, level + 1))
            ELSE:
                // Cross-shard edge - contact target shard directly
                remainingRadius = radius - level - 1
                IF remainingRadius >= 0 AND neighbor NOT IN sharedVisited:
                    // Group by shard for batching
                    crossShardRequests[neighborShardID].append((neighbor, level + 1))
    
    // Send BFS requests to other shards (batched by shard)
    pendingShards = set()
    FOR EACH targetShardID, vertices IN crossShardRequests:
        targetShard = GET_SHARD_CONFIG(targetShardID)
        
        // Batch all vertices going to same shard
        SEND_TO_SHARD(targetShard, BFSRequest(
            startVertices: vertices,  // Multiple starting vertices
            radius: radius,
            timestamp: timestamp,
            requestID: requestID,  // Same requestID for coordination
            requesterAddress: requesterAddress  // Results go back to QM
        ))
        
        pendingShards.add(targetShardID)
        activeRequests[requestID].pendingShards.add(targetShardID)
    
    // Return local results to requester (QM)
    SEND_TO_QM(requesterAddress, BFSResponse(
        requestID: requestID,
        shardID: CURRENT_SHARD_ID,
        vertices: localVisited,
        pendingShards: pendingShards  // Shards we contacted
    ))
```

### Key Features

1. **Request Deduplication**: Multiple shards may contact the same shard with the same requestID. Shards share a `visited` set per requestID to avoid duplicate work.

2. **Batching**: When a shard has multiple vertices going to the same target shard, it batches them in one request.

3. **QM Coordination**: QM tracks which shards are still processing (`waitingFor` set). When a shard responds, QM removes it and adds any shards that shard contacted.

4. **Direct Shard Communication**: Shards contact each other directly using shard addresses from config. No QM involvement in cross-shard edges.

5. **Result Aggregation**: All results flow back to QM, which aggregates and returns to client.

### Simplified Version (Easier to Implement)

The above is complex due to tracking source levels. Here's a simpler version:

```
OPTIMIZED_BFS_SIMPLIFIED(startVertex, radius, timestamp):
    visited = {startVertex: 0}
    frontier = {startVertex: 0}  // vertex -> level
    
    WHILE frontier is not empty:
        // Group by shard
        shardFrontiers = GROUP_BY_SHARD(frontier)  // shardID -> [(vertex, level), ...]
        
        // Phase 1: For each shard, do local BFS starting from frontier vertices
        shardResults = PARALLEL_FOR_EACH_SHARD(shardFrontiers):
            LOCAL_BFS_SIMPLE(shardVertices, radius, timestamp)
            // Returns: {
            //   localVertices: [v1, v2, ...],  // all vertices found locally
            //   crossShardVertices: [v1, v2, ...]  // vertices in other shards (1-hop from local)
            // }
        
        // Phase 2: Process local vertices (assign levels based on minimum distance)
        // This is tricky - we need to know which frontier vertex each local vertex came from
        // For simplicity, we can do: if vertex found locally, assign level = min(frontier levels) + localDepth
        
        // Phase 3: Fetch neighbors for cross-shard vertices
        crossShardByShard = GROUP_BY_SHARD(shardResults.crossShardVertices)
        crossShardNeighbors = PARALLEL_BATCH_GET_NEIGHBORS(crossShardByShard, timestamp)
        
        // Phase 4: Update frontier
        newFrontier = {}
        FOR EACH vertex IN crossShardNeighbors:
            level = visited[frontierVertex] + 1  // where frontierVertex is the source
            IF level <= radius AND vertex NOT IN visited:
                visited[vertex] = level
                newFrontier[vertex] = level
        
        frontier = newFrontier
    
    RETURN visited
```

### Recommended Approach: Hybrid with Level Tracking

A practical middle ground:

```
OPTIMIZED_BFS_HYBRID(startVertex, radius, timestamp):
    visited = {startVertex: 0}
    frontier = {startVertex: 0}  // vertex -> level
    
    WHILE frontier is not empty:
        // Group by shard
        shardFrontiers = GROUP_BY_SHARD(frontier)
        
        // For each shard, get neighbors for all frontier vertices (batched)
        allNeighbors = PARALLEL_BATCH_GET_NEIGHBORS(shardFrontiers, timestamp)
        
        // Group neighbors by shard
        nextLevelByShard = {}  // shardID -> [(vertex, level), ...]
        FOR EACH (sourceVertex, sourceLevel) IN frontier:
            FOR EACH neighbor IN allNeighbors[sourceVertex]:
                neighborLevel = sourceLevel + 1
                IF neighborLevel <= radius AND neighbor NOT IN visited:
                    neighborShard = GET_SHARD_ID(neighbor)
                    nextLevelByShard[neighborShard].append((neighbor, neighborLevel))
        
        // For vertices that stay in same shard, do local multi-hop exploration
        newFrontier = {}
        FOR EACH shardID, vertices IN nextLevelByShard:
            // If all vertices are in same shard as their sources, explore locally
            IF ALL_VERTICES_LOCAL(shardID, vertices):
                localResults = LOCAL_MULTI_HOP_BFS(vertices, radius, timestamp)
                // Add local results to visited and cross-shard edges to newFrontier
                FOR EACH vertex, level IN localResults.localVertices:
                    visited[vertex] = level
                FOR EACH vertex, level IN localResults.crossShardVertices:
                    newFrontier[vertex] = level
            ELSE:
                // Mixed case, add to frontier for next round
                FOR EACH (vertex, level) IN vertices:
                    visited[vertex] = level
                    newFrontier[vertex] = level
        
        frontier = newFrontier
    
    RETURN visited
```

### Communication Complexity

**Current approach**: 
- QM-Shard: R rounds (one per level)
- Total: O(R) QM-Shard communications

**New approach (direct shard communication)**: 
- QM-Shard: O(1) rounds (initiation + result collection)
- Shard-Shard: Parallel, asynchronous communication
- Total: O(1) QM involvement, O(S) shard-shard communications where S = number of shards touched

**Key Benefits**:
- QM is no longer a bottleneck
- Shards work in parallel
- Communication is distributed across shards
- Much better scalability

### Implementation Requirements

1. **Shard needs config access**: Shards must be able to:
   - Extract shardID from vertexID
   - Get shard config for other shards
   - Get leader address for other shards (or use replica manager)

2. **New RPC methods needed**:
   - `Shard.BFS` - receives BFS request with requestID and requester address
   - Shards need ability to call other shards directly

3. **Request tracking**: Shards need to track active BFS requests by requestID to share visited sets

4. **QM coordination**: QM tracks pending shards and aggregates results

### Recommendation

This **Direct Shard Communication** approach is the best because:
1. Eliminates QM as bottleneck (O(1) QM involvement)
2. Fully parallel and distributed
3. Matches the architecture described in README
4. Scales much better than QM-coordinated approaches
5. Shards handle all cross-shard coordination themselves

