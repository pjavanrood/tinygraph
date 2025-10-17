# tinygraph

A lightweight graph library written in Go.

## Getting Started

```bash
go build
./tinygraph
```

## Development

```bash
go run main.go
```

## **TinyGraph: A Distributed Graph Database**

Course: CPSC 538B Fall 2025  
Instructor: Ivan Beschastnikh  
Group Members: Jiyeon, Oleg, and Parshan

---
## **Abstract**

We propose **TinyGraph**, a lightweight distributed graph database designed for **fully dynamic ingestion** and **Breadth-First Search (BFS) queries** across partitioned graphs. TinyGraph leverages **Multi-Version Concurrency Control (MVCC)** to provide consistent, time-travel-enabled views of the graph state. Our design separates concerns between a stateless **Query Manager (QM)** layer and a stateful, replicated **Shard** layer. We plan to evaluate the trade-offs among different replication strategies (strong vs. eventual consistency), dynamic partitioning algorithms (Random vs. Heuristic), and distributed BFS implementations.

---

## **1\. Introduction and Motivation**

Graphs are a powerful way to model relationships in data, capturing complex connections that appear in social networks, financial transactions, biological systems, and more. As datasets continue to grow in both size and complexity, single-machine graph databases quickly reach their limits. This has created a growing need for **distributed graph databases**, which spread the graph across multiple machines to support **larger workloads, faster queries, and real-time updates**. By distributing storage and computation, such systems can scale to billions of edges while still supporting fundamental queries like Breadth-First Search (BFS) traversal.

---

## **2\. Background**

Designing a distributed graph database involves important choices that directly affect performance. These include how to partition the graph, how to replicate data across machines, and how to execute queries efficiently across shards.

In our work, we focus on two especially challenging design decisions:

* **Partitioning on the Fly:** Deciding where to place each new vertex or edge is critical. A poor partitioning strategy can lead to excessive cross-shard communication during queries, while a good strategy keeps related data close together. Doing this **dynamically**, as the graph grows, is a difficult but essential problem.  
* **Replication for Performance and Reliability:** Replicating shards improves **fault tolerance** by ensuring the graph can survive machine failures. Replication can also improve **read performance** if queries are served from replicas. However, it introduces trade-offs: replicas may lag behind the primary, leading to different consistency guarantees depending on the mode of replication (e.g., strong vs. eventual).

---

## **3\. Proposed Approach**

### **3.1 Multi-Version Concurrency Control (MVCC)**

Our system maintains multiple consistent views of the graph over time by adopting a **Multi-Version Concurrency Control (MVCC)** approach.

* Each vertex and edge stores a **version history** with associated **timestamps** that record when an operation (creation, update, or deletion) occurred.  
* When a query is issued with a specific timestamp, the system **reconstructs the graph state** as it existed at that time.  
* This mechanism enables TinyGraph to provide **temporal consistency** (time-travel queries) and concurrent reads without blocking writes.

### **3.2 System Architecture**

Our system comprises two primary components:

#### **3.2.1 Query Manager (QM)**

The QM acts as the **stateless middleware** between the client and the shards. We allow for multiple QMs to prevent a single bottleneck. Its responsibilities include:

* **Partitioning:** Deciding which shard will store a newly created vertex based on the chosen partitioning algorithm.  
* **Query Routing & Execution:** Directing client operations to the correct shard and coordinating distributed queries, such as BFS.  
* **ID Encoding:** The QM encodes the shard ID into the vertex\_id (e.g., vertex\_id \= concat\[shard\_number, uuid\]). This design eliminates the need for a separate vertex\_id to shard\_id mapping service, allowing any QM to immediately locate a vertex's home shard without extra communication.

#### **3.2.2 Shards (Replicated State Machines)**

Shards are the **stateful layer** where vertices and edges are stored.

* Each Shard is a **replicated state machine** (RSM), responsible for holding a subset of vertices and their outgoing edges.  
* The shards use **Raft** for replication to ensure high availability and durability.  
* Edges, even cross-shard edges, are stored locally on the source vertex's shard and use the vertex\_id (which includes the shard ID) to reference the destination vertex.

### **3.3 Data Structures (Conceptual)**

The following conceptual structures are maintained within each Shard:

// Stores the version history for MVCC  
type Vertex struct {  
	id          string  
	edges   map\[string\]\*Edge  // Outgoing edges  
	prop      \*VertexProp  
	ts          float64                  // Latest version's timestamp  
	prevs    \[\]\*Vertex               // Older versions of the vertex  
}

// Stores the version history for MVCC  
type Edge struct {  
	id               string  
	from\_id      string  
	to\_id          string  
	prop           \*EdgeProp  
	ts               float64  
	destroyed  bool         // Marker for deletion  
	prevs         \[\]\*Edge      // Older versions of the edge  
}

// Shard State Machine: Vertices map  
type ShardRSM struct {  
	mu          sync.RWMutex  
	vertices   map\[string\]\*Vertex   
}

### **3.4 Supported Operations**

The system supports the following client-facing operations, which include a **timestamp** argument to enable MVCC-based consistency:

| Operation | Description |
| :---- | :---- |
| add\_vertex(prop, ts) | Creates a vertex with properties and returns its ID. |
| add\_edge(v1, v2, prop, ts) | Adds an edge between vertices `v1` and `v2`. |
| del\_edge(v1, v2, ts) | Deletes the edge between `v1` and `v2`. |
| update\_vertex(v\_id, prop, ts) | Updates the properties of a vertex. |
| update\_edge(v1, v2, prop, ts) | Updates the properties of an edge. |
| get\_vertex\_prop(v\_id, ts) | Retrieves the properties of `v_id` as they existed at `ts`. |
| get\_edge\_prop(v1, v2, ts) | Retrieves the edge properties as they existed at `ts`. |
| bfs(v\_start, radius, ts) | Finds all vertices within the specified radius of `v_start` at time `ts`. |

---

**Shard RPC Operations:** Each Shard supports corresponding RPCs for the above operations, plus key utility calls like GetNeighbors(v\_id, ts).

### **3.5 Replication Modes**

We will evaluate three distinct shard operating modes:

1. **Primary-Only Read/Write (Strong Consistency):** Writes and reads are served only by the primary. This provides strong consistency but may have lower read throughput.  
2. **Primary Write, Replica Read (Eventual Consistency):** Writes go to the primary (via Raft log), but reads are served by replicas. This offers higher read throughput at the cost of potential read staleness, as replicas may lag behind the leader.  
3. **Unreplicated (Baseline):** A single shard with no replication. Provides strong consistency but no fault tolerance.

### **3.6 Timestamp Ordering Approaches**

We will compare two distinct methods for generating the timestamps essential to the MVCC mechanism:

#### **Approach 1: Total Ordering via Monotonic Clock (Oleg)**

My approach was to synchronize a lamport digital clock for write operations (create/delete/update), where one coordinator will request/reserve the next timestamp value, use it for its operation, then the value is incremented across all coordinators and can never be re-used again – this ensures we have a total ordering of all writes. This digital clock synchronization across the coordinators can be done in a number of ways – for instance, single-paxos can be used where all coordinators act as proposers/acceptors (note that we expect to not have a terribly large set of coordinators) and increment the value monotonically (but not strictly by values of 1, in case of disagreement between proposers for a phase for liveness). Another approach would be to have a separate clock-server that will provide monotonically increasing and unique timestamps to the coordinators. This ensures a total ordering of \*all\* events with no risk of drift.

#### 

#### **Approach 2: Client Timestamps with Conflict Resolution (Parshan)**

Clients attach a timestamp (e.g., from an NTP server) to their operations.

* Conflict Resolution: Client IDs resolve ties in the rare case of identical timestamps.  
* Consistency Check: Each vertex records the timestamp of its last access (`t_last`​). If a Write operation with `t_write`​ arrives *after* a Read operation with `t_read​>t_write`​ has been processed (due to network delay), the write is rejected. The client must retry the write with a newer timestamp.

*What is Guaranteed?*

* **Per-operation consistency:** Each request (add/update/delete) is applied one-by-one, replicated with Raft, and visible at its own commit timestamp.  
  * **Leader reads:** Linearizable (up-to-date) view of a shard.  
  * **Replica reads:** Still serve a valid MVCC version ≤ T, though possibly older than the leader’s state.  
* **MVCC snapshots:** Queries at `ts=T` return the latest version ≤ T for every key in that shard.  
* **Monotonicity per key:** Anti-backdating ensures no write can appear in the past relative to a later read on the same object.

*What is NOT Guaranteed:*

* **No transactions:** Multi-operation atomicity (e.g., inserting two edges “together”) is not supported; ops become visible independently.  
* **No global snapshot isolation:** A BFS at `ts=T` uses the same timestamp across shards, but shards do not coordinate, so results may mix states from different commit moments.  
* **Replica staleness:** When reading from replicas, data may lag behind leader commits.

### **3.7 Request Flow Examples**

#### **Add Vertex**

1. **Client → QM:** `add_vertex(prop)`  
2. **QM:** Uses the partitioning algorithm to select a shard number. Generates `vertex_id = concat_bits[shard_number, uuid]`.  
3. **QM → Primary Shard:** RPC `AddVertex(vertex_id, prop, ts)`  
4. **Primary Shard:** Applies the operation to its Raft log.  
5. **Primary Shard → QM → Client:** Returns the new `vertex_id`.

#### **Add Edge**

1. **Client → QM:** `add_edge(v1, v2, prop)`  
2. **QM:** Uses the encoded shard bits in v1​ to find the responsible Primary Shard.  
3. **QM → Primary Shard:** RPC `AddEdge(v1, v2, prop, ts)`  
4. **Primary Shard:** Applies the operation to its Raft log.  
5. **Primary Shard → QM → Client:** Returns success.

#### **Read/BFS**

1. **Client → QM:** `get_vertex_prop(v_id)` or `bfs(v_start, radius)`  
2. **QM:** Finds the target shard(s).  
3. **QM → Shard:** RPC `GetVertexProp(v_id, ts)` or initiates the distributed BFS algorithm.  
   * *Note:* Reads can go to the Primary or a Replica depending on the chosen **Replication Mode**. Since it is a read operation, it does not go through the Raft log.  
4. **Shard(s) → QM → Client:** Returns the result.

**Baseline BFS Pseudocode:**

| function DISTRIBUTED\_BFS(start\_vertex, radius, timestamp):    // 1\. Initialization    Visited \= {start\_vertex}    Frontier \= {start\_vertex}    Results \= {start\_vertex}        // 2\. Traversal Loop (up to max radius)    for level from 1 to radius:        if Frontier is empty:            break                    // Map to group vertices by the shard responsible for them        ShardRequests \= MAP(ShardID \-\> List of Vertices)                // Group all vertices in the current Frontier by their Shard ID        for vertex in Frontier:            shard\_id \= GET\_SHARD\_ID(vertex)            ShardRequests\[shard\_id\].append(vertex)                    NewNeighbors \= SET()                // 3\. Batched Concurrent Query        // Send a batch of neighbor-lookup requests to each relevant Shard        // All requests run in parallel (ASYNC)        AllResponses \= ASYNC\_BATCH\_CALL(            for ShardID, VertexList in ShardRequests:                SHARD\_RPC:GetNeighbors\_Batch(VertexList, timestamp)        )                // 4\. Process Responses and Update State        for response in AllResponses:            for neighbor\_id in response:                if neighbor\_id is not in Visited:                    Visited.add(neighbor\_id)                    Results.add(neighbor\_id)                    NewNeighbors.add(neighbor\_id)                            // Set up the next frontier        Frontier \= NewNeighbors            return Results |
| :---- |

**Optimized BFS** (\# TODO @Oleg)

High level overview – developing this function fully is part of our milestones as this is very complex:

1. Start by client calling BFS(rootVertexID, radius, timestamp) to the QueryManager.  
2. QueryManager will process the request, and reserve a TraversalState object for this request.  
3. The QueryManager will lookup the shard responsible for rootVertexID, and call the shard’s BFS function, passing in the root vertex, the desired radius, and the timestamp, as well as a UNIQUE request ID and requester address (itself). After sending the request, the QueryManager will add the shard to the TraversalState’s waitingFor field.  
4. The QueryManager will then wait until the TraversalState’s waitingFor field is completely empty – once this is true, the QueryManager will send all the vertexIDs and VertexProps it has in its TraversalState’s collected field back to the client.  
5. The Shard, upon receiving a BFS request will check for the requestID and requester address:  
   1. If it already exists on the Shard, the Shard will:  
      1. Start an internal BFS search from the provided rootVertexID, however it will share the Visited variable with any ongoing searches for the same requestID and requester address pair.  
      2. Once that BFS traversal finishes (note, it is possible for it to finish immediately, if the rootVertexID was already visited), it will respond with the collected set of VertexIDs and VertexProps (for this BFS traversal) to the requester’s address (the coordinating QueryManager), as well as its own ShardID.  
   2. If it does not yet exist on the Shard, the Shard will:  
      1. Create a new Visited variable that will be shared by all requestID requester address pairs to this shard  
      2. Perform the same steps as (a).  
6. Once the QueryManager receives a response from a shard, it will add it to the set of returned VertexID, VertexProp pairs of the request’s TraversalState, and remove the corresponding ShardID from the TraversalState’s waitingFor field.

This puts the onus of traversing the BFS tree on the shards, by doing the following: if a shard encounters an edge on its traversal whose VertexID is not associated with its ShardID, it will create a new BFS request for the Shard that stores the corresponding VertexID (note, the ShardID is encoded into the VertexID, and Shards are aware of each other), however instead of passing the original radius, it will pass the original radius minus the number of edges it took to get to the edge (since this is BFS, this will always be the minimum), as well as passing the original requestID and requester address pair (that of the QueryManager), instead of its own, so that the results are not processed by this Shard, but rather by the QueryManager associated with the original request. It may do this multiple times, to multiple shards, but the Shard will keep track of all the different Shards it sends requests to (may be multiple times to the same shard\!), and include this information when it sends its response to the QueryManager, who, as described above, will remove this Shard from its TraversalState’s waitingFor field, and add the set that that Shard returned to the TraversalState’s waitingFor field. Thus, the QueryManager must wait for all outgoing requests that the Shards have handed off to other Shards to return before it knows to return the final result to the user.

---

## **4\. Evaluation and Benchmarking**

We will evaluate TinyGraph across a range of conditions and design choices to understand their impact on performance, consistency, and scalability.

### **4.1 Replication Strategies**

| Strategy | Consistency | Fault Tolerance | Read Throughput |
| :---- | :---- | :---- | :---- |
| **No Replication** (Baseline) | Strong | None | Low/Medium |
| **Primary-Only Read/Write** | Strong | High | Medium |
| **Primary Write, Replica Read** | Eventual | High | High |

### 

### **4.2 Partitioning Algorithms**

We will compare:

* **Random Partitioning:** A simple baseline with minimal computational overhead.  
* **Heuristic Partitioning:** Strategies such as [Linear Deterministic Greedy (LDG) or Fennel](https://www.vldb.org/pvldb/vol11/p1590-abbas.pdf), which are designed to reduce cross-shard edges and balance shard loads.

### **4.3 Distributed BFS Implementations**

We will analyze the latency and throughput of:

* **Naive Synchronous BFS:** Baseline using sequential RPC calls. (Initial implementation will run on the Query Manager).  
* **Optimized Synchronous BFS:** Improves performance by batching RPCs to reduce network overhead.  
* **Asynchronous BFS:** Explores parallelism by allowing shards to dispatch sub-searches to other shards.

### **4.4 Correctness and Reliability**

We will use **TLA+ Modeling** to validate replication correctness and system safety under failures. Our goal is to use TLA+ to confirm that:

1. All replicas apply committed operations in the same order (no divergent commits).  
2. The system preserves a consistent ordering of writes even under leader failures.

### **4.5 Scalability**

We will measure how the system handles larger graphs and higher ingestion/query rates by:

* **Varying the number of Query Managers:** Evaluate the effect of multiple coordinators on throughput, consistency, and fault tolerance.  
* **Varying the number of Shards:** Study the trade-offs between query performance and partitioning overhead.

---

## **5\. Timeline**

That's a more strategic way to view the project\! Focusing on the **terminal goal** of each phase makes the timeline clearer.

Here are the target achievements for each of the four phases, leading up to the final submission on December 21st.

---

## **Project Timeline: Phase Goals**

### **Phase 1: System Foundations and MVCC Implementation**

**(Ends: October 22nd, 18:00 PST \- Milestone 1 Due)**

The primary goal of Phase 1 is to establish the core architectural foundation and the time-traveling MVCC mechanism.

| Achievement Goal (Milestone 1\) | Details |
| :---- | :---- |
| **Architectural Blueprint** | Finalized RPC interface definitions for the QM and Shard components. |
| **MVCC Core Working** | A working, **single-node prototype** of a Shard capable of handling add\_vertex and add\_edge requests while storing version history using timestamps. |
| **Consistent Reads** | Successful demonstration of get\_prop calls that accurately retrieve the state of a vertex or edge **as it existed at a specified past timestamp**. |
| **Single QM and simple timestamping** | We deploy a single QM which can also provide timestamps for operations, therefore no need to worry about timestamps. |

---

### **Phase 2: Distributed Core and Replication**

**(Ends: November 12th, 18:00 PST \- Milestone 2 Due)**

The goal of Phase 2 is to achieve full distribution and implement the required consistency and search baselines.

| Achievement Goal (Milestone 2\) | Details |
| :---- | :---- |
| **Distributed & Replicated Storage** | The Shards are fully integrated into a **Raft** cluster, allowing the system to operate reliably in all three proposed **replication modes** (Strong vs. Eventual Consistency). |
| **Full Write Flow** | The QM successfully routes all write operations (add/update/del) to the appropriate Shard Primary and through the consensus mechanism. |
| **BFS Baseline** | A fully functional, distributed, **Baseline BFS** is running on the Query Manager, using **Random Partitioning**. |

---

### **Phase 3: Optimizations and Evaluation Preparation**

**(Ends: November 26th, 18:00 PST \- Milestone 3 Due)**

Phase 3 is dedicated to implementing all the **advanced features and optimizations** required for the evaluation.

| Achievement Goal (Milestone 3\) | Details |
| :---- | :---- |
| **Complete Feature Set** | Implementation of **optimized BFS algorithms** and both **partitioning strategies** (Random and Heuristic). |
| **System Stability** | The system is stable and robust enough to handle high-volume, concurrent load necessary for running benchmarks without crashing or deadlocking. |
| **Benchmarking Ready** | All performance logging and test configurations are set up and validated, allowing the team to immediately begin the formal evaluation phase. |
| **Multiple QMs** | Allow for multiple QMs, and implement timestamping strategy.  |

---

### **Phase 4: Evaluation and Final Documentation**

**(Ends: December 21st, 18:00 PST \- Final Report Due)**

The final phase is focused entirely on validation, analysis, and professional presentation of the results.

| Achievement Goal (Final Report) | Details |
| :---- | :---- |
| **Comprehensive Evaluation** | Completion of all benchmarking runs, including the detailed analysis of the trade-offs between replication, partitioning, and BFS algorithms. |
| **Correctness Validation (TLA+)** | Successful modeling and verification of the system's critical consistency and ordering properties using TLA+. |
| **Final Submission** | A polished **Final Project Report** that clearly documents the design, comprehensively presents the results (with graphs and data), and concludes with key findings and contributions. |

---

## **6\. Related Work**

Our system design was inspired by **SystemG**, which shares the architectural pattern of separating Query Managers (coordinators) from Shards (storage/state).

**Key Difference from SystemG:** The primary distinction in our approach is that TinyGraph's **Query Manager is responsible for coordinating the distributed BFS execution**, whereas our initial plan involved having the **Shards manage the query execution and result collection**. This centralization in the QM simplifies shard implementation and adheres to a more typical client-server-storage pattern.

