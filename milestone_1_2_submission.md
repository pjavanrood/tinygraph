# Milestone 1 & 2 Submission

## Phase 1: System Foundations and MVCC Implementation

**(Ends: October 22nd, 18:00 PST - Milestone 1 Due)**

The primary goal of Phase 1 is to establish the core architectural foundation and the time-traveling MVCC mechanism.

| Achievement Goal (Milestone 1) | Details |
| :---- | :---- |
| **Architectural Blueprint** | Finalized RPC interface definitions for the QM and Shard components. |
| **MVCC Core Working** | A working, **single-node prototype** of a Shard capable of handling add\_vertex and add\_edge requests while storing version history using timestamps. |
| **Consistent Reads** | Successful demonstration of get\_prop calls that accurately retrieve the state of a vertex or edge **as it existed at a specified past timestamp**. |
| **Single QM and simple timestamping** | We deploy a single QM which can also provide timestamps for operations, therefore no need to worry about timestamps. |

### Implementation References - Phase 1

#### **Architectural Blueprint - RPC Interface Definitions**
- **File**: [`pkg/rpc/types.go`](pkg/rpc/types.go)
- **Lines**: 1-164
- **Key Components**:
  - Request/Response types for all operations (AddVertex, AddEdge, DeleteEdge, BFS, etc.)
  - QM-to-Shard interface definitions (lines 27-60, 75-83)
  - Client-to-QM interface definitions (lines 17-25, 40-49, 65-73)

#### **MVCC Core Working**
- **Vertex MVCC Implementation**: [`pkg/mvcc/vertex.go`](pkg/mvcc/vertex.go)
  - Lines 14-53: Vertex structure with version history via `Prev` field
  - Lines 24-32: `NewVertex()` creates first version
  - Lines 34-53: `UpdateVertex()` maintains version chain
  - Lines 102-114: `GetAt()` retrieves vertex state at specific timestamp

- **Edge MVCC Implementation**: [`pkg/mvcc/edge.go`](pkg/mvcc/edge.go)
  - Lines 13-33: Edge structure with version history and `Destroyed` flag
  - Lines 24-33: `NewEdge()` creates first version
  - Lines 36-56: `UpdateEdge()` maintains version chain
  - Lines 59-63: `MarkDeleted()` for logical deletion
  - Lines 65-77: `GetAt()` retrieves edge state at specific timestamp
  - Lines 80-83: `AliveAt()` checks if edge exists at timestamp

- **Shard Storage**: [`pkg/shard/shard_fsm.go`](pkg/shard/shard_fsm.go)
  - Lines 24-28: ShardFSM stores vertices with MVCC
  - Lines 32-50: `addVertex()` handles vertex creation with timestamps
  - Lines 55-75: `addEdge()` handles edge creation with timestamps
  - Lines 80-95: `deleteEdge()` handles edge deletion with timestamps

#### **Consistent Reads at Past Timestamps**
- **Vertex Time-Travel**: [`pkg/mvcc/vertex.go`](pkg/mvcc/vertex.go)
  - Lines 102-114: `GetAt(ts)` traverses version history to find vertex state at timestamp
  - Lines 87-100: `GetAllEdges(ts)` returns edges alive at timestamp

- **Edge Time-Travel**: [`pkg/mvcc/edge.go`](pkg/mvcc/edge.go)
  - Lines 65-77: `GetAt(ts)` traverses edge version history
  - Lines 80-83: `AliveAt(ts)` checks edge existence at timestamp

- **Read Operations**: [`pkg/shard/shard_fsm.go`](pkg/shard/shard_fsm.go)
  - Lines 100-112: `getNeighbors()` retrieves neighbors at specified timestamp (line 106)

#### **Single QM with Timestamping**
- **File**: [`pkg/qm/query_manager.go`](pkg/qm/query_manager.go)
  - Lines 34-36: `generateTimestamp()` provides Unix timestamps for all operations
  - Lines 252: Timestamp generation in `AddVertex()` handler
  - Lines 291: Timestamp generation in `AddEdge()` handler
  - Lines 328: Timestamp generation in `DeleteEdge()` handler

---

## Phase 2: Distributed Core and Replication

**(Ends: November 12th, 18:00 PST - Milestone 2 Due)**

The goal of Phase 2 is to achieve full distribution and implement the required consistency and search baselines.

| Achievement Goal (Milestone 2) | Details |
| :---- | :---- |
| **Distributed & Replicated Storage** | The Shards are fully integrated into a **Raft** cluster, allowing the system to operate reliably in all three proposed **replication modes** (Strong vs. Eventual Consistency). |
| **Full Write Flow** | The QM successfully routes all write operations (add/update/del) to the appropriate Shard Primary and through the consensus mechanism. |
| **BFS Baseline** | A fully functional, distributed, **Baseline BFS** is running on the Query Manager, using **Random Partitioning**. |

### Implementation References - Phase 2

#### **Distributed & Replicated Storage with Raft**
- **Raft Integration**: [`pkg/shard/shard.go`](pkg/shard/shard.go)
  - Lines 35-39: Shard struct wraps ShardFSM with Raft
  - Lines 366-445: `NewShard()` initializes Raft cluster with replicas
  - Lines 387-395: Raft configuration setup (in-memory stores, snapshot)
  - Lines 398-407: Raft transport setup for network communication
  - Lines 423-442: Raft cluster bootstrapping with all replicas
  
- **FSM Interface Implementation**: [`pkg/shard/shard.go`](pkg/shard/shard.go)
  - Lines 62-124: `Apply()` applies committed log entries to FSM
  - Lines 130-141: `Snapshot()` creates point-in-time snapshot
  - Lines 146-159: `Restore()` restores FSM from snapshot

- **Write Operations Through Consensus**: [`pkg/shard/shard.go`](pkg/shard/shard.go)
  - Lines 188-221: `AddVertex()` - leader check + Raft.Apply()
  - Lines 224-257: `AddEdge()` - leader check + Raft.Apply()
  - Lines 260-293: `DeleteEdge()` - leader check + Raft.Apply()
  - Lines 324-357: `DeleteAll()` - leader check + Raft.Apply()

- **Linearizable Reads**: [`pkg/shard/shard.go`](pkg/shard/shard.go)
  - Lines 296-307: `GetNeighbors()` uses `VerifyLeader()` for consistency
  - Lines 310-321: `FetchAll()` uses `VerifyLeader()` for consistency

- **Configuration**: [`internal/config/config.go`](internal/config/config.go)
  - Lines 24-39: ShardConfig and ReplicaConfig structures
  - Lines 42-50: Partitioning and Replication configuration
  - Lines 162-174: Raft replication factor validation

#### **Full Write Flow - QM to Shard Routing**
- **Leader Discovery**: [`pkg/qm/replica_manager.go`](pkg/qm/replica_manager.go)
  - Lines 35-58: `PushBasedReplicaManager` tracks shard leaders
  - Lines 49-58: `NotifyLeaderIDUpdate()` receives leader updates from shards

- **Leader Observer**: [`pkg/shard/raft_observer.go`](pkg/shard/raft_observer.go)
  - Lines 1-79: Observer pattern notifies QM when leader changes
  - Lines 28-77: `SetupLeaderElectionObserver()` watches Raft state changes

- **QM Write Routing**: [`pkg/qm/query_manager.go`](pkg/qm/query_manager.go)
  - Lines 61-91: `addVertexToShard()` - connects to shard leader (lines 65-76)
  - Lines 93-123: `addEdgeToShard()` - routes to leader via replica manager
  - Lines 125-155: `deleteEdgeToShard()` - routes to leader via replica manager
  - Lines 246-271: `AddVertex()` handler - selects shard + routes to leader
  - Lines 273-308: `AddEdge()` handler - determines shard from vertex ID + routes to leader
  - Lines 310-344: `DeleteEdge()` handler - routes to appropriate shard leader

#### **BFS Baseline with Random Partitioning**
- **Random Partitioning**: [`pkg/qm/partitioner.go`](pkg/qm/partitioner.go)
  - Lines 9-12: `RandomPartitioner()` randomly assigns vertices to shards

- **Vertex ID Generation**: [`pkg/qm/query_manager.go`](pkg/qm/query_manager.go)
  - Lines 39-44: `generateVertexID()` embeds shard ID in vertex ID format: `shardID-randomHex`
  - Lines 46-59: `getShardIDFromVertexID()` extracts shard from vertex ID

- **Distributed BFS**: [`pkg/qm/query_manager.go`](pkg/qm/query_manager.go)
  - Lines 347-405: `BFS()` handler implements distributed breadth-first search
  - Lines 353-356: BFS queue and visited set initialization
  - Lines 358-395: Main BFS loop queries neighbors across shards
  - Lines 362-372: Determines shard from vertex ID for each vertex
  - Lines 378-387: Calls `getNeighborsToShard()` to fetch neighbors at timestamp
  - Lines 389-394: Tracks visited vertices and adds new neighbors to queue

- **Neighbor Retrieval**: [`pkg/qm/query_manager.go`](pkg/qm/query_manager.go)
  - Lines 157-183: `getNeighborsToShard()` fetches neighbors from appropriate shard

