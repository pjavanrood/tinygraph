# End-to-End Tests for TinyGraph

This directory contains comprehensive end-to-end tests for the TinyGraph distributed graph database system with MVCC (Multi-Version Concurrency Control) support.

## Overview

The E2E tests verify the correctness of:
- **Basic Operations**: AddVertex, AddEdge, DeleteEdge, BFS Query
- **MVCC Correctness**: Time-travel queries that verify the graph state at different timestamps
- **Distributed System Behavior**: Coordination between Query Manager and Shards

## Test Structure

### Test Files
- `e2e_test.go` - Main end-to-end test suite
- `mvcc_test.go` - MVCC unit tests (existing)

### Test Suite Components

#### 1. TestClient
A helper wrapper around the RPC client that provides:
- `AddVertex(properties)` → `(vertexID, timestamp)`
- `AddEdge(from, to, properties)` → `timestamp`
- `DeleteEdge(from, to)` → `timestamp`
- `BFS(startVertex, radius, timestamp)` → `[]vertexIDs`

All operations **capture and return timestamps** from the server, which are crucial for MVCC testing.

#### 2. Test Cases

##### TestBasicOperations
Tests fundamental graph operations:
- Add vertices v1, v2, v3
- Create edges v1→v2→v3
- Query BFS with different radii
- Verify connectivity

##### TestMVCCSimpleGraph
Tests basic MVCC time-travel queries:
1. Create graph: v1→v2
2. Query at timestamp T1 → should see [v1, v2]
3. Add v3 and edge v2→v3
4. Query at timestamp T2 → should see [v1, v2, v3]
5. Query again at T1 → should still see only [v1, v2] (time-travel verification)

##### TestMVCCEdgeDeletion
Tests MVCC correctness with edge deletion:
1. Create graph: v1→v2→v3
2. Capture timestamp T1
3. Delete edge v1→v2
4. Query at current time → should see only [v1]
5. Query at T1 → should still see [v1, v2, v3] (deletion not visible in past)

##### TestMVCCComplexEvolution
Tests multiple graph state transitions:
- State 1: v1→v2→v3 (linear chain)
- State 2: Add branch v1→v4
- State 3: Delete v1→v2
- State 4: Add v3→v4

Verifies that:
- Each state is correct when queried at its timestamp
- Historical queries return the correct historical state

##### TestMVCCMultipleEdgeUpdates
Tests edge versioning:
- Add same edge (v1→v2) multiple times with different properties
- Each version should have its own timestamp
- All queries should see the edge (MVCC maintains all versions)

##### TestMVCCDeleteAndReadd
Tests deletion and re-addition:
1. Add edge v1→v2
2. Delete edge v1→v2
3. Re-add edge v1→v2
4. Verify:
   - After add: edge exists
   - After delete: edge doesn't exist
   - After re-add: edge exists again
   - Historical queries work correctly

## Running the Tests

### Prerequisites

1. **Configuration File**: Ensure `config.yaml` is properly configured:
```yaml
query_manager:
  host: "localhost"
  port: 9090

shards:
  - id: 0
    host: "localhost"
    port: 9091
```

2. **Dependencies**: Make sure all Go dependencies are installed:
```bash
go mod download
```

### Running All Tests

From the project root:
```bash
cd test
go test -v
```

### Running Specific Tests

Run a specific test:
```bash
cd test
go test -v -run TestBasicOperations
go test -v -run TestMVCCSimpleGraph
go test -v -run TestMVCCEdgeDeletion
```

### Understanding Test Output

Each test logs:
- Vertex IDs and their creation timestamps
- Edge additions/deletions and their timestamps
- Query timestamps
- Expected vs actual results

Example output:
```
=== RUN   TestMVCCSimpleGraph
    e2e_test.go:174: State 1 - Created v1 (ts=1698765432), v2 (ts=1698765433), v1->v2 (ts=1698765434)
    e2e_test.go:183: State 2 - Created v3 (ts=1698765436), v2->v3 (ts=1698765437)
    e2e_test.go:201: MVCC simple graph test passed
--- PASS: TestMVCCSimpleGraph (8.02s)
```

## Test Environment Setup

The tests use `TestMain` to automatically:
1. Start all configured shards
2. Start the Query Manager
3. Wait for services to initialize
4. Run tests
5. Clean up processes

**Note**: The tests start actual server processes, so they require available ports as specified in `config.yaml`.

## MVCC Testing Strategy

### Key Principles

1. **Timestamp Capture**: All operations return timestamps which are stored and used for later queries
2. **Time Delays**: Strategic `time.Sleep()` calls ensure distinct timestamps between operations
3. **Historical Queries**: Tests query the graph at past timestamps to verify MVCC correctness
4. **State Verification**: Each test verifies both current state and historical states

### MVCC Invariants Being Tested

1. **Visibility**: An operation is only visible at timestamps >= its creation timestamp
2. **Deletion**: Deleted edges are not visible after deletion timestamp, but remain visible before it
3. **Versioning**: Multiple versions of the same edge can coexist
4. **Time-Travel**: Queries at historical timestamps see the graph as it was at that time
