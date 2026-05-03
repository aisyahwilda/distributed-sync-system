# Distributed Synchronization System Architecture

## Overview

A distributed system implementing core consensus and coordination protocols for multi-node synchronization, with focus on:

- Distributed consensus (Raft)
- Distributed locking with deadlock detection
- Distributed queue with at-least-once delivery
- Cache coherence (MESI protocol)

## System Architecture

```
┌─────────────────────────────────────────────────────┐
│  Distributed Sync System (3+ nodes)                │
├─────────────────────────────────────────────────────┤
│                                                      │
│  ┌────────────────┐  ┌────────────────┐            │
│  │  Consensus     │  │  Communication │            │
│  │  (Raft)        │◄─►  Layer         │            │
│  └────────────────┘  └────────────────┘            │
│         ▲                     ▲                      │
│         │                     │                      │
│  ┌──────┴──────┬──────────────┴──────────┐         │
│  │             │                         │          │
│  ▼             ▼                         ▼          │
│ ┌─────┐   ┌─────────┐    ┌─────────┐   ┌────────┐ │
│ │Lock │   │ Queue   │    │ Cache   │   │Failure │ │
│ │Mgr  │   │ System  │    │ Node    │   │Detect  │ │
│ └─────┘   └─────────┘    └─────────┘   └────────┘ │
│                                                      │
│  ┌─────────────────────────────────────────────┐  │
│  │  REST API (FastAPI)                         │  │
│  └─────────────────────────────────────────────┘  │
│                                                      │
└─────────────────────────────────────────────────────┘
```

## Core Components

### 1. Raft Consensus (`src/consensus/raft.py`)

**Purpose**: Provide distributed consensus for state replication

**Features**:

- **Leader Election**: Timeout-based election with vote counting
- **Log Replication**: AppendEntries RPC for log synchronization
- **State Persistence**: Persistent state saved to disk (term, votedFor, log)
- **Follower/Candidate/Leader States**: Full state machine implementation

**Key Classes**:

- `RaftNode`: Main Raft node implementation
- `LogEntry`: Individual log entries with term and command
- `RaftState`: Persistent state storage
- `NodeState`: Enum for node states (FOLLOWER, CANDIDATE, LEADER)

### 2. Distributed Lock Manager (`src/nodes/lock_manager.py`)

**Purpose**: Manage distributed locks across nodes with deadlock detection

**Features**:

- **Exclusive Locks**: Only one node can hold at a time
- **Shared Locks**: Multiple nodes can hold simultaneously
- **Deadlock Detection**: Wait-for graph cycle detection using DFS
- **Lock Queuing**: Waiting queue for lock requests
- **State Persistence**: Lock state saved to disk

**Lock Types**:

```
Exclusive: Blocks shared and exclusive
Shared: Multiple readers, blocks exclusive
```

**Deadlock Detection Algorithm**:

- Builds wait-for graph where edge (A → B) means A waits for B to release lock
- DFS-based cycle detection identifies circular dependencies
- Detected cycles trigger deadlock abort

### 3. Distributed Queue System (`src/nodes/queue_node.py`)

**Purpose**: Implement reliable message passing with at-least-once delivery

**Features**:

- **Consistent Hashing**: MD5-based key distribution across nodes
- **Message Replication**: Configurable replication factor (default: 2)
- **At-Least-Once Delivery**: ACK/NACK mechanism with retry logic
- **Persistence**: Queue state persisted to JSON file
- **Idempotent Operations**: Message deduplication support

**Message Flow**:

```
Producer → Enqueue (hashed to replica nodes) → Replicas Store
          ↓
Consumer → Dequeue → ACK/NACK
          ↓
- ACK: Message marked acknowledged
- NACK: Message retried (max 3 attempts)
```

### 4. MESI Cache Coherence (`src/nodes/cache_node.py`)

**Purpose**: Maintain cache consistency across multiple cache nodes

**Features**:

- **MESI States**: Modified, Exclusive, Shared, Invalid
- **Invalidation Protocol**: Broadcast invalidation on write
- **LRU Replacement**: Evict least recently used on capacity
- **Coherence Operations Tracking**: Monitor protocol messages

**State Transitions**:

```
PUT   → MODIFIED
  ↓
  ├─→ Other caches: INVALID
  └─→ Self: MODIFIED (dirty, local copy only)

READ from other
  ├─→ EXCLUSIVE/MODIFIED → SHARED
  └─→ Other: copy becomes SHARED
```

### 5. Communication Layer (`src/communication/`)

**Components**:

- **message_passing.py**: Reliable delivery with retry and acknowledgment
- **failure_detectore.py**: Heartbeat-based node failure detection
- **api.py**: REST API endpoints for all operations

**Reliability Features**:

- Automatic retry with exponential backoff
- Network partition simulation
- Message loss simulation
- In-flight message tracking

## Data Flow

### Lock Acquisition Flow

```
Client Request
     ↓
Lock Manager (check availability)
     ├─→ Success: Return immediately
     ├─→ Blocked: Add to wait queue & check deadlock
     └─→ Deadlock: Return failure
     ↓
Replicate via Raft (in real deployment)
     ↓
Response
```

### Queue Enqueue Flow

```
Producer
     ↓
Compute hash(key) → determine replica nodes
     ↓
Store in local queue + all replicas
     ↓
Track message state + replication status
     ↓
Return msg_id for tracking
```

### Cache Write Flow

```
Cache PUT(key, value)
     ↓
Transition to MODIFIED state
     ↓
Broadcast INVALIDATE to other nodes
     ↓
Other nodes' copies → INVALID state
     ↓
Acknowledge invalidations
```

## Node Types

### Node Roles (Raft-based)

1. **Leader**: Receives commands, replicates log
2. **Follower**: Accepts writes from leader
3. **Candidate**: Participates in leader election

### Component Roles

- **Lock Manager Node**: Manages distributed locks
- **Cache Node**: Holds cached data
- **Queue Node**: Stores messages

## Deployment Model

### Single Node Deployment

```
Node1 (node_id=node1, port=8000)
├─ Raft (candidate for leadership)
├─ Lock Manager
├─ Queue
└─ Cache (MESI)
```

### Multi-Node Deployment (3+ nodes)

```
Network: distnet (Docker bridge)

Node1 (localhost:8001)
├─ /health
├─ /lock/acquire, /queue/enqueue, /cache/put
└─ Connected to Node2, Node3

Node2 (localhost:8002)
├─ /health
├─ /lock/acquire, /queue/enqueue, /cache/put
└─ Connected to Node1, Node3

Node3 (localhost:8003)
├─ /health
├─ /lock/acquire, /queue/enqueue, /cache/put
└─ Connected to Node1, Node2
```

## Persistence

### File-Based Persistence

- **Raft State**: `data/raft_{node_id}.json`
  - Persistent state: term, votedFor, log entries
- **Lock State**: `data/locks_{node_id}.json`
  - Exclusive locks, shared locks, wait queues
- **Queue State**: `/app/data/queue_data.json`
  - Messages with status (pending/acknowledged/failed)

## Configuration

### Environment Variables

```
NODE_ID=node1                    # Node identifier
PORT=8000                        # Listen port
PEERS=node2,node3               # Peer nodes
CACHE_SIZE=100                  # Max cache entries
REPLICATION_FACTOR=2            # Queue replication copies
FAILURE_DETECTION_TIMEOUT=5     # Heartbeat timeout (seconds)
```

## API Endpoints

### Health & Status

- `GET /health` - Health check
- `GET /status` - Overall system status

### Lock Manager

- `POST /lock/acquire` - Acquire lock
- `POST /lock/release` - Release lock
- `GET /lock/status/{resource_id}` - Lock status

### Queue

- `POST /queue/enqueue` - Enqueue message
- `GET /queue/dequeue?key=...` - Dequeue message
- `GET /queue/status` - Queue status

### Cache

- `POST /cache/put` - Store in cache
- `GET /cache/get/{key}` - Retrieve from cache
- `GET /cache/stats` - Cache statistics

### Raft

- `GET /raft/status` - Node status
- `POST /raft/request-vote` - Vote request
- `POST /raft/append-entries` - Log replication

## Fault Tolerance

### Handled Scenarios

1. **Node Failure**: Failure detector identifies dead nodes
2. **Network Partition**: Message retry + partition detection
3. **Deadlock**: Wait-for graph cycle detection
4. **Message Loss**: Retry with exponential backoff
5. **Data Loss**: Persistence to disk

### Not Fully Implemented (Simulated)

- Byzantine fault tolerance
- Cross-cluster replication
- Log compaction
- Snapshotting

## Performance Characteristics

### Lock Manager

- **Acquire time**: O(1) if available, O(wait_queue) if blocked
- **Release time**: O(log waiting_requests) for queue processing
- **Deadlock detection**: O(V + E) DFS on wait-for graph

### Queue

- **Enqueue**: O(1)
- **Dequeue**: O(1)
- **Replication**: O(replication_factor)

### Cache

- **Get hit**: O(1)
- **Put**: O(log n) for LRU eviction
- **Invalidation**: O(sharers count)

## Scalability

- **Nodes**: 3 to N nodes supported
- **Locks**: Limited by memory
- **Queue messages**: Disk-limited
- **Cache size**: Configurable per node
- **Replication factor**: 1 to N copies
