from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
import time
import asyncio
from src.nodes.lock_manager import DistributedLockManager
from src.nodes.queue_node import DistributedQueue
from src.nodes.cache_node import MESICacheNode
from src.consensus.raft import RaftNode, NodeState
import requests
from src.communication.failure_detector import PhiAccrualFailureDetector

# Initialize FastAPI app
app = FastAPI(title="Distributed Sync System", version="1.0.0")

# Track startup time
startup_time = time.time()

# Environment configuration
node_id = os.getenv("NODE_ID", "node1")
peers = os.getenv("PEERS", "node2,node3").split(",")
cache_size = int(os.getenv("CACHE_SIZE", 100))
replication_factor = int(os.getenv("REPLICATION_FACTOR", 2))

# Initialize components
raft_node = RaftNode(node_id, peers)
lock_manager = DistributedLockManager(node_id, peers, raft_node)
queue = DistributedQueue(peers, replication_factor=replication_factor)
cache = MESICacheNode(node_id, capacity=cache_size, peers=peers)
failure_detector = PhiAccrualFailureDetector(phi_threshold=5.0, initial_timeout=5.0)

# Startup event to run Raft monitoring
@app.on_event("startup")
async def startup_raft_monitor():
    """Start Raft monitoring task on app startup"""
    asyncio.create_task(raft_node.monitor())
    print(f"[{node_id}] Raft monitor started")

# Pydantic models for request/response
class LockRequest(BaseModel):
    node_id: str
    resource_id: str
    lock_type: str  # "exclusive" or "shared"

class QueueMessage(BaseModel):
    key: str
    content: str

class CacheData(BaseModel):
    key: str
    value: str = None


# Raft RPC request models
class RequestVoteRequest(BaseModel):
    candidate_id: str
    term: int
    last_log_index: int
    last_log_term: int


class AppendEntriesRequest(BaseModel):
    leader_id: str
    term: int
    prev_log_index: int
    prev_log_term: int
    entries: list = []
    leader_commit: int = 0

# ==================== LOCK ENDPOINTS ====================

@app.post("/lock/acquire")
async def acquire_lock(req: LockRequest):
    """Acquire a distributed lock"""
    request_id = f"{req.node_id}_{req.resource_id}_{int(__import__('time').time() * 1000)}"
    # If this node is not the Raft leader, forward the request to the leader
    try:
        if raft_node and raft_node.node_state != NodeState.LEADER and raft_node.leader_id:
            leader = raft_node.leader_id
            url = f"http://{leader}:8000/lock/acquire"
            payload = req.dict()
            # forward request to leader
            resp = requests.post(url, json=payload, timeout=2)
            return resp.json()
    except Exception:
        # fallback to local handling
        pass

    if req.lock_type == "exclusive":
        success = lock_manager.acquire_exclusive(req.node_id, req.resource_id, request_id)
    elif req.lock_type == "shared":
        success = lock_manager.acquire_shared(req.node_id, req.resource_id, request_id)
    else:
        raise HTTPException(status_code=400, detail="Invalid lock_type")
    
    return {
        "success": success,
        "request_id": request_id,
        "lock_type": req.lock_type,
        "resource_id": req.resource_id
    }

@app.post("/lock/release")
async def release_lock(req: LockRequest):
    """Release a distributed lock"""
    # If not leader, forward to leader
    try:
        if raft_node and raft_node.node_state != NodeState.LEADER and raft_node.leader_id:
            leader = raft_node.leader_id
            url = f"http://{leader}:8000/lock/release"
            payload = req.dict()
            resp = requests.post(url, json=payload, timeout=2)
            return resp.json()
    except Exception:
        pass

    success = lock_manager.release_lock(req.node_id, req.resource_id)
    
    return {
        "success": success,
        "node_id": req.node_id,
        "resource_id": req.resource_id
    }

@app.get("/lock/status/{resource_id}")
async def lock_status(resource_id: str):
    """Get lock status of a resource"""
    return lock_manager.get_lock_status(resource_id)

@app.get("/lock/deadlock-check/{node_id}")
async def check_deadlock(node_id: str):
    """Check if node is in deadlock"""
    is_deadlocked = lock_manager._detect_cycle(node_id)
    
    return {
        "node_id": node_id,
        "is_deadlocked": is_deadlocked
    }

# ==================== QUEUE ENDPOINTS ====================

@app.post("/queue/enqueue")
async def enqueue(msg: QueueMessage):
    """Enqueue a message with replication to peer nodes"""
    result = queue.enqueue(msg.key, msg.content)
    
    # Get replica nodes and replicate asynchronously
    try:
        replica_nodes = queue._get_replica_nodes(msg.key)
        for replica_node in replica_nodes:
            if replica_node != node_id:
                try:
                    url = f"http://{replica_node}:8000/queue/replicate"
                    requests.post(url, json={"key": msg.key, "content": msg.content}, timeout=1)
                    print(f"[{node_id}] Replicated {msg.key} to {replica_node}")
                except Exception as e:
                    print(f"[{node_id}] Failed to replicate to {replica_node}: {e}")
    except Exception as e:
        print(f"[{node_id}] Replication error: {e}")
    
    return result

@app.get("/queue/dequeue")
async def dequeue(key: str):
    """Dequeue a message"""
    msg = queue.dequeue(key)
    
    if msg is None:
        return {"message": None, "key": key}
    
    return msg

@app.post("/queue/acknowledge/{msg_id}")
async def acknowledge_message(msg_id: str):
    """Acknowledge message delivery"""
    success = queue.acknowledge_message(msg_id)
    
    return {"success": success, "msg_id": msg_id}

@app.post("/queue/nack/{msg_id}")
async def nack_message(msg_id: str, retry: bool = True):
    """Negative acknowledgment - message processing failed"""
    success = queue.nack_message(msg_id, retry)
    
    return {"success": success, "msg_id": msg_id}

@app.get("/queue/status")
async def queue_status():
    """Get queue status"""
    return queue.get_queue_status()

@app.get("/queue/sync-status")
async def queue_sync_status():
    """Get message replication sync status"""
    return queue.sync_replicas()

@app.post("/queue/replicate")
async def replicate_queue_message(msg: QueueMessage):
    """Receive replicated message from another node (internal use)"""
    from src.nodes.queue_node import Message, MessageStatus
    import time as time_mod
    
    if msg.key not in queue.queues:
        queue.queues[msg.key] = []
    
    message = Message(
        msg_id=f"{msg.key}_{int(time_mod.time() * 1000)}",
        key=msg.key,
        content=msg.content,
        timestamp=time_mod.time(),
        status=MessageStatus.PENDING
    )
    queue.queues[msg.key].append(message)
    queue.message_registry[message.msg_id] = message
    queue.save()
    print(f"[{node_id}] Received replicated message {msg.key}")
    
    return {"success": True, "msg_id": message.msg_id}

# ==================== CACHE ENDPOINTS ====================

@app.get("/cache/get/{key}")
async def get_cache(key: str):
    """Get value from cache"""
    value = cache.get(key)
    
    return {
        "key": key,
        "value": value,
        "found": value is not None
    }

@app.post("/cache/put")
async def put_cache(data: CacheData):
    """Put value in cache and propagate invalidation to peers"""
    result = cache.put(data.key, data.value)
    
    # Broadcast cache invalidation to all peer nodes
    for peer in peers:
        if peer != node_id:
            try:
                url = f"http://{peer}:8000/cache/invalidate-peer"
                requests.post(url, json={"key": data.key, "sender_node": node_id}, timeout=1)
                print(f"[{node_id}] Invalidated cache {data.key} on peer {peer}")
            except Exception as e:
                print(f"[{node_id}] Failed to invalidate peer {peer} cache: {e}")
    
    return result

@app.post("/cache/invalidate/{key}")
async def invalidate_cache(key: str, sender: str = None):
    """Invalidate cache entry"""
    success = cache.handle_invalidate(key, sender or node_id)
    
    return {
        "success": success,
        "key": key
    }

@app.post("/cache/invalidate-peer")
async def invalidate_cache_peer(data: dict):
    """Receive cache invalidation from peer node (internal)"""
    key = data.get("key")
    sender_node = data.get("sender_node", "unknown")
    if key:
        cache.invalidate(key)
        print(f"[{node_id}] Received cache invalidation for key={key} from peer {sender_node}")
    return {"success": True, "key": key}

@app.post("/cache/flush")
async def flush_cache():
    """Invalidate all cache entries"""
    count = cache.invalidate_all()
    
    return {
        "invalidated_count": count
    }

@app.get("/cache/stats")
async def cache_stats():
    """Get cache statistics"""
    return cache.get_cache_stats()

# ==================== RAFT ENDPOINTS ====================

@app.post("/raft/append-entries")
async def raft_append_entries(req: AppendEntriesRequest):
    """Handle AppendEntries RPC"""
    success = await raft_node.append_entries(
        req.leader_id, req.term, req.prev_log_index, req.prev_log_term,
        req.entries or [], req.leader_commit
    )

    return {
        "node_id": node_id,
        "term": raft_node.state.current_term,
        "success": success
    }

@app.post("/raft/request-vote")
async def raft_request_vote(req: RequestVoteRequest):
    """Handle RequestVote RPC"""
    vote_granted = await raft_node.request_vote(
        req.candidate_id, req.term, req.last_log_index, req.last_log_term
    )

    return {
        "node_id": node_id,
        "term": raft_node.state.current_term,
        "vote_granted": vote_granted
    }

@app.get("/raft/status")
async def raft_status():
    """Get Raft node status"""
    return {
        "node_id": node_id,
        "state": raft_node.node_state.value,
        "term": raft_node.state.current_term,
        "log_size": len(raft_node.state.log),
        "commit_index": raft_node.commit_index,
        "last_applied": raft_node.last_applied
    }

# ==================== FAILURE DETECTION ENDPOINTS ====================

@app.post("/failure-detector/heartbeat")
async def heartbeat_endpoint(node: str):
    """Record heartbeat from a node"""
    failure_detector.heartbeat(node)
    
    return {
        "node": node,
        "timestamp": __import__('time').time()
    }

@app.get("/failure-detector/status")
async def failure_detector_status():
    """Get failure detector status"""
    return failure_detector.get_cluster_status()

@app.get("/failure-detector/node-status/{node}")
async def node_status_endpoint(node: str):
    """Get status of a specific node"""
    return failure_detector.get_node_status(node)

# ==================== SYSTEM ENDPOINTS ====================

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    uptime_seconds = time.time() - startup_time

    # Prefer concise raft role string (leader/follower/candidate).
    try:
        raft_role = getattr(raft_node, 'node_state', None)
        if raft_role is None:
            raft_state = 'unknown'
        else:
            # If it's an Enum, use its value; otherwise str()
            raft_state = raft_role.value if hasattr(raft_role, 'value') else str(raft_role)
    except Exception:
        raft_state = 'unknown'

    return {
        "status": "healthy",
        "node_id": node_id,
        "raft_state": raft_state,
        "timestamp": time.time(),
        "uptime_seconds": round(uptime_seconds, 1)
    }

@app.get("/status")
async def system_status():
    """Get overall system status"""
    return {
        "node_id": node_id,
        "peers": peers,
        "raft": await raft_status(),
        "failure_detector": failure_detector.get_cluster_status(),
        "cache": cache.get_cache_stats(),
        "queue": queue.get_queue_status()
    }

# Prometheus metrics endpoint
@app.get("/metrics")
async def metrics():
    """Prometheus-compatible metrics export"""
    from src.utils.metrics import get_metrics
    metrics_collector = get_metrics(node_id)
    return metrics_collector.to_prometheus_format()

# Metrics summary endpoint
@app.get("/metrics/summary")
async def metrics_summary():
    """Get human-readable metrics summary"""
    from src.utils.metrics import get_metrics
    metrics_collector = get_metrics(node_id)
    return metrics_collector.get_summary()

# Root endpoint
@app.get("/")
async def root():
    """Root endpoint with API documentation link"""
    return {
        "message": "Distributed Sync System API",
        "node_id": node_id,
        "docs": "/docs",
        "openapi": "/openapi.json",
        "metrics": "/metrics",
        "metrics_summary": "/metrics/summary"
    }
