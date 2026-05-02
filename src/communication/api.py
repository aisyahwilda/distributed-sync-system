from fastapi import FastAPI
from src.nodes.lock_manager import LockManager
from src.nodes.queue_node import DistributedQueue
from src.nodes.cache_node import CacheNode

app = FastAPI()

# INIT SYSTEM
lock_manager = LockManager()
queue = DistributedQueue(["node1", "node2", "node3"])
cache = CacheNode()

# LOCK ENDPOINT
@app.post("/lock/exclusive/{node_id}")
def acquire_lock(node_id: str):
    return {"success": lock_manager.acquire_exclusive(node_id)}

@app.post("/lock/release/{node_id}")
def release_lock(node_id: str):
    lock_manager.release(node_id)
    return {"status": "released"}

# QUEUE ENDPOINT
@app.post("/queue/enqueue")
def enqueue(key: str, message: str):
    queue.enqueue(key, message)
    return {"status": "queued"}

@app.get("/queue/dequeue")
def dequeue(key: str):
    msg = queue.dequeue(key)
    return {"message": msg}

# CACHE ENDPOINT
@app.post("/cache/put")
def put(key: str, value: str):
    cache.put(key, value)
    return {"status": "stored"}

@app.get("/cache/get")
def get(key: str):
    return {"value": cache.get(key)}