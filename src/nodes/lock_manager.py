import asyncio
import time
from typing import Dict, Set, List, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict
import json
import os

@dataclass
class LockRequest:
    """Represents a lock request"""
    request_id: str
    node_id: str
    lock_type: str  # "exclusive" or "shared"
    resource_id: str
    timestamp: float

class DistributedLockManager:
    """
    Distributed lock manager using Raft for consensus.
    Supports both shared and exclusive locks with deadlock detection.
    """
    
    def __init__(self, node_id: str, peers: List[str], raft_node=None):
        self.node_id = node_id
        self.peers = peers
        self.raft_node = raft_node  # Reference to Raft consensus node
        
        # Lock state - replicated across all nodes via Raft
        self.exclusive_locks: Dict[str, str] = {}  # resource_id -> node_id
        self.shared_locks: Dict[str, Set[str]] = defaultdict(set)  # resource_id -> set of node_ids
        
        # Wait queues
        self.waiting_queue: Dict[str, List[LockRequest]] = defaultdict(list)
        self.lock_table: Dict[str, LockRequest] = {}  # request_id -> LockRequest
        
        # Deadlock detection - wait-for graph
        self.wait_for_graph: Dict[str, Set[str]] = defaultdict(set)
        
        # Lock state file for persistence
        self.state_file = f"data/locks_{node_id}.json"
        self._load_lock_state()
    
    def _load_lock_state(self):
        """Load lock state from disk"""
        os.makedirs("data", exist_ok=True)
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    data = json.load(f)
                    self.exclusive_locks = data.get('exclusive_locks', {})
                    self.shared_locks = defaultdict(set)
                    for res_id, nodes in data.get('shared_locks', {}).items():
                        self.shared_locks[res_id] = set(nodes)
                    print(f"[{self.node_id}] Loaded lock state from disk")
            except Exception as e:
                print(f"[{self.node_id}] Error loading lock state: {e}")
    
    def _save_lock_state(self):
        """Save lock state to disk"""
        try:
            data = {
                'exclusive_locks': self.exclusive_locks,
                'shared_locks': {k: list(v) for k, v in self.shared_locks.items()}
            }
            with open(self.state_file, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            print(f"[{self.node_id}] Error saving lock state: {e}")
    
    def acquire_exclusive(self, node_id: str, resource_id: str, request_id: str) -> bool:
        """
        Acquire an exclusive lock on a resource.
        Exclusive lock blocks both shared and exclusive locks from other nodes.
        """
        print(f"[{self.node_id}] {node_id} requests exclusive lock on {resource_id}")
        
        # Check if resource is locked
        if resource_id in self.exclusive_locks or resource_id in self.shared_locks:
            request = LockRequest(
                request_id=request_id,
                node_id=node_id,
                lock_type="exclusive",
                resource_id=resource_id,
                timestamp=time.time()
            )
            self.waiting_queue[resource_id].append(request)
            self.lock_table[request_id] = request
            
            # Update wait-for graph for deadlock detection
            if resource_id in self.exclusive_locks:
                holder = self.exclusive_locks[resource_id]
                if holder != node_id:
                    self.wait_for_graph[node_id].add(holder)
            
            # Check for deadlock
            if self._detect_cycle(node_id):
                print(f"[{self.node_id}] DEADLOCK DETECTED for {node_id}")
                self.waiting_queue[resource_id].remove(request)
                del self.lock_table[request_id]
                return False
            
            return False
        
        # Grant exclusive lock
        self.exclusive_locks[resource_id] = node_id
        self._save_lock_state()
        print(f"[{self.node_id}] Granted exclusive lock to {node_id} on {resource_id}")
        return True
    
    def acquire_shared(self, node_id: str, resource_id: str, request_id: str) -> bool:
        """
        Acquire a shared lock on a resource.
        Shared lock allows multiple readers but blocks exclusive locks.
        """
        print(f"[{self.node_id}] {node_id} requests shared lock on {resource_id}")
        
        # Check if there's an exclusive lock
        if resource_id in self.exclusive_locks:
            request = LockRequest(
                request_id=request_id,
                node_id=node_id,
                lock_type="shared",
                resource_id=resource_id,
                timestamp=time.time()
            )
            self.waiting_queue[resource_id].append(request)
            self.lock_table[request_id] = request
            
            holder = self.exclusive_locks[resource_id]
            self.wait_for_graph[node_id].add(holder)
            
            if self._detect_cycle(node_id):
                print(f"[{self.node_id}] DEADLOCK DETECTED for {node_id}")
                self.waiting_queue[resource_id].remove(request)
                del self.lock_table[request_id]
                return False
            
            return False
        
        # Grant shared lock
        self.shared_locks[resource_id].add(node_id)
        self._save_lock_state()
        print(f"[{self.node_id}] Granted shared lock to {node_id} on {resource_id}")
        return True
    
    def release_lock(self, node_id: str, resource_id: str) -> bool:
        """Release a lock (exclusive or shared) held by a node"""
        print(f"[{self.node_id}] {node_id} releases lock on {resource_id}")
        
        # Release exclusive lock
        if resource_id in self.exclusive_locks and self.exclusive_locks[resource_id] == node_id:
            del self.exclusive_locks[resource_id]
            print(f"[{self.node_id}] Released exclusive lock from {node_id} on {resource_id}")
        
        # Release shared lock
        elif resource_id in self.shared_locks and node_id in self.shared_locks[resource_id]:
            self.shared_locks[resource_id].discard(node_id)
            if not self.shared_locks[resource_id]:
                del self.shared_locks[resource_id]
            print(f"[{self.node_id}] Released shared lock from {node_id} on {resource_id}")
        else:
            return False
        
        # Clean up wait-for graph
        self.wait_for_graph[node_id].clear()
        
        # Try to grant locks from waiting queue
        self._process_waiting_queue(resource_id)
        
        self._save_lock_state()
        return True
    
    def _process_waiting_queue(self, resource_id: str):
        """Grant locks to waiting requests if possible"""
        while self.waiting_queue[resource_id]:
            request = self.waiting_queue[resource_id][0]
            
            if request.lock_type == "exclusive":
                if not self.exclusive_locks.get(resource_id) and not self.shared_locks.get(resource_id):
                    self.exclusive_locks[resource_id] = request.node_id
                    self.waiting_queue[resource_id].pop(0)
                    del self.lock_table[request.request_id]
                    print(f"[{self.node_id}] Granted queued exclusive lock to {request.node_id}")
                else:
                    break
            
            elif request.lock_type == "shared":
                if not self.exclusive_locks.get(resource_id):
                    self.shared_locks[resource_id].add(request.node_id)
                    self.waiting_queue[resource_id].pop(0)
                    del self.lock_table[request.request_id]
                    print(f"[{self.node_id}] Granted queued shared lock to {request.node_id}")
                else:
                    break
    
    def _detect_cycle(self, node_id: str) -> bool:
        """
        Detect cycles in wait-for graph using DFS.
        Returns True if a cycle is detected (deadlock).
        """
        visited = set()
        rec_stack = set()
        
        def has_cycle(node):
            visited.add(node)
            rec_stack.add(node)
            
            for neighbor in self.wait_for_graph.get(node, set()):
                if neighbor not in visited:
                    if has_cycle(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True
            
            rec_stack.remove(node)
            return False
        
        return has_cycle(node_id)
    
    def get_lock_status(self, resource_id: str) -> Dict:
        """Get current lock status of a resource"""
        return {
            'resource_id': resource_id,
            'exclusive_lock_holder': self.exclusive_locks.get(resource_id),
            'shared_lock_holders': list(self.shared_locks.get(resource_id, set())),
            'waiting_requests': len(self.waiting_queue.get(resource_id, []))
        }
    
    def handle_network_partition(self):
        """
        Handle network partition - clear locks that can't be replicated.
        In a real implementation, this would coordinate with Raft.
        """
        print(f"[{self.node_id}] Handling network partition")
        # This would be coordinated via Raft in a real implementation
        pass
    
    async def replicate_via_raft(self, operation: Dict):
        """Replicate lock operations through Raft"""
        if self.raft_node:
            self.raft_node.append_entry(operation)
            print(f"[{self.node_id}] Replicated lock operation via Raft: {operation}")
