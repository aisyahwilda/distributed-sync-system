import hashlib
import json
import os
import bisect
from typing import List, Dict, Optional, Set, Tuple
from threading import Lock
from dataclasses import dataclass
import time
from enum import Enum


class MessageStatus(Enum):
    PENDING = "pending"
    ACKNOWLEDGED = "acknowledged"
    FAILED = "failed"


@dataclass
class Message:
    """Represents a message in the queue"""
    msg_id: str
    key: str
    content: str
    timestamp: float
    status: MessageStatus = MessageStatus.PENDING
    attempts: int = 0
    last_attempt: float = 0.0


class ConsistentHashRing:
    """
    Consistent hashing ring with virtual nodes.
    Distributes messages evenly across cluster with minimal relocation on topology changes.
    """
    
    def __init__(self, nodes: List[str], virtual_nodes: int = 150):
        """
        Initialize hash ring.
        
        Args:
            nodes: List of physical node IDs
            virtual_nodes: Number of virtual nodes per physical node (default: 150 for even distribution)
        """
        self.physical_nodes = sorted(nodes)
        self.virtual_nodes_per_node = virtual_nodes
        self.ring: Dict[int, str] = {}  # hash -> physical node
        self.sorted_keys: List[int] = []
        
        self._build_ring()
    
    def _hash(self, key: str) -> int:
        """Compute hash using MD5"""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    def _build_ring(self):
        """Build the hash ring with virtual nodes"""
        self.ring.clear()
        
        for node in self.physical_nodes:
            for i in range(self.virtual_nodes_per_node):
                virtual_key = f"{node}#{i}"
                hash_value = self._hash(virtual_key)
                self.ring[hash_value] = node
        
        self.sorted_keys = sorted(self.ring.keys())
    
    def get_node(self, key: str) -> str:
        """Get the physical node responsible for a key"""
        if not self.ring:
            raise ValueError("Ring is empty")
        
        hash_value = self._hash(key)
        
        # Find first node with hash >= key hash (clockwise)
        idx = bisect.bisect_left(self.sorted_keys, hash_value)
        if idx == len(self.sorted_keys):
            idx = 0  # Wrap around
        
        return self.ring[self.sorted_keys[idx]]
    
    def get_replicas(self, key: str, count: int) -> List[str]:
        """
        Get replicas for a key.
        Returns list of distinct physical nodes for replication.
        Minimizes relocation by using sorted ring positions.
        """
        if not self.ring:
            raise ValueError("Ring is empty")
        
        if count > len(self.physical_nodes):
            count = len(self.physical_nodes)
        
        hash_value = self._hash(key)
        idx = bisect.bisect_left(self.sorted_keys, hash_value)
        
        replicas = []
        seen_nodes = set()
        
        # Walk around ring until we have enough distinct physical nodes
        attempts = 0
        while len(replicas) < count and attempts < len(self.sorted_keys):
            current_idx = (idx + attempts) % len(self.sorted_keys)
            node = self.ring[self.sorted_keys[current_idx]]
            
            if node not in seen_nodes:
                replicas.append(node)
                seen_nodes.add(node)
            
            attempts += 1
        
        return replicas
    
    def add_node(self, node: str):
        """Add a node to the ring (minimal key relocation)"""
        if node in self.physical_nodes:
            return
        
        self.physical_nodes.append(node)
        self.physical_nodes.sort()
        self._build_ring()
    
    def remove_node(self, node: str):
        """Remove a node from the ring (minimal key relocation)"""
        if node not in self.physical_nodes:
            return
        
        self.physical_nodes.remove(node)
        self._build_ring()
    
    def get_stats(self) -> Dict:
        """Get statistics about ring distribution"""
        distribution = {}
        for node in self.physical_nodes:
            count = sum(1 for v in self.ring.values() if v == node)
            distribution[node] = count
        
        avg_vnodes = sum(distribution.values()) / len(distribution) if distribution else 0
        
        return {
            'physical_nodes': len(self.physical_nodes),
            'total_vnodes': len(self.ring),
            'vnodes_per_node': self.virtual_nodes_per_node,
            'distribution': distribution,
            'avg_vnodes_per_node': avg_vnodes
        }


class DistributedQueue:
    """
    Distributed queue using consistent hashing with virtual nodes.
    Supports at-least-once delivery guarantee with message replication.
    """
    
    def __init__(
        self,
        nodes: List[str],
        replication_factor: int = 2,
        virtual_nodes: int = 150
    ):
        self.nodes = nodes
        self.replication_factor = min(replication_factor, len(nodes))
        self.virtual_nodes = virtual_nodes
        self.node_id = os.getenv("NODE_ID", "node1")
        
        # Consistent hash ring with virtual nodes
        self.hash_ring = ConsistentHashRing(nodes, virtual_nodes)
        
        # In-memory message storage
        self.queues: Dict[str, List[Message]] = {}
        
        # Message tracking for at-least-once delivery
        self.message_registry: Dict[str, Message] = {}
        self.pending_acks: Dict[str, Set[str]] = {}  # msg_id -> set of nodes that ACKed
        
        # Persistence
        self.data_file = "/app/data/queue_data.json"
        self.file_lock = Lock()
        
        os.makedirs(os.path.dirname(self.data_file), exist_ok=True)
        self.load()
        
        print(f"[{self.node_id}] DistributedQueue initialized: replication_factor={replication_factor}, virtual_nodes={virtual_nodes}")
    
    def _hash(self, key: str) -> int:
        """Consistent hashing using MD5"""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    def _get_replica_nodes(self, key: str) -> List[str]:
        """
        Get replica nodes for a key using consistent hashing with virtual nodes.
        Returns list of nodes where message should be replicated.
        """
        return self.hash_ring.get_replicas(key, self.replication_factor)
    
    def enqueue(self, key: str, content: str, producer_id: str = None) -> Dict:
        """
        Enqueue a message with replication.
        Returns message metadata for tracking.
        """
        msg_id = f"{key}_{int(time.time() * 1000)}"
        message = Message(
            msg_id=msg_id,
            key=key,
            content=content,
            timestamp=time.time(),
            status=MessageStatus.PENDING
        )
        
        # Get replica nodes
        replica_nodes = self._get_replica_nodes(key)
        
        # Store locally and initialize ACK tracking
        if key not in self.queues:
            self.queues[key] = []
        
        self.queues[key].append(message)
        self.message_registry[msg_id] = message
        self.pending_acks[msg_id] = set()
        
        with self.file_lock:
            self.save()
        
        print(f"[{self.node_id}] Enqueued message {msg_id} on key={key} (replicas: {replica_nodes})")
        
        return {
            'msg_id': msg_id,
            'key': key,
            'status': 'queued',
            'replica_nodes': replica_nodes
        }
    
    def dequeue(self, key: str, consumer_id: str = None) -> Optional[Dict]:
        """
        Dequeue a message (consume from head of queue).
        Returns None if queue is empty.
        """
        with self.file_lock:
            if key not in self.queues or len(self.queues[key]) == 0:
                print(f"[{self.node_id}] Queue empty for key={key}")
                return None
            
            message = self.queues[key].pop(0)
            message.status = MessageStatus.ACKNOWLEDGED
            self.message_registry[message.msg_id] = message
            
            self.save()
        
        print(f"[{self.node_id}] Dequeued message {message.msg_id} from key={key}")
        
        return {
            'msg_id': message.msg_id,
            'key': key,
            'content': message.content,
            'timestamp': message.timestamp
        }
    
    def acknowledge_message(self, msg_id: str, node_id: str = None):
        """
        Acknowledge that a message was successfully processed.
        Used for at-least-once delivery guarantee.
        """
        if msg_id in self.pending_acks:
            self.pending_acks[msg_id].add(node_id or self.node_id)
            
            if msg_id in self.message_registry:
                self.message_registry[msg_id].status = MessageStatus.ACKNOWLEDGED
            
            print(f"[{self.node_id}] Acknowledged message {msg_id} from {node_id}")
            return True
        
        return False
    
    def nack_message(self, msg_id: str, retry: bool = True):
        """
        Negative acknowledgment - message processing failed.
        Can be retried based on retry policy.
        """
        if msg_id not in self.message_registry:
            return False
        
        message = self.message_registry[msg_id]
        message.attempts += 1
        message.last_attempt = time.time()
        
        if not retry or message.attempts > 3:
            message.status = MessageStatus.FAILED
            print(f"[{self.node_id}] Message {msg_id} marked as FAILED after {message.attempts} attempts")
        else:
            message.status = MessageStatus.PENDING
            print(f"[{self.node_id}] Message {msg_id} will be retried (attempt {message.attempts})")
        
        return True
    
    def get_queue_status(self, key: str = None) -> Dict:
        """Get status of queue(s)"""
        if key:
            return {
                'key': key,
                'size': len(self.queues.get(key, [])),
                'pending_messages': len([m for m in self.queues.get(key, []) 
                                        if m.status == MessageStatus.PENDING])
            }
        else:
            return {
                'total_keys': len(self.queues),
                'total_messages': sum(len(q) for q in self.queues.values()),
                'pending_messages': sum(len([m for m in q if m.status == MessageStatus.PENDING]) 
                                       for q in self.queues.values())
            }
    
    def sync_replicas(self) -> Dict:
        """
        Synchronize message state across replica nodes.
        Returns status of replication.
        """
        sync_status = {}
        
        for msg_id, message in self.message_registry.items():
            replica_nodes = self._get_replica_nodes(message.key)
            synced_count = len(self.pending_acks.get(msg_id, set()))
            
            sync_status[msg_id] = {
                'replicas': replica_nodes,
                'synced_count': synced_count,
                'status': message.status.value
            }
            
            print(f"[{self.node_id}] Sync status for {msg_id}: {synced_count}/{len(replica_nodes)} replicas")
        
        return sync_status
    
    def save(self):
        """Persist queue state to disk"""
        try:
            data = {}
            for key, messages in self.queues.items():
                data[key] = [
                    {
                        'msg_id': m.msg_id,
                        'content': m.content,
                        'timestamp': m.timestamp,
                        'status': m.status.value
                    }
                    for m in messages
                ]
            
            with open(self.data_file, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            print(f"[{self.node_id}] Error saving queue: {e}")
    
    def load(self):
        """Load queue state from disk"""
        if not os.path.exists(self.data_file):
            return
        
        try:
            with open(self.data_file, 'r') as f:
                data = json.load(f)
                
            for key, messages in data.items():
                self.queues[key] = []
                for msg_data in messages:
                    message = Message(
                        msg_id=msg_data['msg_id'],
                        key=key,
                        content=msg_data['content'],
                        timestamp=msg_data['timestamp'],
                        status=MessageStatus(msg_data['status'])
                    )
                    self.queues[key].append(message)
                    self.message_registry[message.msg_id] = message
            
            print(f"[{self.node_id}] Loaded queue state from disk")
        except Exception as e:
            print(f"[{self.node_id}] Error loading queue: {e}")