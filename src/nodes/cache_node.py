from collections import OrderedDict
from typing import Dict, Optional, List, Set
from enum import Enum
from dataclasses import dataclass
import time


class CacheState(Enum):
    """MESI cache coherence protocol states"""
    MODIFIED = "M"      # Locally modified, not in memory, no other copy
    EXCLUSIVE = "E"     # Clean copy, only in this cache, not in memory
    SHARED = "S"        # Clean copy in this cache and possibly other caches
    INVALID = "I"       # Invalid/not present


class ReplacementPolicy(Enum):
    """Cache replacement policies"""
    LRU = "lru"  # Least Recently Used
    LFU = "lfu"  # Least Frequently Used


@dataclass
class CacheEntry:
    """Represents a cache entry with MESI state"""
    key: str
    value: any
    state: CacheState
    timestamp: float
    access_count: int = 0


class MESICacheNode:
    """
    Distributed cache using MESI (Modified-Exclusive-Shared-Invalid) protocol.
    Implements cache coherence across multiple nodes.
    
    Supports both LRU and LFU replacement policies.
    """
    
    def __init__(
        self,
        node_id: str,
        capacity: int = 100,
        peers: List[str] = None,
        policy: ReplacementPolicy = ReplacementPolicy.LRU
    ):
        self.node_id = node_id
        self.capacity = capacity
        self.peers = peers or []
        self.policy = policy
        
        # Cache storage
        self.cache: Dict[str, CacheEntry] = OrderedDict()
        
        # Track which other nodes have copies of each key
        self.sharers: Dict[str, Set[str]] = {}  # key -> set of nodes with SHARED copy
        
        # Pending invalidation acknowledgments
        self.pending_inv_acks: Dict[str, int] = {}  # key -> count of pending acks
        
        # Performance metrics
        self.hits = 0
        self.misses = 0
        self.coherence_operations = 0
        
        print(f"[{self.node_id}] MESICacheNode initialized: capacity={capacity}, policy={policy.value}")
    
    def get(self, key: str) -> Optional[any]:
        """
        Retrieve value from cache using MESI protocol.
        On SHARED state, checks coherence with other caches.
        Updates access count for LFU policy.
        """
        if key not in self.cache:
            self.misses += 1
            print(f"[{self.node_id}] [CACHE MISS] {key}")
            return None
        
        entry = self.cache[key]
        
        # Update access count for LFU
        entry.access_count += 1
        
        # Move to end for LRU
        if self.policy == ReplacementPolicy.LRU:
            self.cache.move_to_end(key)
        
        self.hits += 1
        print(f"[{self.node_id}] [CACHE HIT] {key} (state={entry.state.value}, access_count={entry.access_count})")
        
        return entry.value
    
    def put(self, key: str, value: any) -> Dict:
        """
        Store value in cache using MESI protocol.
        Transitions to MODIFIED state and invalidates other copies.
        """
        print(f"[{self.node_id}] [CACHE PUT] {key} = {value}")
        
        if key in self.cache:
            # Update existing entry
            entry = self.cache[key]
            entry.value = value
            entry.timestamp = time.time()
            entry.access_count += 1
            
            # Move to end for LRU
            if self.policy == ReplacementPolicy.LRU:
                self.cache.move_to_end(key)
        else:
            # Create new entry
            entry = CacheEntry(
                key=key,
                value=value,
                state=CacheState.EXCLUSIVE,
                timestamp=time.time(),
                access_count=1
            )
            self.cache[key] = entry
        
        # Transition to MODIFIED state
        entry.state = CacheState.MODIFIED
        
        # Invalidate copies in other caches
        self._invalidate_others(key)
        
        # Clean up eviction if exceeds capacity
        if len(self.cache) > self.capacity:
            self._evict()
        
        return {
            'key': key,
            'state': entry.state.value,
            'coherence_ops': self.coherence_operations
        }
    
    def _invalidate_others(self, key: str):
        """
        Send invalidation messages to other caches holding copies.
        Implements Invalidate protocol from MESI.
        """
        if key not in self.sharers:
            self.sharers[key] = set()
            return
        
        nodes_to_invalidate = self.sharers[key].copy()
        
        for node in nodes_to_invalidate:
            self._send_invalidate_message(key, node)
            self.coherence_operations += 1
        
        # Clear sharers after invalidation
        self.sharers[key].clear()
        
        print(f"[{self.node_id}] [INVALIDATE] {key} -> {len(nodes_to_invalidate)} nodes")
    
    def _send_invalidate_message(self, key: str, target_node: str):
        """
        Send invalidation message to target node.
        In real implementation, would use RPC/network.
        """
        print(f"[{self.node_id}] [SEND-INV] {key} -> {target_node}")
        # In real implementation: network.send(target_node, Invalidate(key))
    
    def handle_invalidate(self, key: str, sender: str) -> bool:
        """
        Handle invalidation message from another cache.
        Transitions entry to INVALID state.
        """
        if key in self.cache:
            entry = self.cache[key]
            old_state = entry.state
            entry.state = CacheState.INVALID
            
            print(f"[{self.node_id}] [RECV-INV] {key} from {sender} ({old_state.value} -> {entry.state.value})")
            
            # Send back acknowledgment
            self._send_inv_ack(key, sender)
            self.coherence_operations += 1
            
            return True
        
        return False
    
    def _send_inv_ack(self, key: str, target_node: str):
        """Send invalidation acknowledgment"""
        print(f"[{self.node_id}] [SEND-INV-ACK] {key} -> {target_node}")
    
    def handle_read_request(self, key: str, requester: str) -> Optional[any]:
        """
        Handle read request from another cache.
        May share data or return miss.
        """
        if key not in self.cache:
            print(f"[{self.node_id}] [READ-MISS] {key} from {requester}")
            return None
        
        entry = self.cache[key]
        
        if entry.state == CacheState.INVALID:
            return None
        
        # Add requester to sharers list
        if key not in self.sharers:
            self.sharers[key] = set()
        self.sharers[key].add(requester)
        
        # Transition to SHARED if currently EXCLUSIVE or MODIFIED
        if entry.state in [CacheState.EXCLUSIVE, CacheState.MODIFIED]:
            entry.state = CacheState.SHARED
        
        print(f"[{self.node_id}] [SEND-READ] {key} -> {requester} (state={entry.state.value})")
        
        return entry.value
    
    def _evict(self):
        """
        Evict entry based on configured replacement policy.
        Supports LRU (Least Recently Used) and LFU (Least Frequently Used).
        """
        if not self.cache:
            return
        
        if self.policy == ReplacementPolicy.LRU:
            self._evict_lru()
        elif self.policy == ReplacementPolicy.LFU:
            self._evict_lfu()
    
    def _evict_lru(self):
        """Evict least recently used entry when cache is full"""
        if not self.cache:
            return
        
        # Get first (oldest) entry - OrderedDict maintains insertion order
        key, entry = self.cache.popitem(last=False)
        
        print(f"[{self.node_id}] [LRU-EVICT] {key} (state={entry.state.value})")
        
        # Clean up sharers
        if key in self.sharers:
            del self.sharers[key]
    
    def _evict_lfu(self):
        """
        Evict least frequently used entry when cache is full.
        Breaks ties by selecting the least recently used.
        """
        if not self.cache:
            return
        
        # Find entry with minimum access count
        min_key = min(
            self.cache.keys(),
            key=lambda k: (self.cache[k].access_count, self.cache[k].timestamp)
        )
        
        entry = self.cache.pop(min_key)
        
        print(f"[{self.node_id}] [LFU-EVICT] {min_key} (access_count={entry.access_count}, state={entry.state.value})")
        
        # Clean up sharers
        if min_key in self.sharers:
            del self.sharers[min_key]
    
    def get_cache_stats(self) -> Dict:
        """Get cache performance statistics"""
        total_accesses = self.hits + self.misses
        hit_rate = (self.hits / total_accesses * 100) if total_accesses > 0 else 0
        
        state_distribution = {}
        for state in CacheState:
            count = sum(1 for entry in self.cache.values() if entry.state == state)
            state_distribution[state.value] = count
        
        # Calculate average access count
        avg_access_count = (
            sum(entry.access_count for entry in self.cache.values()) / len(self.cache)
            if self.cache
            else 0
        )
        
        return {
            'node_id': self.node_id,
            'cache_size': len(self.cache),
            'cache_capacity': self.capacity,
            'hits': self.hits,
            'misses': self.misses,
            'hit_rate': f"{hit_rate:.2f}%",
            'coherence_operations': self.coherence_operations,
            'replacement_policy': self.policy.value,
            'avg_access_count': f"{avg_access_count:.2f}",
            'state_distribution': state_distribution
        }
    
    def handle_write_update(self, key: str, value: any, sender: str):
        """
        Handle write-update message from another cache.
        Updates local copy if in SHARED state.
        """
        if key in self.cache and self.cache[key].state == CacheState.SHARED:
            self.cache[key].value = value
            self.cache[key].timestamp = time.time()
            print(f"[{self.node_id}] [RECV-UPDATE] {key} from {sender}")
            self.coherence_operations += 1
            return True
        
        return False
    
    def invalidate_all(self):
        """Invalidate all cache entries"""
        count = 0
        for entry in self.cache.values():
            entry.state = CacheState.INVALID
            count += 1
        
        print(f"[{self.node_id}] [FLUSH] Invalidated {count} entries")
        return count
    
    def set_policy(self, policy: ReplacementPolicy) -> None:
        """Switch replacement policy at runtime"""
        old_policy = self.policy
        self.policy = policy
        print(f"[{self.node_id}] Changed policy from {old_policy.value} to {policy.value}")
    
    def get(self, key: str) -> Optional[any]:
        """
        Retrieve value from cache using MESI protocol.
        On SHARED state, checks coherence with other caches.
        """
        if key not in self.cache:
            self.misses += 1
            print(f"[{self.node_id}] [CACHE MISS] {key}")
            return None
        
        entry = self.cache[key]
        
        # Move to end for LRU
        self.cache.move_to_end(key)
        entry.access_count += 1
        
        self.hits += 1
        print(f"[{self.node_id}] [CACHE HIT] {key} (state={entry.state.value})")
        
        return entry.value
    
    def put(self, key: str, value: any) -> Dict:
        """
        Store value in cache using MESI protocol.
        Transitions to MODIFIED state and invalidates other copies.
        """
        print(f"[{self.node_id}] [CACHE PUT] {key} = {value}")
        
        if key in self.cache:
            # Update existing entry
            self.cache.move_to_end(key)
            entry = self.cache[key]
            entry.value = value
            entry.timestamp = time.time()
        else:
            # Create new entry
            entry = CacheEntry(
                key=key,
                value=value,
                state=CacheState.EXCLUSIVE,
                timestamp=time.time()
            )
            self.cache[key] = entry
        
        # Transition to MODIFIED state
        entry.state = CacheState.MODIFIED
        
        # Invalidate copies in other caches
        self._invalidate_others(key)
        
        # Clean up eviction if exceeds capacity
        if len(self.cache) > self.capacity:
            self._evict()
        
        return {
            'key': key,
            'state': entry.state.value,
            'coherence_ops': self.coherence_operations
        }
    
    def _invalidate_others(self, key: str):
        """
        Send invalidation messages to other caches holding copies.
        Implements Invalidate protocol from MESI.
        """
        if key not in self.sharers:
            self.sharers[key] = set()
            return
        
        nodes_to_invalidate = self.sharers[key].copy()
        
        for node in nodes_to_invalidate:
            self._send_invalidate_message(key, node)
            self.coherence_operations += 1
        
        # Clear sharers after invalidation
        self.sharers[key].clear()
        
        print(f"[{self.node_id}] [INVALIDATE] {key} -> {len(nodes_to_invalidate)} nodes")
    
    def _send_invalidate_message(self, key: str, target_node: str):
        """
        Send invalidation message to target node.
        In real implementation, would use RPC/network.
        """
        print(f"[{self.node_id}] [SEND-INV] {key} -> {target_node}")
        # In real implementation: network.send(target_node, Invalidate(key))
    
    def handle_invalidate(self, key: str, sender: str) -> bool:
        """
        Handle invalidation message from another cache.
        Transitions entry to INVALID state.
        """
        if key in self.cache:
            entry = self.cache[key]
            old_state = entry.state
            entry.state = CacheState.INVALID
            
            print(f"[{self.node_id}] [RECV-INV] {key} from {sender} ({old_state.value} -> {entry.state.value})")
            
            # Send back acknowledgment
            self._send_inv_ack(key, sender)
            self.coherence_operations += 1
            
            return True
        
        return False
    
    def _send_inv_ack(self, key: str, target_node: str):
        """Send invalidation acknowledgment"""
        print(f"[{self.node_id}] [SEND-INV-ACK] {key} -> {target_node}")
    
    def handle_read_request(self, key: str, requester: str) -> Optional[any]:
        """
        Handle read request from another cache.
        May share data or return miss.
        """
        if key not in self.cache:
            print(f"[{self.node_id}] [READ-MISS] {key} from {requester}")
            return None
        
        entry = self.cache[key]
        
        if entry.state == CacheState.INVALID:
            return None
        
        # Add requester to sharers list
        if key not in self.sharers:
            self.sharers[key] = set()
        self.sharers[key].add(requester)
        
        # Transition to SHARED if currently EXCLUSIVE or MODIFIED
        if entry.state in [CacheState.EXCLUSIVE, CacheState.MODIFIED]:
            entry.state = CacheState.SHARED
        
        print(f"[{self.node_id}] [SEND-READ] {key} -> {requester} (state={entry.state.value})")
        
        return entry.value
    
    def _evict_lru(self):
        """Evict least recently used entry when cache is full"""
        if not self.cache:
            return
        
        # Get first (oldest) entry
        key, entry = self.cache.popitem(last=False)
        
        print(f"[{self.node_id}] [LRU-EVICT] {key} (state={entry.state.value})")
        
        # Clean up sharers
        if key in self.sharers:
            del self.sharers[key]
    
    def get_cache_stats(self) -> Dict:
        """Get cache performance statistics"""
        total_accesses = self.hits + self.misses
        hit_rate = (self.hits / total_accesses * 100) if total_accesses > 0 else 0
        
        state_distribution = {}
        for state in CacheState:
            count = sum(1 for entry in self.cache.values() if entry.state == state)
            state_distribution[state.value] = count
        
        return {
            'node_id': self.node_id,
            'cache_size': len(self.cache),
            'cache_capacity': self.capacity,
            'hits': self.hits,
            'misses': self.misses,
            'hit_rate': f"{hit_rate:.2f}%",
            'coherence_operations': self.coherence_operations,
            'state_distribution': state_distribution
        }
    
    def handle_write_update(self, key: str, value: any, sender: str):
        """
        Handle write-update message from another cache.
        Updates local copy if in SHARED state.
        """
        if key in self.cache and self.cache[key].state == CacheState.SHARED:
            self.cache[key].value = value
            self.cache[key].timestamp = time.time()
            print(f"[{self.node_id}] [RECV-UPDATE] {key} from {sender}")
            self.coherence_operations += 1
            return True
        
        return False
    
    def invalidate_all(self):
        """Invalidate all cache entries"""
        count = 0
        for entry in self.cache.values():
            entry.state = CacheState.INVALID
            count += 1
        
        print(f"[{self.node_id}] [FLUSH] Invalidated {count} entries")
        return count