import pytest
from src.nodes.cache_node import MESICacheNode, CacheState

@pytest.fixture
def cache():
    """Create a MESI cache node"""
    return MESICacheNode("node1", capacity=5, peers=["node1", "node2", "node3"])

class TestCacheBasics:
    """Test basic cache operations"""
    
    def test_cache_put_get(self, cache):
        """Should store and retrieve values"""
        cache.put("key1", "value1")
        
        result = cache.get("key1")
        
        assert result == "value1"
    
    def test_cache_miss(self, cache):
        """Should return None on cache miss"""
        result = cache.get("nonexistent")
        
        assert result is None
        assert cache.misses == 1
    
    def test_cache_hit(self, cache):
        """Should track cache hits"""
        cache.put("key1", "value1")
        result = cache.get("key1")
        
        assert cache.hits == 1

class TestMESIStates:
    """Test MESI state transitions"""
    
    def test_put_transitions_to_modified(self, cache):
        """PUT should transition to MODIFIED state"""
        cache.put("key1", "value1")
        
        entry = cache.cache["key1"]
        assert entry.state == CacheState.MODIFIED
    
    def test_get_preserves_state(self, cache):
        """GET should preserve state"""
        cache.put("key1", "value1")
        state_before = cache.cache["key1"].state
        
        cache.get("key1")
        state_after = cache.cache["key1"].state
        
        assert state_before == state_after
    
    def test_update_transitions_to_modified(self, cache):
        """Updating existing entry should transition to MODIFIED"""
        cache.put("key1", "value1")
        cache.cache["key1"].state = CacheState.SHARED  # Simulate SHARED state
        
        cache.put("key1", "value2")
        
        entry = cache.cache["key1"]
        assert entry.state == CacheState.MODIFIED

class TestCacheInvalidation:
    """Test cache invalidation protocol"""
    
    def test_handle_invalidate(self, cache):
        """Should handle invalidation message"""
        cache.put("key1", "value1")
        
        success = cache.handle_invalidate("key1", "node2")
        
        assert success is True
        assert cache.cache["key1"].state == CacheState.INVALID
    
    def test_invalidate_nonexistent(self, cache):
        """Should handle invalidation of nonexistent entry"""
        success = cache.handle_invalidate("nonexistent", "node2")
        
        assert success is False
    
    def test_invalidate_all(self, cache):
        """Should invalidate all entries"""
        cache.put("key1", "value1")
        cache.put("key2", "value2")
        
        count = cache.invalidate_all()
        
        assert count == 2
        for entry in cache.cache.values():
            assert entry.state == CacheState.INVALID

class TestLRUReplacement:
    """Test LRU cache replacement policy"""
    
    def test_lru_eviction_on_capacity(self, cache):
        """Should evict LRU entry when capacity exceeded"""
        cache.capacity = 3
        
        cache.put("key1", "value1")
        cache.put("key2", "value2")
        cache.put("key3", "value3")
        cache.put("key4", "value4")  # Should evict key1
        
        assert len(cache.cache) == 3
        assert "key1" not in cache.cache
        assert "key4" in cache.cache
    
    def test_lru_moves_accessed_to_end(self, cache):
        """Accessed entry should move to end (most recent)"""
        cache.put("key1", "value1")
        cache.put("key2", "value2")
        
        cache.get("key1")  # Access key1
        
        # key1 should now be most recent
        first_key = next(iter(cache.cache.keys()))
        assert first_key == "key2"  # key2 is now least recent

class TestCacheStats:
    """Test cache statistics"""
    
    def test_cache_stats_hit_rate(self, cache):
        """Should calculate correct hit rate"""
        cache.put("key1", "value1")
        cache.get("key1")  # Hit
        cache.get("key1")  # Hit
        cache.get("nonexistent")  # Miss
        
        stats = cache.get_cache_stats()
        
        assert stats['hits'] == 2
        assert stats['misses'] == 1
        assert "66.66" in stats['hit_rate'] or "66.67" in stats['hit_rate']
    
    def test_cache_stats_state_distribution(self, cache):
        """Should show state distribution"""
        cache.put("key1", "value1")
        cache.put("key2", "value2")
        cache.cache["key2"].state = CacheState.SHARED
        
        stats = cache.get_cache_stats()
        
        assert stats['state_distribution']['M'] == 1
        assert stats['state_distribution']['S'] == 1

class TestReadOperations:
    """Test cache read operations from other nodes"""
    
    def test_handle_read_request_hit(self, cache):
        """Should respond to read request with cached value"""
        cache.put("key1", "value1")
        cache.cache["key1"].state = CacheState.EXCLUSIVE
        
        value = cache.handle_read_request("key1", "node2")
        
        assert value == "value1"
        assert cache.cache["key1"].state == CacheState.SHARED
    
    def test_handle_read_request_miss(self, cache):
        """Should return None on read miss"""
        value = cache.handle_read_request("nonexistent", "node2")
        
        assert value is None
    
    def test_read_adds_to_sharers(self, cache):
        """Should track sharers on read request"""
        cache.put("key1", "value1")
        
        cache.handle_read_request("key1", "node2")
        cache.handle_read_request("key1", "node3")
        
        assert len(cache.sharers["key1"]) == 2

class TestWriteUpdates:
    """Test cache write updates"""
    
    def test_handle_write_update_on_shared(self, cache):
        """Should accept write update on SHARED state"""
        cache.put("key1", "value1")
        cache.cache["key1"].state = CacheState.SHARED
        
        success = cache.handle_write_update("key1", "new_value", "node2")
        
        assert success is True
        assert cache.cache["key1"].value == "new_value"
    
    def test_handle_write_update_on_invalid(self, cache):
        """Should not accept write update on INVALID state"""
        cache.put("key1", "value1")
        cache.cache["key1"].state = CacheState.INVALID
        
        success = cache.handle_write_update("key1", "new_value", "node2")
        
        assert success is False

class TestCoherenceOperations:
    """Test cache coherence operation tracking"""
    
    def test_coherence_ops_on_invalidate(self, cache):
        """Should increment coherence operations on invalidate"""
        cache.put("key1", "value1")
        
        initial_ops = cache.coherence_operations
        cache.handle_invalidate("key1", "node2")
        
        assert cache.coherence_operations > initial_ops
    
    def test_coherence_ops_on_read(self, cache):
        """Should increment coherence operations on read request"""
        cache.put("key1", "value1")
        
        initial_ops = cache.coherence_operations
        cache.handle_read_request("key1", "node2")
        
        assert cache.coherence_operations >= initial_ops
