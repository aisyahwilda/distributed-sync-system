import pytest
from src.nodes.lock_manager import DistributedLockManager

@pytest.fixture
def lock_manager():
    """Create a distributed lock manager"""
    return DistributedLockManager("node1", ["node1", "node2", "node3"])

class TestExclusiveLocks:
    """Test exclusive lock acquisition and release"""
    
    def test_acquire_exclusive_lock_success(self, lock_manager):
        """Node should acquire exclusive lock on available resource"""
        success = lock_manager.acquire_exclusive("node1", "resource_x", "req1")
        
        assert success is True
        assert lock_manager.exclusive_locks["resource_x"] == "node1"
    
    def test_exclusive_lock_blocks_other_exclusive(self, lock_manager):
        """Exclusive lock should block other exclusive locks"""
        lock_manager.acquire_exclusive("node1", "resource_x", "req1")
        
        success = lock_manager.acquire_exclusive("node2", "resource_x", "req2")
        
        assert success is False
        assert "req2" in lock_manager.lock_table
    
    def test_exclusive_lock_blocks_shared(self, lock_manager):
        """Exclusive lock should block shared locks"""
        lock_manager.acquire_exclusive("node1", "resource_x", "req1")
        
        success = lock_manager.acquire_shared("node2", "resource_x", "req2")
        
        assert success is False

class TestSharedLocks:
    """Test shared lock acquisition and release"""
    
    def test_acquire_shared_lock_success(self, lock_manager):
        """Multiple nodes should acquire shared locks"""
        success1 = lock_manager.acquire_shared("node1", "resource_x", "req1")
        success2 = lock_manager.acquire_shared("node2", "resource_x", "req2")
        
        assert success1 is True
        assert success2 is True
        assert len(lock_manager.shared_locks["resource_x"]) == 2
    
    def test_shared_lock_blocks_exclusive(self, lock_manager):
        """Shared locks should block exclusive locks"""
        lock_manager.acquire_shared("node1", "resource_x", "req1")
        
        success = lock_manager.acquire_exclusive("node2", "resource_x", "req2")
        
        assert success is False

class TestLockRelease:
    """Test lock release"""
    
    def test_release_exclusive_lock(self, lock_manager):
        """Should release exclusive lock"""
        lock_manager.acquire_exclusive("node1", "resource_x", "req1")
        
        success = lock_manager.release_lock("node1", "resource_x")
        
        assert success is True
        assert "resource_x" not in lock_manager.exclusive_locks
    
    def test_release_shared_lock(self, lock_manager):
        """Should release shared lock"""
        lock_manager.acquire_shared("node1", "resource_x", "req1")
        
        success = lock_manager.release_lock("node1", "resource_x")
        
        assert success is True
        assert "node1" not in lock_manager.shared_locks["resource_x"]

class TestDeadlockDetection:
    """Test deadlock detection"""
    
    def test_no_deadlock_simple_wait(self, lock_manager):
        """Simple wait should not be detected as deadlock"""
        lock_manager.acquire_exclusive("node1", "resource_x", "req1")
        
        success = lock_manager.acquire_exclusive("node2", "resource_x", "req2")
        
        assert success is False
        # No cycle yet
        assert not lock_manager._detect_cycle("node2")
    
    def test_deadlock_detection_on_cycle(self, lock_manager):
        """Should detect deadlock when wait-for cycle exists"""
        # Create a simple wait-for cycle
        lock_manager.wait_for_graph["node1"].add("node2")
        lock_manager.wait_for_graph["node2"].add("node1")
        
        has_cycle = lock_manager._detect_cycle("node1")
        
        assert has_cycle is True
    
    def test_deadlock_detection_complex_cycle(self, lock_manager):
        """Should detect complex wait-for cycle (node1 -> node2 -> node3 -> node1)"""
        lock_manager.wait_for_graph["node1"].add("node2")
        lock_manager.wait_for_graph["node2"].add("node3")
        lock_manager.wait_for_graph["node3"].add("node1")
        
        has_cycle = lock_manager._detect_cycle("node1")
        
        assert has_cycle is True

class TestLockProcessing:
    """Test lock queue processing"""
    
    def test_process_waiting_queue_exclusive(self, lock_manager):
        """Should grant queued exclusive locks when resource is released"""
        # First lock holder
        lock_manager.acquire_exclusive("node1", "resource_x", "req1")
        
        # Second tries to lock (queued)
        lock_manager.acquire_exclusive("node2", "resource_x", "req2")
        
        # Release first lock
        lock_manager.release_lock("node1", "resource_x")
        
        # Second should now hold lock
        assert lock_manager.exclusive_locks["resource_x"] == "node2"
    
    def test_process_waiting_queue_shared(self, lock_manager):
        """Should grant multiple queued shared locks"""
        # Exclusive lock holder
        lock_manager.acquire_exclusive("node1", "resource_x", "req1")
        
        # Two request shared locks (queued)
        lock_manager.acquire_shared("node2", "resource_x", "req2")
        lock_manager.acquire_shared("node3", "resource_x", "req3")
        
        # Release exclusive lock
        lock_manager.release_lock("node1", "resource_x")
        
        # Both should now hold shared locks
        assert len(lock_manager.shared_locks["resource_x"]) == 2

class TestLockPersistence:
    """Test lock state persistence"""
    
    def test_lock_state_saved(self, lock_manager):
        """Lock state should be saved to disk"""
        lock_manager.acquire_exclusive("node1", "resource_x", "req1")
        lock_manager._save_lock_state()
        
        import os
        assert os.path.exists(lock_manager.state_file)
    
    def test_lock_state_loaded(self, lock_manager):
        """Lock state should be loaded from disk"""
        lock_manager.acquire_exclusive("node1", "resource_x", "req1")
        lock_manager._save_lock_state()
        
        # Create new manager and load state
        new_manager = DistributedLockManager("node1", ["node1", "node2", "node3"])
        
        assert "resource_x" in new_manager.exclusive_locks
        assert new_manager.exclusive_locks["resource_x"] == "node1"
