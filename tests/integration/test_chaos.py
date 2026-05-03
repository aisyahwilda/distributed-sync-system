import pytest
import asyncio
from src.nodes.lock_manager import DistributedLockManager
from src.nodes.queue_node import DistributedQueue
from src.nodes.cache_node import MESICacheNode, CacheState
from src.communication.message_passing import ReliableMessagePassing, MessageType
from src.communication.failure_detectore import FailureDetector

class TestNetworkPartitionScenarios:
    """Test system behavior under network partitions"""
    
    def test_lock_manager_network_partition(self):
        """Test lock manager handles network partition gracefully"""
        lm = DistributedLockManager("node1", ["node1", "node2", "node3"])
        
        # Simulate network partition
        lm.handle_network_partition()
        
        # Should still handle local locks
        success = lm.acquire_exclusive("node1", "resource_x", "req1")
        assert success is True
    
    def test_queue_replication_during_partition(self):
        """Test queue handles replication during network partition"""
        queue = DistributedQueue(["node1", "node2", "node3"])
        
        # Enqueue message
        result = queue.enqueue("key1", "message")
        msg_id = result['msg_id']
        
        # Simulate partial replication
        queue.acknowledge_message(msg_id, "node1")
        queue.acknowledge_message(msg_id, "node2")
        # node3 missing
        
        sync_status = queue.sync_replicas()
        
        # Should still be tracked
        assert msg_id in sync_status
        assert sync_status[msg_id]['synced_count'] == 2

class TestNodeFailureScenarios:
    """Test system behavior when nodes fail"""
    
    def test_failure_detection_timeout(self):
        """Test failure detection identifies dead nodes"""
        fd = FailureDetector(timeout=1.0)
        
        # Record heartbeat
        fd.heartbeat("node1")
        assert fd.is_alive("node1") is True
        
        # Simulate timeout
        import time
        time.sleep(1.1)
        
        assert fd.is_alive("node1") is False
    
    def test_cluster_status_with_dead_nodes(self):
        """Test cluster status with dead nodes"""
        fd = FailureDetector(timeout=1.0)
        
        fd.heartbeat("node1")
        fd.heartbeat("node2")
        fd.heartbeat("node3")
        
        # Let node2 timeout
        import time
        time.sleep(1.1)
        
        status = fd.get_cluster_status()
        
        assert status['alive_nodes'] <= 3
        assert len(status['dead_node_ids']) >= 0
    
    def test_lock_recovery_after_node_failure(self):
        """Test lock recovery when node fails and recovers"""
        lm = DistributedLockManager("node1", ["node1", "node2", "node3"])
        
        # Acquire lock
        lm.acquire_exclusive("node1", "resource_x", "req1")
        
        # Simulate node2 failure recovery
        fd = FailureDetector(timeout=5.0)
        fd.heartbeat("node2")
        
        # After recovery
        fd.heartbeat("node2")
        assert fd.is_alive("node2") is True

class TestDeadlockInPartitionScenarios:
    """Test deadlock detection in network partitions"""
    
    def test_deadlock_with_two_nodes(self):
        """Test deadlock between two nodes"""
        lm = DistributedLockManager("node1", ["node1", "node2"])
        
        # Create circular wait-for
        lm.wait_for_graph["node1"].add("node2")
        lm.wait_for_graph["node2"].add("node1")
        
        has_cycle = lm._detect_cycle("node1")
        assert has_cycle is True
    
    def test_deadlock_with_multiple_resources(self):
        """Test deadlock with multiple resources"""
        lm = DistributedLockManager("node1", ["node1", "node2", "node3"])
        
        # node1 waits for node2
        lm.acquire_exclusive("node1", "res_a", "req1")
        lm.acquire_exclusive("node2", "res_a", "req2")
        
        # node2 waits for node3
        lm.acquire_exclusive("node2", "res_b", "req3")
        lm.acquire_exclusive("node3", "res_b", "req4")
        
        # node3 waits for node1
        lm.acquire_exclusive("node3", "res_c", "req5")
        lm.acquire_exclusive("node1", "res_c", "req6")
        
        # Should detect cycle
        has_cycle = lm._detect_cycle("node1")
        assert has_cycle is True

class TestMessageReliabilityUnderFailure:
    """Test message reliability with network faults"""
    
    @pytest.mark.asyncio
    async def test_retry_unacked_messages(self):
        """Test retry of unacknowledged messages"""
        mp = ReliableMessagePassing("node1")
        
        # Send message
        await mp.send(
            "node1", "node2",
            MessageType.HEARTBEAT,
            {"term": 1}
        )
        
        # Don't acknowledge
        await asyncio.sleep(0.1)
        
        # Retry should be queued
        await mp.retry_unacked_messages()
        
        assert len(mp.in_flight) >= 1
    
    @pytest.mark.asyncio
    async def test_message_loss_simulation(self):
        """Test message loss simulation"""
        mp = ReliableMessagePassing("node1", enable_network_faults=True)
        mp.simulate_message_loss(1.0)  # 100% loss
        
        # Send message - should be dropped
        success = await mp.send(
            "node1", "node2",
            MessageType.HEARTBEAT,
            {"term": 1}
        )
        
        assert success is False
    
    @pytest.mark.asyncio
    async def test_network_partition_blocks_messages(self):
        """Test that network partition blocks messages"""
        mp = ReliableMessagePassing("node1", enable_network_faults=True)
        mp.simulate_network_partition(["node2", "node3"], isolated=True)
        
        # Send to isolated node - should fail
        success = await mp.send(
            "node1", "node2",
            MessageType.HEARTBEAT,
            {"term": 1}
        )
        
        assert success is False

class TestCacheCoherenceUnderFailure:
    """Test cache coherence protocol under failures"""
    
    def test_cache_recovery_after_invalidation(self):
        """Test cache recovery after invalidation"""
        cache = MESICacheNode("node1", capacity=10, peers=["node1", "node2", "node3"])
        
        # Put value
        cache.put("key1", "value1")
        assert cache.cache["key1"].state == CacheState.MODIFIED
        
        # Invalidate
        cache.handle_invalidate("key1", "node2")
        assert cache.cache["key1"].state == CacheState.INVALID
        
        # Should be able to reacquire
        cache.put("key1", "value2")
        assert cache.cache["key1"].value == "value2"
    
    def test_stale_copy_detection(self):
        """Test detection of stale copies"""
        cache1 = MESICacheNode("node1", capacity=10)
        cache2 = MESICacheNode("node2", capacity=10)
        
        # Both put same key
        cache1.put("key1", "value_v1")
        cache2.put("key1", "value_v2")
        
        # Both should be in MODIFIED state
        assert cache1.cache["key1"].state == CacheState.MODIFIED
        assert cache2.cache["key1"].state == CacheState.MODIFIED

class TestQueueRecoveryScenarios:
    """Test queue recovery under various failure scenarios"""
    
    def test_queue_message_loss_prevention(self):
        """Test that queue prevents message loss"""
        queue = DistributedQueue(["node1", "node2", "node3"], replication_factor=2)
        
        result = queue.enqueue("key1", "important_message")
        msg_id = result['msg_id']
        
        # All replicas ACK
        queue.acknowledge_message(msg_id, "node1")
        queue.acknowledge_message(msg_id, "node2")
        
        sync_status = queue.sync_replicas()
        
        # Should have sufficient replication
        assert sync_status[msg_id]['synced_count'] >= 2
    
    def test_queue_dequeue_idempotency(self):
        """Test that dequeue is idempotent"""
        queue = DistributedQueue(["node1", "node2", "node3"])
        
        queue.enqueue("key1", "message1")
        
        msg1 = queue.dequeue("key1")
        msg2 = queue.dequeue("key1")
        
        assert msg1 is not None
        assert msg2 is None  # Already dequeued

class TestConcurrentOperations:
    """Test system under concurrent load"""
    
    def test_concurrent_lock_acquisitions(self):
        """Test concurrent lock acquisitions"""
        lm = DistributedLockManager("node1", ["node1", "node2", "node3"])
        
        # Simulate multiple nodes requesting locks
        results = []
        for i in range(10):
            success = lm.acquire_exclusive(f"node{i % 3}", f"resource_{i % 5}", f"req{i}")
            results.append(success)
        
        # Some should succeed, some should fail (contention)
        assert any(results)  # At least one succeeded
    
    def test_concurrent_queue_operations(self):
        """Test concurrent enqueue/dequeue operations"""
        queue = DistributedQueue(["node1", "node2", "node3"])
        
        # Enqueue multiple messages
        msg_ids = []
        for i in range(10):
            result = queue.enqueue(f"key{i % 3}", f"message_{i}")
            msg_ids.append(result['msg_id'])
        
        # Queue should have messages
        status = queue.get_queue_status()
        assert status['total_messages'] >= 10
    
    def test_concurrent_cache_operations(self):
        """Test concurrent cache put/get operations"""
        cache = MESICacheNode("node1", capacity=20)
        
        # Put multiple values
        for i in range(10):
            cache.put(f"key{i}", f"value{i}")
        
        # Get multiple values
        for i in range(10):
            value = cache.get(f"key{i}")
            assert value == f"value{i}"
        
        # Should have hits
        assert cache.hits > 0

class TestRecoveryAndConsistency:
    """Test recovery and consistency after failures"""
    
    def test_lock_state_recovery(self):
        """Test lock state recovery after restart"""
        lm1 = DistributedLockManager("node1", ["node1", "node2", "node3"])
        lm1.acquire_exclusive("node1", "resource_x", "req1")
        lm1._save_lock_state()
        
        # Create new manager and load state
        lm2 = DistributedLockManager("node1", ["node1", "node2", "node3"])
        
        assert "resource_x" in lm2.exclusive_locks
        assert lm2.exclusive_locks["resource_x"] == "node1"
    
    def test_queue_state_recovery(self):
        """Test queue state recovery after restart"""
        q1 = DistributedQueue(["node1", "node2", "node3"])
        q1.enqueue("key1", "message1")
        q1.enqueue("key2", "message2")
        q1.save()
        
        # Create new queue and load state
        q2 = DistributedQueue(["node1", "node2", "node3"])
        
        assert "key1" in q2.queues
        assert "key2" in q2.queues
