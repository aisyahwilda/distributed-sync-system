import pytest
from src.nodes.queue_node import DistributedQueue, MessageStatus

@pytest.fixture
def queue():
    """Create a distributed queue"""
    return DistributedQueue(["node1", "node2", "node3"], replication_factor=2)

class TestConsistentHashing:
    """Test consistent hashing for queue distribution"""
    
    def test_get_replica_nodes(self, queue):
        """Should return correct replica nodes using consistent hashing"""
        replicas = queue._get_replica_nodes("key1")
        
        assert len(replicas) == 2
        assert all(node in ["node1", "node2", "node3"] for node in replicas)
    
    def test_consistent_hashing_stability(self, queue):
        """Same key should always hash to same replicas"""
        replicas1 = queue._get_replica_nodes("key1")
        replicas2 = queue._get_replica_nodes("key1")
        
        assert replicas1 == replicas2
    
    def test_different_keys_may_differ(self, queue):
        """Different keys may hash to different replicas"""
        replicas_a = queue._get_replica_nodes("key_a")
        replicas_b = queue._get_replica_nodes("key_b")
        
        # They might be the same or different, but operation should be consistent
        assert len(replicas_a) == 2
        assert len(replicas_b) == 2

class TestEnqueue:
    """Test message enqueuing"""
    
    def test_enqueue_single_message(self, queue):
        """Should enqueue a single message"""
        result = queue.enqueue("key1", "hello world")
        
        assert result['status'] == 'queued'
        assert 'msg_id' in result
        assert "key1" in queue.queues
    
    def test_enqueue_multiple_messages_same_key(self, queue):
        """Should enqueue multiple messages on same key"""
        queue.enqueue("key1", "message 1")
        queue.enqueue("key1", "message 2")
        queue.enqueue("key1", "message 3")
        
        assert len(queue.queues["key1"]) == 3
    
    def test_enqueue_returns_replica_nodes(self, queue):
        """Enqueue should return replica nodes for replication"""
        result = queue.enqueue("key1", "hello")
        
        assert 'replica_nodes' in result
        assert len(result['replica_nodes']) == 2

class TestDequeue:
    """Test message dequeueing"""
    
    def test_dequeue_fifo_order(self, queue):
        """Messages should be dequeued in FIFO order"""
        queue.enqueue("key1", "first")
        queue.enqueue("key1", "second")
        queue.enqueue("key1", "third")
        
        msg1 = queue.dequeue("key1")
        msg2 = queue.dequeue("key1")
        msg3 = queue.dequeue("key1")
        
        assert msg1['content'] == "first"
        assert msg2['content'] == "second"
        assert msg3['content'] == "third"
    
    def test_dequeue_empty_queue(self, queue):
        """Should return None for empty queue"""
        result = queue.dequeue("nonexistent")
        
        assert result is None
    
    def test_dequeue_marks_acknowledged(self, queue):
        """Dequeued message should be marked as acknowledged"""
        queue.enqueue("key1", "hello")
        msg = queue.dequeue("key1")
        
        assert msg is not None
        assert queue.message_registry[msg['msg_id']].status == MessageStatus.ACKNOWLEDGED

class TestAtLeastOnceDelivery:
    """Test at-least-once delivery guarantee"""
    
    def test_message_acknowledgment(self, queue):
        """Should acknowledge message"""
        queue.enqueue("key1", "hello")
        msg = queue.dequeue("key1")
        
        success = queue.acknowledge_message(msg['msg_id'])
        
        assert success is True
    
    def test_message_nack_and_retry(self, queue):
        """Should handle NACK and allow retry"""
        msg_result = queue.enqueue("key1", "hello")
        msg_id = msg_result['msg_id']
        
        queue.nack_message(msg_id, retry=True)
        
        message = queue.message_registry[msg_id]
        assert message.status == MessageStatus.PENDING
        assert message.attempts == 1
    
    def test_message_nack_max_retries(self, queue):
        """Should fail message after max retries"""
        msg_result = queue.enqueue("key1", "hello")
        msg_id = msg_result['msg_id']
        
        for _ in range(4):
            queue.nack_message(msg_id, retry=True)
        
        message = queue.message_registry[msg_id]
        assert message.status == MessageStatus.FAILED
        assert message.attempts == 4

class TestPersistence:
    """Test message persistence"""
    
    def test_queue_persistence(self, queue):
        """Queue state should be persisted to disk"""
        queue.enqueue("key1", "message 1")
        queue.enqueue("key2", "message 2")
        
        queue.save()
        
        import os
        assert os.path.exists(queue.data_file)
    
    def test_queue_load_from_disk(self, queue):
        """Should load queue state from disk"""
        queue.enqueue("key1", "hello")
        queue.save()
        
        # Create new queue and load
        new_queue = DistributedQueue(["node1", "node2", "node3"])
        
        assert "key1" in new_queue.queues
        assert len(new_queue.queues["key1"]) > 0

class TestQueueStatus:
    """Test queue status queries"""
    
    def test_get_queue_status_single_key(self, queue):
        """Should return status of single queue"""
        queue.enqueue("key1", "msg1")
        queue.enqueue("key1", "msg2")
        
        status = queue.get_queue_status("key1")
        
        assert status['key'] == "key1"
        assert status['size'] == 2
    
    def test_get_queue_status_all(self, queue):
        """Should return status of all queues"""
        queue.enqueue("key1", "msg1")
        queue.enqueue("key2", "msg2")
        
        status = queue.get_queue_status()
        
        assert status['total_keys'] == 2
        assert status['total_messages'] == 2

class TestReplication:
    """Test message replication across nodes"""
    
    def test_sync_replicas_status(self, queue):
        """Should track replication status"""
        result = queue.enqueue("key1", "hello")
        msg_id = result['msg_id']
        
        # Acknowledge from different nodes
        queue.acknowledge_message(msg_id, "node1")
        queue.acknowledge_message(msg_id, "node2")
        
        sync_status = queue.sync_replicas()
        
        assert msg_id in sync_status
