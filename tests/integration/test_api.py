import pytest
import asyncio
from fastapi.testclient import TestClient
from src.communication.api import app
import json

client = TestClient(app)

class TestDistributedLockAPI:
    """Test distributed lock API endpoints"""
    
    def test_acquire_exclusive_lock_api(self):
        """Test acquiring exclusive lock via API"""
        response = client.post("/lock/acquire", json={
            "node_id": "node1",
            "resource_id": "resource_x",
            "lock_type": "exclusive"
        })
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
    
    def test_acquire_shared_lock_api(self):
        """Test acquiring shared lock via API"""
        response = client.post("/lock/acquire", json={
            "node_id": "node1",
            "resource_id": "resource_x",
            "lock_type": "shared"
        })
        
        assert response.status_code == 200
        data = response.json()
        assert "request_id" in data
    
    def test_release_lock_api(self):
        """Test releasing lock via API"""
        # Acquire first
        client.post("/lock/acquire", json={
            "node_id": "node1",
            "resource_id": "resource_x",
            "lock_type": "exclusive"
        })
        
        # Release
        response = client.post("/lock/release", json={
            "node_id": "node1",
            "resource_id": "resource_x"
        })
        
        assert response.status_code == 200
        assert response.json()["success"] is True
    
    def test_lock_status_api(self):
        """Test getting lock status via API"""
        client.post("/lock/acquire", json={
            "node_id": "node1",
            "resource_id": "resource_x",
            "lock_type": "exclusive"
        })
        
        response = client.get("/lock/status/resource_x")
        
        assert response.status_code == 200
        data = response.json()
        assert data["exclusive_lock_holder"] == "node1"

class TestDistributedQueueAPI:
    """Test distributed queue API endpoints"""
    
    def test_enqueue_api(self):
        """Test enqueuing message via API"""
        response = client.post("/queue/enqueue", json={
            "key": "test_key",
            "content": "test message"
        })
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "queued"
        assert "msg_id" in data
    
    def test_dequeue_api(self):
        """Test dequeueing message via API"""
        # Enqueue first
        enqueue_response = client.post("/queue/enqueue", json={
            "key": "test_key",
            "content": "test message"
        })
        
        # Dequeue
        response = client.get("/queue/dequeue?key=test_key")
        
        assert response.status_code == 200
        data = response.json()
        assert data["content"] == "test message"
    
    def test_queue_status_api(self):
        """Test getting queue status via API"""
        client.post("/queue/enqueue", json={
            "key": "test_key",
            "content": "msg1"
        })
        client.post("/queue/enqueue", json={
            "key": "test_key",
            "content": "msg2"
        })
        
        response = client.get("/queue/status")
        
        assert response.status_code == 200
        data = response.json()
        assert data["total_messages"] >= 2

class TestCacheAPI:
    """Test cache API endpoints"""
    
    def test_put_cache_api(self):
        """Test putting value in cache via API"""
        response = client.post("/cache/put", json={
            "key": "key1",
            "value": "value1"
        })
        
        assert response.status_code == 200
        data = response.json()
        assert data["key"] == "key1"
    
    def test_get_cache_api(self):
        """Test getting value from cache via API"""
        # Put first
        client.post("/cache/put", json={
            "key": "key1",
            "value": "value1"
        })
        
        # Get
        response = client.get("/cache/get/key1")
        
        assert response.status_code == 200
        data = response.json()
        assert data["found"] is True
        assert data["value"] == "value1"
    
    def test_cache_stats_api(self):
        """Test getting cache statistics via API"""
        response = client.get("/cache/stats")
        
        assert response.status_code == 200
        data = response.json()
        assert "hits" in data
        assert "misses" in data
        assert "hit_rate" in data

class TestRaftAPI:
    """Test Raft API endpoints"""
    
    def test_raft_status_api(self):
        """Test getting Raft node status via API"""
        response = client.get("/raft/status")
        
        assert response.status_code == 200
        data = response.json()
        assert "node_id" in data
        assert "state" in data
        assert "term" in data
    
    def test_request_vote_api(self):
        """Test RequestVote RPC via API"""
        response = client.post("/raft/request-vote", json={
            "candidate_id": "node2",
            "term": 1,
            "last_log_index": 0,
            "last_log_term": 0
        }, params={})
        
        assert response.status_code == 200
        data = response.json()
        assert "vote_granted" in data

class TestHealthAndStatus:
    """Test health check and status endpoints"""
    
    def test_health_check(self):
        """Test health check endpoint"""
        response = client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
    
    def test_system_status(self):
        """Test overall system status"""
        response = client.get("/status")
        
        assert response.status_code == 200
        data = response.json()
        assert "node_id" in data
        assert "peers" in data
        assert "raft" in data
        assert "failure_detector" in data
        assert "cache" in data
        assert "queue" in data