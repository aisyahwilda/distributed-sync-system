import pytest
import asyncio
from src.consensus.raft import RaftNode, NodeState, LogEntry

@pytest.fixture
def raft_nodes():
    """Create a cluster of 3 Raft nodes"""
    nodes = {
        "node1": RaftNode("node1", ["node2", "node3"]),
        "node2": RaftNode("node2", ["node1", "node3"]),
        "node3": RaftNode("node3", ["node1", "node2"])
    }
    return nodes

class TestRaftLeaderElection:
    """Test Raft leader election"""
    
    def test_initial_state(self, raft_nodes):
        """All nodes should start as followers"""
        for node_id, node in raft_nodes.items():
            assert node.node_state == NodeState.FOLLOWER
            assert node.state.current_term == 0
    
    @pytest.mark.asyncio
    async def test_election_timeout_triggers_election(self, raft_nodes):
        """Node should start election after timeout"""
        node = raft_nodes["node1"]
        node.last_heartbeat = 0  # Force timeout
        
        await node.start_election()
        
        assert node.node_state == NodeState.CANDIDATE
        assert node.state.current_term == 1
        assert node.state.voted_for == "node1"
    
    @pytest.mark.asyncio
    async def test_request_vote_granted_on_valid_candidate(self, raft_nodes):
        """Follower should grant vote to valid candidate"""
        follower = raft_nodes["node2"]
        candidate = raft_nodes["node1"]
        
        vote_granted = await follower.request_vote(
            candidate_id="node1",
            term=1,
            last_log_index=0,
            last_log_term=0
        )
        
        assert vote_granted is True
        assert follower.state.voted_for == "node1"
    
    @pytest.mark.asyncio
    async def test_vote_denied_on_old_term(self, raft_nodes):
        """Should deny vote from older term"""
        follower = raft_nodes["node2"]
        follower.state.current_term = 2
        
        vote_granted = await follower.request_vote(
            candidate_id="node1",
            term=1,
            last_log_index=0,
            last_log_term=0
        )
        
        assert vote_granted is False

class TestRaftLogReplication:
    """Test Raft log replication"""
    
    def test_append_entry_as_leader(self, raft_nodes):
        """Leader should be able to append entries"""
        leader = raft_nodes["node1"]
        leader.node_state = NodeState.LEADER
        
        command = {"operation": "set", "key": "x", "value": 10}
        success = leader.append_entry(command)
        
        assert success is True
        assert len(leader.state.log) == 1
        assert leader.state.log[0].command == command
    
    def test_append_entry_as_follower(self, raft_nodes):
        """Follower should not append entries"""
        follower = raft_nodes["node2"]
        
        command = {"operation": "set", "key": "x", "value": 10}
        success = follower.append_entry(command)
        
        assert success is False
    
    @pytest.mark.asyncio
    async def test_append_entries_replication(self, raft_nodes):
        """Entries should be replicated to followers"""
        leader = raft_nodes["node1"]
        follower = raft_nodes["node2"]
        
        # Leader appends entry
        command = {"operation": "set", "key": "x", "value": 10}
        leader.append_entry(command)
        
        # Leader sends to follower
        success = await leader.append_entries(
            leader_id="node1",
            term=0,
            prev_log_index=-1,
            prev_log_term=0,
            entries=[{"term": 0, "index": 0, "command": command}],
            leader_commit=0
        )
        
        assert success is True
        assert len(leader.state.log) == 1

class TestRaftPersistence:
    """Test Raft state persistence"""
    
    def test_persistent_state_saved(self, raft_nodes):
        """Persistent state should be saved to disk"""
        node = raft_nodes["node1"]
        node.state.current_term = 5
        node.state.voted_for = "node2"
        
        node._save_persistent_state()
        
        # Verify file was created
        import os
        assert os.path.exists(node.persistent_file)
    
    def test_persistent_state_loaded(self, raft_nodes):
        """Persistent state should be loaded from disk"""
        node = raft_nodes["node1"]
        node.state.current_term = 5
        node._save_persistent_state()
        
        # Create new node and load state
        new_node = RaftNode("node1", ["node2", "node3"], node.persistent_file)
        
        assert new_node.state.current_term == 5
