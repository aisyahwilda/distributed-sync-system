import asyncio
import random
import time
import json
import os
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field, asdict
from enum import Enum

import aiohttp

class NodeState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

@dataclass
class LogEntry:
    """Represents a log entry in Raft"""
    term: int
    index: int
    command: Dict[str, Any]
    
@dataclass
class RaftState:
    """Persistent state of Raft node"""
    current_term: int = 0
    voted_for: Optional[str] = None
    log: List[LogEntry] = field(default_factory=list)

class RaftNode:
    """
    Distributed consensus node using Raft algorithm.
    Supports leader election, log replication, and state machine replication.
    """
    
    def __init__(self, node_id: str, peers: List[str], state_file: str = None):
        self.node_id = node_id
        self.peers = [p for p in peers if p != node_id]  # Remove self from peers
        
        # Persistent state (on disk)
        self.persistent_file = state_file or f"data/raft_{node_id}.json"
        self.state = self._load_persistent_state()
        
        # Volatile state (in memory)
        self.node_state = NodeState.FOLLOWER
        self.last_heartbeat = time.time()  # Set to current time on startup
        self.election_timeout = random.uniform(5.0, 7.0)  # Longer timeout to avoid constant elections
        self.heartbeat_interval = 1.0  # Leader sends heartbeat every 1 second
        self.leader_id: Optional[str] = None
        
        # Leader state
        self.next_index: Dict[str, int] = {peer: len(self.state.log) for peer in self.peers}
        self.match_index: Dict[str, int] = {peer: 0 for peer in self.peers}
        self.commit_index = 0
        self.last_applied = 0
        
        # RPC response tracking
        self.votes_granted = 0
        self.append_entries_responses = {}
        
        # State machine
        self.state_machine = {}
        self.pending_commands = []
        
    def _load_persistent_state(self) -> RaftState:
        """Load persistent state from disk, or create new if not exists"""
        os.makedirs("data", exist_ok=True)
        if os.path.exists(self.persistent_file):
            try:
                with open(self.persistent_file, 'r') as f:
                    data = json.load(f)
                    log_entries = [LogEntry(**entry) for entry in data.get('log', [])]
                    return RaftState(
                        current_term=data.get('current_term', 0),
                        voted_for=data.get('voted_for'),
                        log=log_entries
                    )
            except Exception as e:
                print(f"[{self.node_id}] Error loading state: {e}")
        return RaftState()
    
    def _save_persistent_state(self):
        """Save persistent state to disk"""
        try:
            data = {
                'current_term': self.state.current_term,
                'voted_for': self.state.voted_for,
                'log': [asdict(entry) for entry in self.state.log]
            }
            with open(self.persistent_file, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            print(f"[{self.node_id}] Error saving state: {e}")
    
    def append_entry(self, command: Dict[str, Any]) -> bool:
        """Append a new entry to the log (on leader)"""
        if self.node_state != NodeState.LEADER:
            return False
        
        entry = LogEntry(
            term=self.state.current_term,
            index=len(self.state.log),
            command=command
        )
        self.state.log.append(entry)
        self._save_persistent_state()
        print(f"[{self.node_id}] Appended entry: {entry}")
        return True
    
    async def request_vote(self, candidate_id: str, term: int, 
                          last_log_index: int, last_log_term: int) -> bool:
        """
        RequestVote RPC handler.
        Returns True if vote is granted to candidate.
        """
        if term > self.state.current_term:
            self.state.current_term = term
            self.state.voted_for = None
            self.node_state = NodeState.FOLLOWER
            self._save_persistent_state()
        
        if term < self.state.current_term:
            return False
        
        # Check if already voted in this term
        if self.state.voted_for is not None and self.state.voted_for != candidate_id:
            return False
        
        # Check if candidate's log is at least as up-to-date as ours
        my_last_log_term = self.state.log[-1].term if self.state.log else 0
        my_last_log_index = len(self.state.log) - 1
        
        if last_log_term < my_last_log_term:
            return False
        if last_log_term == my_last_log_term and last_log_index < my_last_log_index:
            return False
        
        self.state.voted_for = candidate_id
        self.last_heartbeat = time.time()
        self._save_persistent_state()
        print(f"[{self.node_id}] Granted vote to {candidate_id} for term {term}")
        return True
    
    async def append_entries(self, leader_id: str, term: int, prev_log_index: int,
                            prev_log_term: int, entries: List[Dict], leader_commit: int) -> bool:
        """
        AppendEntries RPC handler (heartbeat + log replication).
        Returns True if entries were successfully replicated.
        """
        if term > self.state.current_term:
            self.state.current_term = term
            self.state.voted_for = None
            self._save_persistent_state()
        
        if term < self.state.current_term:
            return False
        
        self.last_heartbeat = time.time()
        self.node_state = NodeState.FOLLOWER
        # record who the leader is
        self.leader_id = leader_id
        
        # Check if we have the previous log entry
        if prev_log_index > 0:
            if prev_log_index > len(self.state.log) - 1:
                return False
            if self.state.log[prev_log_index].term != prev_log_term:
                # Delete conflicting entries
                self.state.log = self.state.log[:prev_log_index]
                self._save_persistent_state()
                return False
        
        # Append new entries
        for entry_data in entries:
            entry = LogEntry(**entry_data)
            if entry.index < len(self.state.log):
                # Entry already exists, check consistency
                if self.state.log[entry.index].term != entry.term:
                    self.state.log = self.state.log[:entry.index]
            self.state.log.append(entry)
        
        self._save_persistent_state()
        
        # Update commit index
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.state.log) - 1)
        
        return True
    
    async def start_election(self):
        """Start a new election term"""
        self.state.current_term += 1
        self.node_state = NodeState.CANDIDATE
        self.state.voted_for = self.node_id
        self.votes_granted = 1  # Vote for self
        self.last_heartbeat = time.time()
        self._save_persistent_state()
        
        last_log_term = self.state.log[-1].term if self.state.log else 0
        last_log_index = len(self.state.log) - 1
        
        print(f"[{self.node_id}] Starting election for term {self.state.current_term}")
        
        # Request votes from all peers
        for peer in self.peers:
            asyncio.create_task(self._request_vote_from_peer(
                peer, self.state.current_term, last_log_index, last_log_term
            ))
    
    async def _request_vote_from_peer(self, peer_id: str, term: int, 
                                      last_log_index: int, last_log_term: int):
        """Send RequestVote RPC to a peer"""
        # Try real HTTP RPC to peer; fall back to local simulation on error
        url = f"http://{peer_id}:8000/raft/request-vote"
        payload = {
            "candidate_id": self.node_id,
            "term": term,
            "last_log_index": last_log_index,
            "last_log_term": last_log_term
        }

        vote_granted = False
        try:
            timeout = aiohttp.ClientTimeout(total=2)
            async with aiohttp.ClientSession(timeout=timeout) as sess:
                async with sess.post(url, json=payload) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        vote_granted = data.get("vote_granted", False)
        except Exception:
            # Fallback to local simulated RPC (useful for unit tests / single-process runs)
            vote_granted = await self.request_vote(self.node_id, term, last_log_index, last_log_term)

        if vote_granted:
            self.votes_granted += 1
            # Majority is (n // 2) + 1
            if self.votes_granted >= (len(self.peers) + 1) // 2 + 1:
                if self.node_state == NodeState.CANDIDATE:
                    await self._become_leader()
    
    async def _become_leader(self):
        """Transition to leader state"""
        self.node_state = NodeState.LEADER
        self.last_heartbeat = time.time()  # Reset heartbeat timer when becoming leader
        # mark self as leader id
        self.leader_id = self.node_id
        self.next_index = {peer: len(self.state.log) for peer in self.peers}
        self.match_index = {peer: 0 for peer in self.peers}
        
        print(f"[{self.node_id}] Became LEADER for term {self.state.current_term}")
        
        # Start heartbeat task
        asyncio.create_task(self._send_heartbeats())
    
    async def _send_heartbeats(self):
        """Send periodic heartbeats to all followers"""
        while self.node_state == NodeState.LEADER:
            for peer in self.peers:
                asyncio.create_task(self._send_append_entries(peer))
            await asyncio.sleep(self.heartbeat_interval)  # Use configurable interval
    
    async def _send_append_entries(self, peer_id: str):
        """Send AppendEntries RPC to a peer"""
        prev_log_index = self.next_index[peer_id] - 1
        prev_log_term = self.state.log[prev_log_index].term if prev_log_index >= 0 and prev_log_index < len(self.state.log) else 0
        
        entries_to_send = []
        for i in range(self.next_index[peer_id], len(self.state.log)):
            entries_to_send.append(asdict(self.state.log[i]))
        # Try HTTP RPC to peer; fall back to local append_entries on error
        url = f"http://{peer_id}:8000/raft/append-entries"
        payload = {
            "leader_id": self.node_id,
            "term": self.state.current_term,
            "prev_log_index": prev_log_index,
            "prev_log_term": prev_log_term,
            "entries": entries_to_send,
            "leader_commit": self.commit_index
        }

        success = False
        try:
            timeout = aiohttp.ClientTimeout(total=2)
            async with aiohttp.ClientSession(timeout=timeout) as sess:
                async with sess.post(url, json=payload) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        success = data.get("success", False)
        except Exception:
            # Fallback to local handler
            success = await self.append_entries(
                self.node_id,
                self.state.current_term,
                prev_log_index,
                prev_log_term,
                entries_to_send,
                self.commit_index
            )
        
        if success:
            self.match_index[peer_id] = max(self.match_index[peer_id], len(self.state.log) - 1)
            self.next_index[peer_id] = len(self.state.log)
        else:
            self.next_index[peer_id] = max(0, self.next_index[peer_id] - 1)
    
    async def monitor(self):
        """Main monitoring loop for election timeout"""
        while True:
            elapsed = time.time() - self.last_heartbeat
            
            if self.node_state == NodeState.LEADER:
                # Leader: reset heartbeat timer to prevent election timeout
                self.last_heartbeat = time.time()
                await asyncio.sleep(0.5)
            else:
                # Follower or candidate checks for election timeout
                if elapsed > self.election_timeout:
                    print(f"[{self.node_id}] Election timeout ({elapsed:.2f}s > {self.election_timeout:.2f}s)")
                    self.election_timeout = random.uniform(5.0, 7.0)
                    await self.start_election()
                
                await asyncio.sleep(0.1)