"""
PBFT (Practical Byzantine Fault Tolerance) Consensus Algorithm

Implements the PBFT consensus protocol for Byzantine fault tolerance.
Tolerates up to f faulty nodes in a cluster of 3f+1 nodes.

Protocol Phases:
1. PRE-PREPARE: Primary assigns sequence number and sends request
2. PREPARE: Replicas acknowledge and broadcast prepare messages
3. COMMIT: Replicas commit after achieving quorum on prepared state

This implementation handles view changes (primary failures) and
message authentication via request digests.
"""

import asyncio
import hashlib
import json
import logging
import time
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class ReplicaState(Enum):
    """States of a replica in PBFT protocol"""
    NORMAL = "normal"           # Normal operation, accepting requests
    VIEW_CHANGE = "view_change" # View changing (primary suspected)
    RECOVERING = "recovering"   # Recovering from failure


class MessageType(Enum):
    """PBFT message types"""
    PRE_PREPARE = "pre_prepare"
    PREPARE = "prepare"
    COMMIT = "commit"
    CHECKPOINT = "checkpoint"
    VIEW_CHANGE = "view_change"
    NEW_VIEW = "new_view"


@dataclass
class ClientRequest:
    """Represents a client request"""
    client_id: str
    request_id: int
    operation: str
    args: dict
    timestamp: float = field(default_factory=time.time)

    def digest(self) -> str:
        """Compute SHA-256 digest of request"""
        request_str = json.dumps({
            'client_id': self.client_id,
            'request_id': self.request_id,
            'operation': self.operation,
            'args': self.args,
            'timestamp': self.timestamp
        }, sort_keys=True)
        return hashlib.sha256(request_str.encode()).hexdigest()


@dataclass
class PBFTMessage:
    """PBFT protocol message"""
    type: MessageType
    view: int                      # Current view number
    sequence: int                  # Sequence number
    digest: str                    # Request digest
    replica_id: str               # Sender replica ID
    timestamp: float = field(default_factory=time.time)
    
    # For PRE_PREPARE
    client_request: Optional[ClientRequest] = None
    
    # For PREPARE/COMMIT
    prepares: Dict[str, int] = field(default_factory=dict)  # replica_id -> count
    commits: Dict[str, int] = field(default_factory=dict)
    
    # For VIEW_CHANGE
    checkpoint_sequence: int = 0
    prepared_messages: List[Tuple[int, str]] = field(default_factory=list)
    
    # For NEW_VIEW
    view_change_messages: List['PBFTMessage'] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        """Convert message to dictionary for transmission"""
        data = asdict(self)
        data['type'] = self.type.value
        if self.client_request:
            data['client_request'] = asdict(self.client_request)
        return data

    @classmethod
    def from_dict(cls, data: dict) -> 'PBFTMessage':
        """Reconstruct message from dictionary"""
        if isinstance(data.get('type'), str):
            data['type'] = MessageType(data['type'])
        if data.get('client_request'):
            data['client_request'] = ClientRequest(**data['client_request'])
        return cls(**data)


@dataclass
class ReplicaLog:
    """Log entry for replicated state"""
    sequence: int
    digest: str
    prepares: Set[str] = field(default_factory=set)    # Replicas that prepared
    commits: Set[str] = field(default_factory=set)     # Replicas that committed
    committed: bool = False
    result: Optional[str] = None
    execute_time: Optional[float] = None


@dataclass
class ViewChangeInfo:
    """Information about a view change event"""
    old_view: int
    new_view: int
    reason: str
    timestamp: float = field(default_factory=time.time)


class PBFTNode:
    """
    PBFT (Practical Byzantine Fault Tolerance) Node
    
    Implements the PBFT consensus protocol with Byzantine fault tolerance.
    Can tolerate up to f faulty nodes in a cluster of 3f+1 nodes.
    """

    def __init__(
        self,
        node_id: str,
        cluster_nodes: List[str],
        f: Optional[int] = None,
        checkpoint_interval: int = 128,
        timeout_primary: float = 5.0,
        timeout_backup: float = 10.0
    ):
        """
        Initialize PBFT node.
        
        Args:
            node_id: Unique node identifier
            cluster_nodes: List of all node IDs in cluster
            f: Number of tolerated faults (if None, calculated as (n-1)/3)
            checkpoint_interval: Sequence numbers between stable checkpoints
            timeout_primary: Timeout for primary to propose new sequence
            timeout_backup: Timeout for backups waiting for new view
        """
        self.node_id = node_id
        self.cluster_nodes = cluster_nodes
        self.num_nodes = len(cluster_nodes)
        
        # Calculate fault tolerance
        if f is None:
            self.f = (self.num_nodes - 1) // 3
        else:
            self.f = f
        
        # Verify minimum requirements: 3f + 1 nodes
        min_nodes = 3 * self.f + 1
        if self.num_nodes < min_nodes:
            raise ValueError(f"Need at least {min_nodes} nodes for f={self.f} fault tolerance, got {self.num_nodes}")
        
        self.quorum_size = 2 * self.f + 1  # Minimum for consensus
        
        # Protocol state
        self.view = 0                           # Current view number
        self.sequence = 0                       # Current sequence number
        self.state = ReplicaState.NORMAL
        self.last_stable_sequence = 0
        self.checkpoint_interval = checkpoint_interval
        
        # Timeouts
        self.timeout_primary = timeout_primary
        self.timeout_backup = timeout_backup
        self.view_change_timeout = timeout_backup * 2
        self.last_primary_activity = time.time()
        
        # Message logs
        self.pre_prepare_log: Dict[int, Dict[str, PBFTMessage]] = {}  # seq -> digest -> msg
        self.prepare_log: Dict[int, Dict[str, List[PBFTMessage]]] = {}  # seq -> digest -> msgs
        self.commit_log: Dict[int, Dict[str, List[PBFTMessage]]] = {}   # seq -> digest -> msgs
        
        self.replica_logs: Dict[int, ReplicaLog] = {}  # sequence -> log entry
        
        # Client request tracking
        self.pending_requests: Dict[str, ClientRequest] = {}  # digest -> request
        self.executed_requests: Dict[Tuple[str, int], str] = {}  # (client_id, req_id) -> result
        
        # View change tracking
        self.view_change_messages: Dict[int, List[PBFTMessage]] = {}  # view -> messages
        self.view_change_timer: Optional[float] = None
        self.view_changes: List[ViewChangeInfo] = []
        
        # Message receive queues
        self.message_queue: asyncio.Queue = asyncio.Queue()
        self.inbound_messages: List[PBFTMessage] = []
        
        logger.info(f"PBFT Node {node_id} initialized: cluster={cluster_nodes}, f={self.f}, quorum={self.quorum_size}")

    @property
    def primary_id(self) -> str:
        """Get ID of primary replica for current view"""
        primary_index = self.view % self.num_nodes
        return self.cluster_nodes[primary_index]

    @property
    def is_primary(self) -> bool:
        """Check if this node is the primary"""
        return self.node_id == self.primary_id

    def get_primary_for_view(self, view: int) -> str:
        """Get primary for specific view"""
        primary_index = view % self.num_nodes
        return self.cluster_nodes[primary_index]

    async def handle_client_request(self, request: ClientRequest) -> str:
        """
        Handle incoming client request via PRE-PREPARE phase.
        Only primary accepts new requests.
        """
        if not self.is_primary:
            logger.warning(f"Node {self.node_id} received client request but not primary")
            return "error: not primary"
        
        if self.state != ReplicaState.NORMAL:
            return "error: view change in progress"
        
        # Assign sequence number
        self.sequence += 1
        request_digest = request.digest()
        
        # Store pending request
        self.pending_requests[request_digest] = request
        
        # Create and broadcast PRE_PREPARE message
        pre_prepare_msg = PBFTMessage(
            type=MessageType.PRE_PREPARE,
            view=self.view,
            sequence=self.sequence,
            digest=request_digest,
            replica_id=self.node_id,
            client_request=request
        )
        
        # Log the pre-prepare
        if self.sequence not in self.pre_prepare_log:
            self.pre_prepare_log[self.sequence] = {}
        self.pre_prepare_log[self.sequence][request_digest] = pre_prepare_msg
        
        # Initialize replica log
        if self.sequence not in self.replica_logs:
            self.replica_logs[self.sequence] = ReplicaLog(
                sequence=self.sequence,
                digest=request_digest
            )
        
        # Broadcast to all replicas
        await self._broadcast_message(pre_prepare_msg)
        
        logger.info(f"Primary {self.node_id} broadcast PRE_PREPARE: seq={self.sequence}, digest={request_digest[:8]}")
        return f"accepted: seq={self.sequence}"

    async def handle_pre_prepare(self, msg: PBFTMessage) -> bool:
        """
        Handle PRE_PREPARE message (primary proposes new state).
        Transitions to PREPARE phase if valid.
        """
        if msg.view != self.view:
            logger.warning(f"PRE_PREPARE view mismatch: {msg.view} vs {self.view}")
            return False
        
        if msg.replica_id != self.primary_id:
            logger.warning(f"PRE_PREPARE from non-primary {msg.replica_id}")
            return False
        
        # Store the request
        if msg.client_request:
            self.pending_requests[msg.digest] = msg.client_request
        
        # Log pre-prepare
        if msg.sequence not in self.pre_prepare_log:
            self.pre_prepare_log[msg.sequence] = {}
        self.pre_prepare_log[msg.sequence][msg.digest] = msg
        
        # Initialize replica log entry
        if msg.sequence not in self.replica_logs:
            self.replica_logs[msg.sequence] = ReplicaLog(
                sequence=msg.sequence,
                digest=msg.digest
            )
        
        # Update sequence tracking
        if msg.sequence > self.sequence:
            self.sequence = msg.sequence
        
        # Update primary activity timer
        self.last_primary_activity = time.time()
        
        # Broadcast PREPARE message to all replicas
        prepare_msg = PBFTMessage(
            type=MessageType.PREPARE,
            view=msg.view,
            sequence=msg.sequence,
            digest=msg.digest,
            replica_id=self.node_id
        )
        
        await self._broadcast_message(prepare_msg)
        
        logger.debug(f"Node {self.node_id} prepared: seq={msg.sequence}, digest={msg.digest[:8]}")
        return True

    async def handle_prepare(self, msg: PBFTMessage) -> bool:
        """
        Handle PREPARE message (backups acknowledge they prepared).
        Transitions to COMMIT phase after quorum of PREPARE messages.
        """
        if msg.view != self.view:
            return False
        
        if msg.sequence not in self.prepare_log:
            self.prepare_log[msg.sequence] = {}
        
        if msg.digest not in self.prepare_log[msg.sequence]:
            self.prepare_log[msg.sequence][msg.digest] = []
        
        # Add to prepare log if not already present
        if msg not in self.prepare_log[msg.sequence][msg.digest]:
            self.prepare_log[msg.sequence][msg.digest].append(msg)
        
        # Check if we have quorum of prepare messages (including our own)
        prepare_count = len(self.prepare_log[msg.sequence].get(msg.digest, []))
        
        # Count if we sent our own prepare
        has_own_prepare = msg.sequence in self.pre_prepare_log and msg.digest in self.pre_prepare_log[msg.sequence]
        
        if prepare_count + (1 if has_own_prepare else 0) >= self.quorum_size:
            # Reached prepared state
            if msg.sequence in self.replica_logs:
                self.replica_logs[msg.sequence].prepares.add(msg.replica_id)
            
            # Broadcast COMMIT message
            commit_msg = PBFTMessage(
                type=MessageType.COMMIT,
                view=msg.view,
                sequence=msg.sequence,
                digest=msg.digest,
                replica_id=self.node_id
            )
            
            await self._broadcast_message(commit_msg)
            
            logger.debug(f"Node {self.node_id} reached prepared state: seq={msg.sequence}")
            return True
        
        return False

    async def handle_commit(self, msg: PBFTMessage) -> bool:
        """
        Handle COMMIT message (backups acknowledge they committed).
        Executes operation after quorum of COMMIT messages.
        """
        if msg.view != self.view:
            return False
        
        if msg.sequence not in self.commit_log:
            self.commit_log[msg.sequence] = {}
        
        if msg.digest not in self.commit_log[msg.sequence]:
            self.commit_log[msg.sequence][msg.digest] = []
        
        # Add to commit log if not already present
        if msg not in self.commit_log[msg.sequence][msg.digest]:
            self.commit_log[msg.sequence][msg.digest].append(msg)
        
        # Check if we have quorum of commit messages
        commit_count = len(self.commit_log[msg.sequence].get(msg.digest, []))
        
        if commit_count >= self.quorum_size:
            # Ready to commit
            if msg.sequence in self.replica_logs:
                self.replica_logs[msg.sequence].commits.add(msg.replica_id)
                self.replica_logs[msg.sequence].committed = True
                self.replica_logs[msg.sequence].execute_time = time.time()
            
            # Execute the operation
            result = await self._execute_operation(msg.sequence, msg.digest)
            
            logger.info(f"Node {self.node_id} committed: seq={msg.sequence}, result={result[:50] if result else 'None'}")
            return True
        
        return False

    async def _execute_operation(self, sequence: int, digest: str) -> str:
        """Execute the committed operation"""
        if digest in self.pending_requests:
            request = self.pending_requests[digest]
            # Record execution result
            key = (request.client_id, request.request_id)
            result = f"executed: {request.operation} -> OK"
            self.executed_requests[key] = result
            logger.debug(f"Executed operation: {request.operation} for {request.client_id}")
            return result
        return "no_request_found"

    async def handle_view_change(self) -> None:
        """
        Initiate view change when primary is suspected faulty.
        Broadcasts VIEW_CHANGE message to initiate view change protocol.
        """
        if self.state == ReplicaState.VIEW_CHANGE:
            return  # Already changing view
        
        self.state = ReplicaState.VIEW_CHANGE
        self.view += 1
        
        logger.warning(f"Node {self.node_id} initiating view change to view {self.view}")
        
        # Create VIEW_CHANGE message
        prepared_msgs = []
        for seq, logs in self.pre_prepare_log.items():
            for digest, msg in logs.items():
                if seq in self.prepare_log and digest in self.prepare_log[seq]:
                    prepared_msgs.append((seq, digest))
        
        view_change_msg = PBFTMessage(
            type=MessageType.VIEW_CHANGE,
            view=self.view,
            sequence=self.sequence,
            digest="",
            replica_id=self.node_id,
            checkpoint_sequence=self.last_stable_sequence,
            prepared_messages=prepared_msgs
        )
        
        # Broadcast to all nodes
        await self._broadcast_message(view_change_msg)

    async def handle_new_view(self, msg: PBFTMessage) -> bool:
        """
        Handle NEW_VIEW message from new primary.
        Completes view change and resumes normal operation.
        """
        if msg.view <= self.view:
            logger.warning(f"NEW_VIEW with old/same view: {msg.view} <= {self.view}")
            return False
        
        self.view = msg.view
        self.state = ReplicaState.NORMAL
        self.last_primary_activity = time.time()
        
        logger.info(f"Node {self.node_id} accepted NEW_VIEW to view {self.view} from {msg.replica_id}")
        
        # Record the view change
        self.view_changes.append(ViewChangeInfo(
            old_view=self.view - 1,
            new_view=self.view,
            reason="view_change_accepted"
        ))
        
        return True

    async def monitor_primary(self) -> None:
        """
        Monitor primary for signs of failure.
        If timeout elapses without primary activity, initiate view change.
        """
        while True:
            try:
                await asyncio.sleep(self.timeout_primary)
                
                if self.state == ReplicaState.NORMAL and not self.is_primary:
                    elapsed = time.time() - self.last_primary_activity
                    
                    if elapsed > self.timeout_primary:
                        logger.warning(f"Node {self.node_id}: Primary timeout, elapsed={elapsed:.2f}s")
                        await self.handle_view_change()
                
            except Exception as e:
                logger.error(f"Error in monitor_primary: {e}")

    async def handle_incoming_message(self, msg: PBFTMessage) -> None:
        """
        Route incoming message to appropriate handler based on type.
        """
        try:
            if msg.type == MessageType.PRE_PREPARE:
                await self.handle_pre_prepare(msg)
            elif msg.type == MessageType.PREPARE:
                await self.handle_prepare(msg)
            elif msg.type == MessageType.COMMIT:
                await self.handle_commit(msg)
            elif msg.type == MessageType.VIEW_CHANGE:
                # Replica is changing view
                if msg.view not in self.view_change_messages:
                    self.view_change_messages[msg.view] = []
                self.view_change_messages[msg.view].append(msg)
                
                # If primary, collect view change messages and send NEW_VIEW
                if self.is_primary and len(self.view_change_messages[msg.view]) >= self.f + 1:
                    await self._send_new_view(msg.view)
            elif msg.type == MessageType.NEW_VIEW:
                await self.handle_new_view(msg)
            else:
                logger.warning(f"Unknown message type: {msg.type}")
        except Exception as e:
            logger.error(f"Error handling message: {e}")

    async def _broadcast_message(self, msg: PBFTMessage) -> None:
        """Broadcast message to all nodes in cluster"""
        # In real implementation, this would send over network
        # For now, just log
        logger.debug(f"Broadcasting {msg.type.value} from {self.node_id}: seq={msg.sequence}")
        # Store in inbound for other nodes to process
        self.inbound_messages.append(msg)

    async def _send_new_view(self, view: int) -> None:
        """Primary sends NEW_VIEW after collecting view change messages"""
        new_view_msg = PBFTMessage(
            type=MessageType.NEW_VIEW,
            view=view,
            sequence=self.sequence,
            digest="",
            replica_id=self.node_id,
            view_change_messages=self.view_change_messages.get(view, [])
        )
        
        await self._broadcast_message(new_view_msg)
        logger.info(f"Primary {self.node_id} sent NEW_VIEW for view {view}")

    def get_status(self) -> dict:
        """Get node status"""
        return {
            'node_id': self.node_id,
            'is_primary': self.is_primary,
            'view': self.view,
            'sequence': self.sequence,
            'state': self.state.value,
            'cluster_size': self.num_nodes,
            'fault_tolerance': self.f,
            'quorum_size': self.quorum_size,
            'pending_requests': len(self.pending_requests),
            'executed_requests': len(self.executed_requests),
            'view_changes': len(self.view_changes),
            'last_stable_sequence': self.last_stable_sequence,
            'replica_logs_count': len(self.replica_logs),
            'committed_count': sum(1 for log in self.replica_logs.values() if log.committed)
        }

    def get_diagnostics(self) -> dict:
        """Get detailed diagnostic information"""
        return {
            'status': self.get_status(),
            'primary_id': self.primary_id,
            'primary_timeout': f"{time.time() - self.last_primary_activity:.2f}s ago",
            'view_change_history': [
                {
                    'old_view': vc.old_view,
                    'new_view': vc.new_view,
                    'reason': vc.reason,
                    'timestamp': vc.timestamp
                }
                for vc in self.view_changes[-10:]  # Last 10 view changes
            ],
            'replicated_sequences': list(self.replica_logs.keys())[-10:],  # Last 10 sequences
            'committed_sequences': [
                s for s in self.replica_logs.keys() 
                if self.replica_logs[s].committed
            ][-10:]  # Last 10 committed
        }
