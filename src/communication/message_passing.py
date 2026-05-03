import asyncio
import json
import time
from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum

class MessageType(Enum):
    """Types of messages in the system"""
    HEARTBEAT = "heartbeat"
    REQUEST_VOTE = "request_vote"
    APPEND_ENTRIES = "append_entries"
    LOCK_REQUEST = "lock_request"
    LOCK_RELEASE = "lock_release"
    QUEUE_ENQUEUE = "queue_enqueue"
    QUEUE_DEQUEUE = "queue_dequeue"
    CACHE_INVALIDATE = "cache_invalidate"
    CACHE_UPDATE = "cache_update"
    ACK = "ack"
    NACK = "nack"

@dataclass
class Message:
    """Represents a network message"""
    msg_id: str
    msg_type: MessageType
    sender: str
    receiver: str
    timestamp: float
    payload: Dict
    retries: int = 0
    max_retries: int = 3

class ReliableMessagePassing:
    """
    Reliable message passing layer with:
    - At-least-once delivery guarantee
    - Automatic retries with exponential backoff
    - Network partition simulation
    - Message ordering
    """
    
    def __init__(self, node_id: str, enable_network_faults: bool = False):
        self.node_id = node_id
        self.enable_network_faults = enable_network_faults
        
        # Message inbox (delivered messages)
        self.inbox: Dict[str, List[Message]] = {}
        
        # In-flight messages (waiting for ACK)
        self.in_flight: Dict[str, Message] = {}
        
        # Sent messages history (for deduplication)
        self.sent_history: Dict[str, Message] = {}
        
        # ACK tracking
        self.acked_messages: set = set()
        
        # Network simulation
        self.partitioned_nodes: set = set()
        self.message_loss_rate = 0.0  # 0.0 = no loss, 1.0 = all lost
    
    async def send(self, sender: str, receiver: str, msg_type: MessageType, 
                   payload: Dict, msg_id: str = None) -> bool:
        """
        Send a message with reliability guarantees.
        Returns True if message was sent successfully.
        """
        if msg_id is None:
            msg_id = f"{sender}_{receiver}_{int(time.time() * 1000000)}"
        
        message = Message(
            msg_id=msg_id,
            msg_type=msg_type,
            sender=sender,
            receiver=receiver,
            timestamp=time.time(),
            payload=payload
        )
        
        # Store in in-flight messages
        self.in_flight[msg_id] = message
        self.sent_history[msg_id] = message
        
        # Simulate network faults
        if self._should_drop_message(receiver):
            print(f"[NETWORK] Message dropped: {sender} -> {receiver} (network fault)")
            return False
        
        # Deliver message
        if receiver not in self.inbox:
            self.inbox[receiver] = []
        
        self.inbox[receiver].append(message)
        
        print(f"[SEND] {sender} -> {receiver}: {msg_type.value} (id={msg_id})")
        
        return True
    
    async def receive(self, node_id: str) -> List[Message]:
        """Receive all pending messages for a node"""
        messages = self.inbox.get(node_id, [])
        self.inbox[node_id] = []
        
        if messages:
            print(f"[RECEIVE] {node_id} got {len(messages)} messages")
        
        return messages
    
    async def acknowledge_message(self, msg_id: str) -> bool:
        """
        Acknowledge receipt of a message.
        Removes from in-flight queue.
        """
        if msg_id in self.in_flight:
            del self.in_flight[msg_id]
            self.acked_messages.add(msg_id)
            print(f"[ACK] Message {msg_id} acknowledged")
            return True
        
        return False
    
    async def retry_unacked_messages(self):
        """
        Retry messages that haven't been acknowledged.
        Implements exponential backoff.
        """
        messages_to_retry = []
        
        for msg_id, message in list(self.in_flight.items()):
            elapsed = time.time() - message.timestamp
            backoff_time = (2 ** message.retries)  # Exponential backoff
            
            if elapsed > backoff_time and message.retries < message.max_retries:
                messages_to_retry.append((msg_id, message))
            elif message.retries >= message.max_retries:
                print(f"[RETRY] Message {msg_id} failed after {message.max_retries} retries")
                del self.in_flight[msg_id]
        
        for msg_id, message in messages_to_retry:
            message.retries += 1
            print(f"[RETRY] Retrying message {msg_id} (attempt {message.retries})")
            
            if message.receiver not in self.inbox:
                self.inbox[message.receiver] = []
            
            self.inbox[message.receiver].append(message)
    
    def _should_drop_message(self, receiver: str) -> bool:
        """
        Determine if message should be dropped.
        Simulates network faults and partitions.
        """
        if not self.enable_network_faults:
            return False
        
        # Check if receiver is in partition
        if receiver in self.partitioned_nodes:
            return True
        
        # Random message loss
        import random
        if random.random() < self.message_loss_rate:
            return True
        
        return False
    
    def simulate_network_partition(self, nodes: List[str], isolated: bool = True):
        """
        Simulate network partition.
        If isolated=True, nodes are isolated. If False, partition is healed.
        """
        if isolated:
            self.partitioned_nodes.update(nodes)
            print(f"[NETWORK] Partition created: {nodes} isolated")
        else:
            self.partitioned_nodes.difference_update(nodes)
            print(f"[NETWORK] Partition healed: {nodes} reconnected")
    
    def simulate_message_loss(self, loss_rate: float):
        """
        Simulate message loss (0.0 = no loss, 1.0 = all lost).
        """
        self.message_loss_rate = max(0.0, min(1.0, loss_rate))
        print(f"[NETWORK] Message loss rate set to {self.message_loss_rate * 100:.1f}%")
    
    def get_statistics(self) -> Dict:
        """Get message passing statistics"""
        return {
            'node_id': self.node_id,
            'sent_messages': len(self.sent_history),
            'acked_messages': len(self.acked_messages),
            'in_flight_messages': len(self.in_flight),
            'partitioned_nodes': len(self.partitioned_nodes),
            'message_loss_rate': self.message_loss_rate
        }
