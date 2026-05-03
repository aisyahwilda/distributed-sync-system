"""
Phi Accrual Failure Detection

Implements adaptive failure detection using the Phi Accrual algorithm.
Based on "The Phi Accrual Failure Detector" by Hayashibara et al.

Phi = -log10(P(t_now)) where P(t) is probability of heartbeat at time t
Uses normal distribution to model heartbeat intervals, adapting to network conditions.
"""

import math
import time
from typing import Dict, List, Optional, Set
from enum import Enum
import statistics


class NodeState(Enum):
    """States of a node in failure detection"""
    ALIVE = "alive"
    SUSPECTED = "suspected"
    DEAD = "dead"


class HeartbeatHistory:
    """Track heartbeat history for Phi Accrual calculation"""
    
    def __init__(self, max_samples: int = 1000):
        self.timestamps: List[float] = []
        self.max_samples = max_samples
        self.mean = 0.0
        self.variance = 1.0
        self.std_deviation = 1.0
    
    def add_sample(self, timestamp: float) -> None:
        """Add a new heartbeat timestamp"""
        self.timestamps.append(timestamp)
        if len(self.timestamps) > self.max_samples:
            self.timestamps.pop(0)
        if len(self.timestamps) > 1:
            self._update_statistics()
    
    def _update_statistics(self) -> None:
        """Update mean and variance from timestamps"""
        if len(self.timestamps) < 2:
            self.mean = 0.0
            self.variance = 1.0
            self.std_deviation = 1.0
            return
        
        intervals = []
        for i in range(1, len(self.timestamps)):
            interval = self.timestamps[i] - self.timestamps[i-1]
            if interval > 0:
                intervals.append(interval)
        
        if not intervals:
            self.mean = 1.0
            self.variance = 1.0
            self.std_deviation = 1.0
            return
        
        self.mean = statistics.mean(intervals)
        if len(intervals) > 1:
            self.variance = statistics.variance(intervals)
        else:
            self.variance = 1.0
        self.std_deviation = math.sqrt(self.variance)
        if self.std_deviation == 0:
            self.std_deviation = 0.1
        if self.variance == 0:
            self.variance = 0.01
    
    def get_last_timestamp(self) -> Optional[float]:
        """Get the most recent heartbeat timestamp"""
        return self.timestamps[-1] if self.timestamps else None


class PhiAccrualFailureDetector:
    """
    Adaptive failure detector using Phi Accrual algorithm.
    Uses probabilistic approach with normal distribution to detect failures.
    Adapts to network conditions instead of using fixed timeouts.
    """
    
    def __init__(
        self,
        phi_threshold: float = 5.0,
        initial_timeout: float = 5.0,
        max_history_samples: int = 1000,
        min_std_deviation: float = 0.1
    ):
        """
        Initialize Phi Accrual failure detector.
        
        Args:
            phi_threshold: Phi value for suspicion (default: 5.0, ~99.9% confidence)
            initial_timeout: Initial timeout before first heartbeat
            max_history_samples: Maximum heartbeat history samples to keep
            min_std_deviation: Minimum std deviation to prevent division by zero
        """
        self.phi_threshold = phi_threshold
        self.initial_timeout = initial_timeout
        self.max_history_samples = max_history_samples
        self.min_std_deviation = min_std_deviation
        
        # Heartbeat tracking
        self.heartbeat_history: Dict[str, HeartbeatHistory] = {}
        self.node_states: Dict[str, NodeState] = {}
        self.last_heartbeat: Dict[str, float] = {}
        
        # Tracking
        self.dead_nodes: Set[str] = set()
        self.failure_history: List[Dict] = []
        self.phi_values: Dict[str, float] = {}
    
    def heartbeat(self, node_id: str) -> None:
        """Record a heartbeat from a node"""
        current_time = time.time()
        
        if node_id not in self.heartbeat_history:
            self.heartbeat_history[node_id] = HeartbeatHistory(self.max_history_samples)
            self.node_states[node_id] = NodeState.ALIVE
        
        self.heartbeat_history[node_id].add_sample(current_time)
        self.last_heartbeat[node_id] = current_time
        
        if self.node_states[node_id] == NodeState.SUSPECTED:
            self.node_states[node_id] = NodeState.ALIVE
            self.failure_history.append({
                'timestamp': current_time,
                'node_id': node_id,
                'event': 'recovery',
                'phi_value': self.phi_values.get(node_id, 0.0)
            })
        
        self.dead_nodes.discard(node_id)
    
    def _calculate_phi(self, node_id: str, now: float) -> float:
        """
        Calculate Phi value for a node using normal distribution.
        
        Phi = -log10(P(t_now)) where P(t) is survival probability
        Using normal distribution of heartbeat intervals.
        """
        if node_id not in self.last_heartbeat:
            return float('inf')
        
        history = self.heartbeat_history.get(node_id)
        if not history or not history.timestamps:
            return float('inf')
        
        elapsed = now - history.get_last_timestamp()
        
        # Not enough history, use simple timeout
        if len(history.timestamps) < 2:
            if elapsed > self.initial_timeout:
                return float('inf')
            return 0.0
        
        mean = history.mean
        std_dev = max(history.std_deviation, self.min_std_deviation)
        
        # Calculate probability using normal distribution
        # P(t) = (1 / (std_dev * sqrt(2*pi))) * exp(-0.5 * ((t - mean) / std_dev)^2)
        exponent = -((elapsed - mean) ** 2) / (2 * std_dev ** 2)
        p = math.exp(exponent) / (std_dev * math.sqrt(2 * math.pi))
        
        if p <= 0 or math.isinf(p):
            return float('inf')
        
        # Phi = -log10(p)
        phi = -math.log10(p) if p > 0 else float('inf')
        return phi
    
    def check_node(self, node_id: str) -> NodeState:
        """Check current state of a node based on Phi value"""
        now = time.time()
        phi = self._calculate_phi(node_id, now)
        self.phi_values[node_id] = phi
        
        if node_id not in self.node_states:
            self.node_states[node_id] = NodeState.DEAD
            return NodeState.DEAD
        
        current_state = self.node_states[node_id]
        
        # State transitions based on Phi threshold
        if phi >= self.phi_threshold:
            # Node suspected or dead
            if current_state == NodeState.ALIVE:
                self.node_states[node_id] = NodeState.SUSPECTED
                self.failure_history.append({
                    'timestamp': now,
                    'node_id': node_id,
                    'event': 'suspected',
                    'phi_value': phi,
                    'threshold': self.phi_threshold
                })
            elif current_state == NodeState.SUSPECTED:
                # After longer period, mark as dead
                if phi > self.phi_threshold * 2:
                    self.node_states[node_id] = NodeState.DEAD
                    if node_id not in self.dead_nodes:
                        self.dead_nodes.add(node_id)
                        self.failure_history.append({
                            'timestamp': now,
                            'node_id': node_id,
                            'event': 'dead',
                            'phi_value': phi
                        })
        else:
            # Node recovered
            if current_state != NodeState.ALIVE:
                self.node_states[node_id] = NodeState.ALIVE
                self.dead_nodes.discard(node_id)
                self.failure_history.append({
                    'timestamp': now,
                    'node_id': node_id,
                    'event': 'recovered',
                    'phi_value': phi
                })
        
        return self.node_states[node_id]
    
    def is_alive(self, node_id: str) -> bool:
        """Check if node is considered alive"""
        state = self.check_node(node_id)
        return state == NodeState.ALIVE
    
    def is_suspected(self, node_id: str) -> bool:
        """Check if node is suspected"""
        state = self.check_node(node_id)
        return state == NodeState.SUSPECTED
    
    def is_dead(self, node_id: str) -> bool:
        """Check if node is considered dead"""
        state = self.check_node(node_id)
        return state == NodeState.DEAD
    
    def get_dead_nodes(self) -> Set[str]:
        """Get all nodes currently marked as dead"""
        for node_id in self.node_states.keys():
            self.check_node(node_id)
        return self.dead_nodes.copy()
    
    def get_alive_nodes(self) -> Set[str]:
        """Get all nodes currently marked as alive"""
        alive = set()
        for node_id in self.node_states.keys():
            if self.is_alive(node_id):
                alive.add(node_id)
        return alive
    
    def get_suspected_nodes(self) -> Set[str]:
        """Get all nodes currently suspected"""
        suspected = set()
        for node_id in self.node_states.keys():
            if self.is_suspected(node_id):
                suspected.add(node_id)
        return suspected
    
    def get_node_status(self, node_id: str) -> Dict:
        """Get detailed status of a specific node"""
        state = self.check_node(node_id)
        phi = self.phi_values.get(node_id, 0.0)
        
        history = self.heartbeat_history.get(node_id)
        last_hb = self.last_heartbeat.get(node_id, 0)
        elapsed = time.time() - last_hb if last_hb else float('inf')
        
        return {
            'node_id': node_id,
            'state': state.value,
            'phi_value': round(phi, 3),
            'phi_threshold': self.phi_threshold,
            'elapsed_since_heartbeat': round(elapsed, 3),
            'heartbeat_mean': round(history.mean, 3) if history else 0.0,
            'heartbeat_std_dev': round(history.std_deviation, 3) if history else 0.0,
            'samples': len(history.timestamps) if history else 0,
            'is_alive': state == NodeState.ALIVE,
            'is_suspected': state == NodeState.SUSPECTED,
            'is_dead': state == NodeState.DEAD
        }
    
    def get_cluster_status(self) -> Dict:
        """Get status of all nodes in cluster"""
        alive_nodes = self.get_alive_nodes()
        dead_nodes = self.get_dead_nodes()
        suspected_nodes = self.get_suspected_nodes()
        
        return {
            'alive_nodes': sorted(list(alive_nodes)),
            'suspected_nodes': sorted(list(suspected_nodes)),
            'dead_nodes': sorted(list(dead_nodes)),
            'alive_count': len(alive_nodes),
            'suspected_count': len(suspected_nodes),
            'dead_count': len(dead_nodes),
            'total_nodes': len(self.node_states),
            'phi_threshold': self.phi_threshold,
            'failure_history': self.failure_history[-20:]  # Last 20 events
        }
    
    def set_threshold(self, phi_threshold: float) -> None:
        """Set the Phi threshold for detection"""
        self.phi_threshold = phi_threshold
    
    def get_diagnostics(self) -> Dict:
        """Get detailed diagnostics for all nodes"""
        diagnostics = {}
        for node_id in self.node_states.keys():
            diagnostics[node_id] = self.get_node_status(node_id)
        return diagnostics


# Backward compatibility alias
FailureDetector = PhiAccrualFailureDetector
