"""
Prometheus Metrics Collection

Provides Prometheus-compatible metrics collection and export.
Tracks system, business, and Raft metrics for monitoring and observability.
"""

import time
from collections import defaultdict
from typing import Dict, List, Optional
from dataclasses import dataclass, field
import threading


@dataclass
class MetricSnapshot:
    """Snapshot of metrics at a point in time"""
    timestamp: float = field(default_factory=time.time)
    counters: Dict[str, float] = field(default_factory=dict)
    gauges: Dict[str, float] = field(default_factory=dict)
    histograms: Dict[str, List[float]] = field(default_factory=dict)


class Counter:
    """Counter metric - monotonically increasing"""
    
    def __init__(self, name: str, description: str = "", labels: Dict[str, str] = None):
        self.name = name
        self.description = description
        self.labels = labels or {}
        self.value = 0.0
        self.lock = threading.Lock()
    
    def inc(self, amount: float = 1.0):
        """Increment counter"""
        with self.lock:
            self.value += amount
    
    def get_value(self) -> float:
        """Get current counter value"""
        with self.lock:
            return self.value


class Gauge:
    """Gauge metric - can go up and down"""
    
    def __init__(self, name: str, description: str = "", labels: Dict[str, str] = None):
        self.name = name
        self.description = description
        self.labels = labels or {}
        self.value = 0.0
        self.lock = threading.Lock()
    
    def set(self, value: float):
        """Set gauge to exact value"""
        with self.lock:
            self.value = value
    
    def inc(self, amount: float = 1.0):
        """Increment gauge"""
        with self.lock:
            self.value += amount
    
    def dec(self, amount: float = 1.0):
        """Decrement gauge"""
        with self.lock:
            self.value -= amount
    
    def get_value(self) -> float:
        """Get current gauge value"""
        with self.lock:
            return self.value


class Histogram:
    """Histogram metric - tracks distribution of values"""
    
    def __init__(
        self,
        name: str,
        description: str = "",
        buckets: List[float] = None,
        labels: Dict[str, str] = None
    ):
        self.name = name
        self.description = description
        self.labels = labels or {}
        
        # Default buckets: exponential from 1ms to 10s
        if buckets is None:
            self.buckets = [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]
        else:
            self.buckets = sorted(buckets)
        
        self.bucket_counts = defaultdict(int)  # bucket -> count
        self.sum = 0.0
        self.count = 0
        self.lock = threading.Lock()
    
    def observe(self, value: float):
        """Record a value in the histogram"""
        with self.lock:
            self.sum += value
            self.count += 1
            
            # Find appropriate bucket
            for bucket in self.buckets:
                if value <= bucket:
                    self.bucket_counts[bucket] += 1
            # +Inf bucket
            self.bucket_counts[float('inf')] += 1
    
    def get_stats(self) -> Dict:
        """Get statistics from histogram"""
        with self.lock:
            if self.count == 0:
                return {'sum': 0, 'count': 0, 'avg': 0}
            
            return {
                'sum': self.sum,
                'count': self.count,
                'avg': self.sum / self.count,
                'buckets': dict(self.bucket_counts)
            }


class MetricsCollector:
    """
    Centralized metrics collection with Prometheus-compatible export.
    """
    
    def __init__(self, node_id: str = "default"):
        self.node_id = node_id
        self.start_time = time.time()
        
        # Counters
        self.counters: Dict[str, Counter] = {}
        
        # Gauges
        self.gauges: Dict[str, Gauge] = {}
        
        # Histograms
        self.histograms: Dict[str, Histogram] = {}
        
        self.lock = threading.Lock()
        
        self._initialize_metrics()
    
    def _initialize_metrics(self):
        """Initialize standard metrics"""
        # System metrics
        self.gauge("process_uptime_seconds", "Process uptime in seconds")
        
        # Lock manager metrics
        self.counter("locks_acquired_total", "Total locks acquired")
        self.counter("locks_released_total", "Total locks released")
        self.counter("locks_failed_total", "Total lock acquisition failures")
        self.counter("deadlock_detected_total", "Total deadlocks detected")
        self.gauge("locks_active", "Number of active locks")
        self.histogram("lock_wait_time_seconds", "Time spent waiting for locks", 
                      buckets=[0.001, 0.01, 0.1, 1.0, 5.0])
        self.histogram("lock_hold_time_seconds", "Time holding locks",
                      buckets=[0.001, 0.01, 0.1, 1.0, 5.0])
        
        # Queue metrics
        self.counter("messages_enqueued_total", "Total messages enqueued")
        self.counter("messages_dequeued_total", "Total messages dequeued")
        self.counter("messages_acked_total", "Total messages acknowledged")
        self.counter("messages_nacked_total", "Total messages nacked")
        self.gauge("queue_size", "Current queue size")
        self.gauge("messages_pending", "Messages pending acknowledgment")
        self.histogram("message_latency_seconds", "Message delivery latency",
                      buckets=[0.001, 0.01, 0.1, 1.0])
        
        # Cache metrics
        self.counter("cache_hits_total", "Total cache hits")
        self.counter("cache_misses_total", "Total cache misses")
        self.counter("cache_puts_total", "Total cache puts")
        self.counter("cache_invalidations_total", "Total cache invalidations")
        self.gauge("cache_size", "Current cache size")
        self.gauge("cache_hit_rate", "Cache hit rate percentage")
        self.histogram("cache_operation_time_seconds", "Cache operation latency",
                      buckets=[0.0001, 0.001, 0.01, 0.1])
        
        # Raft metrics
        self.counter("raft_elections_total", "Total Raft elections")
        self.counter("raft_leader_changes_total", "Total leader changes")
        self.gauge("raft_is_leader", "Whether this node is leader")
        self.gauge("raft_current_term", "Current Raft term")
        self.gauge("raft_log_size", "Raft log size")
        self.gauge("raft_commit_index", "Raft commit index")
        self.histogram("raft_append_entries_latency_seconds", "AppendEntries RPC latency",
                      buckets=[0.001, 0.01, 0.1, 1.0])
        
        # Communication metrics
        self.counter("rpc_calls_total", "Total RPC calls")
        self.counter("rpc_errors_total", "Total RPC errors")
        self.counter("messages_sent_total", "Total messages sent")
        self.counter("messages_received_total", "Total messages received")
        self.counter("messages_retried_total", "Total message retries")
        self.gauge("network_partitions_active", "Active network partitions")
        self.histogram("rpc_latency_seconds", "RPC call latency",
                      buckets=[0.001, 0.01, 0.1, 1.0, 5.0])
        
        # Failure detection metrics
        self.counter("failures_detected_total", "Total failures detected")
        self.gauge("nodes_alive", "Number of alive nodes")
        self.gauge("nodes_suspected", "Number of suspected nodes")
        self.gauge("nodes_dead", "Number of dead nodes")
    
    def counter(self, name: str, description: str = "") -> Counter:
        """Get or create a counter metric"""
        if name not in self.counters:
            with self.lock:
                if name not in self.counters:
                    self.counters[name] = Counter(name, description)
        return self.counters[name]
    
    def gauge(self, name: str, description: str = "") -> Gauge:
        """Get or create a gauge metric"""
        if name not in self.gauges:
            with self.lock:
                if name not in self.gauges:
                    self.gauges[name] = Gauge(name, description)
        return self.gauges[name]
    
    def histogram(self, name: str, description: str = "", buckets: List[float] = None) -> Histogram:
        """Get or create a histogram metric"""
        if name not in self.histograms:
            with self.lock:
                if name not in self.histograms:
                    self.histograms[name] = Histogram(name, description, buckets)
        return self.histograms[name]
    
    def update_uptime(self):
        """Update process uptime gauge"""
        uptime = time.time() - self.start_time
        self.gauge("process_uptime_seconds").set(uptime)
    
    def to_prometheus_format(self) -> str:
        """Export metrics in Prometheus text format"""
        lines = []
        
        self.update_uptime()
        
        # Export counters
        for name, counter in self.counters.items():
            lines.append(f"# TYPE {name} counter")
            lines.append(f"{name}{{node=\"{self.node_id}\"}} {counter.get_value()}")
        
        # Export gauges
        for name, gauge in self.gauges.items():
            lines.append(f"# TYPE {name} gauge")
            lines.append(f"{name}{{node=\"{self.node_id}\"}} {gauge.get_value()}")
        
        # Export histograms
        for name, histogram in self.histograms.items():
            lines.append(f"# TYPE {name} histogram")
            stats = histogram.get_stats()
            
            # Buckets
            for bucket in histogram.buckets:
                bucket_name = "inf" if bucket == float('inf') else f"{bucket}"
                count = histogram.bucket_counts.get(bucket, 0)
                lines.append(f"{name}_bucket{{le=\"{bucket_name}\",node=\"{self.node_id}\"}} {count}")
            
            # Sum and count
            lines.append(f"{name}_sum{{node=\"{self.node_id}\"}} {stats['sum']}")
            lines.append(f"{name}_count{{node=\"{self.node_id}\"}} {stats['count']}")
        
        return "\n".join(lines) + "\n"
    
    def get_summary(self) -> Dict:
        """Get human-readable summary of key metrics"""
        cache_hit_count = self.counters.get("cache_hits_total", Counter("")).get_value()
        cache_miss_count = self.counters.get("cache_misses_total", Counter("")).get_value()
        cache_total = cache_hit_count + cache_miss_count
        cache_hit_rate = (cache_hit_count / cache_total * 100) if cache_total > 0 else 0
        
        lock_acquired = self.counters.get("locks_acquired_total", Counter("")).get_value()
        lock_failed = self.counters.get("locks_failed_total", Counter("")).get_value()
        
        msg_enqueued = self.counters.get("messages_enqueued_total", Counter("")).get_value()
        msg_dequeued = self.counters.get("messages_dequeued_total", Counter("")).get_value()
        
        return {
            'node_id': self.node_id,
            'uptime_seconds': time.time() - self.start_time,
            'cache': {
                'hit_rate': f"{cache_hit_rate:.2f}%",
                'hits': int(cache_hit_count),
                'misses': int(cache_miss_count),
                'current_size': int(self.gauges.get("cache_size", Gauge("")).get_value())
            },
            'locks': {
                'acquired': int(lock_acquired),
                'failed': int(lock_failed),
                'active': int(self.gauges.get("locks_active", Gauge("")).get_value())
            },
            'queue': {
                'enqueued': int(msg_enqueued),
                'dequeued': int(msg_dequeued),
                'size': int(self.gauges.get("queue_size", Gauge("")).get_value()),
                'pending': int(self.gauges.get("messages_pending", Gauge("")).get_value())
            },
            'raft': {
                'is_leader': bool(self.gauges.get("raft_is_leader", Gauge("")).get_value()),
                'current_term': int(self.gauges.get("raft_current_term", Gauge("")).get_value()),
                'log_size': int(self.gauges.get("raft_log_size", Gauge("")).get_value())
            },
            'failures': {
                'alive_nodes': int(self.gauges.get("nodes_alive", Gauge("")).get_value()),
                'suspected_nodes': int(self.gauges.get("nodes_suspected", Gauge("")).get_value()),
                'dead_nodes': int(self.gauges.get("nodes_dead", Gauge("")).get_value())
            }
        }


# Global metrics instance
_global_metrics: Optional[MetricsCollector] = None


def get_metrics(node_id: str = "default") -> MetricsCollector:
    """Get global metrics collector instance"""
    global _global_metrics
    if _global_metrics is None:
        _global_metrics = MetricsCollector(node_id)
    return _global_metrics


def reset_metrics(node_id: str = "default"):
    """Reset and reinitialize metrics"""
    global _global_metrics
    _global_metrics = MetricsCollector(node_id)
