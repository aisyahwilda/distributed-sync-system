"""
Comprehensive Load Testing with Locust

Provides multiple user types simulating different distributed system workloads:
- LockManagerUser: Lock acquire/release operations
- QueueUser: Producer/consumer patterns  
- CacheUser: Get/put/delete cache operations
- MixedWorkloadUser: Combined operations
- ThroughputTest: Maximum throughput testing

Run with: locust -f locustfile.py -u 100 -r 10 --run-time 5m --host http://localhost:8001
"""

from locust import HttpUser, task, between, TaskSet, events
import random
import time
from typing import Dict, List


class StatsCollector:
    """Collect and analyze performance statistics"""
    
    def __init__(self):
        self.request_times: List[float] = []
        self.lock_times: List[float] = []
        self.queue_times: List[float] = []
        self.cache_times: List[float] = []
        self.errors = 0
        self.start_time = time.time()
    
    def record_request(self, elapsed_time: float, request_type: str):
        """Record a request timing"""
        self.request_times.append(elapsed_time)
        
        if request_type == "lock":
            self.lock_times.append(elapsed_time)
        elif request_type == "queue":
            self.queue_times.append(elapsed_time)
        elif request_type == "cache":
            self.cache_times.append(elapsed_time)
    
    def get_percentile(self, data: List[float], percentile: int) -> float:
        """Calculate percentile from data"""
        if not data:
            return 0.0
        sorted_data = sorted(data)
        idx = int(len(sorted_data) * (percentile / 100.0))
        return sorted_data[min(idx, len(sorted_data) - 1)]
    
    def get_stats(self) -> Dict:
        """Get detailed statistics"""
        elapsed = time.time() - self.start_time
        
        if not self.request_times:
            return {}
        
        return {
            'total_requests': len(self.request_times),
            'total_time': elapsed,
            'rps': len(self.request_times) / elapsed if elapsed > 0 else 0,
            'avg_latency_ms': (sum(self.request_times) / len(self.request_times)) * 1000,
            'p50_ms': self.get_percentile(self.request_times, 50) * 1000,
            'p75_ms': self.get_percentile(self.request_times, 75) * 1000,
            'p90_ms': self.get_percentile(self.request_times, 90) * 1000,
            'p95_ms': self.get_percentile(self.request_times, 95) * 1000,
            'p99_ms': self.get_percentile(self.request_times, 99) * 1000,
            'min_ms': min(self.request_times) * 1000,
            'max_ms': max(self.request_times) * 1000,
            'errors': self.errors
        }


stats_collector = StatsCollector()


class LockManagerUserTasks(TaskSet):
    """Lock manager workload tasks"""
    
    @task(1)
    def acquire_exclusive_lock(self):
        """Acquire exclusive lock"""
        start = time.time()
        resource_id = f"resource_{random.randint(1, 10)}"
        payload = {
            "node_id": f"locust_{random.randint(1, 1000)}",
            "resource_id": resource_id,
            "lock_type": "exclusive"
        }

        with self.client.post(
            "/lock/acquire",
            json=payload,
            catch_response=True
        ) as response:
            elapsed = time.time() - start
            stats_collector.record_request(elapsed, "lock")

            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Failed to acquire lock: {response.status_code}")
    
    @task(1)
    def acquire_shared_lock(self):
        """Acquire shared lock"""
        start = time.time()
        resource_id = f"resource_{random.randint(1, 10)}"
        payload = {
            "node_id": f"locust_{random.randint(1, 1000)}",
            "resource_id": resource_id,
            "lock_type": "shared"
        }

        with self.client.post(
            "/lock/acquire",
            json=payload,
            catch_response=True
        ) as response:
            elapsed = time.time() - start
            stats_collector.record_request(elapsed, "lock")

            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Failed to acquire shared lock: {response.status_code}")
    
    @task(2)
    def check_lock_status(self):
        """Check status of a lock"""
        resource_id = f"resource_{random.randint(1, 10)}"
        
        with self.client.get(
            f"/lock/status/{resource_id}",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Failed to check status: {response.status_code}")


class LockManagerUser(HttpUser):
    """User simulating lock manager workload"""
    tasks = [LockManagerUserTasks]
    wait_time = between(0.5, 2.0)


class QueueUserTasks(TaskSet):
    """Queue manager workload tasks"""
    
    @task(2)
    def enqueue_message(self):
        """Enqueue a message"""
        start = time.time()
        key = f"queue_{random.randint(1, 5)}"
        message = f"message_{time.time()}"

        with self.client.post(
            "/queue/enqueue",
            json={"key": key, "content": message},
            catch_response=True
        ) as response:
            elapsed = time.time() - start
            stats_collector.record_request(elapsed, "queue")

            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Failed to enqueue: {response.status_code}")
                stats_collector.errors += 1
    
    @task(2)
    def dequeue_message(self):
        """Dequeue a message"""
        start = time.time()
        key = f"queue_{random.randint(1, 5)}"
        
        with self.client.get(
            f"/queue/dequeue?key={key}",
            catch_response=True
        ) as response:
            elapsed = time.time() - start
            stats_collector.record_request(elapsed, "queue")
            
            if response.status_code in [200, 204]:
                response.success()
            else:
                response.failure(f"Failed to dequeue: {response.status_code}")
    
    @task(1)
    def check_queue_status(self):
        """Check queue status"""
        with self.client.get(
            "/queue/status",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Failed to check queue status: {response.status_code}")


class QueueUser(HttpUser):
    """User simulating queue workload"""
    tasks = [QueueUserTasks]
    wait_time = between(0.1, 0.5)


class CacheUserTasks(TaskSet):
    """Cache manager workload tasks"""
    
    @task(3)
    def cache_get(self):
        """Get from cache"""
        start = time.time()
        key = f"cache_key_{random.randint(1, 100)}"
        
        with self.client.get(
            f"/cache/get/{key}",
            catch_response=True
        ) as response:
            elapsed = time.time() - start
            stats_collector.record_request(elapsed, "cache")
            
            if response.status_code in [200, 404]:
                response.success()
            else:
                response.failure(f"Failed to get from cache: {response.status_code}")
    
    @task(2)
    def cache_put(self):
        """Put to cache"""
        start = time.time()
        key = f"cache_key_{random.randint(1, 100)}"
        value = f"value_{time.time()}"
        
        with self.client.post(
            f"/cache/put",
            json={'key': key, 'value': value},
            catch_response=True
        ) as response:
            elapsed = time.time() - start
            stats_collector.record_request(elapsed, "cache")
            
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Failed to put to cache: {response.status_code}")
                stats_collector.errors += 1
    
    @task(1)
    def cache_stats(self):
        """Get cache statistics"""
        with self.client.get(
            "/cache/stats",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Failed to get cache stats: {response.status_code}")


class CacheUser(HttpUser):
    """User simulating cache workload"""
    tasks = [CacheUserTasks]
    wait_time = between(0.05, 0.2)


class MixedWorkloadTasks(TaskSet):
    """Mixed workload combining all operations"""
    
    @task(1)
    def lock_operation(self):
        """Random lock operation"""
        resource_id = f"resource_{random.randint(1, 5)}"
        lock_type = random.choice(["shared", "exclusive"])

        self.client.post(
            "/lock/acquire",
            json={
                "node_id": f"locust_{random.randint(1, 1000)}",
                "resource_id": resource_id,
                "lock_type": lock_type
            }
        )
    
    @task(1)
    def queue_operation(self):
        """Random queue operation"""
        key = f"queue_{random.randint(1, 3)}"

        if random.random() > 0.5:
            self.client.post(
                "/queue/enqueue",
                json={"key": key, "content": "test"}
            )
        else:
            self.client.get(f"/queue/dequeue?key={key}")
    
    @task(1)
    def cache_operation(self):
        """Random cache operation"""
        key = f"cache_{random.randint(1, 5)}"
        
        if random.random() > 0.5:
            self.client.get(f"/cache/get/{key}")
        else:
            self.client.post(f"/cache/put", json={'key': key, 'value': 'data'})


class MixedWorkloadUser(HttpUser):
    """User simulating mixed workload"""
    tasks = [MixedWorkloadTasks]
    wait_time = between(0.1, 0.5)


class ThroughputTest(HttpUser):
    """Stress test for maximum throughput"""
    
    @task
    def throughput_test(self):
        """Send requests as fast as possible"""
        self.client.get("/health")
    
    wait_time = between(0, 0.01)  # Minimal delay


@events.quitting.add_listener
def _(environment, **kwargs):
    """Print statistics when test finishes"""
    stats = stats_collector.get_stats()
    print("\n" + "="*60)
    print("LOAD TEST SUMMARY")
    print("="*60)
    print(f"Total Requests: {stats.get('total_requests', 0)}")
    print(f"Duration: {stats.get('total_time', 0):.2f}s")
    print(f"RPS: {stats.get('rps', 0):.2f}")
    print(f"Avg Latency: {stats.get('avg_latency_ms', 0):.2f}ms")
    print(f"P50: {stats.get('p50_ms', 0):.2f}ms")
    print(f"P75: {stats.get('p75_ms', 0):.2f}ms")
    print(f"P90: {stats.get('p90_ms', 0):.2f}ms")
    print(f"P95: {stats.get('p95_ms', 0):.2f}ms")
    print(f"P99: {stats.get('p99_ms', 0):.2f}ms")
    print(f"Min: {stats.get('min_ms', 0):.2f}ms")
    print(f"Max: {stats.get('max_ms', 0):.2f}ms")
    print(f"Errors: {stats.get('errors', 0)}")
    print("="*60)