import time

class Metrics:
    def __init__(self):
        self.requests = 0
        self.start_time = time.time()
        self.latencies = []

    def record_request(self):
        self.requests += 1

    def record_latency(self, latency):
        self.latencies.append(latency)

    def throughput(self):
        duration = time.time() - self.start_time
        if duration == 0:
            return 0
        return self.requests / duration

    def avg_latency(self):
        if not self.latencies:
            return 0
        return sum(self.latencies) / len(self.latencies)

    def report(self):
        return {
            "requests": self.requests,
            "throughput": self.throughput(),
            "avg_latency": self.avg_latency()
        }