import time

class FailureDetector:
    def __init__(self, timeout=5):
        self.heartbeats = {}
        self.timeout = timeout

    def heartbeat(self, node_id):
        self.heartbeats[node_id] = time.time()
        print(f"[HEARTBEAT] {node_id}")

    def is_alive(self, node_id):
        last = self.heartbeats.get(node_id)

        if not last:
            return False

        return (time.time() - last) < self.timeout

    def get_dead_nodes(self):
        dead = []
        for node, t in self.heartbeats.items():
            if (time.time() - t) > self.timeout:
                dead.append(node)
        return dead