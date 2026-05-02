import hashlib

class DistributedQueue:
    def __init__(self, nodes):
        self.nodes = nodes
        self.storage = {node: [] for node in nodes}

    def _hash(self, key):
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def get_node(self, key):
        return self.nodes[self._hash(key) % len(self.nodes)]

    def enqueue(self, key, message):
        node = self.get_node(key)
        self.storage[node].append(message)
        print(f"[{node}] enqueue -> {message}")

    def dequeue(self, key):
        node = self.get_node(key)
        if self.storage[node]:
            msg = self.storage[node].pop(0)
            print(f"[{node}] dequeue -> {msg}")
            return msg
        
        print(f"[{node}] queue empty")
        return None