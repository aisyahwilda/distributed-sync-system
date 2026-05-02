from collections import OrderedDict

class CacheNode:
    def __init__(self, capacity=5):
        self.cache = OrderedDict()
        self.capacity = capacity

    def get(self, key):
        if key in self.cache:
            self.cache.move_to_end(key)
            return self.cache[key]
        return None

    def put(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)

        self.cache[key] = value

        if len(self.cache) > self.capacity:
            removed = self.cache.popitem(last=False)
            print(f"Removed (LRU): {removed}")