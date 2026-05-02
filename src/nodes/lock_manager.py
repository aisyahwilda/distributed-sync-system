class LockManager:
    def __init__(self):
        self.exclusive_lock = None
        self.shared_locks = set()

    def acquire_exclusive(self, node_id):
        if self.exclusive_lock or self.shared_locks:
            return False
        self.exclusive_lock = node_id
        return True

    def acquire_shared(self, node_id):
        if self.exclusive_lock:
            return False
        self.shared_locks.add(node_id)
        return True

    def release(self, node_id):
        if self.exclusive_lock == node_id:
            self.exclusive_lock = None
        self.shared_locks.discard(node_id)