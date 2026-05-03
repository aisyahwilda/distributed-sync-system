from src.nodes.lock_manager import LockManager

def test_lock_acquire():
    lm = LockManager()
    result = lm.acquire_exclusive("node1")
    assert result == True

def test_lock_release():
    lm = LockManager()
    lm.acquire_exclusive("node1")
    lm.release("node1")

    result = lm.acquire_exclusive("node1")
    assert result == True