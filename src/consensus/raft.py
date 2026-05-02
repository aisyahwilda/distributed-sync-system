import asyncio
import random
import time

class RaftNode:
    def __init__(self, node_id, peers):
        self.node_id = node_id
        self.peers = peers
        self.state = "follower"
        self.current_term = 0
        self.last_heartbeat = time.time()

    async def send_heartbeat(self):
        while self.state == "leader":
            print(f"[{self.node_id}] sending heartbeat")
            self.last_heartbeat = time.time()
            await asyncio.sleep(2)

    async def start_election(self):
        self.state = "candidate"
        self.current_term += 1
        votes = 1

        print(f"[{self.node_id}] election started")

        for _ in self.peers:
            votes += 1

        if votes > len(self.peers) // 2:
            self.state = "leader"
            print(f"[{self.node_id}] becomes LEADER")
            asyncio.create_task(self.send_heartbeat())

    async def monitor(self):
        while True:
            if time.time() - self.last_heartbeat > random.randint(3, 5):
                await self.start_election()
            await asyncio.sleep(1)