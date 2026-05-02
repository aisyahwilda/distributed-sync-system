import asyncio
from src.communication.message_passing import MessagePassing
from src.communication.failure_detector import FailureDetector

class BaseNode:
    def __init__(self, node_id, peers):
        self.node_id = node_id
        self.peers = peers
        self.message_system = MessagePassing()
        self.failure_detector = FailureDetector()

    async def send_to_all(self, message):
        for peer in self.peers:
            await self.message_system.send(self.node_id, peer, message)

    async def receive_messages(self):
        messages = await self.message_system.receive(self.node_id)

        for msg in messages:
            print(f"[{self.node_id}] received from {msg['from']}: {msg['message']}")

    async def send_heartbeat(self):
        while True:
            self.failure_detector.heartbeat(self.node_id)
            await asyncio.sleep(2)

    async def monitor_nodes(self):
        while True:
            dead_nodes = self.failure_detector.get_dead_nodes()
            if dead_nodes:
                print(f"[{self.node_id}] detected dead nodes: {dead_nodes}")
            await asyncio.sleep(3)