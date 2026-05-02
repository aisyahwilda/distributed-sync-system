import asyncio
import json

class MessagePassing:
    def __init__(self):
        self.inbox = {}

    async def send(self, sender, receiver, message):
        await asyncio.sleep(0.1) 

        if receiver not in self.inbox:
            self.inbox[receiver] = []

        payload = {
            "from": sender,
            "message": message
        }

        self.inbox[receiver].append(payload)

        print(f"[SEND] {sender} -> {receiver}: {message}")

    async def receive(self, node_id):
        await asyncio.sleep(0.1)

        messages = self.inbox.get(node_id, [])
        self.inbox[node_id] = []

        return messages