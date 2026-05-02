import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    NODE_ID = os.getenv("NODE_ID", "node1")
    PORT = int(os.getenv("PORT", 8000))
    PEERS = os.getenv("PEERS", "node2,node3").split(",")

    HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", 2))
    TIMEOUT = int(os.getenv("TIMEOUT", 5))

    CACHE_SIZE = int(os.getenv("CACHE_SIZE", 5))
    CACHE_TTL = int(os.getenv("CACHE_TTL", 60))