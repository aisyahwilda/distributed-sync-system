import asyncio
from src.consensus.raft import RaftNode
from src.utils.metrics import Metrics

node = RaftNode("node1", ["node2", "node3"])
asyncio.run(node.monitor())

metrics = Metrics()

metrics.record_request()
metrics.record_latency(0.2)

print(metrics.report())