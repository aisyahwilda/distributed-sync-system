# Deployment Guide

## Prerequisites

- Docker & Docker Compose (v3.8+)
- Python 3.10+ (for local development)
- 3+ nodes with network connectivity

## Quick Start with Docker

### 1. Start the Cluster

```bash
cd docker
docker-compose up --build
```

This starts 3 nodes:

- Node1: http://localhost:8001
- Node2: http://localhost:8002
- Node3: http://localhost:8003

### 2. Verify Health

```bash
curl http://localhost:8001/health
```

Expected response:

```json
{
  "status": "healthy",
  "node_id": "node1",
  "timestamp": 1234567890.123
}
```

### 3. Stop the Cluster

```bash
docker-compose down
```

## Local Development

### Setup Python Environment

```bash
# Create virtual environment
python -m venv venv

# Activate
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Run Single Node Locally

```bash
export NODE_ID=node1
export PORT=8000
export PEERS=node2,node3

uvicorn src.communication.api:app --reload --port 8000
```

API docs: http://localhost:8000/docs

### Run Tests

```bash
# All tests
pytest tests/

# Unit tests only
pytest tests/unit/ -v

# Integration tests
pytest tests/integration/ -v

# Chaos engineering tests
pytest tests/integration/test_chaos.py -v

# With coverage
pytest tests/ --cov=src --cov-report=html
```

## Configuration

### Environment Variables

Create `.env` file:

```env
NODE_ID=node1
PORT=8000
PEERS=node2,node3
CACHE_SIZE=100
REPLICATION_FACTOR=2
FAILURE_DETECTION_TIMEOUT=5
```

### Docker Compose Customization

#### Scale to 5 Nodes

Edit `docker-compose.yml`:

```yaml
node4:
  build:
    context: ..
    dockerfile: docker/Dockerfile.node
  container_name: node4
  ports:
    - "8004:8000"
  environment:
    - NODE_ID=node4
    - PORT=8000
    - PEERS=node1,node2,node3,node5
  networks:
    - distnet
  volumes:
    - ./data:/app/data

node5:
  build:
    context: ..
    dockerfile: docker/Dockerfile.node
  container_name: node5
  ports:
    - "8005:8000"
  environment:
    - NODE_ID=node5
    - PORT=8000
    - PEERS=node1,node2,node3,node4
  networks:
    - distnet
  volumes:
    - ./data:/app/data
```

Then update each node's PEERS:

```yaml
environment:
  - PEERS=node1,node2,node3,node4,node5
```

## Testing the System

### 1. Test Lock Manager

```bash
# Acquire exclusive lock
curl -X POST http://localhost:8001/lock/acquire \
  -H "Content-Type: application/json" \
  -d '{
    "node_id": "node1",
    "resource_id": "db_connection",
    "lock_type": "exclusive"
  }'

# Get lock status
curl http://localhost:8001/lock/status/db_connection

# Release lock
curl -X POST http://localhost:8001/lock/release \
  -H "Content-Type: application/json" \
  -d '{
    "node_id": "node1",
    "resource_id": "db_connection"
  }'
```

### 2. Test Distributed Queue

```bash
# Enqueue message
curl -X POST http://localhost:8001/queue/enqueue \
  -H "Content-Type: application/json" \
  -d '{
    "key": "order_queue",
    "content": "Order #12345"
  }'

# Dequeue message
curl http://localhost:8001/queue/dequeue?key=order_queue

# Get queue status
curl http://localhost:8001/queue/status
```

### 3. Test Cache System

```bash
# Put in cache
curl -X POST http://localhost:8001/cache/put \
  -H "Content-Type: application/json" \
  -d '{
    "key": "user_123",
    "value": "John Doe"
  }'

# Get from cache
curl http://localhost:8001/cache/get/user_123

# Get cache stats
curl http://localhost:8001/cache/stats
```

### 4. Test Raft Consensus

```bash
# Get Raft status
curl http://localhost:8001/raft/status

# Get node2 status
curl http://localhost:8002/raft/status

# Get node3 status
curl http://localhost:8003/raft/status
```

### 5. Test Failure Detection

```bash
# Get cluster status
curl http://localhost:8001/failure-detector/status

# Get specific node status
curl http://localhost:8001/failure-detector/node-status/node2
```

## Performance Testing

### Load Testing with Locust

```bash
# Run load test
locust -f benchmarks/locustfile.py \
  -u 100 \
  -r 10 \
  -t 1m \
  -H http://localhost:8001
```

### Benchmark Individual Components

```python
# Create benchmark script
import time
from src.nodes.lock_manager import DistributedLockManager

lm = DistributedLockManager("node1", ["node1", "node2", "node3"])

# Measure lock acquisition
start = time.time()
for i in range(1000):
    lm.acquire_exclusive(f"node{i % 3}", f"res_{i % 10}", f"req_{i}")
elapsed = time.time() - start

print(f"1000 lock acquisitions: {elapsed:.2f}s")
print(f"Throughput: {1000/elapsed:.0f} ops/sec")
```

## Monitoring

### Docker Logs

```bash
# View node1 logs
docker logs node1

# Follow logs
docker logs -f node1

# View all nodes
docker logs node1 node2 node3
```

### Docker Stats

```bash
# View container statistics
docker stats node1 node2 node3
```

### Health Checks

```bash
# Check all nodes health
for node in {1..3}; do
  echo "Node$node:"
  curl http://localhost:800$node/health
  echo ""
done
```

## Troubleshooting

### Issue: Containers fail to start

```bash
# Check logs
docker-compose logs

# Rebuild
docker-compose down
docker-compose up --build --force-recreate
```

### Issue: Network errors between nodes

```bash
# Check network
docker network ls
docker network inspect distnet

# Test connectivity
docker exec node1 ping node2
```

### Issue: Port already in use

```bash
# Find process using port 8001
lsof -i :8001

# Kill process or use different port
docker-compose down  # or change port mapping
```

### Issue: Deadlock in lock manager

```bash
# Check deadlock detection
curl http://localhost:8001/lock/deadlock-check/node1

# Reset locks (restart container)
docker-compose restart node1
```

## Cleanup

```bash
# Stop containers
docker-compose down

# Remove volumes
docker-compose down -v

# Remove all data
rm -rf data/

# Remove images
docker rmi distributed-sync-system-node1 \
           distributed-sync-system-node2 \
           distributed-sync-system-node3
```

## Advanced Configuration

### Enable Network Faults

Modify `docker-compose.yml` environment:

```yaml
environment:
  - NETWORK_PARTITION_ENABLED=true
  - MESSAGE_LOSS_RATE=0.1 # 10% message loss
```

### Custom Failure Timeout

```yaml
environment:
  - FAILURE_DETECTION_TIMEOUT=10 # 10 seconds
```

### Adjust Cache Size

```yaml
environment:
  - CACHE_SIZE=500 # 500 entries per node
```

## Production Deployment

### Kubernetes

For Kubernetes deployment, use StatefulSet:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: distributed-sync
spec:
  serviceName: distributed-sync
  replicas: 3
  selector:
    matchLabels:
      app: distributed-sync
  template:
    metadata:
      labels:
        app: distributed-sync
    spec:
      containers:
        - name: node
          image: distributed-sync-system:latest
          ports:
            - containerPort: 8000
          env:
            - name: NODE_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: PEERS
              value: "distributed-sync-0,distributed-sync-1,distributed-sync-2"
          volumeMounts:
            - name: data
              mountPath: /app/data
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 10Gi
```

## Support

For issues or questions:

1. Check logs: `docker-compose logs`
2. Review architecture: `docs/architecture.md`
3. Check API docs: http://localhost:8001/docs
