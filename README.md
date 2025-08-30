# Disheap

[![Go Report Card](https://goreportcard.com/badge/github.com/frozaken/disheaps)](https://goreportcard.com/report/github.com/frozaken/disheaps)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=flat&logo=docker&logoColor=white)](https://www.docker.com/)

**Disheap** is a high-performance, distributed priority messaging system built with Raft consensus for strong consistency guarantees. It provides exactly-once delivery, distributed transactions, and real-time streaming capabilities.

## Features

### Core Capabilities
- **Distributed Consensus** - Raft-based leader election and log replication
- **Priority Queues** - Message ordering with configurable priority levels
- **Exactly-Once Delivery** - Strong consistency guarantees with message acknowledgments
- **Real-Time Streaming** - Bidirectional gRPC streaming for live message consumption
- **Distributed Transactions** - Two-phase commit protocol for atomic operations
- **Horizontal Scaling** - Multi-partition topics with automatic load balancing

### Production Ready
- **Fault Tolerance** - Automatic failover and recovery
- **Persistent Storage** - BadgerDB for high-performance data persistence
- **Monitoring** - Prometheus metrics and Grafana dashboards
- **Container Native** - Docker and Kubernetes ready
- **Observability** - Structured logging and health checks
- **Comprehensive Testing** - End-to-end integration test suite

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Disheap Cluster                         │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐        │
│  │   Engine 1  │    │   Engine 2  │    │   Engine 3  │        │
│  │  (Leader)   │◄──►│ (Follower)  │◄──►│ (Follower)  │        │
│  └─────────────┘    └─────────────┘    └─────────────┘        │
│         │                   │                   │             │
│         └───────────────────┼───────────────────┘             │
│                             │                                 │
│                    ┌────────▼────────┐                        │
│                    │  Raft Consensus │                        │
│                    │   & Replication │                        │
│                    └─────────────────┘                        │
└─────────────────────────────────────────────────────────────────┘
                              │
                    ┌─────────▼─────────┐
                    │    Client SDKs    │
                    │                   │
                    │  Python │ Go │ JS │
                    └───────────────────┘
```

### Components

| Component | Description | Technology |
|-----------|-------------|------------|
| **[disheap-engine](disheap-engine/)** | Core distributed engine with Raft consensus | Go, gRPC, BadgerDB |
| **[disheap-python](disheap-python/)** | Python SDK with asyncio support | Python, gRPC, Poetry |
| **[disheap-api](disheap-api/)** | REST API gateway for HTTP clients | Go, HTTP, JSON |
| **[disheap-frontend](disheap-frontend/)** | Web dashboard for monitoring and management | React, TypeScript, Tailwind |

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.8+ (for SDK)
- Go 1.21+ (for development)

### 1. Start the Cluster

```bash
# Clone the repository
git clone https://github.com/frozaken/disheaps.git
cd disheaps

# Start 3-node Raft cluster
docker compose up --build
```

### 2. Verify Cluster Health

```bash
# Check all nodes are healthy
curl http://localhost:8080/health  # Engine 1
curl http://localhost:8082/health  # Engine 2  
curl http://localhost:8084/health  # Engine 3

# Check Raft cluster status
curl http://localhost:8080/raft/status | jq
```

### 3. Install Python SDK

```bash
cd disheap-python
pip install poetry
poetry install
```

### 4. Send Your First Message

```python
import asyncio
from datetime import timedelta
from disheap import DisheapClient

async def main():
    # Connect to cluster
    client = DisheapClient(
        endpoints=['localhost:9090', 'localhost:9092', 'localhost:9094'],
        api_key='dh_test_key_secret123'
    )
    
    async with client:
        # Create a topic
        await client.create_topic(
            name="my-priority-queue",
            partitions=3,
            replication_factor=2,
            retention_time=timedelta(hours=24)
        )
        
        # Send messages with priorities
        async with client.producer() as producer:
            await producer.enqueue("my-priority-queue", b"High priority task", priority=100)
            await producer.enqueue("my-priority-queue", b"Low priority task", priority=1)
        
        # Consume messages (highest priority first)
        async with client.consumer() as consumer:
            async for message in consumer.pop_stream("my-priority-queue"):
                print(f"Received: {message.payload} (priority: {message.priority})")
                await message.ack()

if __name__ == "__main__":
    asyncio.run(main())
```

## Documentation

### Getting Started
- [Docker Setup Guide](DOCKER_README.md)
- [Running Tests](tests/README.md)
- [Development Setup](scripts/setup-dev-environment.sh)

### Components
- [Raft Engine](disheap-engine/pkg/raft/README.md)
- [Python SDK](disheap-python/README.md)
- [REST API](disheap-api/README.md)
- [Web Frontend](disheap-frontend/README.md)

### Advanced Topics
- [Spine Index](disheap-engine/pkg/spine/README.md)
- [Transactions](disheap-engine/pkg/txn/README.md)
- [Message Delivery](disheap-engine/pkg/delivery/README.md)

## Testing

Run the comprehensive test suite:

```bash
# Integration tests
python tests/integration/enqueue_pop_test.py
python tests/integration/test_raft_replication.py

# Load testing
python tests/integration/test_disconnect_handling.py

# Cluster verification
python tests/integration/test_raft_replication.py
```

## Monitoring

Access the monitoring dashboard:

- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090

Key metrics:
- Message throughput and latency
- Raft consensus health
- Cluster topology
- Storage utilization

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `NODE_ID` | Unique node identifier | `hostname` |
| `RAFT_BIND` | Raft bind address | `:8300` |
| `RAFT_ADVERTISE` | Raft advertise address | `hostname:8300` |
| `BOOTSTRAP` | Bootstrap new cluster | `false` |
| `RAFT_JOIN_ADDRESSES` | Comma-separated join addresses | `""` |

### Docker Compose

```yaml
version: '3.8'
services:
  disheap-engine-1:
    image: disheap/engine:latest
    environment:
      - NODE_ID=node-1
      - BOOTSTRAP=true
      - RAFT_ADVERTISE=disheap-engine-1:8300
    ports:
      - "9090:9090"  # gRPC
      - "8080:8080"  # HTTP
    volumes:
      - disheap-data-1:/home/disheap/data
```

## Deployment

### Docker Swarm

```bash
docker stack deploy -c docker-compose.yml disheap
```

### Kubernetes

```bash
kubectl apply -f deploy/kubernetes/
```

### Production Considerations

- **Persistent Volumes**: Ensure data persistence across restarts
- **Network Policies**: Secure inter-node communication
- **Resource Limits**: Set appropriate CPU/memory limits
- **Monitoring**: Configure Prometheus scraping
- **Backup**: Regular BadgerDB snapshots

## Development

### Development Setup

```bash
# Install dependencies
./scripts/setup-dev-environment.sh

# Run tests
go test ./...
cd disheap-python && poetry run pytest

# Build all components
docker compose build
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Dependencies

- [HashiCorp Raft](https://github.com/hashicorp/raft) - Consensus protocol implementation
- [BadgerDB](https://github.com/dgraph-io/badger) - High-performance key-value store
- [gRPC](https://grpc.io/) - High-performance RPC framework

---

A distributed priority messaging system with strong consistency guarantees.
