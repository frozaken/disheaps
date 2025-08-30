# Disheap Docker Environment

Complete Docker Compose setup for the Disheap distributed priority queue system.

## 🏗️ Architecture

The Disheap system consists of several components that work together:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Disheap        │    │  Disheap        │    │  Disheap        │
│  Frontend       │◄───┤  API            │◄───┤  Engine         │
│  (React/Vite)   │    │  (Go/Gin)       │    │  (Go/BadgerDB)  │
│  Port: 5173     │    │  Port: 3000     │    │  Port: 9090     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │              ┌─────────────────┐
         │                       │              │  Disheap        │
         │                       │              │  Python SDK     │
         │                       │              │  (Examples)     │
         │                       │              │  Port: 8888     │
         │                       │              └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
```

### Components

- **Disheap Engine**: Core priority queue engine using embedded BadgerDB
- **Disheap API**: REST API gateway with authentication and rate limiting
- **Disheap Frontend**: React-based web management interface
- **Disheap Python SDK**: Python client library with examples and testing
- **Jaeger**: Distributed tracing
- **Prometheus**: Metrics collection
- **Grafana**: Metrics visualization and dashboards

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose v2+ installed
- At least 4GB RAM available for containers
- Ports 3000, 5173, 8888, 9090-9092, 16686 available

### Starting the Stack

```bash
# Clone the repository
git clone <repository-url>
cd disheaps

# Start all services
docker compose up -d

# View logs
docker compose logs -f

# Stop all services
docker compose down

# Stop and remove all data
docker compose down -v
```

### Service URLs

Once running, access the services at:

- **Frontend**: http://localhost:5173 - Web management interface
- **API**: http://localhost:3000 - REST API endpoints
- **Python SDK**: http://localhost:8888 - Jupyter notebook for examples
- **Jaeger UI**: http://localhost:16686 - Distributed tracing
- **Prometheus**: http://localhost:9092 - Metrics and monitoring
- **Grafana**: http://localhost:3001 - Dashboards (admin/admin)

### Health Checks

```bash
# Check all services
docker compose ps

# Test engine health
curl http://localhost:8080/health

# Test API health  
curl http://localhost:3000/health

# Test engine gRPC (requires grpcurl)
grpcurl -plaintext localhost:9090 grpc.health.v1.Health/Check
```

## 🧪 Testing the Python SDK

The Python SDK container includes examples and a complete test suite:

```bash
# Connect to Python SDK container
docker compose exec disheap-python bash

# Run basic examples
python examples/streaming_consumer.py
python examples/work_queue.py

# Run the comprehensive test suite
pytest tests/ -v

# Start Jupyter notebook for interactive development
jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root
# Then access http://localhost:8888
```

## ⚙️ Configuration

### Environment Variables

Key environment variables can be overridden in `docker-compose.yml`:

```yaml
# Engine configuration
ENGINE_LOG_LEVEL: "info"           # debug, info, warn, error
STORAGE_SYNC_WRITES: "true"        # BadgerDB sync writes
ENGINE_MAX_MESSAGE_SIZE: "4194304" # 4MB max message size

# API configuration  
API_LOG_LEVEL: "info"              # Logging level
STORAGE_TYPE: "memory"             # In-memory storage for development
RATE_LIMIT_REQUESTS_PER_MINUTE: "1000"  # Rate limiting
CORS_ALLOWED_ORIGINS: "http://localhost:5173"  # CORS origins

# Python SDK configuration
DISHEAP_LOG_LEVEL: "DEBUG"         # SDK logging level
DISHEAP_ENABLE_METRICS: "true"     # Enable Prometheus metrics
```

### Persistence

Data is persisted in Docker volumes:

```bash
# View volumes
docker volume ls | grep disheaps

# Backup engine data
docker run --rm -v disheaps_engine_data:/data -v $(pwd):/backup alpine tar czf /backup/engine_backup.tar.gz -C /data .

# Restore engine data
docker run --rm -v disheaps_engine_data:/data -v $(pwd):/backup alpine tar xzf /backup/engine_backup.tar.gz -C /data
```

## 📊 Monitoring and Observability

### Prometheus Metrics

Available at http://localhost:9092

Key metrics to monitor:
- `disheap_enqueue_requests_total` - Total enqueue requests
- `disheap_consume_requests_total` - Total consume requests  
- `disheap_lease_extensions_total` - Lease extensions
- `disheap_connection_pool_active` - Active connections

### Grafana Dashboards

Access at http://localhost:3001 (admin/admin)

Pre-configured dashboards include:
- Disheap Engine Performance
- API Gateway Metrics
- Python SDK Usage

### Distributed Tracing

Jaeger UI at http://localhost:16686

Traces are automatically collected from:
- Engine gRPC operations
- API HTTP requests
- Python SDK operations

## 🔧 Development

### Building Individual Components

```bash
# Build only the engine
docker compose build disheap-engine

# Build only the API
docker compose build disheap-api

# Build only the frontend
docker compose build disheap-frontend

# Build only the Python SDK
docker compose build disheap-python
```

### Development Mode

For development, mount source code as volumes:

```yaml
# Add to docker-compose.override.yml
version: '3.8'
services:
  disheap-python:
    volumes:
      - ./disheap-python:/app
    command: bash  # Keep container running for development
```

### Running Tests

```bash
# Run Python SDK tests
docker compose exec disheap-python pytest tests/ -v

# Run API tests (if implemented)
docker compose exec disheap-api go test ./...

# Run engine tests (if implemented)
docker compose exec disheap-engine go test ./...
```

## 🚨 Troubleshooting

### Common Issues

1. **Port conflicts**: Ensure ports 3000, 5173, 8888, 9090-9092, 6379, 16686 are available
2. **Memory issues**: Increase Docker memory limit to at least 4GB
3. **Permission issues**: Ensure Docker has proper permissions

### Debugging

```bash
# View service logs
docker compose logs disheap-engine
docker compose logs disheap-api
docker compose logs disheap-python

# Check service health
docker compose exec disheap-engine curl http://localhost:8080/health
docker compose exec disheap-api curl http://localhost:3000/health

# Interactive debugging
docker compose exec disheap-python bash
docker compose exec disheap-api sh
```

### Reset Environment

```bash
# Complete reset with data loss
docker compose down -v
docker compose up -d

# Reset without losing data
docker compose restart
```

## 🔒 Security Notes

⚠️ **This is a development environment** - not suitable for production!

For production deployment:
- Change all default passwords and secrets
- Enable TLS/SSL for all services
- Configure proper authentication and authorization
- Set up network security and firewalls
- Use production-grade databases if needed
- Enable audit logging

## 📚 Examples and Use Cases

### Basic Producer-Consumer

```python
# In the Python SDK container
import asyncio
from disheap import DisheapClient

async def main():
    client = DisheapClient(
        endpoints=["disheap-engine:9090"],
        api_key="dh_dev_key_for_testing"
    )
    
    async with client:
        # Create a topic
        await client.create_topic("work-queue", mode="max")
        
        # Produce messages
        async with client.producer() as producer:
            await producer.enqueue("work-queue", b"High priority task", 100)
        
        # Consume messages
        async with client.consumer() as consumer:
            async for message in consumer.pop_stream("work-queue"):
                async with message:
                    print(f"Processing: {message.payload_as_text()}")

asyncio.run(main())
```

### Load Testing

```bash
# In the Python SDK container
python -c "
import asyncio
import time
from disheap import DisheapClient

async def load_test():
    client = DisheapClient(endpoints=['disheap-engine:9090'], api_key='dh_dev_key_for_testing')
    async with client:
        await client.create_topic('load-test', partitions=4)
        
        start = time.time()
        async with client.producer() as producer:
            for i in range(1000):
                await producer.enqueue('load-test', f'Message {i}'.encode(), i % 100)
        
        print(f'Produced 1000 messages in {time.time() - start:.2f}s')

asyncio.run(load_test())
"
```

## 🤝 Contributing

1. Make changes to the relevant component
2. Test with Docker Compose: `docker compose up --build`
3. Run tests: `docker compose exec disheap-python pytest`
4. Submit a pull request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

For more information, see the individual component READMEs:
- [Engine README](./disheap-engine/README.md)
- [API README](./disheap-api/README.md) 
- [Frontend README](./disheap-frontend/README.md)
- [Python SDK README](./disheap-python/README.md)
