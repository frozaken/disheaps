# Disheap Test Suite

This directory contains integration and manual tests for the Disheap distributed priority messaging system.

## Directory Structure

```
tests/
├── integration/          # Integration tests for end-to-end workflows
│   ├── enqueue_pop_test.py          # Complete enqueue/pop workflow test
│   ├── test_connection.py           # gRPC connection testing
│   ├── test_create_topic.py         # Topic creation testing
│   ├── test_disconnect_handling.py  # Client disconnect handling
│   ├── test_grpc_methods.py         # Individual gRPC method tests
│   ├── test_ipv4_grpc.py           # IPv4 gRPC connectivity test
│   ├── test_makeheap.py            # MakeHeap method testing
│   ├── test_raft_replication.py    # Raft cluster replication verification
│   ├── test_simple_grpc.py         # Basic gRPC functionality
│   └── test_single_node.py         # Single node testing
└── manual/               # Manual testing scripts and utilities
    └── (future manual test scripts)
```

## Prerequisites

Before running tests, ensure you have:

1. **Docker Compose cluster running**:
   ```bash
   cd /path/to/disheaps
   docker compose up --build
   ```

2. **Python dependencies installed**:
   ```bash
   cd disheap-python
   poetry install
   ```

## Running Integration Tests

### Individual Tests

```bash
# Test basic enqueue/pop workflow
python tests/integration/enqueue_pop_test.py

# Test Raft replication across cluster
python tests/integration/test_raft_replication.py

# Test client disconnect handling
python tests/integration/test_disconnect_handling.py

# Test topic creation
python tests/integration/test_create_topic.py
```

### Connection Tests

```bash
# Test gRPC connectivity
python tests/integration/test_connection.py

# Test IPv4 gRPC binding
python tests/integration/test_ipv4_grpc.py

# Test simple gRPC calls
python tests/integration/test_simple_grpc.py
```

### Method-Specific Tests

```bash
# Test MakeHeap method
python tests/integration/test_makeheap.py

# Test individual gRPC methods
python tests/integration/test_grpc_methods.py
```

### Cluster Tests

```bash
# Test single node functionality
python tests/integration/test_single_node.py

# Verify Raft state replication
python tests/integration/test_raft_replication.py
```

## Test Environment

All tests assume the following default configuration:

- **Engine Endpoints**: `localhost:9090`, `localhost:9092`, `localhost:9094`
- **HTTP Endpoints**: `localhost:8080`, `localhost:8082`, `localhost:8084`
- **API Key**: `dh_test_key_secret123`
- **Docker Network**: `disheaps_default`

## Expected Cluster State

Tests expect a healthy 3-node Raft cluster:

```bash
# Verify cluster health
curl http://localhost:8080/health
curl http://localhost:8082/health  
curl http://localhost:8084/health

# Check Raft status
curl http://localhost:8080/raft/status
```

## Test Categories

### 🔌 **Connection Tests**
- gRPC channel establishment
- Multi-endpoint load balancing
- Connection pooling verification
- IPv4/IPv6 binding tests

### 📝 **CRUD Tests**
- Topic creation (`MakeHeap`)
- Message enqueuing (`Enqueue`)
- Message consumption (`PopStream`)
- Message acknowledgment (`Ack`)

### 🔄 **Workflow Tests**
- End-to-end enqueue/pop cycles
- Producer/consumer patterns
- Batch operations
- Error handling

### 🏛️ **Distributed Tests**
- Raft consensus verification
- State replication across nodes
- Leader election behavior
- Cluster health monitoring

### 🔌 **Disconnect Tests**
- Graceful client disconnection
- Connection timeout handling
- Reconnection behavior
- Error propagation

## Debugging

### Common Issues

1. **Connection Refused**: Ensure Docker Compose cluster is running
2. **Topic Not Found**: Check Raft replication - topic may not be replicated yet
3. **gRPC Errors**: Verify engine logs with `docker compose logs`
4. **Timeout Errors**: Increase timeout values in test scripts

### Useful Commands

```bash
# Check engine logs
docker compose logs disheap-engine-1
docker compose logs disheap-engine-2  
docker compose logs disheap-engine-3

# Check cluster status
curl http://localhost:8080/raft/status | jq

# Test gRPC connectivity
grpcurl -plaintext localhost:9090 list

# Check engine stats
grpcurl -plaintext localhost:9090 disheap.v1.Disheap/Stats
```

## Contributing

When adding new tests:

1. **Integration tests** go in `tests/integration/`
2. **Manual scripts** go in `tests/manual/`
3. Follow the naming convention: `test_<feature>.py`
4. Include proper error handling and cleanup
5. Document expected cluster state and prerequisites
6. Add the test to this README

## Test Results

All integration tests should pass with a healthy 3-node Disheap cluster. If tests fail:

1. Check cluster health endpoints
2. Verify Raft consensus status
3. Review engine logs for errors
4. Ensure proper network connectivity
5. Validate API key and endpoint configuration

---

**Disheap Test Suite** - Comprehensive testing for distributed priority messaging
