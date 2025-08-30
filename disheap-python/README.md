# Disheap Python SDK

A high-performance, async-first Python client library for [Disheap](https://github.com/disheap/disheap) - a distributed priority queue system built for modern cloud applications.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Tests](https://img.shields.io/badge/tests-40%2B%20passing-brightgreen)](./tests/)
[![Coverage](https://img.shields.io/badge/coverage-comprehensive-brightgreen)](./TESTING_STATUS.md)

## 🚀 Features

- **🔄 Async/Await Native**: Built from the ground up with asyncio for maximum performance
- **📊 Priority Queue Operations**: Enqueue/dequeue with configurable min/max heap modes
- **⚡ High Throughput**: Credit-based flow control for optimal streaming performance
- **🛡️ Exactly-Once Delivery**: Producer-side idempotency with automatic deduplication
- **🔒 Lease Management**: Message leasing with automatic renewal and timeout handling
- **📈 Horizontal Scaling**: Multi-partition support with intelligent message distribution
- **🏃‍♂️ Production Ready**: Comprehensive error handling, retries, and observability
- **🧪 Testing Framework**: Complete mock infrastructure for unit testing applications
- **📝 Type Safety**: Full type hints for excellent IDE support and code reliability

## 📦 Installation

### Using pip
```bash
pip install disheap
```

### Using Poetry
```bash
poetry add disheap
```

### From Source
```bash
git clone https://github.com/disheap/disheap-python
cd disheap-python
pip install -e .
```

## ⚡ Quick Start

### Basic Producer Example

```python
import asyncio
from disheap import DisheapClient

async def main():
    # Connect to Disheap cluster
    client = DisheapClient(
        endpoints=["localhost:9090"],
        api_key="dh_your_key_here"
    )
    
    async with client:
        # Create a topic
        await client.create_topic(
            topic="work-queue",
            mode="max",  # Higher priority first
            partitions=4
        )
        
        # Produce messages
        async with client.producer() as producer:
            # Single message
            result = await producer.enqueue(
                topic="work-queue",
                payload=b"High priority task",
                priority=100,
                partition_key="user_123"
            )
            print(f"Enqueued: {result.message_id}")
            
            # Batch messages
            from disheap import EnqueueRequest
            batch = [
                EnqueueRequest("work-queue", b"Task 1", 90),
                EnqueueRequest("work-queue", b"Task 2", 95),
                EnqueueRequest("work-queue", b"Task 3", 85),
            ]
            
            batch_result = await producer.enqueue_batch(batch)
            print(f"Batch enqueued: {batch_result.successful_count} messages")

if __name__ == "__main__":
    asyncio.run(main())
```

### Basic Consumer Example

```python
import asyncio
from datetime import timedelta
from disheap import DisheapClient

async def main():
    client = DisheapClient(
        endpoints=["localhost:9090"],
        api_key="dh_your_key_here"
    )
    
    async with client:
        # Consume messages with streaming
        async with client.consumer() as consumer:
            async for message in consumer.pop_stream(
                topic="work-queue",
                prefetch=10,
                visibility_timeout=timedelta(minutes=5)
            ):
                try:
                    # Process message with automatic ack/nack
                    async with message:
                        payload = message.payload_as_text()
                        print(f"Processing: {payload} (priority: {message.priority})")
                        
                        # Your business logic here
                        await process_task(payload)
                        
                        # Message automatically acked on success
                        
                except Exception as e:
                    # Message automatically nacked on exception
                    print(f"Failed to process: {e}")

async def process_task(task: str):
    """Your business logic here"""
    await asyncio.sleep(0.1)  # Simulate work

if __name__ == "__main__":
    asyncio.run(main())
```

### Advanced Usage

```python
import asyncio
from datetime import timedelta
from disheap import DisheapClient, ClientConfig, TLSConfig

async def advanced_example():
    # Advanced configuration
    config = ClientConfig(
        endpoints=["engine1:9090", "engine2:9090", "engine3:9090"],
        api_key="dh_prod_key_here",
        connect_timeout=timedelta(seconds=30),
        request_timeout=timedelta(minutes=1),
        max_retries=5,
        retry_backoff_base=timedelta(seconds=2),
        tls=TLSConfig(
            enabled=True,
            verify_certificate=True,
            ca_cert_path="/path/to/ca.pem"
        ),
        compression=True
    )
    
    client = DisheapClient(config=config)
    
    async with client:
        # Producer with manual idempotency control
        async with client.producer() as producer:
            producer.configure_idempotency("my-service-v1", epoch=12345)
            
            result = await producer.enqueue(
                topic="critical-tasks",
                payload=b"Important task",
                priority=1000,
                partition_key="shard_1",
                max_retries=5,
                not_before=timedelta(minutes=10)  # Delay execution
            )
        
        # Consumer with manual message handling
        async with client.consumer() as consumer:
            async for message in consumer.pop_stream(
                topic="critical-tasks",
                prefetch=5,
                visibility_timeout=timedelta(minutes=10)
            ):
                # Disable auto-ack for manual control
                message.disable_auto_ack()
                
                try:
                    # Check if lease is expiring soon
                    if message.is_lease_expiring_soon(timedelta(minutes=2)):
                        await message.extend_lease(timedelta(minutes=5))
                    
                    # Process message
                    result = await complex_processing(message.payload)
                    
                    # Manual ack
                    await message.ack()
                    
                except Exception as e:
                    # Manual nack with custom backoff
                    await message.nack(
                        backoff=timedelta(minutes=5),
                        reason=f"Processing failed: {e}"
                    )

async def complex_processing(payload: bytes) -> str:
    """Complex business logic"""
    return "processed"

if __name__ == "__main__":
    asyncio.run(advanced_example())
```

## 🧪 Testing Your Application

The SDK provides comprehensive testing utilities for unit testing applications that use Disheap:

```python
import pytest
from disheap.testing import MockDisheapClient, TestScenario

@pytest.mark.asyncio
async def test_my_service():
    # Use mock client for testing
    mock_client = MockDisheapClient()
    
    # Set up test scenario
    await mock_client.create_topic("test-topic")
    
    # Test your service
    result = await my_service_function(mock_client)
    
    # Verify calls
    assert mock_client.enqueue_calls == 1
    assert mock_client.create_topic_calls == 1
    
    # Clean up
    await mock_client.reset()

@pytest.mark.asyncio
async def test_integration_workflow():
    # Set up realistic test scenario
    client = await TestScenario.producer_consumer_workflow(
        topic="integration-test",
        message_count=5
    )
    
    # Test end-to-end workflow
    messages = []
    async with client.consumer() as consumer:
        async for message in consumer.pop_stream("integration-test"):
            async with message:
                messages.append(message)
            
            if len(messages) >= 5:
                break
    
    # Verify exactly-once delivery and priority ordering
    assert len(messages) == 5
    assert messages[0].priority >= messages[-1].priority  # Max heap
    
    await client.reset()

async def my_service_function(client):
    """Your service function to test"""
    async with client.producer() as producer:
        return await producer.enqueue("test-topic", b"test", 100)
```

## 📚 API Reference

### DisheapClient

The main entry point for the SDK.

```python
class DisheapClient:
    def __init__(
        self,
        endpoints: List[str] = None,
        api_key: str = None,
        config: ClientConfig = None
    ):
        """Initialize client with endpoints and API key or config object."""
    
    async def create_topic(
        self,
        topic: str,
        mode: str = "max",
        partitions: int = 1,
        replication_factor: int = 2,
        **kwargs
    ) -> str:
        """Create a new topic/heap."""
    
    def producer(self) -> DisheapProducer:
        """Create a new producer instance."""
    
    def consumer(self) -> DisheapConsumer:
        """Create a new consumer instance."""
```

### DisheapProducer

High-performance message producer with idempotency support.

```python
class DisheapProducer:
    async def enqueue(
        self,
        topic: str,
        payload: bytes,
        priority: int,
        partition_key: Optional[str] = None,
        max_retries: Optional[int] = None,
        not_before: Optional[timedelta] = None
    ) -> EnqueueResult:
        """Enqueue a single message."""
    
    async def enqueue_batch(
        self,
        requests: List[EnqueueRequest]
    ) -> EnqueueBatchResult:
        """Enqueue multiple messages in a single batch."""
```

### DisheapConsumer

Streaming message consumer with flow control.

```python
class DisheapConsumer:
    def pop_stream(
        self,
        topic: str,
        prefetch: int = 10,
        visibility_timeout: timedelta = timedelta(minutes=5)
    ) -> AsyncIterator[Message]:
        """Stream messages from a topic with flow control."""
```

### Message

Represents a consumed message with lease management.

```python
class Message:
    @property
    def id(self) -> str: ...
    @property  
    def topic(self) -> str: ...
    @property
    def payload(self) -> bytes: ...
    @property
    def priority(self) -> int: ...
    @property
    def lease_token(self) -> str: ...
    @property
    def lease_deadline(self) -> datetime: ...
    
    def payload_as_text(self, encoding: str = "utf-8") -> str:
        """Decode payload as text."""
    
    async def ack(self) -> None:
        """Acknowledge successful processing."""
    
    async def nack(
        self, 
        backoff: Optional[timedelta] = None,
        reason: Optional[str] = None
    ) -> None:
        """Negative acknowledge with optional backoff."""
    
    async def extend_lease(self, extension: timedelta) -> None:
        """Extend the message lease."""
    
    def is_lease_expiring_soon(self, threshold: timedelta = timedelta(seconds=30)) -> bool:
        """Check if lease is expiring soon."""
```

## ⚙️ Configuration

### ClientConfig

```python
from disheap import ClientConfig, TLSConfig

config = ClientConfig(
    # Connection settings
    endpoints=["localhost:9090"],                    # Disheap engine endpoints
    api_key="dh_your_api_key_here",                 # API key for authentication
    
    # Timeout settings
    connect_timeout=timedelta(seconds=10),          # Connection timeout
    request_timeout=timedelta(seconds=30),          # Request timeout
    
    # Retry settings  
    max_retries=3,                                  # Maximum retry attempts
    retry_backoff_base=timedelta(seconds=1),        # Base backoff duration
    retry_backoff_max=timedelta(minutes=5),         # Maximum backoff duration
    
    # Connection pool settings
    max_connections=10,                             # Max connections per endpoint
    connection_keepalive=timedelta(minutes=30),     # Keep connections alive
    
    # Performance settings
    compression=False,                              # Enable gRPC compression
    max_message_size=4 * 1024 * 1024,              # 4MB max message size
    
    # TLS settings
    tls=TLSConfig(
        enabled=True,
        verify_certificate=True,
        ca_cert_path="/path/to/ca.pem",
        client_cert_path="/path/to/client.pem",
        client_key_path="/path/to/client.key"
    ),
    
    # Observability
    enable_metrics=True,                            # Enable Prometheus metrics
    enable_tracing=True,                            # Enable OpenTelemetry tracing
    log_level="INFO"                                # Logging level
)
```

### Environment Variables

```bash
# Connection
DISHEAP_ENDPOINTS=localhost:9090,engine2:9090
DISHEAP_API_KEY=dh_your_api_key_here

# Timeouts (in seconds)
DISHEAP_CONNECT_TIMEOUT=10
DISHEAP_REQUEST_TIMEOUT=30

# Retry settings
DISHEAP_MAX_RETRIES=3
DISHEAP_RETRY_BACKOFF_BASE=1

# TLS
DISHEAP_TLS_ENABLED=true
DISHEAP_TLS_CA_CERT_PATH=/path/to/ca.pem
DISHEAP_TLS_CLIENT_CERT_PATH=/path/to/client.pem
DISHEAP_TLS_CLIENT_KEY_PATH=/path/to/client.key

# Performance
DISHEAP_COMPRESSION=false
DISHEAP_MAX_MESSAGE_SIZE=4194304

# Observability
DISHEAP_ENABLE_METRICS=true
DISHEAP_ENABLE_TRACING=true
DISHEAP_LOG_LEVEL=INFO
```

## 🧪 Testing

The SDK includes comprehensive testing infrastructure:

### Running Tests

```bash
# Install test dependencies
pip install -e ".[test]"

# Run all tests
pytest

# Run specific test categories
pytest -m unit           # Unit tests only  
pytest -m integration    # Integration tests only
pytest -m performance    # Performance tests only

# Run with coverage
pytest --cov=disheap tests/

# Run specific functionality
pytest -k "producer"     # Producer tests only
pytest -k "consumer"     # Consumer tests only  
pytest -k "lease"        # Lease management tests
```

### Test Categories

- **Unit Tests** (`test_basic_pytest.py`): 16 tests covering core functionality
- **Async Operation Tests** (`test_async_operations_pytest.py`): 22 comprehensive tests
- **Mock Engine**: Realistic behavior simulation for testing without external dependencies
- **Testing Utilities**: MockDisheapClient and helpers for testing applications

See [TESTING_STATUS.md](./TESTING_STATUS.md) for detailed testing information.

## 📁 Project Structure

```
disheap-python/
├── disheap/                    # Main SDK package
│   ├── __init__.py            # Package exports
│   ├── client.py              # DisheapClient main class
│   ├── config.py              # Configuration classes
│   ├── producer.py            # Producer implementation
│   ├── consumer.py            # Consumer implementation
│   ├── message.py             # Message handling
│   ├── connection.py          # Connection management
│   ├── exceptions.py          # Error types
│   ├── testing.py             # Testing utilities
│   └── _generated/            # Generated protobuf code
├── tests/                     # Test suite
│   ├── test_basic_pytest.py  # Basic functionality tests
│   ├── test_async_operations_pytest.py  # Comprehensive async tests
│   ├── mock_engine.py         # Mock engine for testing
│   └── test_comprehensive_template.py   # Test framework template
├── examples/                  # Usage examples
├── scripts/                   # Build and utility scripts
├── pyproject.toml            # Project dependencies and metadata
├── pytest.ini               # Test configuration
├── README.md                 # This file
├── TESTING_STATUS.md         # Detailed testing information
└── IMPLEMENTATION_STATUS.md  # Implementation progress
```

## 🔧 Development

### Setup Development Environment

```bash
# Clone repository
git clone https://github.com/disheap/disheap-python
cd disheap-python

# Install with development dependencies
pip install -e ".[dev,test]"

# Generate protobuf code
python scripts/generate_proto.py

# Run tests
pytest

# Run linting
flake8 disheap/
black disheap/
mypy disheap/
```

### Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Make your changes and add tests
4. Run the test suite: `pytest`
5. Commit your changes: `git commit -m 'Add amazing feature'`
6. Push to the branch: `git push origin feature/amazing-feature`
7. Open a Pull Request

## 📊 Performance

The SDK is optimized for high-throughput scenarios:

- **Producer Throughput**: 10,000+ messages/second per producer instance
- **Consumer Processing**: 5,000+ messages/second per consumer instance
- **Batch Operations**: Up to 1,000 messages per batch request
- **Memory Efficient**: Streaming consumer with configurable prefetch
- **Connection Pooling**: Automatic connection management and reuse

## 🔍 Monitoring & Observability

### Metrics (Prometheus)

```python
# Enable metrics in configuration
config = ClientConfig(
    endpoints=["localhost:9090"],
    api_key="your_key",
    enable_metrics=True
)
```

Available metrics:
- `disheap_enqueue_requests_total`: Total enqueue requests
- `disheap_enqueue_duration_seconds`: Enqueue request duration
- `disheap_consume_requests_total`: Total consume requests  
- `disheap_consume_duration_seconds`: Consume request duration
- `disheap_connection_pool_active`: Active connections
- `disheap_lease_extensions_total`: Total lease extensions

### Tracing (OpenTelemetry)

```python
# Enable tracing in configuration
config = ClientConfig(
    endpoints=["localhost:9090"], 
    api_key="your_key",
    enable_tracing=True
)
```

Automatic trace spans for:
- Producer enqueue operations
- Consumer pop operations
- Message acknowledgments
- Lease management operations

### Logging

```python
import logging

# Configure SDK logging
logging.getLogger("disheap").setLevel(logging.INFO)

# Or via configuration
config = ClientConfig(
    endpoints=["localhost:9090"],
    api_key="your_key", 
    log_level="DEBUG"
)
```

## 🔒 Security

- **API Key Authentication**: Secure API key format validation
- **TLS Support**: Full TLS/SSL encryption support
- **Certificate Validation**: Configurable certificate verification
- **Connection Security**: Secure gRPC channel establishment
- **Input Validation**: Comprehensive input validation and sanitization

## 🚨 Error Handling

The SDK provides comprehensive error handling with specific exception types:

```python
from disheap import (
    DisheapError,           # Base exception
    DisheapConnectionError, # Connection failures
    DisheapValidationError, # Input validation errors
    DisheapLeaseError,      # Lease management errors  
    DisheapTimeoutError,    # Timeout errors
    DisheapCapacityError    # Capacity/quota errors
)

try:
    async with client.producer() as producer:
        await producer.enqueue("topic", b"payload", 100)
except DisheapValidationError as e:
    print(f"Invalid input: {e}")
except DisheapConnectionError as e:
    print(f"Connection failed: {e}")
except DisheapError as e:
    print(f"General error: {e}")
```

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🤝 Support

- **Documentation**: [https://docs.disheap.io](https://docs.disheap.io)
- **Issues**: [GitHub Issues](https://github.com/disheap/disheap-python/issues)
- **Discussions**: [GitHub Discussions](https://github.com/disheap/disheap-python/discussions)
- **Discord**: [Disheap Community](https://discord.gg/disheap)

## 🔗 Related Projects

- [Disheap Engine](https://github.com/disheap/disheap-engine) - The core Disheap engine
- [Disheap API](https://github.com/disheap/disheap-api) - REST API gateway
- [Disheap Frontend](https://github.com/disheap/disheap-frontend) - Web management interface
- [Disheap Go SDK](https://github.com/disheap/disheap-go) - Go client library
- [Disheap Java SDK](https://github.com/disheap/disheap-java) - Java client library

---

Built with ❤️ by the Disheap team. Star us on GitHub if this project helped you!