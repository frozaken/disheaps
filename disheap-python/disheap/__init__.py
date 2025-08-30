"""
Disheap Python SDK - Async priority messaging for distributed systems.

This package provides an asyncio-based Python SDK for interacting with the 
Disheap distributed priority messaging system.

Key Features:
- Async gRPC client for high-performance communication
- Producer API with idempotency and automatic sequencing  
- Consumer API with streaming pops and credit-based flow control
- Automatic lease renewal and heartbeat management
- OpenTelemetry integration for observability
- Structured logging and error handling

Example usage:

    import asyncio
    from disheap import DisheapClient
    
    async def main():
        client = DisheapClient(
            endpoints=["localhost:9090"],
            api_key="your-api-key"
        )
        
        # Producer example
        async with client.producer() as producer:
            result = await producer.enqueue(
                topic="work-queue",
                payload=b"Hello, World!",
                priority=100
            )
            print(f"Message queued: {result.message_id}")
        
        # Consumer example  
        async with client.consumer() as consumer:
            async for message in consumer.pop_stream("work-queue", prefetch=10):
                async with message:
                    print(f"Processing: {message.payload.decode()}")
                    # Message is automatically acked on success
    
    asyncio.run(main())
"""

from .client import DisheapClient
from .config import ClientConfig, TLSConfig
from .exceptions import (
    DisheapError,
    DisheapConnectionError,
    DisheapValidationError,
    DisheapLeaseError,
    DisheapTimeoutError,
    DisheapCapacityError,
)
from .message import Message
from .producer import DisheapProducer, EnqueueRequest, EnqueueResult, EnqueueBatchResult
from .consumer import DisheapConsumer

# Testing utilities
try:
    from .testing import MockDisheapClient, TestScenario, create_test_message
    TESTING_AVAILABLE = True
except ImportError:
    TESTING_AVAILABLE = False

__version__ = "0.1.0"
__author__ = "Disheap Team"
__email__ = "team@disheap.io"

__all__ = [
    # Core client
    "DisheapClient",
    
    # Configuration
    "ClientConfig", 
    "TLSConfig",
    
    # Producer/Consumer
    "DisheapProducer",
    "DisheapConsumer", 
    "Message",
    
    # Request/Response types
    "EnqueueRequest",
    "EnqueueResult",
    "EnqueueBatchResult",
    
    # Exceptions
    "DisheapError",
    "DisheapConnectionError", 
    "DisheapValidationError",
    "DisheapLeaseError",
    "DisheapTimeoutError",
    "DisheapCapacityError",
    
    # Version
    "__version__",
]

# Add testing utilities if available
if TESTING_AVAILABLE:
    __all__.extend([
        "MockDisheapClient",
        "TestScenario", 
        "create_test_message",
    ])
