"""
Testing utilities for the Disheap Python SDK.

Provides mock clients and testing helpers for unit testing
applications that use the Disheap SDK.
"""

import asyncio
from typing import Optional, Any, Dict, List, AsyncIterator
from unittest.mock import AsyncMock

from .client import DisheapClient
from .config import ClientConfig
from .connection import ConnectionManager

# Import mock engine - handle import path correctly
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tests'))

from mock_engine import MockDisheapEngine, get_mock_engine, reset_mock_engine


class MockConnectionManager(ConnectionManager):
    """Mock connection manager that uses the mock engine."""
    
    def __init__(self, config: ClientConfig, mock_engine: MockDisheapEngine):
        super().__init__(config)
        self.mock_engine = mock_engine
        self._connected = False
    
    async def connect(self) -> None:
        """Mock connection - just mark as connected."""
        self._connected = True
    
    async def close(self) -> None:
        """Mock close - just mark as disconnected."""
        self._connected = False
    
    async def is_healthy(self) -> bool:
        """Mock health check."""
        return self._connected
    
    async def call(self, method_name: str, request: Any) -> Any:
        """Route calls to the mock engine."""
        if not self._connected:
            from .exceptions import DisheapConnectionError
            raise DisheapConnectionError("Mock connection not established")
        
        # Route to appropriate mock engine method
        method_map = {
            "MakeHeap": self.mock_engine.make_heap,
            "DeleteHeap": self.mock_engine.delete_heap,
            "Stats": self.mock_engine.stats,
            "Enqueue": self.mock_engine.enqueue,
            "EnqueueBatch": self.mock_engine.enqueue_batch,
            "Ack": self.mock_engine.ack,
            "Nack": self.mock_engine.nack,
            "Extend": self.mock_engine.extend,
        }
        
        method = method_map.get(method_name)
        if not method:
            raise NotImplementedError(f"Mock method {method_name} not implemented")
        
        return await method(request)
    
    async def call_streaming(self, method_name: str, request_stream) -> AsyncIterator:
        """Handle streaming calls."""
        if method_name == "PopStream":
            async for item in self.mock_engine.pop_stream(request_stream):
                yield item
        else:
            raise NotImplementedError(f"Streaming method {method_name} not implemented")


class MockDisheapClient(DisheapClient):
    """
    Mock Disheap client for testing.
    
    Uses a mock engine backend instead of connecting to a real engine.
    Perfect for unit testing applications that use Disheap.
    
    Example:
        >>> async def test_my_function():
        ...     mock_client = MockDisheapClient()
        ...     
        ...     # Create a topic for testing
        ...     await mock_client.create_topic("test-topic")
        ...     
        ...     # Use in your code
        ...     result = await my_function_using_disheap(mock_client)
        ...     
        ...     # Verify behavior
        ...     assert mock_client.enqueue_calls == 1
    """
    
    def __init__(
        self,
        failure_rate: float = 0.0,
        network_delay: float = 0.0,
        **kwargs
    ):
        """
        Initialize mock client.
        
        Args:
            failure_rate: Probability of operation failures (0.0-1.0)
            network_delay: Simulated network delay in seconds
            **kwargs: Additional config options (mostly ignored)
        """
        # Create minimal config for mock
        config = ClientConfig(
            endpoints=["mock://localhost:9090"],
            api_key="dh_mock_key_secret"
        )
        
        super().__init__(config=config)
        
        # Track calls for testing assertions
        self.enqueue_calls = 0
        self.batch_enqueue_calls = 0
        self.ack_calls = 0
        self.nack_calls = 0
        self.extend_calls = 0
        self.create_topic_calls = 0
        
        # Mock engine settings
        self._failure_rate = failure_rate
        self._network_delay = network_delay
        self._mock_engine: Optional[MockDisheapEngine] = None
    
    async def _ensure_connected(self) -> ConnectionManager:
        """Override to use mock connection manager."""
        if self._connection_manager is None:
            # Get or create mock engine
            self._mock_engine = await get_mock_engine(
                self._failure_rate, 
                self._network_delay
            )
            
            self._connection_manager = MockConnectionManager(
                self._config, 
                self._mock_engine
            )
            await self._connection_manager.connect()
        
        return self._connection_manager
    
    async def create_topic(self, topic: str, **kwargs) -> str:
        """Override to track calls."""
        self.create_topic_calls += 1
        return await super().create_topic(topic, **kwargs)
    
    def producer(self):
        """Override to return instrumented producer."""
        from .producer import DisheapProducer
        
        class MockProducer(DisheapProducer):
            def __init__(self, client):
                super().__init__(client)
                self._mock_client = client
            
            async def enqueue(self, *args, **kwargs):
                self._mock_client.enqueue_calls += 1
                return await super().enqueue(*args, **kwargs)
            
            async def enqueue_batch(self, *args, **kwargs):
                self._mock_client.batch_enqueue_calls += 1
                return await super().enqueue_batch(*args, **kwargs)
        
        return MockProducer(self)
    
    def consumer(self):
        """Override to return instrumented consumer."""
        from .consumer import DisheapConsumer
        
        class MockConsumer(DisheapConsumer):
            def __init__(self, client):
                super().__init__(client)
                self._mock_client = client
        
        return MockConsumer(self)
    
    async def reset(self) -> None:
        """Reset the mock engine state for clean tests."""
        await reset_mock_engine()
        self._mock_engine = None
        self._connection_manager = None
        
        # Reset call counters
        self.enqueue_calls = 0
        self.batch_enqueue_calls = 0
        self.ack_calls = 0
        self.nack_calls = 0
        self.extend_calls = 0
        self.create_topic_calls = 0
    
    async def get_mock_engine(self) -> MockDisheapEngine:
        """Get the underlying mock engine for advanced testing."""
        if self._mock_engine is None:
            await self._ensure_connected()
        return self._mock_engine


def create_test_message(
    message_id: str = "test_msg_123",
    topic: str = "test-topic",
    payload: bytes = b"test payload",
    priority: int = 100,
    **kwargs
) -> "Message":
    """
    Create a test message for unit testing.
    
    Args:
        message_id: Message ID
        topic: Topic name
        payload: Message payload
        priority: Message priority
        **kwargs: Additional message properties
        
    Returns:
        Message instance for testing
    """
    from datetime import datetime, timedelta
    from .message import Message
    
    defaults = {
        "partition_id": 0,
        "enqueued_at": datetime.now(),
        "attempts": 1,
        "max_retries": 3,
        "lease_token": "test_lease_token",
        "lease_deadline": datetime.now() + timedelta(minutes=5),
    }
    defaults.update(kwargs)
    
    return Message(
        id=message_id,
        topic=topic,
        payload=payload,
        priority=priority,
        **defaults
    )


class TestScenario:
    """Helper for setting up common test scenarios."""
    
    @staticmethod
    async def producer_consumer_workflow(
        topic: str = "test-topic",
        message_count: int = 3,
        failure_rate: float = 0.0
    ) -> MockDisheapClient:
        """
        Set up a basic producer->consumer test scenario.
        
        Args:
            topic: Topic name to use
            message_count: Number of messages to produce
            failure_rate: Failure rate for testing error handling
            
        Returns:
            Configured mock client ready for testing
        """
        client = MockDisheapClient(failure_rate=failure_rate)
        
        # Create topic
        await client.create_topic(topic, mode="max", partitions=2)
        
        # Produce messages
        async with client.producer() as producer:
            for i in range(message_count):
                await producer.enqueue(
                    topic=topic,
                    payload=f"Test message {i}".encode(),
                    priority=100 - i  # Descending priority
                )
        
        return client
    
    @staticmethod
    async def high_load_scenario(
        topic: str = "load-test",
        message_count: int = 100,
        batch_size: int = 10
    ) -> MockDisheapClient:
        """
        Set up a high-load test scenario with batch operations.
        
        Args:
            topic: Topic name
            message_count: Total messages to produce
            batch_size: Messages per batch
            
        Returns:
            Mock client with messages ready for consumption
        """
        from .producer import EnqueueRequest
        
        client = MockDisheapClient()
        await client.create_topic(topic, partitions=4)
        
        async with client.producer() as producer:
            # Send messages in batches
            for start in range(0, message_count, batch_size):
                batch = []
                for i in range(start, min(start + batch_size, message_count)):
                    batch.append(EnqueueRequest(
                        topic=topic,
                        payload=f"Load test message {i}".encode(),
                        priority=i,
                        partition_key=f"key_{i % 4}"
                    ))
                
                await producer.enqueue_batch(batch)
        
        return client
    
    @staticmethod
    async def error_scenario(
        topic: str = "error-test",
        failure_rate: float = 0.3
    ) -> MockDisheapClient:
        """
        Set up an error-prone scenario for testing error handling.
        
        Args:
            topic: Topic name
            failure_rate: Probability of failures (0.0-1.0)
            
        Returns:
            Mock client configured for error testing
        """
        client = MockDisheapClient(failure_rate=failure_rate)
        await client.create_topic(topic)
        
        return client


# Test assertions

def assert_message_order_by_priority(
    messages: List["Message"],
    mode: str = "max"
) -> None:
    """
    Assert that messages are ordered by priority.
    
    Args:
        messages: List of messages to check
        mode: "max" for descending priority, "min" for ascending
    """
    if len(messages) <= 1:
        return
    
    for i in range(1, len(messages)):
        if mode == "max":
            assert messages[i-1].priority >= messages[i].priority, \
                f"Messages not in descending priority order: {messages[i-1].priority} < {messages[i].priority}"
        else:
            assert messages[i-1].priority <= messages[i].priority, \
                f"Messages not in ascending priority order: {messages[i-1].priority} > {messages[i].priority}"


def assert_exactly_once_delivery(
    produced_messages: List[str],
    consumed_messages: List[str]
) -> None:
    """
    Assert exactly-once delivery semantics.
    
    Args:
        produced_messages: List of produced message IDs
        consumed_messages: List of consumed message IDs
    """
    # Every produced message should be consumed exactly once
    produced_set = set(produced_messages)
    consumed_set = set(consumed_messages)
    
    assert produced_set == consumed_set, \
        f"Delivery mismatch: produced={produced_set}, consumed={consumed_set}"
    
    # No duplicates in consumption
    assert len(consumed_messages) == len(consumed_set), \
        f"Duplicate consumption detected: {len(consumed_messages)} != {len(consumed_set)}"
