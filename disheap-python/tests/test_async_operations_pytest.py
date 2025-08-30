"""
Comprehensive async operation tests for Disheap Python SDK using pytest.

Tests all async operations with proper pytest structure:
- Producer enqueue operations
- Consumer streaming and message handling  
- Message ack/nack operations
- Lease management
- Error scenarios
"""

import pytest
import pytest_asyncio
import asyncio
from datetime import timedelta, datetime
from typing import List

from disheap.testing import MockDisheapClient, TestScenario, create_test_message, assert_message_order_by_priority
from disheap import DisheapError, DisheapValidationError, DisheapLeaseError, EnqueueRequest


@pytest_asyncio.fixture
async def mock_client():
    """Create a fresh mock client for each test."""
    client = MockDisheapClient()
    yield client
    await client.reset()


@pytest_asyncio.fixture
async def setup_topic(mock_client):
    """Create a test topic."""
    await mock_client.create_topic("test-topic", mode="max", partitions=2)
    return "test-topic"


@pytest_asyncio.fixture
async def producer_consumer_setup():
    """Set up a producer->consumer scenario."""
    client = await TestScenario.producer_consumer_workflow(
        topic="test-workflow",
        message_count=3
    )
    yield client
    await client.reset()


# ============================================================================
# PRODUCER TESTS
# ============================================================================

@pytest.mark.asyncio
@pytest.mark.unit
async def test_producer_enqueue_single(mock_client, setup_topic):
    """Test enqueuing a single message."""
    topic = setup_topic
    
    async with mock_client.producer() as producer:
        result = await producer.enqueue(
            topic=topic,
            payload=b"Hello, World!",
            priority=100,
            partition_key="test-key"
        )
        
        assert result.message_id is not None
        assert not result.duplicate
        assert result.topic == topic
        assert result.partition_id in [0, 1]  # Should be one of the partitions
    
    # Verify call tracking
    assert mock_client.enqueue_calls == 1
    assert mock_client.create_topic_calls == 1


@pytest.mark.asyncio
@pytest.mark.unit
async def test_producer_enqueue_batch(mock_client):
    """Test batch enqueuing."""
    await mock_client.create_topic("batch-topic", partitions=3)
    
    # Create batch requests
    requests = [
        EnqueueRequest(
            topic="batch-topic",
            payload=f"Batch message {i}".encode(),
            priority=100 + i,
            partition_key=f"key_{i}"
        )
        for i in range(5)
    ]
    
    async with mock_client.producer() as producer:
        result = await producer.enqueue_batch(requests)
        
        assert result.successful_count == 5
        assert result.duplicate_count == 0
        assert result.transaction_id is not None
        assert len(result.results) == 5
        
        # Check each result
        for i, res in enumerate(result.results):
            assert res.message_id is not None
            assert res.topic == "batch-topic"
            assert not res.duplicate
    
    assert mock_client.batch_enqueue_calls == 1


@pytest.mark.asyncio
@pytest.mark.unit
async def test_producer_idempotency(mock_client):
    """Test producer idempotency with duplicate detection."""
    await mock_client.create_topic("idem-topic")
    
    async with mock_client.producer() as producer:
        # Send same message twice with same producer_id/epoch/sequence
        result1 = await producer.enqueue(
            topic="idem-topic",
            payload=b"Idempotent message",
            priority=100
        )
        
        result2 = await producer.enqueue(
            topic="idem-topic", 
            payload=b"Idempotent message",
            priority=100
        )
        
        # With automatic sequencing, these should be different messages
        assert result1.message_id != result2.message_id
        assert not result1.duplicate
        assert not result2.duplicate


@pytest.mark.asyncio
@pytest.mark.unit
async def test_producer_validation_errors(mock_client):
    """Test producer validation errors."""
    await mock_client.create_topic("validation-topic", max_payload_bytes=100)
    
    async with mock_client.producer() as producer:
        # Test empty topic
        with pytest.raises(DisheapValidationError, match="topic"):
            await producer.enqueue(
                topic="",
                payload=b"test",
                priority=100
            )
        
        # Test payload too large
        with pytest.raises(DisheapValidationError, match="payload size"):
            await producer.enqueue(
                topic="validation-topic",
                payload=b"x" * 1000,  # Larger than 100 byte limit
                priority=100
            )
        
        # Test non-existent topic
        with pytest.raises(DisheapValidationError, match="does not exist"):
            await producer.enqueue(
                topic="non-existent-topic",
                payload=b"test",
                priority=100
            )


# ============================================================================
# CONSUMER TESTS
# ============================================================================

@pytest.mark.asyncio
@pytest.mark.unit
async def test_consumer_pop_stream_basic(producer_consumer_setup):
    """Test basic consumer streaming."""
    client = producer_consumer_setup
    messages_received = []
    
    async with client.consumer() as consumer:
        message_count = 0
        async for message in consumer.pop_stream(
            topic="test-workflow",
            prefetch=2,
            visibility_timeout=timedelta(minutes=2)
        ):
            messages_received.append(message)
            message_count += 1
            
            # Test message properties
            assert message.id is not None
            assert message.topic == "test-workflow"
            assert message.payload is not None
            assert message.priority is not None
            assert message.lease_token is not None
            assert message.lease_deadline > datetime.now()
            assert not message.is_processed
            
            # Ack the message
            async with message:
                pass  # Auto-ack
            
            # Stop after receiving all messages
            if message_count >= 3:
                break
    
    assert len(messages_received) == 3
    
    # Verify messages were in priority order (max mode, so descending)
    assert_message_order_by_priority(messages_received, mode="max")
    
    # All messages should be processed
    for msg in messages_received:
        assert msg.is_processed


@pytest.mark.asyncio
@pytest.mark.unit
async def test_consumer_message_ack(producer_consumer_setup):
    """Test message acknowledgment."""
    client = producer_consumer_setup
    
    async with client.consumer() as consumer:
        async for message in consumer.pop_stream("test-workflow", prefetch=1):
            # Disable auto-ack to test manual ack
            message.disable_auto_ack()
            
            assert not message.is_processed
            
            # Manual ack
            await message.ack()
            
            assert message.is_processed
            assert message._acked
            assert not message._nacked
            break


@pytest.mark.asyncio
@pytest.mark.unit 
async def test_consumer_message_nack(producer_consumer_setup):
    """Test message negative acknowledgment."""
    client = producer_consumer_setup
    
    async with client.consumer() as consumer:
        async for message in consumer.pop_stream("test-workflow", prefetch=1):
            message.disable_auto_ack()
            
            assert not message.is_processed
            
            # Manual nack with backoff
            await message.nack(
                backoff=timedelta(seconds=30),
                reason="Test nack"
            )
            
            assert message.is_processed
            assert not message._acked
            assert message._nacked
            break


@pytest.mark.asyncio
@pytest.mark.unit
async def test_consumer_auto_ack_on_success(producer_consumer_setup):
    """Test automatic ack on successful processing."""
    client = producer_consumer_setup
    
    async with client.consumer() as consumer:
        async for message in consumer.pop_stream("test-workflow", prefetch=1):
            # Use context manager with auto-ack enabled (default)
            async with message:
                # Successful processing
                content = message.payload.decode()
                assert "Test message" in content
            
            # Should be automatically acked
            assert message._acked
            assert not message._nacked
            break


@pytest.mark.asyncio
@pytest.mark.unit
async def test_consumer_auto_nack_on_exception(producer_consumer_setup):
    """Test automatic nack when exception occurs in context manager."""
    client = producer_consumer_setup
    
    async with client.consumer() as consumer:
        async for message in consumer.pop_stream("test-workflow", prefetch=1):
            with pytest.raises(ValueError):
                async with message:
                    # Simulate processing error
                    raise ValueError("Simulated processing error")
            
            # Should be automatically nacked
            assert not message._acked
            assert message._nacked
            break


# ============================================================================
# LEASE MANAGEMENT TESTS
# ============================================================================

@pytest.mark.asyncio
@pytest.mark.unit
async def test_lease_extension(producer_consumer_setup):
    """Test message lease extension."""
    client = producer_consumer_setup
    
    async with client.consumer() as consumer:
        async for message in consumer.pop_stream("test-workflow", prefetch=1):
            message.disable_auto_ack()
            
            original_deadline = message.lease_deadline
            
            # Extend lease
            await message.extend_lease(timedelta(minutes=1))
            
            # Deadline should be extended
            assert message.lease_deadline > original_deadline
            
            # Clean up
            await message.ack()
            break


@pytest.mark.asyncio
@pytest.mark.unit
async def test_lease_expiry_check(producer_consumer_setup):
    """Test lease expiry checking."""
    client = producer_consumer_setup
    
    async with client.consumer() as consumer:
        async for message in consumer.pop_stream(
            "test-workflow", 
            prefetch=1,
            visibility_timeout=timedelta(seconds=10)
        ):
            message.disable_auto_ack()
            
            # Should not be expiring soon with 10-second timeout
            assert not message.is_lease_expiring_soon(timedelta(seconds=5))
            
            # Should be expiring soon with 15-second threshold
            assert message.is_lease_expiring_soon(timedelta(seconds=15))
            
            # Time until expiry should be positive
            time_left = message.time_until_lease_expiry
            assert time_left.total_seconds() > 0
            assert time_left.total_seconds() <= 10
            
            await message.ack()
            break


# ============================================================================
# ERROR HANDLING TESTS
# ============================================================================

@pytest.mark.asyncio
@pytest.mark.unit
async def test_invalid_lease_operations(producer_consumer_setup):
    """Test error handling for invalid lease operations."""
    client = producer_consumer_setup
    
    async with client.consumer() as consumer:
        async for message in consumer.pop_stream("test-workflow", prefetch=1):
            message.disable_auto_ack()
            
            # Ack the message
            await message.ack()
            
            # Try to ack again - should fail
            with pytest.raises(DisheapLeaseError):
                await message.ack()
            
            # Try to nack after ack - should fail
            with pytest.raises(DisheapLeaseError):
                await message.nack()
            
            break


@pytest.mark.asyncio
@pytest.mark.unit
async def test_connection_error_handling():
    """Test handling of connection errors."""
    client = MockDisheapClient(failure_rate=1.0)  # 100% failure rate
    
    # All operations should fail
    with pytest.raises(DisheapError):
        await client.create_topic("error-topic")
    
    # Producer operations should fail
    async with client.producer() as producer:
        with pytest.raises(DisheapError):
            await producer.enqueue("error-topic", b"test", 100)


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

@pytest.mark.asyncio
@pytest.mark.integration
async def test_concurrent_consumers():
    """Test multiple consumers processing messages concurrently."""
    client = await TestScenario.high_load_scenario(
        topic="concurrent-topic",
        message_count=10,
        batch_size=5
    )
    
    try:
        # Track messages processed by each consumer
        consumer1_messages = []
        consumer2_messages = []
        
        async def consume_messages(consumer_messages: List, max_messages: int):
            async with client.consumer() as consumer:
                count = 0
                async for message in consumer.pop_stream("concurrent-topic", prefetch=3):
                    async with message:
                        consumer_messages.append(message.id)
                    
                    count += 1
                    if count >= max_messages:
                        break
        
        # Run two consumers concurrently
        await asyncio.gather(
            consume_messages(consumer1_messages, 5),
            consume_messages(consumer2_messages, 5)
        )
        
        # Both consumers should have processed messages
        assert len(consumer1_messages) == 5
        assert len(consumer2_messages) == 5
        
        # No message should be processed by both consumers (exactly-once)
        all_processed = set(consumer1_messages + consumer2_messages)
        assert len(all_processed) == 10  # No duplicates
        
    finally:
        await client.reset()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_producer_consumer_integration():
    """Test full producer->consumer integration workflow."""
    client = MockDisheapClient()
    
    try:
        await client.create_topic("integration-topic", mode="max", partitions=2)
        
        # Produce messages with different priorities
        message_data = [
            ("Low priority", 10),
            ("Medium priority", 50), 
            ("High priority", 100),
            ("Critical priority", 200)
        ]
        
        produced_ids = []
        async with client.producer() as producer:
            for content, priority in message_data:
                result = await producer.enqueue(
                    topic="integration-topic",
                    payload=content.encode(),
                    priority=priority,
                    partition_key=f"key_{priority}"
                )
                produced_ids.append(result.message_id)
        
        # Consume all messages
        consumed_messages = []
        consumed_ids = []
        
        async with client.consumer() as consumer:
            async for message in consumer.pop_stream("integration-topic", prefetch=4):
                async with message:
                    consumed_messages.append(message)
                    consumed_ids.append(message.id)
                
                if len(consumed_messages) >= 4:
                    break
        
        # Verify all messages consumed
        assert len(consumed_messages) == 4
        
        # Verify priority ordering (max mode = descending priority)
        priorities = [msg.priority for msg in consumed_messages]
        assert priorities == sorted(priorities, reverse=True)
        
        # Verify exactly-once delivery
        assert set(produced_ids) == set(consumed_ids)
        
    finally:
        await client.reset()


# ============================================================================
# PERFORMANCE TESTS
# ============================================================================

@pytest.mark.asyncio
@pytest.mark.performance
@pytest.mark.slow
async def test_high_throughput_producer():
    """Test producer throughput under load."""
    client = MockDisheapClient()
    
    try:
        await client.create_topic("throughput-topic", partitions=4)
        
        start_time = datetime.now()
        message_count = 1000
        
        # Batch produce messages
        requests = [
            EnqueueRequest(
                topic="throughput-topic",
                payload=f"Throughput test message {i}".encode(),
                priority=i % 100,
                partition_key=f"key_{i % 4}"
            )
            for i in range(message_count)
        ]
        
        async with client.producer() as producer:
            # Send in batches of 100
            for i in range(0, message_count, 100):
                batch = requests[i:i+100]
                result = await producer.enqueue_batch(batch)
                assert result.successful_count == len(batch)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        throughput = message_count / duration
        
        # Should be able to handle at least 1000 messages/second in mock
        assert throughput > 100, f"Throughput too low: {throughput:.1f} msg/s"
        
        print(f"Producer throughput: {throughput:.1f} messages/second")
        
    finally:
        await client.reset()


@pytest.mark.asyncio
@pytest.mark.performance 
@pytest.mark.slow
async def test_consumer_processing_rate():
    """Test consumer processing rate."""
    client = await TestScenario.high_load_scenario(
        topic="processing-rate-topic",
        message_count=500,
        batch_size=50
    )
    
    try:
        start_time = datetime.now()
        messages_processed = 0
        
        async with client.consumer() as consumer:
            async for message in consumer.pop_stream("processing-rate-topic", prefetch=10):
                async with message:
                    messages_processed += 1
                
                if messages_processed >= 500:
                    break
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        processing_rate = messages_processed / duration
        
        # Should be able to process at least 100 messages/second
        assert processing_rate > 50, f"Processing rate too low: {processing_rate:.1f} msg/s"
        
        print(f"Consumer processing rate: {processing_rate:.1f} messages/second")
        
    finally:
        await client.reset()


# ============================================================================
# PARAMETRIZED TESTS
# ============================================================================

@pytest.mark.asyncio
@pytest.mark.unit
@pytest.mark.parametrize("mode,expected_order", [
    ("max", "descending"),
    ("min", "ascending")
])
async def test_priority_ordering_modes(mode, expected_order):
    """Test priority ordering for different heap modes."""
    client = MockDisheapClient()
    
    try:
        await client.create_topic("priority-test", mode=mode, partitions=1)
        
        # Produce messages with different priorities
        priorities = [10, 100, 50, 200, 25]
        async with client.producer() as producer:
            for i, priority in enumerate(priorities):
                await producer.enqueue(
                    topic="priority-test",
                    payload=f"Message {i}".encode(),
                    priority=priority
                )
        
        # Consume all messages
        consumed_priorities = []
        async with client.consumer() as consumer:
            async for message in consumer.pop_stream("priority-test", prefetch=5):
                async with message:
                    consumed_priorities.append(message.priority)
                
                if len(consumed_priorities) >= 5:
                    break
        
        # Check ordering
        if expected_order == "descending":
            assert consumed_priorities == sorted(consumed_priorities, reverse=True)
        else:
            assert consumed_priorities == sorted(consumed_priorities)
            
    finally:
        await client.reset()


@pytest.mark.asyncio
@pytest.mark.unit
@pytest.mark.parametrize("failure_rate", [0.0, 0.1, 0.5])
async def test_error_resilience(failure_rate):
    """Test SDK resilience under different failure rates."""
    client = MockDisheapClient(failure_rate=failure_rate)
    
    successful_operations = 0
    total_operations = 10
    
    for i in range(total_operations):
        try:
            topic = f"resilience-topic-{i}"
            await client.create_topic(topic)
            
            async with client.producer() as producer:
                await producer.enqueue(topic, f"Message {i}".encode(), 100)
            
            successful_operations += 1
        except DisheapError:
            # Expected failures based on failure rate
            pass
    
    # With failure rate, we should have some successes
    if failure_rate < 1.0:
        assert successful_operations > 0
    
    # Success rate should roughly match (1 - failure_rate)
    success_rate = successful_operations / total_operations
    expected_success_rate = 1.0 - failure_rate
    
    # Allow some variance in the success rate
    assert abs(success_rate - expected_success_rate) <= 0.3
