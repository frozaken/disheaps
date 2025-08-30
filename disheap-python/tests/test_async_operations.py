#!/usr/bin/env python3
"""
Comprehensive async operation tests for Disheap Python SDK.

Tests all async operations that were missing from basic tests:
- Producer enqueue operations
- Consumer streaming and message handling
- Message ack/nack operations
- Lease management
- Error scenarios
"""

import asyncio
import sys
import traceback
from datetime import timedelta, datetime
from typing import List

# Add the disheap package to Python path
sys.path.insert(0, '.')

from disheap.testing import MockDisheapClient, TestScenario, create_test_message, assert_message_order_by_priority
from disheap import DisheapError, DisheapValidationError, DisheapLeaseError, EnqueueRequest


class AsyncTestRunner:
    """Async test runner without pytest dependency."""
    
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []
    
    async def run_test(self, test_name: str, test_func):
        """Run a single async test function."""
        try:
            print(f"Running {test_name}...", end=" ")
            await test_func()
            print("✅ PASS")
            self.passed += 1
        except Exception as e:
            print(f"❌ FAIL: {e}")
            self.failed += 1
            self.errors.append((test_name, str(e), traceback.format_exc()))
    
    def print_summary(self):
        """Print test summary."""
        total = self.passed + self.failed
        print(f"\n{'='*60}")
        print(f"Async Test Results: {self.passed}/{total} passed")
        
        if self.errors:
            print(f"\nFailures:")
            for name, error, trace in self.errors:
                print(f"- {name}: {error}")
                if "assert" in error.lower():
                    # Show assertion details
                    lines = trace.split('\n')
                    for line in lines:
                        if 'assert' in line and 'test_' in line:
                            print(f"  {line.strip()}")
        
        return self.failed == 0


# ============================================================================
# PRODUCER ASYNC OPERATION TESTS
# ============================================================================

async def test_producer_enqueue_single():
    """Test enqueuing a single message."""
    client = MockDisheapClient()
    
    try:
        # Create topic
        await client.create_topic("test-topic", mode="max", partitions=2)
        
        # Enqueue message
        async with client.producer() as producer:
            result = await producer.enqueue(
                topic="test-topic",
                payload=b"Hello, World!",
                priority=100,
                partition_key="test-key"
            )
            
            assert result.message_id is not None
            assert not result.duplicate
            assert result.topic == "test-topic"
            assert result.partition_id in [0, 1]  # Should be one of the partitions
        
        # Verify call tracking
        assert client.enqueue_calls == 1
        assert client.create_topic_calls == 1
    
    finally:
        await client.reset()


async def test_producer_enqueue_batch():
    """Test batch enqueuing."""
    client = MockDisheapClient()
    
    try:
        await client.create_topic("batch-topic", partitions=3)
        
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
        
        async with client.producer() as producer:
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
        
        assert client.batch_enqueue_calls == 1
    
    finally:
        await client.reset()


async def test_producer_idempotency():
    """Test producer idempotency with duplicate detection."""
    client = MockDisheapClient()
    
    try:
        await client.create_topic("idem-topic")
        
        async with client.producer() as producer:
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
        
        # Test manual producer ID and sequence
        producer = client.producer()
        producer._idempotency.producer_id = "test-producer-123"
        producer._idempotency.epoch = 1000
        
        async with producer:
            # Manually set sequence to ensure duplicates
            await producer._idempotency.next_sequence("idem-topic")  # Gets sequence 1
            producer._idempotency._sequences["idem-topic"] = 0  # Reset to 0
            
            result3 = await producer.enqueue(
                topic="idem-topic",
                payload=b"Manual sequence",
                priority=100
            )
            
            # Reset sequence again to create duplicate
            producer._idempotency._sequences["idem-topic"] = 0
            
            result4 = await producer.enqueue(
                topic="idem-topic",
                payload=b"Manual sequence duplicate",
                priority=100
            )
            
            # Should be treated as duplicate (same producer/epoch/sequence)
            assert result3.message_id == result4.message_id
            assert not result3.duplicate  # First occurrence
            assert result4.duplicate     # Duplicate
    
    finally:
        await client.reset()


async def test_producer_validation_errors():
    """Test producer validation errors."""
    client = MockDisheapClient()
    
    try:
        await client.create_topic("validation-topic", max_payload_bytes=100)
        
        async with client.producer() as producer:
            # Test empty topic
            try:
                await producer.enqueue(
                    topic="",
                    payload=b"test",
                    priority=100
                )
                assert False, "Should have raised validation error"
            except DisheapValidationError as e:
                assert "topic" in str(e).lower()
            
            # Test payload too large
            try:
                await producer.enqueue(
                    topic="validation-topic",
                    payload=b"x" * 1000,  # Larger than 100 byte limit
                    priority=100
                )
                assert False, "Should have raised validation error"
            except DisheapValidationError as e:
                assert "payload size" in str(e).lower()
            
            # Test non-existent topic
            try:
                await producer.enqueue(
                    topic="non-existent-topic",
                    payload=b"test",
                    priority=100
                )
                assert False, "Should have raised validation error"
            except DisheapValidationError as e:
                assert "does not exist" in str(e)
    
    finally:
        await client.reset()


# ============================================================================
# CONSUMER ASYNC OPERATION TESTS
# ============================================================================

async def test_consumer_pop_stream_basic():
    """Test basic consumer streaming."""
    client = MockDisheapClient()
    
    try:
        # Set up scenario with messages
        client = await TestScenario.producer_consumer_workflow(
            topic="stream-topic",
            message_count=3
        )
        
        messages_received = []
        
        async with client.consumer() as consumer:
            message_count = 0
            async for message in consumer.pop_stream(
                topic="stream-topic",
                prefetch=2,
                visibility_timeout=timedelta(minutes=2)
            ):
                messages_received.append(message)
                message_count += 1
                
                # Test message properties
                assert message.id is not None
                assert message.topic == "stream-topic"
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
    
    finally:
        await client.reset()


async def test_consumer_message_ack():
    """Test message acknowledgment."""
    client = MockDisheapClient()
    
    try:
        client = await TestScenario.producer_consumer_workflow(topic="ack-topic", message_count=1)
        
        async with client.consumer() as consumer:
            async for message in consumer.pop_stream("ack-topic", prefetch=1):
                # Disable auto-ack to test manual ack
                message.disable_auto_ack()
                
                assert not message.is_processed
                
                # Manual ack
                await message.ack()
                
                assert message.is_processed
                assert message._acked
                assert not message._nacked
                break
    
    finally:
        await client.reset()


async def test_consumer_message_nack():
    """Test message negative acknowledgment."""
    client = MockDisheapClient()
    
    try:
        client = await TestScenario.producer_consumer_workflow(topic="nack-topic", message_count=1)
        
        async with client.consumer() as consumer:
            async for message in consumer.pop_stream("nack-topic", prefetch=1):
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
    
    finally:
        await client.reset()


async def test_consumer_auto_ack_on_success():
    """Test automatic ack on successful processing."""
    client = MockDisheapClient()
    
    try:
        client = await TestScenario.producer_consumer_workflow(topic="auto-ack-topic", message_count=1)
        
        async with client.consumer() as consumer:
            async for message in consumer.pop_stream("auto-ack-topic", prefetch=1):
                # Use context manager with auto-ack enabled (default)
                async with message:
                    # Successful processing
                    content = message.payload.decode()
                    assert "Test message" in content
                
                # Should be automatically acked
                assert message._acked
                assert not message._nacked
                break
    
    finally:
        await client.reset()


async def test_consumer_auto_nack_on_exception():
    """Test automatic nack when exception occurs in context manager."""
    client = MockDisheapClient()
    
    try:
        client = await TestScenario.producer_consumer_workflow(topic="auto-nack-topic", message_count=1)
        
        async with client.consumer() as consumer:
            async for message in consumer.pop_stream("auto-nack-topic", prefetch=1):
                try:
                    async with message:
                        # Simulate processing error
                        raise ValueError("Simulated processing error")
                except ValueError:
                    pass  # Expected error
                
                # Should be automatically nacked
                assert not message._acked
                assert message._nacked
                break
    
    finally:
        await client.reset()


# ============================================================================
# LEASE MANAGEMENT TESTS  
# ============================================================================

async def test_lease_extension():
    """Test message lease extension."""
    client = MockDisheapClient()
    
    try:
        client = await TestScenario.producer_consumer_workflow(topic="lease-topic", message_count=1)
        
        async with client.consumer() as consumer:
            async for message in consumer.pop_stream("lease-topic", prefetch=1):
                message.disable_auto_ack()
                
                original_deadline = message.lease_deadline
                
                # Extend lease
                await message.extend_lease(timedelta(minutes=1))
                
                # Deadline should be extended
                assert message.lease_deadline > original_deadline
                
                # Clean up
                await message.ack()
                break
    
    finally:
        await client.reset()


async def test_lease_expiry_check():
    """Test lease expiry checking."""
    client = MockDisheapClient()
    
    try:
        client = await TestScenario.producer_consumer_workflow(topic="expiry-topic", message_count=1)
        
        async with client.consumer() as consumer:
            async for message in consumer.pop_stream(
                "expiry-topic", 
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
    
    finally:
        await client.reset()


# ============================================================================
# ERROR HANDLING TESTS
# ============================================================================

async def test_invalid_lease_operations():
    """Test error handling for invalid lease operations."""
    client = MockDisheapClient()
    
    try:
        client = await TestScenario.producer_consumer_workflow(topic="invalid-lease-topic", message_count=1)
        
        async with client.consumer() as consumer:
            async for message in consumer.pop_stream("invalid-lease-topic", prefetch=1):
                message.disable_auto_ack()
                
                # Ack the message
                await message.ack()
                
                # Try to ack again - should fail
                try:
                    await message.ack()
                    assert False, "Should not allow double ack"
                except DisheapLeaseError:
                    pass  # Expected
                
                # Try to nack after ack - should fail
                try:
                    await message.nack()
                    assert False, "Should not allow nack after ack"
                except DisheapLeaseError:
                    pass  # Expected
                
                break
    
    finally:
        await client.reset()


async def test_connection_error_handling():
    """Test handling of connection errors."""
    client = MockDisheapClient(failure_rate=1.0)  # 100% failure rate
    
    try:
        # All operations should fail
        try:
            await client.create_topic("error-topic")
            assert False, "Should have failed due to failure rate"
        except DisheapError:
            pass  # Expected
        
        # Producer operations should fail
        async with client.producer() as producer:
            try:
                await producer.enqueue("error-topic", b"test", 100)
                assert False, "Should have failed"
            except DisheapError:
                pass  # Expected
    
    finally:
        await client.reset()


# ============================================================================
# CONCURRENCY TESTS
# ============================================================================

async def test_concurrent_consumers():
    """Test multiple consumers processing messages concurrently."""
    client = MockDisheapClient()
    
    try:
        # Create topic and enqueue multiple messages
        client = await TestScenario.high_load_scenario(
            topic="concurrent-topic",
            message_count=10,
            batch_size=5
        )
        
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
# MAIN TEST RUNNER
# ============================================================================

async def main():
    """Run all async operation tests."""
    print("🧪 Running Comprehensive Async Operation Tests for Disheap Python SDK")
    print("=" * 80)
    
    runner = AsyncTestRunner()
    
    # Producer tests
    print("\n📤 Producer Operation Tests:")
    await runner.run_test("test_producer_enqueue_single", test_producer_enqueue_single)
    await runner.run_test("test_producer_enqueue_batch", test_producer_enqueue_batch) 
    await runner.run_test("test_producer_idempotency", test_producer_idempotency)
    await runner.run_test("test_producer_validation_errors", test_producer_validation_errors)
    
    # Consumer tests
    print("\n📥 Consumer Operation Tests:")
    await runner.run_test("test_consumer_pop_stream_basic", test_consumer_pop_stream_basic)
    await runner.run_test("test_consumer_message_ack", test_consumer_message_ack)
    await runner.run_test("test_consumer_message_nack", test_consumer_message_nack)
    await runner.run_test("test_consumer_auto_ack_on_success", test_consumer_auto_ack_on_success)
    await runner.run_test("test_consumer_auto_nack_on_exception", test_consumer_auto_nack_on_exception)
    
    # Lease management tests
    print("\n🔒 Lease Management Tests:")
    await runner.run_test("test_lease_extension", test_lease_extension)
    await runner.run_test("test_lease_expiry_check", test_lease_expiry_check)
    
    # Error handling tests
    print("\n💥 Error Handling Tests:")
    await runner.run_test("test_invalid_lease_operations", test_invalid_lease_operations)
    await runner.run_test("test_connection_error_handling", test_connection_error_handling)
    
    # Integration tests
    print("\n🔄 Integration Tests:")
    await runner.run_test("test_concurrent_consumers", test_concurrent_consumers)
    await runner.run_test("test_producer_consumer_integration", test_producer_consumer_integration)
    
    success = runner.print_summary()
    
    if success:
        print("\n🎉 All async operation tests passed!")
        print("\n✅ The SDK now has comprehensive test coverage for:")
        print("   - Producer enqueue operations (single & batch)")
        print("   - Consumer streaming with flow control")
        print("   - Message ack/nack operations")
        print("   - Lease management and extension")
        print("   - Error handling scenarios")
        print("   - Concurrent consumer workflows")
        print("   - End-to-end producer→consumer integration")
        print("   - Priority ordering and exactly-once delivery")
    else:
        print("\n💥 Some async tests failed!")
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
