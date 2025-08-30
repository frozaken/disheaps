"""
Basic unit tests for the Disheap Python SDK using pytest.

These tests verify that the SDK components can be imported and
basic functionality works correctly.
"""

import pytest
from datetime import timedelta, datetime

from disheap import (
    DisheapClient,
    ClientConfig, 
    DisheapError,
    DisheapValidationError,
    Message,
    EnqueueRequest,
)


class TestClientConfig:
    """Test client configuration."""
    
    def test_valid_config(self):
        """Test creating a valid client configuration."""
        config = ClientConfig(
            endpoints=["localhost:9090"],
            api_key="dh_test_key"
        )
        
        assert config.endpoints == ["localhost:9090"]
        assert config.api_key == "dh_test_key"
        assert config.connect_timeout == timedelta(seconds=10)
    
    def test_invalid_config_empty_endpoints(self):
        """Test invalid client configuration with empty endpoints."""
        with pytest.raises(ValueError, match="At least one endpoint"):
            ClientConfig(endpoints=[], api_key="dh_test_key")
    
    def test_invalid_config_missing_api_key(self):
        """Test invalid client configuration with missing API key."""
        with pytest.raises(ValueError, match="API key must be provided"):
            ClientConfig(endpoints=["localhost:9090"], api_key="")
    
    def test_invalid_config_wrong_api_key_format(self):
        """Test invalid client configuration with wrong API key format."""
        with pytest.raises(ValueError, match="API key must follow format"):
            ClientConfig(endpoints=["localhost:9090"], api_key="invalid_key")


class TestDisheapClient:
    """Test the main Disheap client."""
    
    def test_client_creation(self):
        """Test creating a client."""
        client = DisheapClient(
            endpoints=["localhost:9090"],
            api_key="dh_test_key"
        )
        
        assert client.config.endpoints == ["localhost:9090"]
        assert client.config.api_key == "dh_test_key"
    
    def test_client_with_config(self):
        """Test creating a client with explicit config."""
        config = ClientConfig(
            endpoints=["engine1:9090", "engine2:9090"],
            api_key="dh_test_key"
        )
        
        client = DisheapClient(config=config)
        
        assert client.config.endpoints == ["engine1:9090", "engine2:9090"]
    
    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test client as async context manager."""
        client = DisheapClient(
            endpoints=["localhost:9090"],
            api_key="dh_test_key"
        )
        
        # Should not raise an exception
        async with client:
            assert not client._closed


class TestProducer:
    """Test producer functionality."""
    
    @pytest.mark.asyncio
    async def test_producer_creation(self):
        """Test creating a producer."""
        client = DisheapClient(
            endpoints=["localhost:9090"],
            api_key="dh_test_key"
        )
        
        producer = client.producer()
        
        assert producer._client is client
        assert producer.producer_id is not None
        assert producer.epoch is not None
    
    def test_enqueue_request(self):
        """Test creating enqueue requests."""
        request = EnqueueRequest(
            topic="test-topic",
            payload=b"test message",
            priority=100,
            partition_key="test-partition"
        )
        
        assert request.topic == "test-topic"
        assert request.payload == b"test message"
        assert request.priority == 100
        assert request.partition_key == "test-partition"


class TestConsumer:
    """Test consumer functionality."""
    
    @pytest.mark.asyncio
    async def test_consumer_creation(self):
        """Test creating a consumer."""
        client = DisheapClient(
            endpoints=["localhost:9090"],
            api_key="dh_test_key"
        )
        
        consumer = client.consumer()
        
        assert consumer._client is client
        assert not consumer._closed


class TestMessage:
    """Test message handling."""
    
    def test_message_creation(self):
        """Test creating a message."""
        now = datetime.now()
        
        message = Message(
            id="test_message_123",
            topic="test-topic",
            partition_id=0,
            payload=b"test payload",
            priority=100,
            enqueued_at=now,
            attempts=1,
            max_retries=3,
            lease_token="lease_token_123",
            lease_deadline=now + timedelta(minutes=5)
        )
        
        assert message.id == "test_message_123"
        assert message.topic == "test-topic"
        assert message.payload == b"test payload"
        assert message.priority == 100
        assert not message.is_processed
    
    def test_message_payload_helpers(self):
        """Test message payload helper methods."""
        message = Message(
            id="test_message_123",
            topic="test-topic",
            partition_id=0,
            payload=b"Hello, World!",
            priority=100,
            enqueued_at=datetime.now(),
            attempts=1,
            max_retries=3,
            lease_token="lease_token_123",
            lease_deadline=datetime.now() + timedelta(minutes=5)
        )
        
        assert message.payload_as_text() == "Hello, World!"
        assert message.payload_as_text("utf-8") == "Hello, World!"
    
    def test_message_lease_expiry(self):
        """Test message lease expiry checking."""
        now = datetime.now()
        
        message = Message(
            id="test_message_123",
            topic="test-topic", 
            partition_id=0,
            payload=b"test payload",
            priority=100,
            enqueued_at=now,
            attempts=1,
            max_retries=3,
            lease_token="lease_token_123",
            lease_deadline=now + timedelta(seconds=10)
        )
        
        # Should not be expiring soon with small threshold
        assert not message.is_lease_expiring_soon(timedelta(seconds=5))
        
        # Should be expiring soon with default threshold (30s > 10s lease)
        assert message.is_lease_expiring_soon()
        
        # Should be expiring soon with large threshold
        assert message.is_lease_expiring_soon(timedelta(minutes=1))


class TestExceptions:
    """Test exception hierarchy."""
    
    def test_exception_hierarchy(self):
        """Test that all exceptions inherit from DisheapError."""
        
        # Test base exception
        base_error = DisheapError("Base error")
        assert str(base_error) == "Base error"
        assert base_error.error_code is None
        
        # Test specific exceptions
        validation_error = DisheapValidationError("Invalid input", "INVALID_ARGUMENT")
        assert isinstance(validation_error, DisheapError)
        assert validation_error.error_code == "INVALID_ARGUMENT"
    
    def test_exception_with_details(self):
        """Test exception with details."""
        error = DisheapError(
            "Test error", 
            error_code="TEST_CODE",
            details={"field": "test_field", "value": "test_value"}
        )
        
        assert error.error_code == "TEST_CODE"
        assert error.details["field"] == "test_field"
        assert error.details["value"] == "test_value"


class TestIntegration:
    """Integration tests that test multiple components together."""
    
    @pytest.mark.asyncio
    async def test_basic_workflow(self):
        """Test a basic producer -> consumer workflow (mocked)."""
        client = DisheapClient(
            endpoints=["localhost:9090"],
            api_key="dh_test_key"
        )
        
        # Test producer creation and basic validation
        producer = client.producer()
        assert producer.producer_id is not None
        
        # Test consumer creation
        consumer = client.consumer()
        assert consumer._client is client
        
        # Test context manager usage
        async with producer:
            assert not producer._closed
        
        async with consumer:
            assert not consumer._closed
