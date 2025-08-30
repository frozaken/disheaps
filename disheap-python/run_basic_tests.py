#!/usr/bin/env python3
"""
Simple test runner to demonstrate basic SDK functionality without pytest.
"""

import sys
import traceback
from datetime import timedelta, datetime

# Add the disheap package to Python path
sys.path.insert(0, '.')

from disheap import (
    DisheapClient,
    ClientConfig, 
    DisheapError,
    DisheapValidationError,
    Message,
    EnqueueRequest,
)


class SimpleTestRunner:
    """Simple test runner without pytest dependency."""
    
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []
    
    def run_test(self, test_name, test_func):
        """Run a single test function."""
        try:
            print(f"Running {test_name}...", end=" ")
            test_func()
            print("✅ PASS")
            self.passed += 1
        except Exception as e:
            print(f"❌ FAIL: {e}")
            self.failed += 1
            self.errors.append((test_name, str(e), traceback.format_exc()))
    
    def print_summary(self):
        """Print test summary."""
        total = self.passed + self.failed
        print(f"\n{'='*50}")
        print(f"Test Results: {self.passed}/{total} passed")
        
        if self.errors:
            print(f"\nFailures:")
            for name, error, trace in self.errors:
                print(f"- {name}: {error}")
        
        return self.failed == 0


def test_client_config_valid():
    """Test creating a valid client configuration."""
    config = ClientConfig(
        endpoints=["localhost:9090"],
        api_key="dh_test_key"
    )
    
    assert config.endpoints == ["localhost:9090"]
    assert config.api_key == "dh_test_key"
    assert config.connect_timeout == timedelta(seconds=10)


def test_client_config_invalid():
    """Test invalid client configurations."""
    try:
        ClientConfig(endpoints=[], api_key="dh_test_key")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "At least one endpoint" in str(e)
    
    try:
        ClientConfig(endpoints=["localhost:9090"], api_key="")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "API key must be provided" in str(e)


def test_client_creation():
    """Test creating a client."""
    client = DisheapClient(
        endpoints=["localhost:9090"],
        api_key="dh_test_key"
    )
    
    assert client.config.endpoints == ["localhost:9090"]
    assert client.config.api_key == "dh_test_key"


def test_producer_creation():
    """Test creating a producer."""
    client = DisheapClient(
        endpoints=["localhost:9090"],
        api_key="dh_test_key"
    )
    
    producer = client.producer()
    
    assert producer._client is client
    assert producer.producer_id is not None
    assert producer.epoch is not None


def test_consumer_creation():
    """Test creating a consumer."""
    client = DisheapClient(
        endpoints=["localhost:9090"],
        api_key="dh_test_key"
    )
    
    consumer = client.consumer()
    
    assert consumer._client is client
    assert not consumer._closed


def test_message_creation():
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


def test_message_payload_helpers():
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


def test_enqueue_request():
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


def test_exception_hierarchy():
    """Test that all exceptions inherit from DisheapError."""
    
    # Test base exception
    base_error = DisheapError("Base error")
    assert str(base_error) == "Base error"
    assert base_error.error_code is None
    
    # Test specific exceptions
    validation_error = DisheapValidationError("Invalid input", "INVALID_ARGUMENT")
    assert isinstance(validation_error, DisheapError)
    assert validation_error.error_code == "INVALID_ARGUMENT"


def main():
    """Run all tests."""
    print("🧪 Running Disheap Python SDK Basic Tests")
    print("=" * 50)
    
    runner = SimpleTestRunner()
    
    # Run all test functions
    runner.run_test("test_client_config_valid", test_client_config_valid)
    runner.run_test("test_client_config_invalid", test_client_config_invalid)
    runner.run_test("test_client_creation", test_client_creation)
    runner.run_test("test_producer_creation", test_producer_creation)
    runner.run_test("test_consumer_creation", test_consumer_creation)
    runner.run_test("test_message_creation", test_message_creation)
    runner.run_test("test_message_payload_helpers", test_message_payload_helpers)
    runner.run_test("test_enqueue_request", test_enqueue_request)
    runner.run_test("test_exception_hierarchy", test_exception_hierarchy)
    
    success = runner.print_summary()
    
    if success:
        print("\n🎉 All basic tests passed!")
        print("\n⚠️  NOTE: These are only BASIC tests.")
        print("   Production requires comprehensive async operation testing.")
    else:
        print("\n💥 Some tests failed!")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
