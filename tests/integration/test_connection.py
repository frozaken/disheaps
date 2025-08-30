#!/usr/bin/env python3
"""
Simple test script to verify Python SDK can connect to the running Disheap cluster.
"""

import asyncio
import logging
import sys
import os

# Add the disheap-python directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'disheap-python'))

from disheap import DisheapClient, DisheapError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


async def test_basic_connection():
    """Test basic connection to the Disheap cluster."""
    logger.info("🔌 Testing basic connection to Disheap cluster...")
    
    # Test connection to each engine node
    endpoints = [
        "localhost:9090",  # Engine 1 (leader)
        "localhost:9092",  # Engine 2 (follower)
        "localhost:9094",  # Engine 3 (follower)
    ]
    
    for endpoint in endpoints:
        logger.info(f"Testing connection to {endpoint}...")
        
        try:
            # Create client for this specific endpoint
            client = DisheapClient(
                endpoints=[endpoint],
                api_key="dh_test_key_secret123"  # Mock API key for testing
            )
            
            async with client:
                logger.info(f"✅ Connected to {endpoint}")
                
                # Test health check
                is_healthy = await client.is_healthy()
                logger.info(f"Health check for {endpoint}: {'✅ Healthy' if is_healthy else '❌ Unhealthy'}")
                
        except Exception as e:
            logger.error(f"❌ Failed to connect to {endpoint}: {e}")
            return False
    
    return True


async def test_multi_endpoint_connection():
    """Test connection with multiple endpoints (load balancing)."""
    logger.info("🔄 Testing multi-endpoint connection...")
    
    try:
        client = DisheapClient(
            endpoints=[
                "localhost:9090",  # Engine 1 (leader)
                "localhost:9092",  # Engine 2 (follower) 
                "localhost:9094",  # Engine 3 (follower)
            ],
            api_key="dh_test_key_secret123"
        )
        
        async with client:
            logger.info("✅ Connected to multi-endpoint cluster")
            
            # Test health check
            is_healthy = await client.is_healthy()
            logger.info(f"Cluster health: {'✅ Healthy' if is_healthy else '❌ Unhealthy'}")
            
            return True
            
    except Exception as e:
        logger.error(f"❌ Failed multi-endpoint connection: {e}")
        return False


async def test_topic_operations():
    """Test basic topic operations."""
    logger.info("📝 Testing topic operations...")
    
    try:
        client = DisheapClient(
            endpoints=["localhost:9090"],  # Connect to leader
            api_key="dh_test_key_secret123"
        )
        
        async with client:
            # Try to create a test topic
            try:
                topic_id = await client.create_topic(
                    topic="test-connection-topic",
                    mode="min",
                    partitions=1,
                    replication_factor=2
                )
                logger.info(f"✅ Created topic: {topic_id}")
                
            except DisheapError as e:
                if "already exists" in str(e).lower():
                    logger.info("✅ Topic already exists (expected)")
                else:
                    logger.error(f"❌ Failed to create topic: {e}")
                    return False
            
            # Try to get stats
            try:
                stats = await client.get_stats()
                logger.info(f"✅ Retrieved stats: {stats}")
                
            except Exception as e:
                logger.error(f"❌ Failed to get stats: {e}")
                return False
                
            return True
            
    except Exception as e:
        logger.error(f"❌ Topic operations failed: {e}")
        return False


async def test_producer_consumer():
    """Test basic producer/consumer operations."""
    logger.info("📤📥 Testing producer/consumer operations...")
    
    try:
        client = DisheapClient(
            endpoints=["localhost:9090"],
            api_key="dh_test_key_secret123"
        )
        
        async with client:
            # Test producer
            try:
                async with client.producer() as producer:
                    result = await producer.enqueue(
                        topic="test-connection-topic",
                        payload=b"Hello from Python SDK!",
                        priority=100
                    )
                    logger.info(f"✅ Enqueued message: {result.message_id}")
                    
            except Exception as e:
                logger.error(f"❌ Producer test failed: {e}")
                return False
            
            # Test consumer (try to consume the message we just sent)
            try:
                async with client.consumer() as consumer:
                    logger.info("Attempting to consume message...")
                    
                    # Use async timeout to avoid hanging
                    try:
                        async with asyncio.timeout(5.0):  # 5 second timeout
                            async for message in consumer.pop_stream("test-connection-topic"):
                                async with message:
                                    payload = message.payload_as_text()
                                    logger.info(f"✅ Consumed message: {payload}")
                                    break  # Only consume one message for test
                    except asyncio.TimeoutError:
                        logger.warning("⚠️ Consumer timeout (no messages available)")
                        
            except Exception as e:
                logger.error(f"❌ Consumer test failed: {e}")
                return False
                
            return True
            
    except Exception as e:
        logger.error(f"❌ Producer/consumer test failed: {e}")
        return False


async def main():
    """Run all connection tests."""
    logger.info("🚀 Starting Disheap Python SDK connection tests...")
    
    tests = [
        ("Basic Connection", test_basic_connection),
        ("Multi-Endpoint Connection", test_multi_endpoint_connection),
        ("Topic Operations", test_topic_operations),
        ("Producer/Consumer", test_producer_consumer),
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        logger.info(f"\n{'='*50}")
        logger.info(f"Running: {test_name}")
        logger.info('='*50)
        
        try:
            success = await test_func()
            results[test_name] = success
            
            if success:
                logger.info(f"✅ {test_name} PASSED")
            else:
                logger.error(f"❌ {test_name} FAILED")
                
        except Exception as e:
            logger.error(f"❌ {test_name} CRASHED: {e}")
            results[test_name] = False
    
    # Print summary
    logger.info(f"\n{'='*50}")
    logger.info("TEST SUMMARY")
    logger.info('='*50)
    
    passed = sum(1 for success in results.values() if success)
    total = len(results)
    
    for test_name, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL"
        logger.info(f"{test_name}: {status}")
    
    logger.info(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 All tests passed! Python SDK can connect to the cluster.")
        return 0
    else:
        logger.error("💥 Some tests failed. Check the logs above.")
        return 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Tests interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Test runner crashed: {e}")
        sys.exit(1)
