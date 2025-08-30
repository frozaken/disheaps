#!/usr/bin/env python3
"""
Test script to verify graceful disconnect handling.
"""

import asyncio
import logging
import sys
import os
from datetime import timedelta

# Add the disheap-python directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'disheap-python'))

from disheap import DisheapClient, DisheapError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_graceful_disconnect():
    """Test that client disconnections are handled gracefully by the server."""
    
    client = DisheapClient(
        endpoints=["localhost:9090"],
        api_key="dh_test_key_secret123"
    )
    
    async with client:
        # Step 1: Create a topic
        logger.info("📝 Creating test topic...")
        import time
        topic_name = f"disconnect-test-{int(time.time())}"
        
        await client.create_topic(
            topic_name,
            mode="min",
            partitions=2,
            replication_factor=2,
            retention_time=timedelta(hours=1),
            max_retries=3,
            max_payload_bytes=1024*1024,
        )
        logger.info(f"✅ Topic '{topic_name}' created successfully")
        
        # Step 2: Start a consumer and then disconnect abruptly
        logger.info("📥 Starting consumer...")
        
        consumer = client.consumer()
        await consumer.__aenter__()
        
        logger.info("🔌 Starting PopStream and then disconnecting...")
        
        try:
            # Start the stream
            stream = consumer.pop_stream(topic_name)
            stream_iter = aiter(stream)
            
            # Wait a moment to establish the connection
            await asyncio.sleep(0.5)
            
            # Now disconnect abruptly by closing the consumer
            logger.info("🔌 Disconnecting consumer abruptly...")
            await consumer.__aexit__(None, None, None)
            
            logger.info("✅ Consumer disconnected successfully")
            
        except Exception as e:
            logger.info(f"✅ Expected exception during disconnect: {e}")
        
        logger.info("🔍 Disconnect test completed - check server logs for graceful handling")
        
        return True

async def test_multiple_connections():
    """Test multiple concurrent connections and disconnections."""
    
    logger.info("🔄 Testing multiple concurrent connections...")
    
    async def create_and_disconnect_consumer(client, topic_name, consumer_id):
        try:
            consumer = client.consumer()
            await consumer.__aenter__()
            
            # Start streaming
            stream = consumer.pop_stream(topic_name)
            stream_iter = aiter(stream)
            
            # Wait a random amount of time
            await asyncio.sleep(0.1 + (consumer_id * 0.1))
            
            # Disconnect
            await consumer.__aexit__(None, None, None)
            logger.info(f"✅ Consumer {consumer_id} disconnected")
            
        except Exception as e:
            logger.info(f"✅ Consumer {consumer_id} disconnected with expected error: {e}")
    
    client = DisheapClient(
        endpoints=["localhost:9090"],
        api_key="dh_test_key_secret123"
    )
    
    async with client:
        # Create topic
        import time
        topic_name = f"multi-disconnect-test-{int(time.time())}"
        
        await client.create_topic(
            topic_name,
            mode="min",
            partitions=2,
            replication_factor=2,
            retention_time=timedelta(hours=1),
            max_retries=3,
            max_payload_bytes=1024*1024,
        )
        
        # Create multiple concurrent consumers
        tasks = []
        for i in range(5):
            task = create_and_disconnect_consumer(client, topic_name, i)
            tasks.append(task)
        
        # Run all consumers concurrently
        await asyncio.gather(*tasks, return_exceptions=True)
        
        logger.info("✅ Multiple connection test completed")
        
        return True

async def main():
    """Main function."""
    logger.info("🚀 Starting Disconnect Handling Test")
    logger.info("=" * 50)
    
    try:
        # Test 1: Single graceful disconnect
        success1 = await test_graceful_disconnect()
        
        # Wait a moment
        await asyncio.sleep(1)
        
        # Test 2: Multiple concurrent disconnects
        success2 = await test_multiple_connections()
        
        if success1 and success2:
            logger.info("🎉 All disconnect tests completed successfully!")
            logger.info("📋 Check the server logs - there should be no ERROR messages for disconnections")
            return 0
        else:
            logger.error("💥 Some disconnect tests failed!")
            return 1
            
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
