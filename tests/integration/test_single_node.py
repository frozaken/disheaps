#!/usr/bin/env python3
"""
Test script using only one node to avoid distributed consistency issues.
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

async def test_single_node_workflow():
    """Test the complete workflow using only one node."""
    
    # Use only the first node to avoid consistency issues
    client = DisheapClient(
        endpoints=["localhost:9090"],  # Only use node 1
        api_key="dh_test_key_secret123"
    )
    
    async with client:
        try:
            # Step 1: Create a topic
            logger.info("📝 Step 1: Creating test topic...")
            import time
            topic_name = f"single-node-test-{int(time.time())}"
            
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
            
            # Step 2: Enqueue test messages
            logger.info("📤 Step 2: Enqueuing test messages...")
            
            test_messages = [
                {"priority": 10, "payload": "High priority message"},
                {"priority": 5, "payload": "Medium priority message"},
                {"priority": 1, "payload": "Low priority message"},
                {"priority": 15, "payload": "Highest priority message"},
            ]
            
            enqueued_ids = []
            async with client.producer() as producer:
                for msg in test_messages:
                    result = await producer.enqueue(
                        topic=topic_name,
                        payload=msg["payload"].encode('utf-8'),
                        priority=msg["priority"]
                    )
                    enqueued_ids.append(result.message_id)
                    logger.info(f"✅ Enqueued: {msg['payload']} (priority={msg['priority']}, id={result.message_id})")
            
            logger.info(f"📊 Total messages enqueued: {len(enqueued_ids)}")
            
            # Step 3: Pop messages
            logger.info("📥 Step 3: Popping messages...")
            
            popped_messages = []
            async with client.consumer() as consumer:
                message_count = 0
                async for message in consumer.pop_stream(topic_name):
                    popped_messages.append({
                        "id": message.id,
                        "payload": message.payload.decode('utf-8'),
                        "priority": message.priority,
                        "attempts": message.attempts
                    })
                    
                    logger.info(f"📨 Popped: {message.payload.decode('utf-8')} "
                              f"(priority={message.priority}, id={message.id})")
                    
                    # Acknowledge the message
                    await message.ack()
                    logger.info(f"✅ Acknowledged message: {message.id}")
                    
                    message_count += 1
                    if message_count >= len(test_messages):
                        break
            
            # Step 4: Verify results
            logger.info("🔍 Step 4: Verifying results...")
            
            if len(popped_messages) == len(test_messages):
                logger.info("✅ All messages were popped successfully")
            else:
                logger.error(f"❌ Expected {len(test_messages)} messages, got {len(popped_messages)}")
            
            # Step 5: Check stats
            logger.info("📊 Step 5: Checking statistics...")
            
            stats = await client.get_stats()
            logger.info(f"📈 Stats: {stats}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Test failed: {e}")
            import traceback
            traceback.print_exc()
            return False

async def main():
    """Main function."""
    logger.info("🚀 Starting Single Node Test")
    logger.info("=" * 50)
    
    success = await test_single_node_workflow()
    
    if success:
        logger.info("🎉 Single node test completed successfully!")
        return 0
    else:
        logger.error("💥 Single node test failed!")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
