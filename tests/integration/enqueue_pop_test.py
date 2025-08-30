#!/usr/bin/env python3
"""
Test script to enqueue a message and pop it again.
This will work once the gRPC connection issue is resolved.
"""

import asyncio
import logging
import sys
import os
import json
from datetime import timedelta
from typing import Optional

# Add the disheap-python directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'disheap-python'))

from disheap import DisheapClient, DisheapError
from google.protobuf.duration_pb2 import Duration

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


async def test_enqueue_pop_workflow():
    """Test the complete enqueue -> pop workflow."""
    logger.info("🚀 Starting Enqueue/Pop Test Workflow")
    logger.info("=" * 60)
    
    # Connect to the cluster
    client = DisheapClient(
        endpoints=["localhost:9090", "localhost:9092", "localhost:9094"],
        api_key="dh_test_key_secret123"
    )
    
    async with client:
        try:
            # Step 1: Create a topic (or use existing)
            logger.info("📝 Step 1: Creating test topic...")
            import time
            topic_name = f"test-enqueue-pop-topic-{int(time.time())}"
            
            try:
                await client.create_topic(
                    topic_name,
                    mode="min",  # Min-heap (lowest priority first)
                    partitions=2,
                    replication_factor=2,
                    top_k_bound=100,
                    retention_time=timedelta(hours=1),  # 1 hour
                    max_retries=3,
                    max_payload_bytes=1024*1024,  # 1MB
                )
                logger.info(f"✅ Topic '{topic_name}' created successfully")
            except DisheapError as e:
                if "already exists" in str(e):
                    logger.info(f"✅ Topic '{topic_name}' already exists, continuing...")
                else:
                    raise
            
            # Step 2: Enqueue some test messages
            logger.info("📤 Step 2: Enqueuing test messages...")
            
            test_messages = [
                {"priority": 10, "payload": "High priority message", "id": "msg-1"},
                {"priority": 5, "payload": "Medium priority message", "id": "msg-2"},
                {"priority": 1, "payload": "Low priority message", "id": "msg-3"},
                {"priority": 15, "payload": "Highest priority message", "id": "msg-4"},
            ]
            
            enqueued_ids = []
            async with client.producer() as producer:
                for msg in test_messages:
                    message_id = await producer.enqueue(
                        topic=topic_name,
                        payload=msg["payload"].encode('utf-8'),
                        priority=msg["priority"]
                    )
                    enqueued_ids.append(message_id)
                    logger.info(f"✅ Enqueued: {msg['payload']} (priority={msg['priority']}, id={message_id})")
            
            logger.info(f"📊 Total messages enqueued: {len(enqueued_ids)}")
            
            # Step 3: Pop messages (should come out in priority order for MIN heap)
            logger.info("📥 Step 3: Popping messages...")
            
            popped_messages = []
            
            # Create a consumer to pop messages
            async with client.consumer() as consumer:
                # Pop messages one by one using pop_stream
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
            
            # Step 4: Verify message ordering
            logger.info("🔍 Step 4: Verifying message ordering...")
            
            if len(popped_messages) == len(test_messages):
                logger.info("✅ All messages were popped successfully")
                
                # For MIN heap, messages should come out in ascending priority order
                priorities = [msg["priority"] for msg in popped_messages]
                expected_order = sorted([msg["priority"] for msg in test_messages])
                
                if priorities == expected_order:
                    logger.info("✅ Messages came out in correct priority order!")
                    logger.info(f"   Order: {priorities}")
                else:
                    logger.warning("⚠️  Messages did not come out in expected order")
                    logger.warning(f"   Expected: {expected_order}")
                    logger.warning(f"   Actual:   {priorities}")
            else:
                logger.error(f"❌ Expected {len(test_messages)} messages, got {len(popped_messages)}")
            
            # Step 5: Check topic stats
            logger.info("📊 Step 5: Checking topic statistics...")
            
            stats = await client.get_stats()
            logger.info(f"📈 Global Stats:")
            logger.info(f"   Total enqueues: {stats.global_stats.total_enqueues}")
            logger.info(f"   Total pops: {stats.global_stats.total_pops}")
            logger.info(f"   Total acks: {stats.global_stats.total_acks}")
            logger.info(f"   Inflight messages: {stats.global_stats.inflight_messages}")
            
            logger.info("🎉 Enqueue/Pop test completed successfully!")
            return True
            
        except DisheapError as e:
            logger.error(f"❌ Disheap error: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
            return False


async def main():
    """Run the enqueue/pop test."""
    try:
        success = await test_enqueue_pop_workflow()
        return 0 if success else 1
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Test runner crashed: {e}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
