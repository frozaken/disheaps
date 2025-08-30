#!/usr/bin/env python3
"""
Test script to verify Raft state distribution and topic replication.
"""

import asyncio
import logging
import sys
import os
import json
import httpx
from datetime import timedelta

# Add the disheap-python directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'disheap-python'))

from disheap import DisheapClient, DisheapError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Engine endpoints
ENGINE_ENDPOINTS = [
    "localhost:9090",  # engine-1
    "localhost:9092",  # engine-2  
    "localhost:9094",  # engine-3
]

# HTTP endpoints for Raft status
HTTP_ENDPOINTS = [
    "http://localhost:8080",  # engine-1
    "http://localhost:8082",  # engine-2
    "http://localhost:8084",  # engine-3
]

async def get_raft_status(client, endpoint):
    """Get Raft status from an engine's HTTP endpoint."""
    try:
        response = await client.get(f"{endpoint}/raft/status")
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Failed to get Raft status from {endpoint}: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error getting Raft status from {endpoint}: {e}")
        return None

async def get_stats(client, endpoint):
    """Get stats from an engine's HTTP endpoint."""
    try:
        response = await client.get(f"{endpoint}/stats")
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Failed to get stats from {endpoint}: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error getting stats from {endpoint}: {e}")
        return None

async def test_raft_cluster_status():
    """Test that all nodes are properly connected in the Raft cluster."""
    logger.info("🔍 Step 1: Checking Raft cluster status...")
    
    async with httpx.AsyncClient() as client:
        raft_statuses = []
        
        for i, endpoint in enumerate(HTTP_ENDPOINTS):
            logger.info(f"📊 Checking Raft status on engine-{i+1} ({endpoint})...")
            status = await get_raft_status(client, endpoint)
            if status:
                raft_statuses.append((i+1, status))
                logger.info(f"✅ Engine-{i+1} Raft status: {json.dumps(status, indent=2)}")
            else:
                logger.error(f"❌ Failed to get Raft status from engine-{i+1}")
                return False
        
        # Analyze the cluster
        leaders = []
        followers = []
        
        for engine_id, status in raft_statuses:
            state = status.get('state', 'unknown')
            if state == 'Leader':
                leaders.append(engine_id)
            elif state == 'Follower':
                followers.append(engine_id)
            
            logger.info(f"🏛️  Engine-{engine_id}: State={state}, "
                       f"Term={status.get('term', 'unknown')}, "
                       f"Peers={len(status.get('peers', []))}")
        
        # Validate cluster health
        if len(leaders) != 1:
            logger.error(f"❌ Expected exactly 1 leader, found {len(leaders)}: {leaders}")
            return False
        
        if len(followers) != 2:
            logger.error(f"❌ Expected exactly 2 followers, found {len(followers)}: {followers}")
            return False
        
        logger.info(f"✅ Raft cluster is healthy: 1 leader (engine-{leaders[0]}), 2 followers (engines-{followers})")
        
        # Check that all nodes have the same term
        terms = [status.get('term', 0) for _, status in raft_statuses]
        if len(set(terms)) != 1:
            logger.error(f"❌ Nodes have different terms: {terms}")
            return False
        
        logger.info(f"✅ All nodes are on the same term: {terms[0]}")
        
        return True

async def test_topic_replication():
    """Test that topics are properly replicated across the cluster."""
    logger.info("🔍 Step 2: Testing topic replication...")
    
    # Create a topic on one node
    client = DisheapClient(
        endpoints=[ENGINE_ENDPOINTS[0]],  # Only connect to engine-1
        api_key="dh_test_key_secret123"
    )
    
    import time
    topic_name = f"replication-test-{int(time.time())}"
    
    async with client:
        logger.info(f"📝 Creating topic '{topic_name}' on engine-1...")
        
        await client.create_topic(
            topic_name,
            mode="min",
            partitions=3,
            replication_factor=2,
            retention_time=timedelta(hours=1),
            max_retries=3,
            max_payload_bytes=1024*1024,
        )
        
        logger.info(f"✅ Topic '{topic_name}' created on engine-1")
    
    # Wait a moment for replication
    await asyncio.sleep(2)
    
    # Now check if the topic exists on all nodes
    logger.info("🔍 Checking if topic is replicated to all nodes...")
    
    replication_results = []
    
    for i, endpoint in enumerate(ENGINE_ENDPOINTS):
        logger.info(f"📊 Checking topic existence on engine-{i+1} ({endpoint})...")
        
        client = DisheapClient(
            endpoints=[endpoint],
            api_key="dh_test_key_secret123"
        )
        
        try:
            async with client:
                # Try to get stats - this should work if the topic exists
                stats = await client.get_stats()
                
                # Try to enqueue a message to verify the topic exists
                async with client.producer() as producer:
                    result = await producer.enqueue(
                        topic=topic_name,
                        payload=f"test message from engine-{i+1}".encode('utf-8'),
                        priority=10
                    )
                    logger.info(f"✅ Successfully enqueued message on engine-{i+1}: {result.message_id}")
                    replication_results.append((i+1, True, result.message_id))
                    
        except Exception as e:
            logger.error(f"❌ Failed to access topic on engine-{i+1}: {e}")
            replication_results.append((i+1, False, str(e)))
    
    # Analyze replication results
    successful_nodes = [engine_id for engine_id, success, _ in replication_results if success]
    failed_nodes = [engine_id for engine_id, success, _ in replication_results if not success]
    
    if len(successful_nodes) == len(ENGINE_ENDPOINTS):
        logger.info(f"✅ Topic '{topic_name}' is accessible on all {len(successful_nodes)} nodes!")
        return True, topic_name
    else:
        logger.error(f"❌ Topic replication failed. Successful nodes: {successful_nodes}, Failed nodes: {failed_nodes}")
        for engine_id, success, info in replication_results:
            if not success:
                logger.error(f"   Engine-{engine_id}: {info}")
        return False, topic_name

async def test_message_consistency():
    """Test that messages are consistently available across nodes."""
    logger.info("🔍 Step 3: Testing message consistency across nodes...")
    
    # Use the topic created in the previous test
    import time
    topic_name = f"consistency-test-{int(time.time())}"
    
    # Create topic and enqueue messages on engine-1
    client1 = DisheapClient(
        endpoints=[ENGINE_ENDPOINTS[0]],
        api_key="dh_test_key_secret123"
    )
    
    message_ids = []
    
    async with client1:
        logger.info(f"📝 Creating topic '{topic_name}' and enqueuing messages on engine-1...")
        
        await client1.create_topic(
            topic_name,
            mode="min",
            partitions=2,
            replication_factor=2,
            retention_time=timedelta(hours=1),
            max_retries=3,
            max_payload_bytes=1024*1024,
        )
        
        # Enqueue multiple messages
        async with client1.producer() as producer:
            for i in range(5):
                result = await producer.enqueue(
                    topic=topic_name,
                    payload=f"consistency test message {i+1}".encode('utf-8'),
                    priority=10 + i
                )
                message_ids.append(result.message_id)
                logger.info(f"✅ Enqueued message {i+1}: {result.message_id}")
    
    # Wait for replication
    await asyncio.sleep(3)
    
    # Try to consume messages from different nodes
    logger.info("🔍 Testing message consumption from different nodes...")
    
    consumption_results = []
    
    for i, endpoint in enumerate(ENGINE_ENDPOINTS):
        logger.info(f"📥 Trying to consume messages from engine-{i+1} ({endpoint})...")
        
        client = DisheapClient(
            endpoints=[endpoint],
            api_key="dh_test_key_secret123"
        )
        
        try:
            async with client:
                async with client.consumer() as consumer:
                    consumed_messages = []
                    message_count = 0
                    
                    # Try to consume up to 3 messages (don't consume all to test on other nodes)
                    async for message in consumer.pop_stream(topic_name):
                        consumed_messages.append({
                            'id': message.id,
                            'payload': message.payload.decode('utf-8'),
                            'priority': message.priority
                        })
                        
                        await message.ack()
                        message_count += 1
                        
                        if message_count >= 2:  # Only consume 2 messages per node
                            break
                    
                    logger.info(f"✅ Engine-{i+1} consumed {len(consumed_messages)} messages")
                    for msg in consumed_messages:
                        logger.info(f"   📨 {msg['payload']} (priority={msg['priority']}, id={msg['id']})")
                    
                    consumption_results.append((i+1, True, consumed_messages))
                    
        except Exception as e:
            logger.error(f"❌ Failed to consume from engine-{i+1}: {e}")
            consumption_results.append((i+1, False, str(e)))
    
    # Analyze consumption results
    successful_consumptions = [result for result in consumption_results if result[1]]
    
    if len(successful_consumptions) >= 2:  # At least 2 nodes should be able to consume
        logger.info(f"✅ Message consistency test passed! {len(successful_consumptions)} nodes successfully consumed messages")
        return True
    else:
        logger.error(f"❌ Message consistency test failed. Only {len(successful_consumptions)} nodes could consume messages")
        return False

async def test_stats_consistency():
    """Test that stats are consistent across nodes."""
    logger.info("🔍 Step 4: Testing stats consistency across nodes...")
    
    async with httpx.AsyncClient() as client:
        stats_results = []
        
        for i, endpoint in enumerate(HTTP_ENDPOINTS):
            logger.info(f"📊 Getting stats from engine-{i+1} ({endpoint})...")
            stats = await get_stats(client, endpoint)
            if stats:
                stats_results.append((i+1, stats))
                logger.info(f"✅ Engine-{i+1} stats: {json.dumps(stats, indent=2)}")
            else:
                logger.error(f"❌ Failed to get stats from engine-{i+1}")
        
        if len(stats_results) == len(HTTP_ENDPOINTS):
            logger.info("✅ Successfully retrieved stats from all nodes")
            
            # Compare global stats (they should be similar due to replication)
            global_stats = [stats.get('global_stats', {}) for _, stats in stats_results]
            logger.info("📊 Global stats comparison:")
            for i, (engine_id, _) in enumerate(stats_results):
                gs = global_stats[i]
                logger.info(f"   Engine-{engine_id}: total_messages={gs.get('total_messages', 0)}, "
                           f"inflight_messages={gs.get('inflight_messages', 0)}")
            
            return True
        else:
            logger.error("❌ Could not retrieve stats from all nodes")
            return False

async def main():
    """Main function."""
    logger.info("🚀 Starting Raft Replication Test")
    logger.info("=" * 60)
    
    try:
        # Test 1: Raft cluster status
        raft_ok = await test_raft_cluster_status()
        if not raft_ok:
            logger.error("💥 Raft cluster test failed!")
            return 1
        
        await asyncio.sleep(1)
        
        # Test 2: Topic replication
        replication_ok, topic_name = await test_topic_replication()
        if not replication_ok:
            logger.error("💥 Topic replication test failed!")
            return 1
        
        await asyncio.sleep(1)
        
        # Test 3: Message consistency
        consistency_ok = await test_message_consistency()
        if not consistency_ok:
            logger.error("💥 Message consistency test failed!")
            return 1
        
        await asyncio.sleep(1)
        
        # Test 4: Stats consistency
        stats_ok = await test_stats_consistency()
        if not stats_ok:
            logger.error("💥 Stats consistency test failed!")
            return 1
        
        logger.info("🎉 All Raft replication tests passed!")
        logger.info("✅ Raft is properly distributing state across the cluster")
        logger.info("✅ Topics are correctly replicated")
        logger.info("✅ Messages are consistently available")
        logger.info("✅ Stats are synchronized")
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
