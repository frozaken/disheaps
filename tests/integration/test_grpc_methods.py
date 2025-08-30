#!/usr/bin/env python3
"""
Test specific gRPC methods that are implemented in the Disheap engine.
"""

import asyncio
import logging
import sys
import os

# Add the disheap-python directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'disheap-python'))

import grpc
from disheap._generated import disheap_pb2, disheap_pb2_grpc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


async def test_grpc_methods():
    """Test individual gRPC methods directly."""
    logger.info("🔧 Testing individual gRPC methods...")
    
    # Create gRPC channel
    channel = grpc.aio.insecure_channel('localhost:9090')
    stub = disheap_pb2_grpc.DisheapStub(channel)
    
    try:
        # Test 1: Stats (should work)
        logger.info("Testing Stats method...")
        try:
            stats_req = disheap_pb2.StatsReq()
            stats_resp = await stub.Stats(stats_req)
            logger.info(f"✅ Stats: global_messages={stats_resp.global_stats.total_messages}")
        except Exception as e:
            logger.error(f"❌ Stats failed: {e}")
        
        # Test 2: MakeHeap (might work)
        logger.info("Testing MakeHeap method...")
        try:
            heap_req = disheap_pb2.MakeHeapReq(
                topic="test-grpc-topic",
                mode=disheap_pb2.Mode.MIN,
                partitions=1,
                replication_factor=2,
                top_k_bound=100,
                max_retries_default=3,
                max_payload_bytes=1024*1024
            )
            heap_resp = await stub.MakeHeap(heap_req)
            logger.info(f"✅ MakeHeap: heap_id={heap_resp.heap_id}")
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.ALREADY_EXISTS:
                logger.info("✅ MakeHeap: Topic already exists (expected)")
            else:
                logger.error(f"❌ MakeHeap failed: {e.details()}")
        except Exception as e:
            logger.error(f"❌ MakeHeap failed: {e}")
        
        # Test 3: Enqueue (might be partially implemented)
        logger.info("Testing Enqueue method...")
        try:
            enqueue_req = disheap_pb2.EnqueueReq(
                topic="test-grpc-topic",
                payload=b"Hello from direct gRPC test!",
                priority=100
            )
            enqueue_resp = await stub.Enqueue(enqueue_req)
            logger.info(f"✅ Enqueue: message_id={enqueue_resp.message_id}")
        except Exception as e:
            logger.error(f"❌ Enqueue failed: {e}")
        
        # Test 4: Pop (might work)
        logger.info("Testing Pop method...")
        try:
            pop_req = disheap_pb2.PopReq(
                topic="test-grpc-topic",
                max_messages=1
            )
            pop_resp = await stub.Pop(pop_req)
            logger.info(f"✅ Pop: got {len(pop_resp.messages)} messages")
            for msg in pop_resp.messages:
                logger.info(f"  Message: id={msg.id}, payload={msg.payload.decode()}")
        except Exception as e:
            logger.error(f"❌ Pop failed: {e}")
        
        # Test 5: List available methods via reflection (if enabled)
        logger.info("Testing server reflection...")
        try:
            # This requires grpcio-reflection to be enabled on server
            from grpc_reflection.v1alpha import reflection_pb2, reflection_pb2_grpc
            reflection_stub = reflection_pb2_grpc.ServerReflectionStub(channel)
            
            # List services
            request = reflection_pb2.ServerReflectionRequest(
                list_services=""
            )
            response = await reflection_stub.ServerReflectionInfo([request]).__anext__()
            
            if response.HasField('list_services_response'):
                logger.info("✅ Available services:")
                for service in response.list_services_response.service:
                    logger.info(f"  - {service.name}")
            else:
                logger.info("⚠️ Server reflection not available")
                
        except Exception as e:
            logger.info(f"⚠️ Server reflection not available: {e}")
    
    finally:
        await channel.close()


async def test_health_check():
    """Test gRPC health check."""
    logger.info("🏥 Testing gRPC health check...")
    
    try:
        from grpc_health.v1 import health_pb2, health_pb2_grpc
        
        channel = grpc.aio.insecure_channel('localhost:9090')
        health_stub = health_pb2_grpc.HealthStub(channel)
        
        # Check overall health
        health_req = health_pb2.HealthCheckRequest()
        health_resp = await health_stub.Check(health_req)
        
        status_name = health_pb2.HealthCheckResponse.ServingStatus.Name(health_resp.status)
        logger.info(f"✅ Health check: {status_name}")
        
        await channel.close()
        
    except Exception as e:
        logger.error(f"❌ Health check failed: {e}")


async def main():
    """Run all gRPC tests."""
    logger.info("🚀 Starting direct gRPC method tests...")
    
    await test_grpc_methods()
    await test_health_check()
    
    logger.info("🎯 Direct gRPC tests completed!")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Tests interrupted by user")
    except Exception as e:
        logger.error(f"Test runner crashed: {e}")
        sys.exit(1)
