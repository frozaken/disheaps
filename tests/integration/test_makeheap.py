#!/usr/bin/env python3
"""
Test the MakeHeap gRPC method specifically.
"""

import asyncio
import logging
import sys
import os

# Add the disheap-python directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'disheap-python'))

import grpc
from disheap._generated import disheap_pb2, disheap_pb2_grpc
from google.protobuf.duration_pb2 import Duration

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


async def test_makeheap():
    """Test the MakeHeap gRPC method."""
    logger.info("🔧 Testing MakeHeap gRPC method...")
    
    # Create gRPC channel
    options = [
        ('grpc.keepalive_time_ms', 30000),
        ('grpc.keepalive_timeout_ms', 5000),
        ('grpc.keepalive_permit_without_calls', True),
    ]
    
    channel = grpc.aio.insecure_channel('localhost:9090', options=options)
    stub = disheap_pb2_grpc.DisheapStub(channel)
    
    try:
        logger.info("Creating MakeHeap request...")
        
        # Create a test topic
        makeheap_req = disheap_pb2.MakeHeapReq(
            topic="test-grpc-topic",
            mode=disheap_pb2.Mode.MIN,
            partitions=2,
            replication_factor=2,
            top_k_bound=100,
            retention_time=Duration(seconds=3600),  # 1 hour
            visibility_timeout_default=Duration(seconds=300),  # 5 minutes
            max_retries_default=3,
            max_payload_bytes=1024*1024,  # 1MB
            compression_enabled=False
        )
        
        logger.info("Calling MakeHeap method...")
        makeheap_resp = await stub.MakeHeap(makeheap_req, timeout=10.0)
        
        logger.info(f"✅ MakeHeap successful!")
        logger.info(f"  Topic: {makeheap_resp.topic}")
        logger.info(f"  Heap ID: {makeheap_resp.heap_id}")
        
        return True
        
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.ALREADY_EXISTS:
            logger.info("✅ MakeHeap: Topic already exists (expected on retry)")
            return True
        else:
            logger.error(f"❌ gRPC error: {e.code()} - {e.details()}")
            return False
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        return False
    finally:
        logger.info("Closing gRPC channel...")
        await channel.close()


async def test_stats():
    """Test the Stats method to see if our topic was created."""
    logger.info("🔧 Testing Stats to verify topic creation...")
    
    channel = grpc.aio.insecure_channel('localhost:9090')
    stub = disheap_pb2_grpc.DisheapStub(channel)
    
    try:
        stats_req = disheap_pb2.StatsReq()
        stats_resp = await stub.Stats(stats_req, timeout=10.0)
        
        logger.info(f"✅ Stats successful!")
        logger.info(f"  Total topics: {stats_resp.global_stats.total_messages}")
        logger.info(f"  Total enqueues: {stats_resp.global_stats.total_enqueues}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Stats error: {e}")
        return False
    finally:
        await channel.close()


async def main():
    """Run the MakeHeap test."""
    logger.info("🚀 Starting MakeHeap test...")
    
    # Test MakeHeap
    makeheap_success = await test_makeheap()
    
    # Test Stats to verify
    stats_success = await test_stats()
    
    if makeheap_success and stats_success:
        logger.info("🎉 MakeHeap test successful!")
        return 0
    else:
        logger.error("💥 MakeHeap test failed!")
        return 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Test runner crashed: {e}")
        sys.exit(1)
