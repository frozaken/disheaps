#!/usr/bin/env python3
"""
Simple test to verify gRPC connection to Disheap engine.
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


async def test_stats():
    """Test the Stats gRPC method."""
    logger.info("🔧 Testing Stats gRPC method...")
    
    # Create gRPC channel with longer timeout
    options = [
        ('grpc.keepalive_time_ms', 30000),
        ('grpc.keepalive_timeout_ms', 5000),
        ('grpc.keepalive_permit_without_calls', True),
        ('grpc.http2.max_pings_without_data', 0),
        ('grpc.http2.min_time_between_pings_ms', 10000),
        ('grpc.http2.min_ping_interval_without_data_ms', 300000),
    ]
    
    channel = grpc.aio.insecure_channel('localhost:9090', options=options)
    stub = disheap_pb2_grpc.DisheapStub(channel)
    
    try:
        logger.info("Creating Stats request...")
        stats_req = disheap_pb2.StatsReq()
        
        logger.info("Calling Stats method...")
        stats_resp = await stub.Stats(stats_req, timeout=10.0)
        
        logger.info(f"✅ Stats successful!")
        logger.info(f"  Global messages: {stats_resp.global_stats.total_messages}")
        logger.info(f"  Inflight messages: {stats_resp.global_stats.inflight_messages}")
        logger.info(f"  Total enqueues: {stats_resp.global_stats.total_enqueues}")
        
        return True
        
    except grpc.RpcError as e:
        logger.error(f"❌ gRPC error: {e.code()} - {e.details()}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        return False
    finally:
        logger.info("Closing gRPC channel...")
        await channel.close()


async def main():
    """Run the simple gRPC test."""
    logger.info("🚀 Starting simple gRPC test...")
    
    success = await test_stats()
    
    if success:
        logger.info("🎉 gRPC test successful!")
        return 0
    else:
        logger.error("💥 gRPC test failed!")
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
