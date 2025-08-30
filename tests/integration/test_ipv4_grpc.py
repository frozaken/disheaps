#!/usr/bin/env python3
"""
Test gRPC connection using explicit IPv4 address.
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

async def test_ipv4_connection():
    """Test gRPC connection using explicit IPv4."""
    logger.info("🔧 Testing IPv4 gRPC connection...")
    
    # Try different addresses
    addresses = [
        "127.0.0.1:9090",  # IPv4 localhost
        "0.0.0.0:9090",    # All interfaces IPv4
        "[::1]:9090",      # IPv6 localhost
        "localhost:9090"   # Let system decide
    ]
    
    for addr in addresses:
        logger.info(f"Testing address: {addr}")
        
        channel = grpc.aio.insecure_channel(addr)
        stub = disheap_pb2_grpc.DisheapStub(channel)
        
        try:
            # Test with a short timeout
            logger.info(f"  Checking channel ready...")
            await asyncio.wait_for(channel.channel_ready(), timeout=3.0)
            logger.info(f"  ✅ Channel ready for {addr}")
            
            # Try a quick Stats call
            logger.info(f"  Making Stats call...")
            request = disheap_pb2.StatsReq()
            response = await asyncio.wait_for(stub.Stats(request), timeout=3.0)
            logger.info(f"  ✅ Stats successful for {addr}: {response}")
            
            await channel.close()
            return addr  # Return successful address
            
        except asyncio.TimeoutError:
            logger.error(f"  ❌ Timeout for {addr}")
        except grpc.RpcError as e:
            logger.error(f"  ❌ gRPC error for {addr}: {e.code()} - {e.details()}")
        except Exception as e:
            logger.error(f"  ❌ Unexpected error for {addr}: {e}")
        finally:
            await channel.close()
    
    return None

async def main():
    """Run the IPv4 connection test."""
    successful_addr = await test_ipv4_connection()
    
    if successful_addr:
        logger.info(f"🎉 Successfully connected using: {successful_addr}")
        return 0
    else:
        logger.error("💥 Failed to connect to any address")
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
