#!/usr/bin/env python3
"""
Simple test to create a topic.
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

async def test_create_topic():
    """Test creating a topic."""
    client = DisheapClient(
        endpoints=["localhost:9090"],
        api_key="dh_test_key_secret123"
    )
    
    async with client:
        try:
            logger.info("Creating topic...")
            result = await client.create_topic(
                "test-topic",
                mode="min",
                partitions=2,
                replication_factor=2,
                retention_time=timedelta(hours=1)
            )
            logger.info(f"Topic created: {result}")
            return True
        except Exception as e:
            logger.error(f"Error: {e}")
            import traceback
            traceback.print_exc()
            return False

if __name__ == "__main__":
    success = asyncio.run(test_create_topic())
    sys.exit(0 if success else 1)
