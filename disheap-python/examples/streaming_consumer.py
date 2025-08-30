#!/usr/bin/env python3
"""
Streaming consumer example for the Disheap Python SDK.

This example demonstrates:
- Creating a consumer with automatic flow control
- Streaming messages from a topic with context managers
- Automatic lease management and renewal
- Error handling and message nacking
- Graceful shutdown
"""

import asyncio
import logging
import signal
import random
from datetime import timedelta

from disheap import DisheapClient, DisheapError


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


class GracefulShutdown:
    """Helper class for graceful shutdown handling."""
    
    def __init__(self):
        self.shutdown_event = asyncio.Event()
        self.setup_signal_handlers()
    
    def setup_signal_handlers(self):
        """Set up signal handlers for graceful shutdown."""
        try:
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
        except ValueError:
            # Signals not available (e.g., on Windows)
            pass
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.shutdown_event.set()


async def process_message(message) -> bool:
    """
    Process a single message.
    
    Args:
        message: Message instance to process
        
    Returns:
        True if processing succeeded, False if it should be retried
        
    Raises:
        Exception: If processing fails and message should be nacked
    """
    try:
        # Decode the message payload
        content = message.payload_as_text()
        
        logger.info(
            f"Processing message {message.id}: '{content}' "
            f"(priority={message.priority}, attempts={message.attempts})"
        )
        
        # Simulate processing time
        processing_time = random.uniform(0.5, 3.0)
        await asyncio.sleep(processing_time)
        
        # Simulate occasional failures for demonstration
        if random.random() < 0.1:  # 10% failure rate
            raise ValueError(f"Random processing failure for message {message.id}")
        
        # Check if processing took too long and lease might expire
        if message.is_lease_expiring_soon(threshold=timedelta(seconds=60)):
            logger.warning(f"Lease expiring soon for message {message.id}, extending...")
            await message.extend_lease(timedelta(minutes=2))
        
        logger.info(f"Successfully processed message {message.id}")
        return True
        
    except ValueError as e:
        # This is a business logic error - nack the message for retry
        logger.error(f"Business logic error processing message {message.id}: {e}")
        raise
    
    except Exception as e:
        # Unexpected error - log and re-raise
        logger.error(f"Unexpected error processing message {message.id}: {e}")
        raise


async def consume_messages(client: DisheapClient, topic: str, shutdown_event: asyncio.Event):
    """
    Main message consumption loop.
    
    Args:
        client: Disheap client instance
        topic: Topic to consume from
        shutdown_event: Event to signal shutdown
    """
    async with client.consumer() as consumer:
        logger.info(f"Starting to consume messages from topic '{topic}'")
        
        try:
            async for message in consumer.pop_stream(
                topic=topic,
                prefetch=5,  # Process up to 5 messages concurrently
                visibility_timeout=timedelta(minutes=5)
            ):
                # Check for shutdown signal
                if shutdown_event.is_set():
                    logger.info("Shutdown requested, stopping message consumption")
                    break
                
                # Process message with automatic ack/nack
                async with message:
                    # The context manager will automatically ack on success
                    # or nack on exception
                    success = await process_message(message)
                    
                    if not success:
                        # Manually nack with custom backoff
                        message.disable_auto_ack()
                        await message.nack(
                            backoff=timedelta(minutes=1),
                            reason="Processing returned False"
                        )
                
                # Small delay to demonstrate flow control
                await asyncio.sleep(0.1)
                
        except DisheapError as e:
            logger.error(f"Consumer error: {e}")
            raise


async def consume_with_custom_handling(client: DisheapClient, topic: str):
    """
    Example of custom message handling without auto-ack.
    
    This shows how to manually control ack/nack behavior.
    """
    logger.info("Demonstrating custom message handling...")
    
    async with client.consumer() as consumer:
        message_count = 0
        
        async for message in consumer.pop_stream(topic=topic, prefetch=3):
            message_count += 1
            
            # Disable auto-ack for manual control
            message.disable_auto_ack()
            
            try:
                content = message.payload_as_text()
                logger.info(f"Custom handling message: '{content}'")
                
                # Simulate different handling based on content
                if "critical" in content.lower():
                    # Process critical messages immediately
                    logger.info(f"Processing critical message {message.id}")
                    await asyncio.sleep(0.5)
                    await message.ack()
                    
                elif "delay" in content.lower():
                    # Delay non-critical messages
                    logger.info(f"Delaying message {message.id}")
                    await message.nack(
                        backoff=timedelta(minutes=5),
                        reason="Delayed processing requested"
                    )
                    
                else:
                    # Normal processing
                    await asyncio.sleep(1.0)
                    await message.ack()
                
            except Exception as e:
                logger.error(f"Error in custom handling: {e}")
                await message.nack(reason=f"Custom handling error: {e}")
            
            # Process only a few messages for demo
            if message_count >= 3:
                break


async def main():
    """Main example function."""
    
    # Set up graceful shutdown
    shutdown_handler = GracefulShutdown()
    
    # Create client
    client = DisheapClient(
        endpoints=["localhost:9090"],
        api_key="dh_example_key_secret123"
    )
    
    topic = "work-queue"
    
    async with client:
        logger.info("Connected to Disheap engine")
        
        # Start consuming messages
        try:
            # Run two consumption patterns concurrently
            consumption_task = asyncio.create_task(
                consume_messages(client, topic, shutdown_handler.shutdown_event)
            )
            
            # Wait for shutdown signal or task completion
            done, pending = await asyncio.wait(
                [
                    consumption_task,
                    asyncio.create_task(shutdown_handler.shutdown_event.wait())
                ],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # Cancel pending tasks
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            
            # Check if consumption task completed with an error
            for task in done:
                if task == consumption_task:
                    try:
                        await task
                    except Exception as e:
                        logger.error(f"Consumption task failed: {e}")
                        raise
            
            logger.info("Message consumption completed")
            
            # Demonstrate custom handling
            await consume_with_custom_handling(client, topic)
            
        except DisheapError as e:
            logger.error(f"Consumer failed: {e}")
            raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Example failed: {e}")
        raise
