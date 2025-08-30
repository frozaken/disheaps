"""
Consumer implementation for the Disheap Python SDK.

Provides streaming message consumption with credit-based flow control,
automatic lease renewal, and concurrent message processing.
"""

import asyncio
import logging
from asyncio import Queue
from datetime import datetime, timedelta
from typing import AsyncIterator, Optional, Dict, Set, TYPE_CHECKING

from .exceptions import DisheapError, DisheapValidationError, DisheapLeaseError
from .message import Message

if TYPE_CHECKING:
    from .client import DisheapClient

logger = logging.getLogger(__name__)


class FlowController:
    """
    Manages credit-based flow control for streaming consumption.
    
    Controls how many messages the consumer is willing to receive
    in-flight and automatically requests more credits when the
    watermark falls below the threshold.
    """
    
    def __init__(self, prefetch: int, low_watermark: float = 0.3):
        self.prefetch = prefetch
        self.low_watermark = low_watermark
        self.available_credits = 0
        self.in_flight = 0
        self._lock = asyncio.Lock()
    
    async def should_request_credits(self) -> bool:
        """Check if we need to request more credits."""
        async with self._lock:
            return self.available_credits <= (self.prefetch * self.low_watermark)
    
    async def credits_to_request(self) -> int:
        """Calculate how many credits to request."""
        async with self._lock:
            return self.prefetch - self.available_credits - self.in_flight
    
    async def consume_credit(self) -> None:
        """Consume a credit when receiving a message."""
        async with self._lock:
            if self.available_credits > 0:
                self.available_credits -= 1
                self.in_flight += 1
    
    async def release_credit(self) -> None:
        """Release a credit when processing completes."""
        async with self._lock:
            if self.in_flight > 0:
                self.in_flight -= 1
    
    async def add_credits(self, credits: int) -> None:
        """Add credits to the available pool."""
        async with self._lock:
            self.available_credits += credits


class LeaseManager:
    """
    Manages automatic lease renewal for active messages.
    
    Runs a background task that monitors active leases and renews
    them before they expire to prevent message redelivery.
    """
    
    def __init__(self, client: "DisheapClient"):
        self._client = client
        self._active_leases: Dict[str, Message] = {}  # lease_token -> message
        self._renewal_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()
    
    async def start(self) -> None:
        """Start the lease renewal background task."""
        if self._renewal_task is not None:
            return
        
        self._shutdown_event.clear()
        self._renewal_task = asyncio.create_task(self._renewal_loop())
        logger.debug("Started lease manager")
    
    async def stop(self) -> None:
        """Stop the lease renewal background task."""
        if self._renewal_task is None:
            return
        
        self._shutdown_event.set()
        
        try:
            await asyncio.wait_for(self._renewal_task, timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("Lease manager renewal task didn't stop gracefully")
            self._renewal_task.cancel()
        
        self._renewal_task = None
        logger.debug("Stopped lease manager")
    
    async def add_message(self, message: Message) -> None:
        """Add a message to lease management."""
        async with self._lock:
            self._active_leases[message.lease_token] = message
        
        logger.debug(f"Added message {message.id} to lease management")
    
    async def remove_message(self, lease_token: str) -> None:
        """Remove a message from lease management."""
        async with self._lock:
            self._active_leases.pop(lease_token, None)
    
    async def _renewal_loop(self) -> None:
        """Background loop that renews expiring leases."""
        while not self._shutdown_event.is_set():
            try:
                await self._renew_expiring_leases()
            except Exception as e:
                logger.error(f"Error in lease renewal loop: {e}")
            
            try:
                # Check every 15 seconds
                await asyncio.wait_for(
                    self._shutdown_event.wait(), 
                    timeout=15.0
                )
                break
            except asyncio.TimeoutError:
                continue
    
    async def _renew_expiring_leases(self) -> None:
        """Renew leases that are close to expiring."""
        now = datetime.now()
        renewal_threshold = self._client.config.lease.renewal_threshold
        
        async with self._lock:
            messages_to_renew = []
            
            for message in self._active_leases.values():
                if message.is_processed:
                    # Message was processed, remove from management
                    continue
                
                time_left = message.lease_deadline - now
                if time_left <= renewal_threshold:
                    messages_to_renew.append(message)
        
        # Renew leases outside the lock to avoid blocking
        for message in messages_to_renew:
            try:
                extension = self._client.config.lease.renewal_interval
                await message.extend_lease(extension)
                
                logger.debug(
                    f"Renewed lease for message {message.id}",
                    extra={
                        "message_id": message.id,
                        "topic": message.topic,
                        "extension_seconds": extension.total_seconds(),
                    }
                )
                
            except Exception as e:
                logger.warning(f"Failed to renew lease for message {message.id}: {e}")
                
                # Remove from management if renewal fails
                await self.remove_message(message.lease_token)


class DisheapConsumer:
    """
    High-level consumer for streaming messages from Disheap topics.
    
    Provides streaming message consumption with automatic flow control,
    lease management, and concurrent processing support.
    
    Example:
        >>> async with client.consumer() as consumer:
        ...     async for message in consumer.pop_stream(
        ...         topic="work-queue",
        ...         prefetch=10,
        ...         visibility_timeout=timedelta(minutes=5)
        ...     ):
        ...         async with message:
        ...             # Process message
        ...             result = await process_message(message.payload)
        ...             print(f"Processed: {result}")
        ...             # Message automatically acked on success
    """
    
    def __init__(self, client: "DisheapClient"):
        """
        Initialize the consumer.
        
        Args:
            client: Disheap client instance
        """
        self._client = client
        self._closed = False
        self._active_streams: Set[asyncio.Task] = set()
        self._lease_manager = LeaseManager(client)
        
        logger.info("Created consumer")
    
    async def pop_stream(
        self,
        topic: str,
        prefetch: int = 10,
        visibility_timeout: Optional[timedelta] = None
    ) -> AsyncIterator[Message]:
        """
        Stream messages from a topic with flow control.
        
        Args:
            topic: Topic to consume from
            prefetch: Number of messages to prefetch (flow control)
            visibility_timeout: Message visibility timeout (uses config default if None)
            
        Yields:
            Message instances with automatic lease management
            
        Raises:
            DisheapValidationError: If parameters are invalid
            DisheapError: If streaming fails
        """
        if self._closed:
            raise DisheapError("Consumer has been closed")
        
        # Validate parameters
        if not topic:
            raise DisheapValidationError("Topic name cannot be empty")
        
        if prefetch < 1:
            raise DisheapValidationError("Prefetch must be at least 1")
        
        if prefetch > self._client.config.flow_control.max_prefetch:
            raise DisheapValidationError(
                f"Prefetch {prefetch} exceeds maximum {self._client.config.flow_control.max_prefetch}"
            )
        
        # Use configured visibility timeout if not provided
        if visibility_timeout is None:
            visibility_timeout = self._client.config.lease.default_visibility_timeout
        
        logger.info(
            f"Starting to consume from topic {topic}",
            extra={
                "topic": topic,
                "prefetch": prefetch,
                "visibility_timeout_seconds": visibility_timeout.total_seconds(),
            }
        )
        
        # Start lease manager if not already running
        await self._lease_manager.start()
        
        # Create flow controller
        flow_controller = FlowController(
            prefetch=prefetch,
            low_watermark=self._client.config.flow_control.low_watermark
        )
        
        try:
            async for message in self._stream_messages(topic, visibility_timeout, flow_controller):
                # Add message to lease management
                await self._lease_manager.add_message(message)
                
                # Set up message cleanup on completion
                original_aexit = message.__aexit__
                
                async def cleanup_message(exc_type, exc_val, exc_tb):
                    # Call original cleanup first
                    result = await original_aexit(exc_type, exc_val, exc_tb)
                    
                    # Remove from lease management and flow control
                    await self._lease_manager.remove_message(message.lease_token)
                    await flow_controller.release_credit()
                    
                    return result
                
                # Replace the message's __aexit__ with our cleanup version
                message.__aexit__ = cleanup_message
                
                yield message
                
        except Exception as e:
            logger.error(f"Error streaming from topic {topic}: {e}")
            if isinstance(e, DisheapError):
                raise
            raise DisheapError(f"Streaming failed: {e}") from e
    
    async def _stream_messages(
        self,
        topic: str,
        visibility_timeout: timedelta,
        flow_controller: FlowController
    ) -> AsyncIterator[Message]:
        """
        Internal method to handle the actual message streaming.
        
        This method manages the gRPC streaming connection and handles
        flow control messages.
        """
        connection_manager = await self._client._ensure_connected()
        
        # Create the streaming request queue
        request_queue: Queue = Queue()
        
        # Send initial flow control request
        from ._generated.disheap_pb2 import PopOpen, FlowControl, PopFilter
        from google.protobuf.duration_pb2 import Duration
        
        # Send filter to specify topic and visibility timeout
        filter_msg = PopOpen(
            filter=PopFilter(
                topic=topic,
                visibility_timeout=Duration(seconds=int(visibility_timeout.total_seconds()), nanos=0)
            )
        )
        await request_queue.put(filter_msg)
        
        # Send initial credits
        initial_credits = flow_controller.prefetch
        flow_control_msg = PopOpen(
            flow_control=FlowControl(credits=initial_credits)
        )
        await request_queue.put(flow_control_msg)
        await flow_controller.add_credits(initial_credits)
        
        # Start background task to manage flow control
        flow_control_task = asyncio.create_task(
            self._manage_flow_control(request_queue, flow_controller)
        )
        
        try:
            # Use real gRPC PopStream
            async for item in self._real_pop_stream(request_queue, connection_manager):
                await flow_controller.consume_credit()
                message = self._pop_item_to_message(item)
                yield message
                
        finally:
            flow_control_task.cancel()
            try:
                await flow_control_task
            except asyncio.CancelledError:
                pass
    
    async def _manage_flow_control(
        self, 
        request_queue: Queue,
        flow_controller: FlowController
    ) -> None:
        """
        Background task to manage flow control credit requests.
        """
        while True:
            try:
                # Check if we need more credits
                if await flow_controller.should_request_credits():
                    credits_needed = await flow_controller.credits_to_request()
                    
                    if credits_needed > 0:
                        from ._generated.disheap_pb2 import PopOpen, FlowControl
                        
                        flow_control_msg = PopOpen(
                            flow_control=FlowControl(credits=credits_needed)
                        )
                        await request_queue.put(flow_control_msg)
                        await flow_controller.add_credits(credits_needed)
                        
                        logger.debug(f"Requested {credits_needed} more credits")
                
                # Check every 5 seconds
                await asyncio.sleep(5)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in flow control management: {e}")
                await asyncio.sleep(1)
    
    async def _real_pop_stream(self, request_queue: Queue, connection_manager) -> AsyncIterator:
        """
        Real gRPC PopStream implementation.
        
        This method handles the bidirectional streaming with the server.
        """
        # Get a connection from the first healthy endpoint
        for endpoint in connection_manager._get_endpoint_order():
            try:
                pool = connection_manager.pools[endpoint]
                if not await pool.is_healthy():
                    continue
                
                connection = await pool.get_connection()
                
                # Create an async generator from the request queue
                async def request_generator():
                    while True:
                        try:
                            request = await asyncio.wait_for(request_queue.get(), timeout=1.0)
                            yield request
                        except asyncio.TimeoutError:
                            # Check if we should continue streaming
                            continue
                        except Exception:
                            break
                
                # Call the gRPC PopStream method
                try:
                    async for response in connection.stub.PopStream(request_generator()):
                        yield response
                    return  # Success, exit the endpoint loop
                except Exception as e:
                    logger.error(f"PopStream gRPC error on {endpoint}: {e}")
                    continue  # Try next endpoint
                    
            except Exception as e:
                logger.error(f"Failed to get connection to {endpoint}: {e}")
                continue
        
        # If we get here, all endpoints failed
        raise Exception("PopStream failed on all endpoints")

    async def _mock_pop_stream(self, request_queue: Queue, topic: str) -> AsyncIterator:
        """
        Mock implementation of PopStream for development.
        
        In production, this would be replaced with real gRPC streaming.
        """
        # Simulate receiving messages
        message_counter = 0
        
        while True:
            # Simulate receiving a message every 2 seconds
            await asyncio.sleep(2)
            
            message_counter += 1
            
            # Create a mock PopItem
            from ._generated.disheap_pb2 import PopItem
            from google.protobuf.timestamp_pb2 import Timestamp
            
            now = datetime.now()
            item = PopItem(
                message_id=f"msg_{int(now.timestamp() * 1000000)}_{message_counter}",
                topic=topic,
                partition_id=0,
                payload=f"Mock message {message_counter} for {topic}".encode(),
                priority=100,
                enqueued_time=Timestamp(seconds=int(now.timestamp()), nanos=int((now.timestamp() % 1) * 1e9)),
                lease_token=f"lease_{message_counter}_{int(now.timestamp())}",
                lease_deadline=Timestamp(seconds=int((now + timedelta(minutes=5)).timestamp()), nanos=int(((now + timedelta(minutes=5)).timestamp() % 1) * 1e9)),
                attempts=1,
                max_retries=3,
            )
            
            yield item
            
            # Stop after 5 messages for demo purposes
            if message_counter >= 5:
                break
    
    def _pop_item_to_message(self, item) -> Message:
        """
        Convert a PopItem protobuf to a Message instance.
        
        Args:
            item: PopItem protobuf message
            
        Returns:
            Message instance with client reference
        """
        # Convert protobuf timestamps to datetime
        enqueued_at = datetime.fromtimestamp(item.enqueued_time.seconds)
        lease_deadline = datetime.fromtimestamp(item.lease_deadline.seconds)
        
        message = Message(
            id=item.message_id,
            topic=item.topic,
            partition_id=item.partition_id,
            payload=item.payload,
            priority=item.priority,
            enqueued_at=enqueued_at,
            attempts=item.attempts,
            max_retries=item.max_retries,
            lease_token=item.lease_token,
            lease_deadline=lease_deadline,
            producer_id=item.producer_id,
            epoch=item.epoch,
            sequence=item.sequence,
        )
        
        # Associate with client for ack/nack operations
        message._client = self._client
        
        return message
    
    async def close(self) -> None:
        """
        Close the consumer and clean up resources.
        
        This stops the lease manager and cancels any active streams.
        """
        self._closed = True
        
        # Stop lease manager
        await self._lease_manager.stop()
        
        # Cancel active streams
        for task in self._active_streams:
            task.cancel()
        
        if self._active_streams:
            await asyncio.gather(*self._active_streams, return_exceptions=True)
            self._active_streams.clear()
        
        logger.info("Closed consumer")
    
    # Context manager support
    async def __aenter__(self) -> "DisheapConsumer":
        """Enter async context manager."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit async context manager."""
        await self.close()
