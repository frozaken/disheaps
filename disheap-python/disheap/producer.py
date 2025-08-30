"""
Producer implementation for the Disheap Python SDK.

Provides high-level interface for enqueuing messages with idempotency
support, automatic sequencing, and batch operations.
"""

import asyncio
import logging
import time
import uuid
import socket
import os
from dataclasses import dataclass
from datetime import timedelta
from typing import List, Optional, Dict, Any, TYPE_CHECKING

from .exceptions import DisheapError, DisheapValidationError

if TYPE_CHECKING:
    from .client import DisheapClient

logger = logging.getLogger(__name__)


@dataclass
class EnqueueRequest:
    """
    Request to enqueue a single message.
    
    Attributes:
        topic: Target topic name
        payload: Message payload as bytes
        priority: Message priority (higher = more important)
        partition_key: Key for partition selection (optional)
        max_retries: Max retry attempts for this message (optional)
        delay: Delay before message becomes visible (optional)
    """
    topic: str
    payload: bytes
    priority: int
    partition_key: Optional[str] = None
    max_retries: Optional[int] = None
    delay: Optional[timedelta] = None


@dataclass
class EnqueueResult:
    """
    Result of enqueuing a single message.
    
    Attributes:
        message_id: Unique message identifier (ULID)
        duplicate: True if this was a duplicate message
        partition_id: Partition where message was stored
        topic: Topic where message was enqueued
    """
    message_id: str
    duplicate: bool
    partition_id: int
    topic: str


@dataclass
class EnqueueBatchResult:
    """
    Result of enqueuing a batch of messages.
    
    Attributes:
        results: Individual results for each message
        transaction_id: Batch transaction identifier
        all_duplicates: True if all messages were duplicates
        successful_count: Number of successfully enqueued messages
        duplicate_count: Number of duplicate messages
    """
    results: List[EnqueueResult]
    transaction_id: str
    all_duplicates: bool
    
    @property
    def successful_count(self) -> int:
        """Count of successfully enqueued messages."""
        return len(self.results)
    
    @property
    def duplicate_count(self) -> int:
        """Count of duplicate messages."""
        return sum(1 for r in self.results if r.duplicate)


class ProducerIdempotency:
    """
    Manages producer idempotency with automatic sequence generation.
    
    Each producer instance gets a unique producer ID and maintains
    sequence numbers per topic to ensure exactly-once delivery
    semantics at the producer level.
    """
    
    def __init__(self, producer_id: Optional[str] = None):
        self.producer_id = producer_id or self._generate_producer_id()
        self.epoch = int(time.time())
        self._sequences: Dict[str, int] = {}  # topic -> next sequence
        self._lock = asyncio.Lock()
    
    def _generate_producer_id(self) -> str:
        """
        Generate a unique producer ID.
        
        Format: hostname-pid-uuid
        This ensures uniqueness across processes and restarts.
        """
        hostname = socket.gethostname()
        pid = os.getpid()
        unique_id = str(uuid.uuid4())[:8]
        return f"{hostname}-{pid}-{unique_id}"
    
    async def next_sequence(self, topic: str) -> int:
        """
        Get the next sequence number for a topic.
        
        Args:
            topic: Topic name
            
        Returns:
            Next sequence number for the topic
        """
        async with self._lock:
            current = self._sequences.get(topic, 0)
            self._sequences[topic] = current + 1
            return self._sequences[topic]
    
    async def reset_sequences(self) -> None:
        """
        Reset all sequence numbers.
        
        This should be called when the producer is restarted to
        begin a new epoch.
        """
        async with self._lock:
            self._sequences.clear()
            self.epoch = int(time.time())


class DisheapProducer:
    """
    High-level producer for enqueuing messages to Disheap topics.
    
    Provides automatic idempotency management, sequence generation,
    and batch operations. Supports both individual message enqueuing
    and batch operations for higher throughput.
    
    Example:
        >>> async with client.producer() as producer:
        ...     # Single message
        ...     result = await producer.enqueue(
        ...         topic="work-queue",
        ...         payload=b"Hello, World!",
        ...         priority=100
        ...     )
        ...     print(f"Enqueued: {result.message_id}")
        ...     
        ...     # Batch messages
        ...     requests = [
        ...         EnqueueRequest("work-queue", b"Message 1", 50),
        ...         EnqueueRequest("work-queue", b"Message 2", 75),
        ...     ]
        ...     batch_result = await producer.enqueue_batch(requests)
        ...     print(f"Enqueued {batch_result.successful_count} messages")
    """
    
    def __init__(
        self, 
        client: "DisheapClient", 
        producer_id: Optional[str] = None,
        auto_sequence: bool = True
    ):
        """
        Initialize the producer.
        
        Args:
            client: Disheap client instance
            producer_id: Custom producer ID (auto-generated if None)
            auto_sequence: Enable automatic sequence generation
        """
        self._client = client
        self._auto_sequence = auto_sequence
        self._idempotency = ProducerIdempotency(producer_id) if auto_sequence else None
        self._closed = False
        
        logger.info(
            f"Created producer",
            extra={
                "producer_id": self.producer_id,
                "auto_sequence": auto_sequence,
            }
        )
    
    @property
    def producer_id(self) -> Optional[str]:
        """Get the producer ID."""
        return self._idempotency.producer_id if self._idempotency else None
    
    @property
    def epoch(self) -> Optional[int]:
        """Get the current producer epoch."""
        return self._idempotency.epoch if self._idempotency else None
    
    async def enqueue(
        self,
        topic: str,
        payload: bytes,
        priority: int,
        partition_key: Optional[str] = None,
        max_retries: Optional[int] = None,
        delay: Optional[timedelta] = None
    ) -> EnqueueResult:
        """
        Enqueue a single message.
        
        Args:
            topic: Target topic name
            payload: Message payload as bytes
            priority: Message priority (higher values = higher priority)
            partition_key: Key for partition selection (optional)
            max_retries: Max retry attempts (uses topic default if None)
            delay: Delay before message becomes visible (optional)
            
        Returns:
            Result containing message ID and metadata
            
        Raises:
            DisheapValidationError: If parameters are invalid
            DisheapError: If enqueue operation fails
        """
        if self._closed:
            raise DisheapError("Producer has been closed")
        
        # Validate inputs
        self._validate_enqueue_params(topic, payload, priority)
        
        try:
            connection_manager = await self._client._ensure_connected()
            
            # Build the protobuf request
            from ._generated.disheap_pb2 import EnqueueReq
            from google.protobuf.duration_pb2 import Duration
            
            # Get sequence number if idempotency is enabled
            sequence = None
            if self._idempotency:
                sequence = await self._idempotency.next_sequence(topic)
            
            # Convert delay to protobuf Duration
            not_before = None
            if delay is not None:
                not_before = Duration(seconds=int(delay.total_seconds()), nanos=0)
            
            request = EnqueueReq(
                topic=topic,
                payload=payload,
                priority=priority,
                partition_key=partition_key,
                producer_id=self.producer_id,
                epoch=self.epoch,
                sequence=sequence,
                max_retries=max_retries,
                not_before=not_before,
            )
            
            response = await connection_manager.call("Enqueue", request)
            
            result = EnqueueResult(
                message_id=response.message_id,
                duplicate=response.duplicate,
                partition_id=response.partition_id,
                topic=topic
            )
            
            logger.debug(
                f"Enqueued message",
                extra={
                    "message_id": result.message_id,
                    "topic": topic,
                    "priority": priority,
                    "partition_id": result.partition_id,
                    "duplicate": result.duplicate,
                    "producer_id": self.producer_id,
                    "sequence": sequence,
                }
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to enqueue message to {topic}: {e}")
            if isinstance(e, DisheapError):
                raise
            raise DisheapError(f"Enqueue failed: {e}") from e
    
    async def enqueue_batch(
        self, 
        requests: List[EnqueueRequest],
        transaction_id: Optional[str] = None
    ) -> EnqueueBatchResult:
        """
        Enqueue a batch of messages atomically.
        
        All messages in the batch will be enqueued together or none will be.
        This provides atomicity across partitions using 2PC.
        
        Args:
            requests: List of enqueue requests
            transaction_id: Custom transaction ID (auto-generated if None)
            
        Returns:
            Batch result with individual message results
            
        Raises:
            DisheapValidationError: If any request is invalid
            DisheapError: If batch enqueue fails
        """
        if self._closed:
            raise DisheapError("Producer has been closed")
        
        if not requests:
            raise DisheapValidationError("Batch cannot be empty")
        
        if len(requests) > self._client.config.batch_max_size:
            raise DisheapValidationError(
                f"Batch size {len(requests)} exceeds maximum {self._client.config.batch_max_size}"
            )
        
        # Validate all requests
        for i, req in enumerate(requests):
            try:
                self._validate_enqueue_params(req.topic, req.payload, req.priority)
            except DisheapValidationError as e:
                raise DisheapValidationError(f"Request {i}: {e}") from e
        
        try:
            connection_manager = await self._client._ensure_connected()
            
            # Build protobuf requests
            from ._generated.disheap_pb2 import EnqueueBatchReq, EnqueueReq
            from google.protobuf.duration_pb2 import Duration
            
            pb_requests = []
            for req in requests:
                # Get sequence number if idempotency is enabled
                sequence = None
                if self._idempotency:
                    sequence = await self._idempotency.next_sequence(req.topic)
                
                # Convert delay to protobuf Duration
                not_before = None
                if req.delay is not None:
                    not_before = Duration(seconds=int(req.delay.total_seconds()), nanos=0)
                
                pb_req = EnqueueReq(
                    topic=req.topic,
                    payload=req.payload,
                    priority=req.priority,
                    partition_key=req.partition_key,
                    producer_id=self.producer_id,
                    epoch=self.epoch,
                    sequence=sequence,
                    max_retries=req.max_retries,
                    not_before=not_before,
                )
                pb_requests.append(pb_req)
            
            # Generate transaction ID if not provided
            if transaction_id is None:
                transaction_id = f"batch_{int(time.time() * 1000000)}_{uuid.uuid4().hex[:8]}"
            
            batch_request = EnqueueBatchReq(
                requests=pb_requests,
                transaction_id=transaction_id
            )
            
            response = await connection_manager.call("EnqueueBatch", batch_request)
            
            # Convert response
            results = [
                EnqueueResult(
                    message_id=resp.message_id,
                    duplicate=resp.duplicate,
                    partition_id=resp.partition_id,
                    topic=requests[i].topic
                )
                for i, resp in enumerate(response.responses)
            ]
            
            batch_result = EnqueueBatchResult(
                results=results,
                transaction_id=response.transaction_id,
                all_duplicates=response.all_duplicates
            )
            
            logger.info(
                f"Enqueued batch",
                extra={
                    "transaction_id": batch_result.transaction_id,
                    "message_count": len(results),
                    "duplicate_count": batch_result.duplicate_count,
                    "producer_id": self.producer_id,
                }
            )
            
            return batch_result
            
        except Exception as e:
            logger.error(f"Failed to enqueue batch: {e}")
            if isinstance(e, DisheapError):
                raise
            raise DisheapError(f"Batch enqueue failed: {e}") from e
    
    def _validate_enqueue_params(self, topic: str, payload: bytes, priority: int) -> None:
        """
        Validate enqueue parameters.
        
        Args:
            topic: Topic name
            payload: Message payload
            priority: Message priority
            
        Raises:
            DisheapValidationError: If any parameter is invalid
        """
        if not topic:
            raise DisheapValidationError("Topic name cannot be empty")
        
        if self._client.config.validate_topic_names:
            # Topic name validation per interface contracts
            import re
            if not re.match(r"^[a-zA-Z0-9_-]+$", topic):
                raise DisheapValidationError(
                    "Topic name must contain only alphanumeric characters, hyphens, and underscores"
                )
            
            if len(topic) > 255:
                raise DisheapValidationError("Topic name cannot exceed 255 characters")
        
        if len(payload) > self._client.config.max_payload_size:
            raise DisheapValidationError(
                f"Payload size {len(payload)} exceeds maximum {self._client.config.max_payload_size} bytes"
            )
        
        # Priority is typically an int64, so check reasonable bounds
        if not (-2**63 <= priority <= 2**63 - 1):
            raise DisheapValidationError("Priority value out of valid range")
    
    async def close(self) -> None:
        """
        Close the producer and clean up resources.
        
        After calling this method, the producer cannot be used for
        further operations.
        """
        self._closed = True
        logger.info(f"Closed producer {self.producer_id}")
    
    # Context manager support
    async def __aenter__(self) -> "DisheapProducer":
        """Enter async context manager."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit async context manager."""
        await self.close()
