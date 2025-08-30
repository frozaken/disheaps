"""
Comprehensive mock Disheap engine for testing.

Provides realistic behavior for testing the Python SDK without
requiring a real Disheap engine instance.
"""

import asyncio
import logging
import time
import random
import sys
import os
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, AsyncIterator
from uuid import uuid4

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

logger = logging.getLogger(__name__)


@dataclass
class MockMessage:
    """Internal message representation in mock engine."""
    message_id: str
    topic: str
    partition_id: int
    payload: bytes
    priority: int
    enqueued_time: datetime
    producer_id: Optional[str] = None
    epoch: Optional[int] = None
    sequence: Optional[int] = None
    attempts: int = 0
    max_retries: int = 3
    not_before: Optional[datetime] = None
    
    # Lease tracking
    lease_token: Optional[str] = None
    lease_holder: Optional[str] = None
    lease_deadline: Optional[datetime] = None
    
    @property
    def is_leased(self) -> bool:
        """Check if message is currently leased."""
        if not self.lease_token or not self.lease_deadline:
            return False
        return datetime.now() < self.lease_deadline
    
    @property
    def is_ready(self) -> bool:
        """Check if message is ready for consumption."""
        if self.is_leased:
            return False
        if self.not_before and datetime.now() < self.not_before:
            return False
        return True


@dataclass
class MockTopic:
    """Mock topic configuration."""
    name: str
    mode: str = "max"  # "min" or "max"
    partitions: int = 1
    replication_factor: int = 2
    top_k_bound: int = 100
    retention_time: timedelta = field(default_factory=lambda: timedelta(days=7))
    max_retries_default: int = 3
    max_payload_bytes: int = 1024 * 1024
    visibility_timeout_default: timedelta = field(default_factory=lambda: timedelta(minutes=5))


class MockPartition:
    """Mock partition that maintains message ordering."""
    
    def __init__(self, partition_id: int, topic: MockTopic):
        self.partition_id = partition_id
        self.topic = topic
        self.messages: Dict[str, MockMessage] = {}  # message_id -> message
        self.ready_queue = deque()  # Ready messages in priority order
        self.delayed_queue = deque()  # Messages with not_before set
        self._lock = asyncio.Lock()
    
    async def enqueue_message(self, message: MockMessage) -> None:
        """Add a message to the partition."""
        async with self._lock:
            self.messages[message.message_id] = message
            
            if message.not_before and datetime.now() < message.not_before:
                # Add to delayed queue
                self.delayed_queue.append(message)
            else:
                # Add to ready queue in priority order
                self._insert_by_priority(message)
    
    def _insert_by_priority(self, message: MockMessage) -> None:
        """Insert message into ready queue maintaining priority order."""
        # For max heap (higher priority first)
        if self.topic.mode == "max":
            # Insert in descending priority order
            inserted = False
            for i, existing in enumerate(self.ready_queue):
                if message.priority > existing.priority:
                    self.ready_queue.insert(i, message)
                    inserted = True
                    break
            if not inserted:
                self.ready_queue.append(message)
        else:
            # Min heap (lower priority first)
            inserted = False
            for i, existing in enumerate(self.ready_queue):
                if message.priority < existing.priority:
                    self.ready_queue.insert(i, message)
                    inserted = True
                    break
            if not inserted:
                self.ready_queue.append(message)
    
    async def pop_message(self, consumer_id: str, visibility_timeout: timedelta) -> Optional[MockMessage]:
        """Pop the highest priority ready message."""
        async with self._lock:
            # Check delayed queue for messages that are now ready
            await self._process_delayed_messages()
            
            # Find first ready message
            for i, message in enumerate(self.ready_queue):
                if message.is_ready:
                    # Lease the message
                    message.lease_token = f"lease_{uuid4().hex[:16]}"
                    message.lease_holder = consumer_id
                    message.lease_deadline = datetime.now() + visibility_timeout
                    message.attempts += 1
                    
                    # Remove from ready queue
                    del self.ready_queue[i]
                    
                    logger.debug(f"Popped message {message.message_id} with lease {message.lease_token}")
                    return message
            
            return None
    
    async def _process_delayed_messages(self) -> None:
        """Move delayed messages to ready queue when their time comes."""
        now = datetime.now()
        ready_messages = []
        
        # Check delayed queue
        remaining_delayed = deque()
        while self.delayed_queue:
            message = self.delayed_queue.popleft()
            if message.not_before and now >= message.not_before:
                ready_messages.append(message)
            else:
                remaining_delayed.append(message)
        
        self.delayed_queue = remaining_delayed
        
        # Add ready messages to ready queue
        for message in ready_messages:
            self._insert_by_priority(message)
    
    async def ack_message(self, message_id: str, lease_token: str) -> bool:
        """Acknowledge a message."""
        async with self._lock:
            message = self.messages.get(message_id)
            if not message:
                return False
            
            if message.lease_token != lease_token:
                return False
            
            # Remove message completely
            del self.messages[message_id]
            logger.debug(f"Acked message {message_id}")
            return True
    
    async def nack_message(self, message_id: str, lease_token: str, backoff: Optional[timedelta] = None) -> bool:
        """Negative acknowledge a message."""
        async with self._lock:
            message = self.messages.get(message_id)
            if not message:
                return False
            
            if message.lease_token != lease_token:
                return False
            
            # Clear lease
            message.lease_token = None
            message.lease_holder = None
            message.lease_deadline = None
            
            # Check if message should go to DLQ
            if message.attempts >= message.max_retries:
                # Move to DLQ (for now, just delete)
                del self.messages[message_id]
                logger.debug(f"Message {message_id} moved to DLQ after {message.attempts} attempts")
                return True
            
            # Set backoff delay
            if backoff:
                message.not_before = datetime.now() + backoff
                self.delayed_queue.append(message)
            else:
                # Default exponential backoff
                backoff_seconds = min(300, 2 ** (message.attempts - 1))  # Max 5 minutes
                message.not_before = datetime.now() + timedelta(seconds=backoff_seconds)
                self.delayed_queue.append(message)
            
            logger.debug(f"Nacked message {message_id}, will retry after backoff")
            return True
    
    async def extend_lease(self, lease_token: str, extension: timedelta) -> bool:
        """Extend a message lease."""
        async with self._lock:
            for message in self.messages.values():
                if message.lease_token == lease_token:
                    if message.lease_deadline:
                        message.lease_deadline += extension
                        logger.debug(f"Extended lease {lease_token} by {extension.total_seconds()}s")
                        return True
            return False
    
    async def expire_leases(self) -> None:
        """Expire old leases and return messages to ready queue."""
        async with self._lock:
            now = datetime.now()
            expired_messages = []
            
            for message in self.messages.values():
                if (message.lease_token and message.lease_deadline and 
                    now >= message.lease_deadline):
                    expired_messages.append(message)
            
            for message in expired_messages:
                logger.debug(f"Lease expired for message {message.message_id}")
                message.lease_token = None
                message.lease_holder = None
                message.lease_deadline = None
                
                # Return to ready queue
                self._insert_by_priority(message)


class MockDisheapEngine:
    """
    Comprehensive mock Disheap engine for testing.
    
    Simulates realistic behavior including:
    - Message ordering by priority
    - Lease management with timeouts
    - Flow control and streaming
    - Idempotency and deduplication
    - Error scenarios and failures
    """
    
    def __init__(self, failure_rate: float = 0.0, network_delay: float = 0.0):
        """
        Initialize mock engine.
        
        Args:
            failure_rate: Probability of operation failures (0.0-1.0)
            network_delay: Simulated network delay in seconds
        """
        self.topics: Dict[str, MockTopic] = {}
        self.partitions: Dict[str, List[MockPartition]] = {}  # topic -> partitions
        self.dedup_cache: Dict[str, str] = {}  # (producer_id, epoch, seq) -> message_id
        self.failure_rate = failure_rate
        self.network_delay = network_delay
        self.active_streams: Set[str] = set()
        
        # Background task for lease expiration
        self._lease_cleanup_task: Optional[asyncio.Task] = None
        self._running = False
    
    async def start(self) -> None:
        """Start background tasks."""
        if self._running:
            return
        
        self._running = True
        self._lease_cleanup_task = asyncio.create_task(self._lease_cleanup_loop())
    
    async def stop(self) -> None:
        """Stop background tasks."""
        self._running = False
        if self._lease_cleanup_task:
            self._lease_cleanup_task.cancel()
            try:
                await self._lease_cleanup_task
            except asyncio.CancelledError:
                pass
    
    async def _lease_cleanup_loop(self) -> None:
        """Background loop to expire old leases."""
        while self._running:
            try:
                for partitions in self.partitions.values():
                    for partition in partitions:
                        await partition.expire_leases()
                
                await asyncio.sleep(5)  # Check every 5 seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in lease cleanup: {e}")
    
    async def _simulate_network_delay(self) -> None:
        """Simulate network delay."""
        if self.network_delay > 0:
            await asyncio.sleep(self.network_delay)
    
    async def _maybe_fail(self, operation: str) -> None:
        """Randomly fail operations based on failure rate."""
        if random.random() < self.failure_rate:
            from disheap.exceptions import DisheapConnectionError
            raise DisheapConnectionError(f"Simulated failure in {operation}")
    
    # Admin operations
    
    async def make_heap(self, request) -> object:
        """Create a new topic/heap."""
        await self._simulate_network_delay()
        await self._maybe_fail("make_heap")
        
        from disheap._generated.disheap_pb2 import MakeHeapResp
        
        topic = MockTopic(
            name=request.topic,
            mode="max" if request.mode == 1 else "min",
            partitions=request.partitions,
            replication_factor=request.replication_factor,
            top_k_bound=request.top_k_bound,
            max_retries_default=request.max_retries_default,
            max_payload_bytes=request.max_payload_bytes,
        )
        
        self.topics[request.topic] = topic
        
        # Create partitions
        partitions = []
        for i in range(request.partitions):
            partition = MockPartition(i, topic)
            partitions.append(partition)
        
        self.partitions[request.topic] = partitions
        
        return MakeHeapResp(
            topic=request.topic,
            heap_id=f"heap_{request.topic}_{uuid4().hex[:8]}"
        )
    
    async def delete_heap(self, request) -> object:
        """Delete a topic/heap."""
        await self._simulate_network_delay()
        await self._maybe_fail("delete_heap")
        
        from disheap._generated.disheap_pb2 import Empty
        
        if request.topic in self.topics:
            del self.topics[request.topic]
            del self.partitions[request.topic]
        
        return Empty()
    
    async def stats(self, request) -> object:
        """Get topic statistics."""
        await self._simulate_network_delay()
        await self._maybe_fail("stats")
        
        from disheap._generated.disheap_pb2 import StatsResp, HeapStats
        
        global_stats = HeapStats()
        topic_stats = {}
        
        for topic_name, partitions in self.partitions.items():
            total_messages = sum(len(p.messages) for p in partitions)
            inflight_messages = sum(
                len([m for m in p.messages.values() if m.is_leased]) 
                for p in partitions
            )
            
            topic_stats[topic_name] = HeapStats(
                total_messages=total_messages,
                inflight_messages=inflight_messages,
            )
            
            global_stats.total_messages += total_messages
            global_stats.inflight_messages += inflight_messages
        
        return StatsResp(
            global_stats=global_stats,
            topic_stats=topic_stats
        )
    
    # Data operations
    
    async def enqueue(self, request) -> object:
        """Enqueue a single message."""
        await self._simulate_network_delay()
        await self._maybe_fail("enqueue")
        
        from disheap._generated.disheap_pb2 import EnqueueResp
        
        # Check if topic exists
        if request.topic not in self.topics:
            from disheap.exceptions import DisheapValidationError
            raise DisheapValidationError(f"Topic '{request.topic}' does not exist")
        
        topic = self.topics[request.topic]
        partitions = self.partitions[request.topic]
        
        # Check payload size
        if len(request.payload) > topic.max_payload_bytes:
            from disheap.exceptions import DisheapValidationError
            raise DisheapValidationError(f"Payload size exceeds maximum {topic.max_payload_bytes} bytes")
        
        # Check for duplicate
        duplicate = False
        message_id = None
        
        if request.producer_id and request.epoch is not None and request.sequence is not None:
            dedup_key = f"{request.producer_id}:{request.epoch}:{request.sequence}"
            if dedup_key in self.dedup_cache:
                duplicate = True
                message_id = self.dedup_cache[dedup_key]
            else:
                message_id = f"msg_{int(time.time() * 1000000)}_{uuid4().hex[:8]}"
                self.dedup_cache[dedup_key] = message_id
        else:
            message_id = f"msg_{int(time.time() * 1000000)}_{uuid4().hex[:8]}"
        
        if not duplicate:
            # Select partition (hash partition key or round-robin)
            if request.partition_key:
                partition_id = hash(request.partition_key) % len(partitions)
            else:
                partition_id = random.randint(0, len(partitions) - 1)
            
            partition = partitions[partition_id]
            
            # Create message
            not_before = None
            if request.not_before:
                not_before = datetime.now() + timedelta(seconds=request.not_before.seconds)
            
            message = MockMessage(
                message_id=message_id,
                topic=request.topic,
                partition_id=partition_id,
                payload=request.payload,
                priority=request.priority,
                enqueued_time=datetime.now(),
                producer_id=request.producer_id,
                epoch=request.epoch,
                sequence=request.sequence,
                max_retries=request.max_retries or topic.max_retries_default,
                not_before=not_before,
            )
            
            await partition.enqueue_message(message)
            
            logger.debug(f"Enqueued message {message_id} to {request.topic}:{partition_id}")
        
        return EnqueueResp(
            message_id=message_id,
            duplicate=duplicate,
            partition_id=partition_id if not duplicate else 0
        )
    
    async def enqueue_batch(self, request) -> object:
        """Enqueue a batch of messages."""
        await self._simulate_network_delay()
        await self._maybe_fail("enqueue_batch")
        
        from disheap._generated.disheap_pb2 import EnqueueBatchResp, EnqueueResp
        
        results = []
        all_duplicates = True
        
        for req in request.requests:
            # Process each request individually
            result = await self.enqueue(req)
            results.append(result)
            
            if not result.duplicate:
                all_duplicates = False
        
        return EnqueueBatchResp(
            responses=results,
            transaction_id=request.transaction_id or f"batch_{int(time.time() * 1000000)}",
            all_duplicates=all_duplicates
        )
    
    async def pop_stream(self, request_stream) -> AsyncIterator:
        """Handle streaming message consumption."""
        from disheap._generated.disheap_pb2 import PopItem, Timestamp
        
        stream_id = f"stream_{uuid4().hex[:8]}"
        self.active_streams.add(stream_id)
        
        try:
            # Process stream setup
            topic = None
            visibility_timeout = timedelta(minutes=5)
            consumer_id = stream_id
            credits = 0
            
            async for request in request_stream:
                await self._simulate_network_delay()
                await self._maybe_fail("pop_stream")
                
                if request.filter:
                    topic = request.filter.topic
                    if request.filter.visibility_timeout:
                        visibility_timeout = timedelta(seconds=request.filter.visibility_timeout.seconds)
                
                if request.flow_control:
                    credits += request.flow_control.credits
                    
                    # Send messages up to credit limit
                    if topic and credits > 0:
                        partitions = self.partitions.get(topic, [])
                        messages_sent = 0
                        
                        for partition in partitions:
                            if credits <= 0:
                                break
                                
                            message = await partition.pop_message(consumer_id, visibility_timeout)
                            if message:
                                # Convert to PopItem
                                item = PopItem(
                                    message_id=message.message_id,
                                    topic=message.topic,
                                    partition_id=message.partition_id,
                                    payload=message.payload,
                                    priority=message.priority,
                                    enqueued_time=Timestamp.from_datetime(message.enqueued_time),
                                    lease_token=message.lease_token,
                                    lease_deadline=Timestamp.from_datetime(message.lease_deadline),
                                    attempts=message.attempts,
                                    max_retries=message.max_retries,
                                    producer_id=message.producer_id,
                                    epoch=message.epoch,
                                    sequence=message.sequence,
                                )
                                
                                credits -= 1
                                messages_sent += 1
                                
                                yield item
                                
                                # Small delay to simulate realistic streaming
                                await asyncio.sleep(0.01)
                        
                        if messages_sent == 0:
                            # No messages available, wait a bit
                            await asyncio.sleep(0.1)
        
        finally:
            self.active_streams.discard(stream_id)
    
    async def ack(self, request) -> object:
        """Acknowledge a message."""
        await self._simulate_network_delay()
        await self._maybe_fail("ack")
        
        from disheap._generated.disheap_pb2 import Empty
        
        partitions = self.partitions.get(request.topic, [])
        for partition in partitions:
            if await partition.ack_message(request.message_id, request.lease_token):
                return Empty()
        
        from disheap.exceptions import DisheapLeaseError
        raise DisheapLeaseError(f"Invalid lease token or message not found")
    
    async def nack(self, request) -> object:
        """Negative acknowledge a message."""
        await self._simulate_network_delay()
        await self._maybe_fail("nack")
        
        from disheap._generated.disheap_pb2 import Empty
        
        backoff = None
        if request.backoff_override:
            backoff = timedelta(seconds=request.backoff_override.seconds)
        
        partitions = self.partitions.get(request.topic, [])
        for partition in partitions:
            if await partition.nack_message(request.message_id, request.lease_token, backoff):
                return Empty()
        
        from disheap.exceptions import DisheapLeaseError
        raise DisheapLeaseError(f"Invalid lease token or message not found")
    
    async def extend(self, request) -> object:
        """Extend a message lease."""
        await self._simulate_network_delay()
        await self._maybe_fail("extend")
        
        from disheap._generated.disheap_pb2 import Empty
        
        extension = timedelta(seconds=request.extension.seconds)
        
        partitions = self.partitions.get(request.topic, [])
        for partition in partitions:
            if await partition.extend_lease(request.lease_token, extension):
                return Empty()
        
        from disheap.exceptions import DisheapLeaseError
        raise DisheapLeaseError(f"Invalid lease token")


# Global mock engine instance for tests
_mock_engine: Optional[MockDisheapEngine] = None


async def get_mock_engine(failure_rate: float = 0.0, network_delay: float = 0.0) -> MockDisheapEngine:
    """Get or create global mock engine instance."""
    global _mock_engine
    
    if _mock_engine is None:
        _mock_engine = MockDisheapEngine(failure_rate, network_delay)
        await _mock_engine.start()
    
    return _mock_engine


async def reset_mock_engine() -> None:
    """Reset the global mock engine."""
    global _mock_engine
    
    if _mock_engine:
        await _mock_engine.stop()
        _mock_engine = None
