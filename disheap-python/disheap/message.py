"""
Message wrapper for the Disheap Python SDK.

Provides a high-level interface for working with messages,
including automatic ack/nack through context managers and
lease management.
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, TYPE_CHECKING, Any

from .exceptions import DisheapError, DisheapLeaseError

if TYPE_CHECKING:
    from .client import DisheapClient

logger = logging.getLogger(__name__)


@dataclass
class Message:
    """
    Represents a message from a Disheap topic with operations.
    
    This class wraps a message received from the engine and provides
    convenient methods for acknowledgment, negative acknowledgment,
    and lease extension. It also supports context manager usage for
    automatic message handling.
    
    Attributes:
        id: Unique message identifier (ULID)
        topic: Topic name the message belongs to
        partition_id: Partition ID within the topic
        payload: Message payload as bytes
        priority: Message priority value
        enqueued_at: When the message was originally enqueued
        attempts: Number of delivery attempts for this message
        max_retries: Maximum retry attempts before moving to DLQ
        lease_token: Opaque lease token for this message
        lease_deadline: When the current lease expires
        producer_id: ID of the producer that created this message (optional)
        epoch: Producer epoch when message was created (optional)
        sequence: Sequence number within producer epoch (optional)
    """
    
    id: str
    topic: str
    partition_id: int
    payload: bytes
    priority: int
    enqueued_at: datetime
    attempts: int
    max_retries: int
    lease_token: str
    lease_deadline: datetime
    
    # Optional producer tracking
    producer_id: Optional[str] = None
    epoch: Optional[int] = None
    sequence: Optional[int] = None
    
    # Internal fields
    _client: Optional["DisheapClient"] = None
    _acked: bool = False
    _nacked: bool = False
    _auto_ack: bool = True
    
    @property
    def time_until_lease_expiry(self) -> timedelta:
        """
        Get the time remaining until the lease expires.
        
        Returns:
            Time delta until lease expiry
        """
        return self.lease_deadline - datetime.now()
    
    def is_lease_expiring_soon(self, threshold: timedelta = timedelta(seconds=30)) -> bool:
        """
        Check if the lease is expiring soon.
        
        Args:
            threshold: Time threshold to consider "soon"
            
        Returns:
            True if lease expires within the threshold
        """
        return self.time_until_lease_expiry <= threshold
    
    @property
    def is_processed(self) -> bool:
        """Check if the message has been acked or nacked."""
        return self._acked or self._nacked
    
    def payload_as_text(self, encoding: str = "utf-8") -> str:
        """
        Decode the payload as text.
        
        Args:
            encoding: Text encoding to use
            
        Returns:
            Decoded payload text
            
        Raises:
            UnicodeDecodeError: If payload cannot be decoded
        """
        return self.payload.decode(encoding)
    
    def payload_as_json(self) -> Any:
        """
        Decode the payload as JSON.
        
        Returns:
            Parsed JSON object
            
        Raises:
            ValueError: If payload is not valid JSON
        """
        import json
        return json.loads(self.payload.decode("utf-8"))
    
    async def ack(self) -> None:
        """
        Acknowledge the message, marking it as successfully processed.
        
        This removes the message from the topic permanently and prevents
        further redelivery.
        
        Raises:
            DisheapLeaseError: If message was already processed or lease expired
            DisheapError: If acknowledgment fails
        """
        if self._acked:
            logger.warning(f"Message {self.id} was already acknowledged")
            return
            
        if self._nacked:
            raise DisheapLeaseError("Cannot ack message that was already nacked")
        
        if self._client is None:
            raise DisheapError("Message is not associated with a client")
        
        try:
            # Get connection manager and make the call
            connection_manager = await self._client._ensure_connected()
            
            from ._generated.disheap_pb2 import AckReq
            request = AckReq(
                topic=self.topic,
                message_id=self.id,
                lease_token=self.lease_token
            )
            
            await connection_manager.call("Ack", request)
            self._acked = True
            
            logger.debug(
                f"Acknowledged message {self.id}",
                extra={
                    "message_id": self.id,
                    "topic": self.topic,
                    "attempts": self.attempts,
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to ack message {self.id}: {e}")
            raise DisheapError(f"Failed to acknowledge message: {e}") from e
    
    async def nack(
        self, 
        backoff: Optional[timedelta] = None,
        reason: Optional[str] = None
    ) -> None:
        """
        Negative acknowledge the message, indicating processing failed.
        
        This will cause the message to be redelivered after a backoff period,
        or moved to the DLQ if max retries have been reached.
        
        Args:
            backoff: Custom backoff duration (uses default if None)
            reason: Optional reason for the nack (for debugging)
            
        Raises:
            DisheapLeaseError: If message was already processed or lease expired
            DisheapError: If negative acknowledgment fails
        """
        if self._nacked:
            logger.warning(f"Message {self.id} was already nacked")
            return
            
        if self._acked:
            raise DisheapLeaseError("Cannot nack message that was already acked")
        
        if self._client is None:
            raise DisheapError("Message is not associated with a client")
        
        try:
            connection_manager = await self._client._ensure_connected()
            
            from ._generated.disheap_pb2 import NackReq
            from google.protobuf.duration_pb2 import Duration
            
            # Convert backoff to protobuf Duration if provided
            backoff_duration = None
            if backoff is not None:
                backoff_duration = Duration(seconds=int(backoff.total_seconds()), nanos=0)
            
            request = NackReq(
                topic=self.topic,
                message_id=self.id,
                lease_token=self.lease_token,
                backoff_override=backoff_duration,
                reason=reason
            )
            
            await connection_manager.call("Nack", request)
            self._nacked = True
            
            logger.debug(
                f"Nacked message {self.id}",
                extra={
                    "message_id": self.id,
                    "topic": self.topic,
                    "attempts": self.attempts,
                    "reason": reason,
                    "backoff_seconds": backoff.total_seconds() if backoff else None,
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to nack message {self.id}: {e}")
            raise DisheapError(f"Failed to nack message: {e}") from e
    
    async def extend_lease(self, extension: timedelta) -> None:
        """
        Extend the lease on this message.
        
        This gives more time to process the message before it's considered
        timed out and redelivered to another consumer.
        
        Args:
            extension: How much time to add to the lease
            
        Raises:
            DisheapLeaseError: If message was already processed or lease expired
            DisheapError: If lease extension fails
        """
        if self.is_processed:
            raise DisheapLeaseError("Cannot extend lease on processed message")
        
        if self._client is None:
            raise DisheapError("Message is not associated with a client")
        
        try:
            connection_manager = await self._client._ensure_connected()
            
            from ._generated.disheap_pb2 import ExtendReq
            from google.protobuf.duration_pb2 import Duration
            
            request = ExtendReq(
                topic=self.topic,
                lease_token=self.lease_token,
                extension=Duration(seconds=int(extension.total_seconds()), nanos=0)
            )
            
            await connection_manager.call("Extend", request)
            
            # Update local lease deadline
            self.lease_deadline += extension
            
            logger.debug(
                f"Extended lease for message {self.id} by {extension.total_seconds()}s",
                extra={
                    "message_id": self.id,
                    "topic": self.topic,
                    "new_deadline": self.lease_deadline.isoformat(),
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to extend lease for message {self.id}: {e}")
            raise DisheapError(f"Failed to extend lease: {e}") from e
    
    def disable_auto_ack(self) -> None:
        """
        Disable automatic acknowledgment when used as context manager.
        
        When disabled, you must manually call ack() or nack() on the message.
        """
        self._auto_ack = False
    
    def enable_auto_ack(self) -> None:
        """
        Enable automatic acknowledgment when used as context manager.
        
        This is the default behavior - the message will be automatically
        acked on successful context exit, or nacked on exception.
        """
        self._auto_ack = True
    
    # Context manager support
    async def __aenter__(self) -> "Message":
        """Enter async context manager."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Exit async context manager with automatic ack/nack.
        
        If an exception occurred during processing, the message will be nacked.
        Otherwise, it will be acked (unless auto-ack is disabled or the message
        was manually processed).
        """
        if not self._auto_ack or self.is_processed:
            return
        
        try:
            if exc_type is not None:
                # Exception occurred, nack the message
                reason = f"{exc_type.__name__}: {exc_val}" if exc_val else str(exc_type)
                await self.nack(reason=reason)
            else:
                # No exception, ack the message
                await self.ack()
        except Exception as e:
            # Log error but don't raise to avoid masking original exception
            logger.error(f"Error during automatic message handling: {e}")
    
    def __repr__(self) -> str:
        """String representation of the message."""
        return (
            f"Message(id={self.id!r}, topic={self.topic!r}, "
            f"priority={self.priority}, attempts={self.attempts}, "
            f"payload_size={len(self.payload)})"
        )
