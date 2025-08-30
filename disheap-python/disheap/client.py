"""
Main Disheap client for async Python SDK.

Provides the primary interface for interacting with the Disheap
distributed priority messaging system.
"""

import asyncio
import logging
import random
from typing import Optional, List, AsyncContextManager
from datetime import timedelta

from .config import ClientConfig, TLSConfig
from .connection import ConnectionManager
from .producer import DisheapProducer
from .consumer import DisheapConsumer
from .exceptions import DisheapError, DisheapConnectionError


logger = logging.getLogger(__name__)


class DisheapClient:
    """
    Main client for interacting with Disheap.
    
    This client manages connections to the Disheap engine and provides
    factory methods for creating producers and consumers.
    
    Example:
        >>> client = DisheapClient(
        ...     endpoints=["engine1:9090", "engine2:9090"],
        ...     api_key="dh_key123_secret456"
        ... )
        >>> 
        >>> # Use as context manager for automatic cleanup
        >>> async with client:
        ...     async with client.producer() as producer:
        ...         result = await producer.enqueue(
        ...             topic="work-queue",
        ...             payload=b"Hello, World!",
        ...             priority=100
        ...         )
        ...         print(f"Enqueued message: {result.message_id}")
        ...     
        ...     async with client.consumer() as consumer:
        ...         async for message in consumer.pop_stream("work-queue"):
        ...             async with message:
        ...                 print(f"Got message: {message.payload.decode()}")
    """
    
    def __init__(
        self, 
        endpoints: Optional[List[str]] = None,
        api_key: Optional[str] = None,
        config: Optional[ClientConfig] = None,
        **kwargs
    ):
        """
        Initialize the Disheap client.
        
        Args:
            endpoints: List of engine endpoints (e.g., ["host1:9090", "host2:9090"])
            api_key: API key for authentication (format: dh_<key_id>_<secret>)
            config: Complete client configuration (overrides other args)
            **kwargs: Additional config options (passed to ClientConfig)
            
        Raises:
            DisheapError: If configuration is invalid
        """
        # Build configuration
        if config is not None:
            self._config = config
        else:
            # Create config from individual parameters
            config_dict = kwargs.copy()
            if endpoints is not None:
                config_dict["endpoints"] = endpoints
            if api_key is not None:
                config_dict["api_key"] = api_key
                
            # Set defaults if not provided
            if "endpoints" not in config_dict:
                config_dict["endpoints"] = ["localhost:9090"]
            if "api_key" not in config_dict:
                raise DisheapError("API key is required")
                
            self._config = ClientConfig(**config_dict)
        
        # Initialize connection manager
        self._connection_manager: Optional[ConnectionManager] = None
        self._closed = False
        
        logger.info(
            "Initialized Disheap client",
            extra={
                "endpoints": self._config.endpoints,
                "tls_enabled": self._config.tls is not None and self._config.tls.enabled,
                "observability_enabled": self._config.observability.tracing_enabled,
            }
        )
    
    @property
    def config(self) -> ClientConfig:
        """Get the client configuration."""
        return self._config
    
    async def connect(self) -> None:
        """
        Establish connections to the Disheap engine.
        
        This method is called automatically when needed, but can be
        called explicitly to establish connections early.
        
        Raises:
            DisheapConnectionError: If connection fails
        """
        if self._connection_manager is not None:
            return  # Already connected
            
        try:
            self._connection_manager = ConnectionManager(self._config)
            await self._connection_manager.connect()
            logger.info("Connected to Disheap engine")
            
        except Exception as e:
            logger.error(f"Failed to connect to Disheap engine: {e}")
            raise DisheapConnectionError(f"Connection failed: {e}") from e
    
    async def disconnect(self) -> None:
        """
        Close all connections to the Disheap engine.
        
        After calling this method, the client cannot be used for
        new operations until connect() is called again.
        """
        if self._connection_manager is not None:
            await self._connection_manager.close()
            self._connection_manager = None
            
        self._closed = True
        logger.info("Disconnected from Disheap engine")
    
    async def is_healthy(self) -> bool:
        """
        Check if the client has healthy connections to the engine.
        
        Returns:
            True if client is connected and healthy, False otherwise
        """
        if self._connection_manager is None:
            return False
            
        return await self._connection_manager.is_healthy()
    
    def producer(self) -> DisheapProducer:
        """
        Create a new producer instance.
        
        Returns:
            A DisheapProducer for enqueuing messages
            
        Example:
            >>> async with client.producer() as producer:
            ...     result = await producer.enqueue(
            ...         topic="my-topic",
            ...         payload=b"message data",
            ...         priority=100
            ...     )
        """
        return DisheapProducer(self)
    
    def consumer(self) -> DisheapConsumer:
        """
        Create a new consumer instance.
        
        Returns:
            A DisheapConsumer for consuming messages
            
        Example:
            >>> async with client.consumer() as consumer:
            ...     async for message in consumer.pop_stream("my-topic"):
            ...         async with message:
            ...             process_message(message.payload)
        """
        return DisheapConsumer(self)
    
    async def _ensure_connected(self) -> ConnectionManager:
        """
        Ensure the client is connected and return the connection manager.
        
        Returns:
            The active connection manager
            
        Raises:
            DisheapConnectionError: If connection cannot be established
        """
        if self._closed:
            raise DisheapConnectionError("Client has been closed")
            
        if self._connection_manager is None:
            await self.connect()
            
        assert self._connection_manager is not None
        return self._connection_manager
    
    # Context manager support
    async def __aenter__(self) -> "DisheapClient":
        """Enter async context manager."""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit async context manager."""
        await self.disconnect()
    
    # Admin operations (convenience methods)
    
    async def create_topic(
        self,
        topic: str,
        *,
        mode: str = "min",
        partitions: int = 1,
        replication_factor: int = 2,
        top_k_bound: int = 100,
        retention_time: Optional[timedelta] = None,
        max_retries: int = 3,
        max_payload_bytes: int = 1024 * 1024,
    ) -> str:
        """
        Create a new topic/heap.
        
        Args:
            topic: Topic name
            mode: Heap mode ("min" or "max")
            partitions: Number of partitions
            replication_factor: Replication factor (2 or 3)
            top_k_bound: Top-K bound for bounded staleness
            retention_time: Message retention time
            max_retries: Default max retries for messages
            max_payload_bytes: Maximum payload size in bytes
            
        Returns:
            The created heap ID
            
        Raises:
            DisheapError: If creation fails
        """
        connection_manager = await self._ensure_connected()
        
        # Import here to avoid circular imports
        from ._generated.disheap_pb2 import MakeHeapReq, Mode, DLQPolicy
        from google.protobuf.duration_pb2 import Duration
        
        # Convert mode string to enum
        mode_enum = Mode.MIN if mode.lower() == "min" else Mode.MAX
        
        # Convert retention time to protobuf Duration
        retention_duration = None
        if retention_time is not None:
            retention_duration = Duration(seconds=int(retention_time.total_seconds()), nanos=0)
        
        # Default visibility timeout (5 minutes)
        visibility_timeout = Duration(seconds=300, nanos=0)
        
        request = MakeHeapReq(
            topic=topic,
            mode=mode_enum,
            partitions=partitions,
            replication_factor=replication_factor,
            top_k_bound=top_k_bound,
            retention_time=retention_duration,
            visibility_timeout_default=visibility_timeout,
            max_retries_default=max_retries,
            max_payload_bytes=max_payload_bytes,
            compression_enabled=False,
            dlq_policy=DLQPolicy(enabled=True),
        )
        
        response = await connection_manager.call("MakeHeap", request)
        return response.heap_id
    
    async def delete_topic(
        self, 
        topic: str, 
        *, 
        force: bool = False,
        confirmation_token: Optional[str] = None
    ) -> None:
        """
        Delete a topic/heap.
        
        Args:
            topic: Topic name to delete
            force: Force deletion without confirmation
            confirmation_token: Confirmation token for safe deletion
            
        Raises:
            DisheapError: If deletion fails
        """
        connection_manager = await self._ensure_connected()
        
        from ._generated.disheap_pb2 import DeleteHeapReq
        
        request = DeleteHeapReq(
            topic=topic,
            force=force,
            confirmation_token=confirmation_token or "",
        )
        
        await connection_manager.call("DeleteHeap", request)
    
    async def get_stats(self, topic: Optional[str] = None) -> dict:
        """
        Get statistics for topics.
        
        Args:
            topic: Specific topic to get stats for, or None for global stats
            
        Returns:
            Statistics dictionary
            
        Raises:
            DisheapError: If stats retrieval fails
        """
        connection_manager = await self._ensure_connected()
        
        from ._generated.disheap_pb2 import StatsReq
        
        request = StatsReq(topic=topic)
        response = await connection_manager.call("Stats", request)
        
        # Convert response to dictionary
        # This is simplified - in production you'd want better serialization
        return {
            "global_stats": {
                "total_messages": response.global_stats.total_messages if response.global_stats else 0,
                "inflight_messages": response.global_stats.inflight_messages if response.global_stats else 0,
            },
            "topic_stats": {
                name: {
                    "total_messages": stats.total_messages,
                    "inflight_messages": stats.inflight_messages,
                }
                for name, stats in response.topic_stats.items()
            } if response.topic_stats else {},
        }
