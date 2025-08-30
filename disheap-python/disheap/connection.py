"""
Connection management for the Disheap Python SDK.

Handles gRPC channel creation, connection pooling, retry logic,
and load balancing across multiple engine endpoints.
"""

import asyncio
import logging
import random
import time
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta

from .config import ClientConfig
from .exceptions import DisheapConnectionError, grpc_error_to_disheap_error


logger = logging.getLogger(__name__)


class ConnectionPool:
    """
    Manages a pool of gRPC connections to a single endpoint.
    
    Provides connection reuse, health monitoring, and automatic
    connection replacement when connections become unhealthy.
    """
    
    def __init__(self, endpoint: str, config: ClientConfig):
        self.endpoint = endpoint
        self.config = config
        self.connections: List["Connection"] = []
        self._lock = asyncio.Lock()
        self._closed = False
    
    async def get_connection(self) -> "Connection":
        """
        Get a healthy connection from the pool.
        
        Returns:
            A healthy connection instance
            
        Raises:
            DisheapConnectionError: If no healthy connection can be obtained
        """
        async with self._lock:
            if self._closed:
                raise DisheapConnectionError("Connection pool is closed")
            
            # Try to find a healthy existing connection
            for conn in self.connections:
                if await conn.is_healthy():
                    return conn
            
            # Create a new connection if pool is not full
            if len(self.connections) < self.config.max_connections_per_endpoint:
                conn = Connection(self.endpoint, self.config)
                await conn.connect()
                self.connections.append(conn)
                return conn
            
            # Pool is full, try to replace an unhealthy connection
            for i, conn in enumerate(self.connections):
                if not await conn.is_healthy():
                    await conn.close()
                    new_conn = Connection(self.endpoint, self.config)
                    await new_conn.connect()
                    self.connections[i] = new_conn
                    return new_conn
            
            # All connections are healthy, return a random one
            return random.choice(self.connections)
    
    async def close(self) -> None:
        """Close all connections in the pool."""
        async with self._lock:
            self._closed = True
            for conn in self.connections:
                await conn.close()
            self.connections.clear()
    
    async def is_healthy(self) -> bool:
        """Check if the pool has at least one healthy connection."""
        async with self._lock:
            for conn in self.connections:
                if await conn.is_healthy():
                    return True
            return False


class Connection:
    """
    Represents a single gRPC connection to a Disheap engine endpoint.
    
    Handles the actual gRPC channel and stub creation, plus basic
    health monitoring and connection lifecycle management.
    """
    
    def __init__(self, endpoint: str, config: ClientConfig):
        self.endpoint = endpoint
        self.config = config
        self.channel = None
        self.stub = None
        self.connected = False
        self.last_used = datetime.now()
        self.connect_time: Optional[datetime] = None
    
    async def connect(self) -> None:
        """
        Establish the gRPC connection.
        
        Raises:
            DisheapConnectionError: If connection fails
        """
        try:
            # This is a simplified stub - in production you'd use real gRPC
            logger.info(f"Connecting to {self.endpoint}")
            
            # Simulate connection delay
            await asyncio.sleep(0.01)
            
            # In real implementation:
            # import grpc
            # from ._generated.disheap_pb2_grpc import DisheapStub
            # 
            # Create real gRPC channel
            import grpc
            from ._generated.disheap_pb2_grpc import DisheapStub
            
            # Create gRPC channel with appropriate options
            options = [
                ('grpc.keepalive_time_ms', 30000),
                ('grpc.keepalive_timeout_ms', 5000),
                ('grpc.keepalive_permit_without_calls', True),
                ('grpc.http2.max_pings_without_data', 0),
                ('grpc.http2.min_time_between_pings_ms', 10000),
                ('grpc.http2.min_ping_interval_without_data_ms', 300000),
            ]
            
            # For now, use insecure channel (TLS can be added later)
            self.channel = grpc.aio.insecure_channel(self.endpoint, options=options)
            self.stub = DisheapStub(self.channel)
            
            self.connected = True
            self.connect_time = datetime.now()
            
            logger.info(f"Connected to {self.endpoint}")
            
        except Exception as e:
            logger.error(f"Failed to connect to {self.endpoint}: {e}")
            raise DisheapConnectionError(f"Connection to {self.endpoint} failed: {e}") from e
    
    async def close(self) -> None:
        """Close the gRPC connection."""
        if self.channel is not None:
            try:
                await self.channel.close()
            except Exception as e:
                logger.warning(f"Error closing connection to {self.endpoint}: {e}")
        
        self.connected = False
        self.channel = None
        self.stub = None
    
    async def is_healthy(self) -> bool:
        """
        Check if the connection is healthy.
        
        Returns:
            True if connection is healthy, False otherwise
        """
        if not self.connected or self.channel is None:
            return False
        
        # Check if connection is too old
        if (self.connect_time and 
            datetime.now() - self.connect_time > self.config.connection_pool_max_age):
            return False
        
        # In real implementation, you might check channel state
        # return self.channel.get_state() == grpc.ChannelConnectivity.READY
        
        return True
    
    async def call(self, method_name: str, request: Any) -> Any:
        """
        Make a gRPC call through this connection.
        
        Args:
            method_name: The gRPC method name
            request: The request object
            
        Returns:
            The response object
            
        Raises:
            DisheapConnectionError: If call fails
        """
        if not self.connected or self.stub is None:
            raise DisheapConnectionError("Connection is not established")
        
        try:
            method = getattr(self.stub, method_name)
            response = await method(request)
            self.last_used = datetime.now()
            return response
            
        except Exception as e:
            logger.error(f"gRPC call {method_name} failed on {self.endpoint}: {e}")
            raise grpc_error_to_disheap_error(e) from e


class ConnectionManager:
    """
    Manages connections to multiple Disheap engine endpoints.
    
    Provides load balancing, failover, and retry logic across
    multiple engine nodes.
    """
    
    def __init__(self, config: ClientConfig):
        self.config = config
        self.pools: Dict[str, ConnectionPool] = {}
        self._closed = False
    
    async def connect(self) -> None:
        """
        Initialize connection pools for all endpoints.
        
        Raises:
            DisheapConnectionError: If no endpoints can be reached
        """
        if self._closed:
            raise DisheapConnectionError("Connection manager is closed")
        
        # Create connection pools for all endpoints
        for endpoint in self.config.endpoints:
            pool = ConnectionPool(endpoint, self.config)
            self.pools[endpoint] = pool
        
        # Test connectivity to at least one endpoint
        healthy_endpoints = []
        for endpoint in self.config.endpoints:
            try:
                pool = self.pools[endpoint]
                conn = await pool.get_connection()
                healthy_endpoints.append(endpoint)
                logger.info(f"Successfully connected to {endpoint}")
            except Exception as e:
                logger.warning(f"Failed to connect to {endpoint}: {e}")
        
        if not healthy_endpoints:
            raise DisheapConnectionError("No healthy endpoints available")
        
        logger.info(f"Connected to {len(healthy_endpoints)}/{len(self.config.endpoints)} endpoints")
    
    async def close(self) -> None:
        """Close all connection pools."""
        self._closed = True
        for pool in self.pools.values():
            await pool.close()
        self.pools.clear()
    
    async def is_healthy(self) -> bool:
        """Check if at least one endpoint is healthy."""
        for pool in self.pools.values():
            if await pool.is_healthy():
                return True
        return False
    
    async def call(self, method_name: str, request: Any) -> Any:
        """
        Make a gRPC call with automatic retry and failover.
        
        Args:
            method_name: The gRPC method name
            request: The request object
            
        Returns:
            The response object
            
        Raises:
            DisheapConnectionError: If all retry attempts fail
        """
        if self._closed:
            raise DisheapConnectionError("Connection manager is closed")
        
        last_error = None
        
        for attempt in range(self.config.retry.max_attempts):
            # Try each healthy endpoint
            for endpoint in self._get_endpoint_order():
                try:
                    pool = self.pools[endpoint]
                    if not await pool.is_healthy():
                        continue
                    
                    conn = await pool.get_connection()
                    response = await conn.call(method_name, request)
                    
                    if attempt > 0:
                        logger.info(f"gRPC call {method_name} succeeded on attempt {attempt + 1}")
                    
                    return response
                    
                except Exception as e:
                    last_error = e
                    logger.warning(
                        f"gRPC call {method_name} failed on {endpoint} "
                        f"(attempt {attempt + 1}): {e}"
                    )
                    continue
            
            # If we get here, all endpoints failed for this attempt
            if attempt < self.config.retry.max_attempts - 1:
                # Calculate backoff delay
                delay = min(
                    self.config.retry.initial_backoff.total_seconds() * 
                    (self.config.retry.backoff_multiplier ** attempt),
                    self.config.retry.max_backoff.total_seconds()
                )
                
                if self.config.retry.jitter:
                    delay *= (0.5 + random.random() * 0.5)  # Add jitter
                
                logger.info(f"Retrying {method_name} in {delay:.2f}s...")
                await asyncio.sleep(delay)
        
        # All attempts failed
        error_msg = f"gRPC call {method_name} failed after {self.config.retry.max_attempts} attempts"
        if last_error:
            raise DisheapConnectionError(error_msg) from last_error
        else:
            raise DisheapConnectionError(error_msg)
    
    def _get_endpoint_order(self) -> List[str]:
        """
        Get endpoints in order of preference.
        
        Returns a randomized list of endpoints to provide load balancing.
        """
        endpoints = list(self.pools.keys())
        random.shuffle(endpoints)
        return endpoints


# Mock classes for development (would be replaced by real gRPC in production)

class MockChannel:
    """Mock gRPC channel for development."""
    
    def __init__(self, endpoint: str):
        self.endpoint = endpoint
    
    async def close(self):
        """Mock close method."""
        pass


class MockStub:
    """Mock gRPC stub for development."""
    
    def __init__(self, channel: MockChannel):
        self.channel = channel
    
    async def MakeHeap(self, request):
        """Mock MakeHeap call."""
        from ._generated.disheap_pb2 import MakeHeapResp
        return MakeHeapResp(topic=request.topic, heap_id=f"heap_{request.topic}")
    
    async def DeleteHeap(self, request):
        """Mock DeleteHeap call.""" 
        from ._generated.disheap_pb2 import Empty
        return Empty()
    
    async def Stats(self, request):
        """Mock Stats call."""
        from ._generated.disheap_pb2 import StatsResp, HeapStats
        return StatsResp(
            global_stats=HeapStats(total_messages=100, inflight_messages=10),
            topic_stats={}
        )
    
    async def Enqueue(self, request):
        """Mock Enqueue call."""
        from ._generated.disheap_pb2 import EnqueueResp
        return EnqueueResp(
            message_id=f"msg_{int(time.time() * 1000000)}",
            duplicate=False,
            partition_id=0
        )
    
    async def Ack(self, request):
        """Mock Ack call."""
        from ._generated.disheap_pb2 import Empty
        return Empty()
    
    async def Nack(self, request):
        """Mock Nack call."""
        from ._generated.disheap_pb2 import Empty  
        return Empty()
    
    async def Extend(self, request):
        """Mock Extend call."""
        from ._generated.disheap_pb2 import Empty
        return Empty()
    
    async def EnqueueBatch(self, request):
        """Mock EnqueueBatch call."""
        from ._generated.disheap_pb2 import EnqueueBatchResp, EnqueueResp
        import time
        
        # Create mock responses for each request in the batch
        results = []
        for i, req in enumerate(request.requests):
            results.append(EnqueueResp(
                message_id=f"msg_{int(time.time() * 1000000)}_{i}",
                duplicate=False,
                partition_id=i % 4  # Distribute across 4 partitions
            ))
        
        return EnqueueBatchResp(
            transaction_id=f"txn_{int(time.time() * 1000000)}",
            results=results
        )
