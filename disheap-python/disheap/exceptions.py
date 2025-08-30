"""
Exception hierarchy for the Disheap Python SDK.

Provides comprehensive error handling with specific exception types
for different categories of errors that can occur during SDK operations.
"""

from typing import Optional, Dict, Any


class DisheapError(Exception):
    """Base exception for all Disheap errors."""
    
    def __init__(
        self, 
        message: str, 
        error_code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.details = details or {}


class DisheapConnectionError(DisheapError):
    """Connection-related errors (network, gRPC connection issues)."""
    pass


class DisheapValidationError(DisheapError):
    """Request validation errors (invalid parameters, malformed data)."""
    pass


class DisheapLeaseError(DisheapError):
    """Lease-related errors (expired lease, invalid lease token)."""
    pass


class DisheapTimeoutError(DisheapError):
    """Operation timeout errors (request timeout, lease timeout)."""
    pass


class DisheapCapacityError(DisheapError):
    """Capacity/rate limit errors (queue full, rate limit exceeded)."""
    pass


class DisheapAuthenticationError(DisheapError):
    """Authentication errors (invalid API key, missing credentials)."""
    pass


class DisheapPermissionError(DisheapError):
    """Permission/authorization errors (access denied, insufficient privileges)."""
    pass


class DisheapNotFoundError(DisheapError):
    """Resource not found errors (topic not found, message not found)."""
    pass


class DisheapConflictError(DisheapError):
    """Conflict errors (duplicate resource, concurrent modification)."""
    pass


class DisheapUnavailableError(DisheapError):
    """Service unavailable errors (engine down, partition unavailable)."""
    pass


# gRPC status code to exception mapping
GRPC_ERROR_MAPPING = {
    "INVALID_ARGUMENT": DisheapValidationError,
    "UNAUTHENTICATED": DisheapAuthenticationError,
    "PERMISSION_DENIED": DisheapPermissionError,
    "NOT_FOUND": DisheapNotFoundError,
    "ALREADY_EXISTS": DisheapConflictError,
    "FAILED_PRECONDITION": DisheapLeaseError,
    "ABORTED": DisheapConflictError,
    "UNAVAILABLE": DisheapUnavailableError,
    "DEADLINE_EXCEEDED": DisheapTimeoutError,
    "RESOURCE_EXHAUSTED": DisheapCapacityError,
    "INTERNAL": DisheapError,
}


def grpc_error_to_disheap_error(grpc_error: Exception) -> DisheapError:
    """
    Convert a gRPC error to a Disheap exception.
    
    Args:
        grpc_error: The gRPC error to convert
        
    Returns:
        A Disheap exception instance
    """
    try:
        import grpc
        from grpc import StatusCode
        
        if isinstance(grpc_error, grpc.RpcError):
            status_code = grpc_error.code()
            message = grpc_error.details() or str(grpc_error)
            
            # Map gRPC status codes to exception types
            if status_code == StatusCode.INVALID_ARGUMENT:
                return DisheapValidationError(message, "INVALID_ARGUMENT")
            elif status_code == StatusCode.UNAUTHENTICATED:
                return DisheapAuthenticationError(message, "UNAUTHENTICATED")
            elif status_code == StatusCode.PERMISSION_DENIED:
                return DisheapPermissionError(message, "PERMISSION_DENIED")
            elif status_code == StatusCode.NOT_FOUND:
                return DisheapNotFoundError(message, "NOT_FOUND")
            elif status_code == StatusCode.ALREADY_EXISTS:
                return DisheapConflictError(message, "ALREADY_EXISTS")
            elif status_code == StatusCode.FAILED_PRECONDITION:
                return DisheapLeaseError(message, "FAILED_PRECONDITION")
            elif status_code == StatusCode.ABORTED:
                return DisheapConflictError(message, "ABORTED")
            elif status_code == StatusCode.UNAVAILABLE:
                return DisheapUnavailableError(message, "UNAVAILABLE")
            elif status_code == StatusCode.DEADLINE_EXCEEDED:
                return DisheapTimeoutError(message, "DEADLINE_EXCEEDED")
            elif status_code == StatusCode.RESOURCE_EXHAUSTED:
                return DisheapCapacityError(message, "RESOURCE_EXHAUSTED")
            else:
                return DisheapError(message, str(status_code))
        
        # Handle connection errors
        if "connection" in str(grpc_error).lower() or "network" in str(grpc_error).lower():
            return DisheapConnectionError(str(grpc_error))
            
        # Default to generic error
        return DisheapError(str(grpc_error))
        
    except ImportError:
        # gRPC not available, return generic error
        return DisheapError(str(grpc_error))
