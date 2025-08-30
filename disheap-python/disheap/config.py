"""
Configuration classes for the Disheap Python SDK.

Provides configuration options for client connection, TLS, timeouts,
retry behavior, and observability settings.
"""

from dataclasses import dataclass, field
from datetime import timedelta
from typing import List, Optional, Dict, Any
import ssl


@dataclass
class TLSConfig:
    """TLS configuration for secure connections."""
    
    enabled: bool = False
    cert_file: Optional[str] = None
    key_file: Optional[str] = None
    ca_file: Optional[str] = None
    verify_server_cert: bool = True
    server_name_override: Optional[str] = None
    
    def to_ssl_context(self) -> Optional[ssl.SSLContext]:
        """
        Create an SSL context from the TLS configuration.
        
        Returns:
            SSL context for secure connections, or None if TLS is disabled
        """
        if not self.enabled:
            return None
            
        context = ssl.create_default_context()
        
        if self.ca_file:
            context.load_verify_locations(cafile=self.ca_file)
            
        if self.cert_file and self.key_file:
            context.load_cert_chain(self.cert_file, self.key_file)
            
        if not self.verify_server_cert:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            
        return context


@dataclass  
class RetryConfig:
    """Retry configuration for failed operations."""
    
    max_attempts: int = 3
    initial_backoff: timedelta = field(default_factory=lambda: timedelta(milliseconds=100))
    max_backoff: timedelta = field(default_factory=lambda: timedelta(seconds=30))
    backoff_multiplier: float = 2.0
    jitter: bool = True
    
    # Specific retry settings for different operation types
    connect_max_attempts: int = 5
    enqueue_max_attempts: int = 3
    consumer_max_attempts: int = 10  # Consumer should be more resilient


@dataclass
class ObservabilityConfig:
    """Observability configuration for tracing and metrics."""
    
    # OpenTelemetry tracing
    tracing_enabled: bool = True
    tracing_service_name: str = "disheap-python-sdk"
    tracing_sample_rate: float = 0.1  # Sample 10% of traces
    
    # Metrics  
    metrics_enabled: bool = True
    metrics_export_interval: timedelta = field(default_factory=lambda: timedelta(seconds=30))
    
    # Structured logging
    logging_enabled: bool = True
    log_level: str = "INFO"
    log_format: str = "json"  # or "text"
    log_payload_on_error: bool = False  # Security: avoid logging sensitive data
    
    # Custom attributes to add to all spans/metrics
    custom_attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FlowControlConfig:
    """Flow control configuration for consumers."""
    
    default_prefetch: int = 10
    max_prefetch: int = 1000
    low_watermark: float = 0.3  # Request more credits when below 30%
    credit_request_size: int = 10  # Number of credits to request at once
    

@dataclass
class LeaseConfig:
    """Lease management configuration."""
    
    default_visibility_timeout: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    max_visibility_timeout: timedelta = field(default_factory=lambda: timedelta(hours=1))
    renewal_threshold: timedelta = field(default_factory=lambda: timedelta(seconds=30))
    renewal_interval: timedelta = field(default_factory=lambda: timedelta(seconds=15))
    max_renewal_attempts: int = 3


@dataclass
class ClientConfig:
    """Main client configuration for the Disheap SDK."""
    
    # Connection settings
    endpoints: List[str] = field(default_factory=lambda: ["localhost:9090"])
    api_key: str = ""
    
    # TLS configuration
    tls: Optional[TLSConfig] = None
    
    # Timeout settings
    connect_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=10))
    request_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=30))
    keepalive_time: timedelta = field(default_factory=lambda: timedelta(seconds=30))
    keepalive_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=5))
    
    # Connection pool settings
    max_connections_per_endpoint: int = 10
    connection_pool_max_idle: int = 5
    connection_pool_max_age: timedelta = field(default_factory=lambda: timedelta(minutes=30))
    
    # Retry configuration
    retry: RetryConfig = field(default_factory=RetryConfig)
    
    # Producer settings
    default_producer_id: Optional[str] = None
    auto_generate_producer_id: bool = True
    batch_max_size: int = 100
    batch_max_wait: timedelta = field(default_factory=lambda: timedelta(milliseconds=100))
    
    # Consumer settings
    flow_control: FlowControlConfig = field(default_factory=FlowControlConfig)
    lease: LeaseConfig = field(default_factory=LeaseConfig)
    
    # Observability
    observability: ObservabilityConfig = field(default_factory=ObservabilityConfig)
    
    # Validation
    validate_topic_names: bool = True
    max_payload_size: int = 1024 * 1024  # 1 MiB default
    
    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if not self.endpoints:
            raise ValueError("At least one endpoint must be specified")
            
        if not self.api_key:
            raise ValueError("API key must be provided")
            
        if not self.api_key.startswith("dh_"):
            raise ValueError("API key must follow format: dh_<key_id>_<secret>")
            
        if self.connect_timeout.total_seconds() <= 0:
            raise ValueError("Connect timeout must be positive")
            
        if self.request_timeout.total_seconds() <= 0:
            raise ValueError("Request timeout must be positive")
            
        if self.retry.max_attempts < 1:
            raise ValueError("Max retry attempts must be at least 1")
            
        if self.flow_control.default_prefetch < 1:
            raise ValueError("Default prefetch must be at least 1")
            
        if self.flow_control.max_prefetch < self.flow_control.default_prefetch:
            raise ValueError("Max prefetch must be >= default prefetch")
            
        if not 0.0 < self.flow_control.low_watermark < 1.0:
            raise ValueError("Low watermark must be between 0.0 and 1.0")
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "ClientConfig":
        """
        Create a ClientConfig from a dictionary.
        
        Args:
            config_dict: Configuration dictionary
            
        Returns:
            ClientConfig instance
        """
        # Handle nested configurations
        if "tls" in config_dict and config_dict["tls"]:
            config_dict["tls"] = TLSConfig(**config_dict["tls"])
            
        if "retry" in config_dict:
            config_dict["retry"] = RetryConfig(**config_dict["retry"])
            
        if "flow_control" in config_dict:
            config_dict["flow_control"] = FlowControlConfig(**config_dict["flow_control"])
            
        if "lease" in config_dict:
            config_dict["lease"] = LeaseConfig(**config_dict["lease"])
            
        if "observability" in config_dict:
            config_dict["observability"] = ObservabilityConfig(**config_dict["observability"])
            
        return cls(**config_dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert ClientConfig to a dictionary.
        
        Returns:
            Configuration as a dictionary
        """
        # This is a simplified implementation
        # In production, you'd want more sophisticated serialization
        result = {}
        for key, value in self.__dict__.items():
            if hasattr(value, "__dict__"):
                result[key] = value.__dict__
            else:
                result[key] = value
        return result
