"""
Configuration type definitions using TypedDict for type safety
with python-decouple integration for environment variables.
"""

from typing import TypedDict, Optional, Literal, Any
from pathlib import Path

# Core Configuration Types

class LogConfig(TypedDict):
    """Logging configuration"""
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    format: str
    file: Optional[Path]

class CacheConfig(TypedDict):
    """Local cache configuration for immutable datasets"""
    enabled: bool
    cache_dir: str
    max_size: str                    # "10GB", "1TB", etc.
    compression: Literal["zstd", "lz4", "gzip", "none"]
    ttl_days: Optional[int]          # Time-to-live in days
    immutable_only: bool             # Only cache immutable datasets
    include_patterns: list[str]      # Patterns for cacheable sources
    exclude_patterns: list[str]      # Patterns to exclude from cache

class KafkaConfig(TypedDict):
    """Kafka streaming configuration"""
    bootstrap_servers: list[str]
    topic: str
    group_id: Optional[str]          # For consumers
    auto_offset_reset: Literal["earliest", "latest", "none"]
    serialization: Literal["json", "avro", "protobuf", "raw"]
    schema_registry_url: Optional[str]
    partition_key: Optional[str]     # For producers
    compression_type: Literal["none", "gzip", "snappy", "lz4", "zstd"]
    batch_size: int                  # Producer batch size
    max_poll_records: int           # Consumer max records per poll

class SourceConfig(TypedDict):
    """Source configuration"""
    uri: str  # "s3://bucket/path", "file:///path", "kafka://topic", etc.
    format: Literal["parquet", "csv", "iceberg", "ducklake", "arrow", "kafka"]
    compression: Optional[str]
    schema: Optional[dict[str, Any]]
    cache: Optional[CacheConfig]     # Cache configuration
    kafka: Optional[KafkaConfig]     # Kafka-specific config

class SinkConfig(TypedDict):
    """Sink configuration"""
    uri: str
    format: Literal["parquet", "iceberg", "delta", "csv", "kafka"]
    compression: Optional[str]
    partition_by: Optional[list[str]]
    kafka: Optional[KafkaConfig]     # Kafka-specific config

class TransformConfig(TypedDict):
    """Transform configuration"""
    engine: Literal["polars", "duckdb", "arrow", "user"]
    operations: list[dict[str, Any]]
    user_functions: Optional[list[str]]

class MonitoringConfig(TypedDict):
    """Monitoring configuration"""
    enable_metrics: bool
    memory_threshold: str
    progress_bar: bool

class StreamingConfig(TypedDict):
    """Streaming configuration"""
    batch_size: int
    max_memory: str
    concurrent_tasks: int

class PipelineConfig(TypedDict):
    """Main pipeline configuration"""
    log: LogConfig
    source: SourceConfig
    sink: SinkConfig
    transform: TransformConfig
    cache: CacheConfig               # Global cache settings
    monitoring: MonitoringConfig
    streaming: StreamingConfig 