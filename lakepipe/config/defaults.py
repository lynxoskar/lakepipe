"""
Environment-aware configuration builder combining TypedDict + python-decouple
for type-safe configuration management.
"""

from typing import Optional, Any
from pathlib import Path
from decouple import Config as DecoupleConfig, RepositoryEnv

from lakepipe.config.types import (
    PipelineConfig,
    LogConfig,
    CacheConfig,
    KafkaConfig,
    SourceConfig,
    SinkConfig,
    TransformConfig,
    MonitoringConfig,
    StreamingConfig,
)


def get_decouple_config(env_file: str = ".env") -> DecoupleConfig:
    """Get decouple config with proper fallbacks"""
    try:
        return DecoupleConfig(RepositoryEnv(env_file))
    except FileNotFoundError:
        # Fallback to environment variables only - use simple Config without repository
        from decouple import config
        return config


def build_pipeline_config(
    config_overrides: Optional[dict[str, Any]] = None,
    env_file: str = ".env"
) -> PipelineConfig:
    """Build type-safe configuration from environment variables and overrides"""
    
    decouple_config = get_decouple_config(env_file)
    
    # Cache configuration from environment
    cache_config: CacheConfig = {
        "enabled": decouple_config("LAKEPIPE_CACHE_ENABLED", default=True, cast=bool),
        "cache_dir": decouple_config("LAKEPIPE_CACHE_DIR", default="/tmp/lakepipe_cache"),
        "max_size": decouple_config("LAKEPIPE_CACHE_MAX_SIZE", default="10GB"),
        "compression": decouple_config("LAKEPIPE_CACHE_COMPRESSION", default="zstd"),
        "ttl_days": decouple_config("LAKEPIPE_CACHE_TTL_DAYS", default=7, cast=int),
        "immutable_only": decouple_config("LAKEPIPE_CACHE_IMMUTABLE_ONLY", default=True, cast=bool),
        "include_patterns": decouple_config(
            "LAKEPIPE_CACHE_INCLUDE_PATTERNS", 
            default="", 
            cast=lambda x: x.split(",") if x else []
        ),
        "exclude_patterns": decouple_config(
            "LAKEPIPE_CACHE_EXCLUDE_PATTERNS", 
            default="", 
            cast=lambda x: x.split(",") if x else []
        )
    }
    
    # Kafka configuration from environment
    kafka_source_config: KafkaConfig = {
        "bootstrap_servers": decouple_config(
            "LAKEPIPE_KAFKA_BOOTSTRAP_SERVERS", 
            default="localhost:9092", 
            cast=lambda x: x.split(",")
        ),
        "topic": decouple_config("LAKEPIPE_KAFKA_SOURCE_TOPIC", default=""),
        "group_id": decouple_config("LAKEPIPE_KAFKA_GROUP_ID", default=None),
        "auto_offset_reset": decouple_config("LAKEPIPE_KAFKA_AUTO_OFFSET_RESET", default="latest"),
        "serialization": decouple_config("LAKEPIPE_KAFKA_SERIALIZATION", default="json"),
        "schema_registry_url": decouple_config("LAKEPIPE_KAFKA_SCHEMA_REGISTRY_URL", default=None),
        "partition_key": decouple_config("LAKEPIPE_KAFKA_PARTITION_KEY", default=None),
        "compression_type": decouple_config("LAKEPIPE_KAFKA_COMPRESSION_TYPE", default="snappy"),
        "batch_size": decouple_config("LAKEPIPE_KAFKA_BATCH_SIZE", default=16384, cast=int),
        "max_poll_records": decouple_config("LAKEPIPE_KAFKA_MAX_POLL_RECORDS", default=1000, cast=int)
    }
    
    # Source configuration from environment
    source_config: SourceConfig = {
        "uri": decouple_config("LAKEPIPE_SOURCE_URI", default=""),
        "format": decouple_config("LAKEPIPE_SOURCE_FORMAT", default="parquet"),
        "compression": decouple_config("LAKEPIPE_SOURCE_COMPRESSION", default=None),
        "schema": None,  # Complex schemas should be in config files
        "cache": cache_config,
        "kafka": kafka_source_config if decouple_config("LAKEPIPE_SOURCE_FORMAT", default="parquet") == "kafka" else None
    }
    
    # Sink configuration from environment
    sink_config: SinkConfig = {
        "uri": decouple_config("LAKEPIPE_SINK_URI", default=""),
        "format": decouple_config("LAKEPIPE_SINK_FORMAT", default="parquet"),
        "compression": decouple_config("LAKEPIPE_SINK_COMPRESSION", default="zstd"),
        "partition_by": decouple_config(
            "LAKEPIPE_SINK_PARTITION_BY", 
            default="", 
            cast=lambda x: x.split(",") if x else None
        ),
        "kafka": None  # Will be populated if sink format is kafka
    }
    
    # Log configuration from environment
    log_config: LogConfig = {
        "level": decouple_config("LAKEPIPE_LOG_LEVEL", default="INFO"),
        "format": decouple_config("LAKEPIPE_LOG_FORMAT", default="%(time)s | %(level)s | %(message)s"),
        "file": Path(decouple_config("LAKEPIPE_LOG_FILE")) if decouple_config("LAKEPIPE_LOG_FILE", default=None) else None
    }
    
    # Transform configuration from environment
    transform_config: TransformConfig = {
        "engine": decouple_config("LAKEPIPE_TRANSFORM_ENGINE", default="polars"),
        "operations": [],  # Complex operations should be in config files
        "user_functions": decouple_config(
            "LAKEPIPE_USER_FUNCTIONS", 
            default="", 
            cast=lambda x: x.split(",") if x else None
        )
    }
    
    # Monitoring configuration from environment
    monitoring_config: MonitoringConfig = {
        "enable_metrics": decouple_config("LAKEPIPE_MONITORING_ENABLED", default=True, cast=bool),
        "memory_threshold": decouple_config("LAKEPIPE_MEMORY_THRESHOLD", default="8GB"),
        "progress_bar": decouple_config("LAKEPIPE_PROGRESS_BAR", default=True, cast=bool)
    }
    
    # Streaming configuration from environment
    streaming_config: StreamingConfig = {
        "batch_size": decouple_config("LAKEPIPE_STREAMING_BATCH_SIZE", default=100_000, cast=int),
        "max_memory": decouple_config("LAKEPIPE_STREAMING_MAX_MEMORY", default="4GB"),
        "concurrent_tasks": decouple_config("LAKEPIPE_STREAMING_CONCURRENT_TASKS", default=4, cast=int)
    }
    
    # Build complete configuration
    config: PipelineConfig = {
        "log": log_config,
        "source": source_config,
        "sink": sink_config,
        "transform": transform_config,
        "cache": cache_config,
        "monitoring": monitoring_config,
        "streaming": streaming_config
    }
    
    # Apply any provided overrides
    if config_overrides:
        config = _deep_merge_config(config, config_overrides)
    
    return config


def _deep_merge_config(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Deep merge configuration dictionaries"""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge_config(result[key], value)
        else:
            result[key] = value
    return result 