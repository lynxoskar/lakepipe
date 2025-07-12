# Lakepipe System Architecture Plan

## Executive Summary

Lakepipe is a modern, functional data pipeline library designed for high-performance batch and streaming transformations across multiple lakehouse formats. Built on railway-oriented programming principles with zero-copy operations, it provides unified access to DuckLake, S3, Iceberg, Parquet, CSV, Kafka streams, and streaming Arrow data sources. Features intelligent local caching for immutable datasets and real-time Kafka integration.

## Reference Architecture

- **DuckLake**: https://github.com/duckdb/ducklake/tree/main/src
- **GenAI Processors**: https://github.com/google-gemini/genai-processors
- **Style Guide**: Railway-oriented programming with functional composition

## 1. Core Architecture Overview

### 1.1 Stream-Based Processing System

Inspired by GenAI Processors, lakepipe implements a stream-based architecture where:
- All data flows through asynchronous streams of `DataParts`
- Processors are composable using functional operators
- Zero-copy operations between Polars, DuckDB, and PyArrow
- Automatic concurrent execution optimization
- Intelligent local caching for immutable datasets
- Real-time Kafka streaming integration

```python
# Core pipeline composition (inspired by genai-processors)
source = S3ParquetSource(config) 
cache = LocalCache(config)  # New: intelligent caching layer
transform = UserTransform(config)
kafka_sink = KafkaSink(config)  # New: Kafka streaming sink

# Pipeline composition using functional operators
pipeline = source + cache + transform + kafka_sink

# Execute with streaming
async for result in pipeline(stream_config):
    await handle_result(result)
```

### 1.2 Technology Stack

**Core Data Stack (Zero-Copy Focus)**:
- **Polars 0.21+**: DataFrame operations with lazy evaluation
- **DuckDB 0.10+**: SQL analytics and complex joins
- **PyArrow 16+**: Zero-copy data interchange and Parquet operations
- **Python 3.13+**: Modern async/await with optional free-threaded support

**Streaming & Caching Libraries**:
- **aiokafka**: Async Kafka producer/consumer
- **confluent-kafka**: High-performance Kafka client
- **diskcache**: Persistent local caching
- **fsspec**: Unified filesystem interface

**Supporting Libraries**:
- **loguru**: Structured logging with Rich integration
- **typer**: CLI interface with comprehensive help
- **rich**: Beautiful console output and progress tracking
- **python-decouple**: Environment-based configuration with TypedDict integration
- **orjson**: Ultra-fast JSON serialization (2-5x faster than stdlib json)
- **returns**: Result types for railway-oriented programming

## 2. Component Architecture

### 2.1 Core Components

```
lakepipe/
├── core/
│   ├── processors.py     # Base processor interface
│   ├── streams.py        # Stream manipulation utilities
│   ├── results.py        # Result types for railway programming
│   ├── monitoring.py     # Resource monitoring and metrics
│   └── cache.py          # Local cache management system
├── sources/
│   ├── s3.py            # S3 data source processor
│   ├── ducklake.py      # DuckLake source processor
│   ├── iceberg.py       # Iceberg table source processor
│   ├── parquet.py       # Parquet file source processor
│   ├── arrow.py         # Streaming Arrow source processor
│   └── kafka.py         # Kafka streaming source processor
├── sinks/
│   ├── iceberg.py       # Iceberg table sink processor
│   ├── s3.py            # S3 sink processor
│   ├── parquet.py       # Parquet file sink processor
│   ├── delta.py         # Delta Lake sink processor
│   └── kafka.py         # Kafka streaming sink processor
├── cache/
│   ├── local.py         # Local filesystem cache
│   ├── memory.py        # In-memory cache for hot data
│   ├── policies.py      # Cache eviction and validation policies
│   └── storage.py       # Cache storage backends
├── transforms/
│   ├── polars.py        # Polars-based transformations
│   ├── duckdb.py        # DuckDB SQL transformations
│   ├── user.py          # User-defined transformation interface
│   └── arrow.py         # Arrow compute transformations
├── config/
│   ├── types.py         # TypedDict configuration schemas
│   ├── defaults.py      # Default configuration builders
│   └── validation.py    # Configuration validation
├── cli/
│   ├── main.py          # Main CLI entry point
│   ├── commands.py      # CLI command implementations
│   └── display.py       # Rich console display utilities
└── api/
    ├── pipeline.py      # High-level pipeline API
    ├── operators.py     # Functional operators for composition
    └── monitoring.py    # Pipeline monitoring and metrics
```

### 2.2 Enhanced Configuration Schema with Environment Integration

```python
from typing import TypedDict, Optional, Literal
from pathlib import Path
from python_decouple import Config as DecoupleConfig, RepositoryEnv
import orjson

# Environment-aware configuration loading
def get_decouple_config(env_file: str = ".env") -> DecoupleConfig:
    """Get decouple config with proper fallbacks"""
    try:
        return DecoupleConfig(RepositoryEnv(env_file))
    except FileNotFoundError:
        # Fallback to environment variables only
        return DecoupleConfig()

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
    uri: str  # "s3://bucket/path", "file:///path", "kafka://topic", etc.
    format: Literal["parquet", "csv", "iceberg", "ducklake", "arrow", "kafka"]
    compression: Optional[str]
    schema: Optional[dict]
    cache: Optional[CacheConfig]     # Cache configuration
    kafka: Optional[KafkaConfig]     # Kafka-specific config

class SinkConfig(TypedDict):
    uri: str
    format: Literal["parquet", "iceberg", "delta", "csv", "kafka"]
    compression: Optional[str]
    partition_by: Optional[list[str]]
    kafka: Optional[KafkaConfig]     # Kafka-specific config

class PipelineConfig(TypedDict):
    log: LogConfig
    source: SourceConfig
    sink: SinkConfig
    transform: TransformConfig
    cache: CacheConfig               # Global cache settings
    monitoring: dict[str, Any]
    streaming: dict[str, Any]

# Environment-aware configuration builder combining TypedDict + python-decouple
def build_pipeline_config(
    config_overrides: Optional[dict] = None,
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
        "include_patterns": decouple_config("LAKEPIPE_CACHE_INCLUDE_PATTERNS", default="", cast=lambda x: x.split(",") if x else []),
        "exclude_patterns": decouple_config("LAKEPIPE_CACHE_EXCLUDE_PATTERNS", default="", cast=lambda x: x.split(",") if x else [])
    }
    
    # Kafka configuration from environment
    kafka_source_config: KafkaConfig = {
        "bootstrap_servers": decouple_config("LAKEPIPE_KAFKA_BOOTSTRAP_SERVERS", default="localhost:9092", cast=lambda x: x.split(",")),
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
    
    # Build complete configuration
    config: PipelineConfig = {
        "log": {
            "level": decouple_config("LAKEPIPE_LOG_LEVEL", default="INFO"),
            "format": decouple_config("LAKEPIPE_LOG_FORMAT", default="%(time)s | %(level)s | %(message)s"),
            "file": Path(decouple_config("LAKEPIPE_LOG_FILE")) if decouple_config("LAKEPIPE_LOG_FILE", default=None) else None
        },
        "source": source_config,
        "sink": {
            "uri": decouple_config("LAKEPIPE_SINK_URI", default=""),
            "format": decouple_config("LAKEPIPE_SINK_FORMAT", default="parquet"),
            "compression": decouple_config("LAKEPIPE_SINK_COMPRESSION", default="zstd"),
            "partition_by": decouple_config("LAKEPIPE_SINK_PARTITION_BY", default="", cast=lambda x: x.split(",") if x else None),
            "kafka": None  # Will be populated if sink format is kafka
        },
        "transform": {
            "engine": decouple_config("LAKEPIPE_TRANSFORM_ENGINE", default="polars"),
            "operations": [],  # Complex operations should be in config files
            "user_functions": decouple_config("LAKEPIPE_USER_FUNCTIONS", default="", cast=lambda x: x.split(",") if x else None)
        },
        "cache": cache_config,
        "monitoring": {
            "enable_metrics": decouple_config("LAKEPIPE_MONITORING_ENABLED", default=True, cast=bool),
            "memory_threshold": decouple_config("LAKEPIPE_MEMORY_THRESHOLD", default="8GB"),
            "progress_bar": decouple_config("LAKEPIPE_PROGRESS_BAR", default=True, cast=bool)
        },
        "streaming": {
            "batch_size": decouple_config("LAKEPIPE_STREAMING_BATCH_SIZE", default=100_000, cast=int),
            "max_memory": decouple_config("LAKEPIPE_STREAMING_MAX_MEMORY", default="4GB"),
            "concurrent_tasks": decouple_config("LAKEPIPE_STREAMING_CONCURRENT_TASKS", default=4, cast=int)
        }
    }
    
    # Apply any provided overrides
    if config_overrides:
        config = _deep_merge_config(config, config_overrides)
    
    return config

def _deep_merge_config(base: dict, override: dict) -> dict:
    """Deep merge configuration dictionaries"""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge_config(result[key], value)
        else:
            result[key] = value
    return result
```

### 2.3 Environment Configuration Examples

Example `.env` file for development:
```bash
# .env - Environment configuration for lakepipe

# Logging configuration
LAKEPIPE_LOG_LEVEL=DEBUG
LAKEPIPE_LOG_FORMAT=%(time)s | %(level)s | %(name)s | %(message)s
LAKEPIPE_LOG_FILE=/var/log/lakepipe.log

# Source configuration
LAKEPIPE_SOURCE_URI=s3://data-lake/raw/*.parquet
LAKEPIPE_SOURCE_FORMAT=parquet
LAKEPIPE_SOURCE_COMPRESSION=zstd

# Sink configuration  
LAKEPIPE_SINK_URI=kafka://processed-events
LAKEPIPE_SINK_FORMAT=kafka
LAKEPIPE_SINK_COMPRESSION=snappy
LAKEPIPE_SINK_PARTITION_BY=user_id,event_type

# Cache configuration
LAKEPIPE_CACHE_ENABLED=true
LAKEPIPE_CACHE_DIR=/opt/lakepipe/cache
LAKEPIPE_CACHE_MAX_SIZE=50GB
LAKEPIPE_CACHE_TTL_DAYS=30
LAKEPIPE_CACHE_IMMUTABLE_ONLY=true
LAKEPIPE_CACHE_INCLUDE_PATTERNS=*.parquet,s3://data-lake/immutable/*
LAKEPIPE_CACHE_EXCLUDE_PATTERNS=*/temp/*,*/staging/*

# Kafka configuration
LAKEPIPE_KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
LAKEPIPE_KAFKA_SOURCE_TOPIC=raw-events
LAKEPIPE_KAFKA_GROUP_ID=lakepipe-processors
LAKEPIPE_KAFKA_AUTO_OFFSET_RESET=latest
LAKEPIPE_KAFKA_SERIALIZATION=json
LAKEPIPE_KAFKA_COMPRESSION_TYPE=snappy
LAKEPIPE_KAFKA_BATCH_SIZE=32768
LAKEPIPE_KAFKA_MAX_POLL_RECORDS=5000

# Transform configuration
LAKEPIPE_TRANSFORM_ENGINE=polars
LAKEPIPE_USER_FUNCTIONS=mymodule.clean_data,mymodule.enrich_data

# Monitoring configuration
LAKEPIPE_MONITORING_ENABLED=true
LAKEPIPE_MEMORY_THRESHOLD=16GB
LAKEPIPE_PROGRESS_BAR=true

# Streaming configuration
LAKEPIPE_STREAMING_BATCH_SIZE=250000
LAKEPIPE_STREAMING_MAX_MEMORY=8GB
LAKEPIPE_STREAMING_CONCURRENT_TASKS=8
```

Usage with type-safe configuration:
```python
# Using the environment-aware configuration builder
from lakepipe.config import build_pipeline_config

# Load configuration from environment and .env file
config = build_pipeline_config()

# Override specific settings programmatically while maintaining type safety
config_overrides = {
    "source": {
        "uri": "s3://special-bucket/data/*.parquet"
    },
    "cache": {
        "enabled": False  # Disable cache for this specific run
    }
}

config = build_pipeline_config(config_overrides=config_overrides)

# TypedDict ensures type safety - this will be caught by mypy
# config["invalid_key"] = "value"  # mypy error!
# config["cache"]["enabled"] = "not_a_bool"  # mypy error!

# Create pipeline with type-safe configuration
pipeline = create_pipeline(config)
```

## 3. Local Cache Layer

### 3.1 Intelligent Cache System

```python
import hashlib
import time
from pathlib import Path
from typing import Optional
import diskcache as dc
import polars as pl
from returns.result import Result, Success, Failure

class LocalCache:
    """Intelligent local cache for immutable datasets"""
    
    def __init__(self, config: CacheConfig):
        self.config = config
        self.cache_dir = Path(config["cache_dir"])
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize disk cache with size limits
        self.disk_cache = dc.Cache(
            str(self.cache_dir),
            size_limit=self._parse_size(config["max_size"])
        )
        
        # In-memory cache for frequently accessed data
        self.memory_cache = {}
        self.access_times = {}
    
    def _generate_cache_key(self, source_uri: str, transform_hash: str) -> str:
        """Generate deterministic cache key"""
        key_data = f"{source_uri}:{transform_hash}"
        return hashlib.sha256(key_data.encode()).hexdigest()
    
    def _is_cacheable(self, source_uri: str, metadata: dict) -> bool:
        """Determine if data source should be cached"""
        if not self.config["enabled"]:
            return False
        
        # Only cache immutable datasets if configured
        if self.config["immutable_only"] and not metadata.get("immutable", False):
            return False
        
        # Check include/exclude patterns
        for pattern in self.config["exclude_patterns"]:
            if pattern in source_uri:
                return False
        
        if self.config["include_patterns"]:
            return any(pattern in source_uri for pattern in self.config["include_patterns"])
        
        return True
    
    async def get_cached_data(
        self, 
        source_uri: str, 
        transform_hash: str,
        metadata: dict
    ) -> Optional[pl.LazyFrame]:
        """Retrieve data from cache if available and valid"""
        
        if not self._is_cacheable(source_uri, metadata):
            return None
        
        cache_key = self._generate_cache_key(source_uri, transform_hash)
        
        # Check memory cache first
        if cache_key in self.memory_cache:
            self.access_times[cache_key] = time.time()
            return self.memory_cache[cache_key]
        
        # Check disk cache
        cached_path = self.disk_cache.get(cache_key)
        if cached_path and Path(cached_path).exists():
            # Validate cache age
            if self._is_cache_valid(cached_path):
                # Load into memory cache for faster future access
                lazy_df = pl.scan_parquet(cached_path)
                self.memory_cache[cache_key] = lazy_df
                self.access_times[cache_key] = time.time()
                return lazy_df
        
        return None
    
    async def store_cached_data(
        self,
        source_uri: str,
        transform_hash: str,
        data: pl.LazyFrame,
        metadata: dict
    ) -> Result[str, str]:
        """Store data in cache"""
        
        if not self._is_cacheable(source_uri, metadata):
            return Success("Not cacheable - skipped")
        
        try:
            cache_key = self._generate_cache_key(source_uri, transform_hash)
            cache_file = self.cache_dir / f"{cache_key}.parquet"
            
            # Write to disk with compression
            data.sink_parquet(
                str(cache_file),
                compression=self.config["compression"]
            )
            
            # Store path in disk cache
            self.disk_cache.set(cache_key, str(cache_file))
            
            # Add to memory cache if small enough
            if self._should_memory_cache(cache_file):
                self.memory_cache[cache_key] = pl.scan_parquet(str(cache_file))
                self.access_times[cache_key] = time.time()
            
            return Success(f"Cached to {cache_file}")
            
        except Exception as e:
            return Failure(f"Cache storage failed: {e}")
    
    def _is_cache_valid(self, cache_path: str) -> bool:
        """Check if cached data is still valid"""
        if not self.config.get("ttl_days"):
            return True
        
        cache_age = time.time() - Path(cache_path).stat().st_mtime
        max_age = self.config["ttl_days"] * 24 * 3600
        return cache_age < max_age
    
    def cleanup_cache(self) -> dict[str, int]:
        """Clean up expired cache entries"""
        removed_files = 0
        removed_memory = 0
        
        # Clean disk cache
        if self.config.get("ttl_days"):
            cutoff_time = time.time() - (self.config["ttl_days"] * 24 * 3600)
            
            for cache_file in self.cache_dir.glob("*.parquet"):
                if cache_file.stat().st_mtime < cutoff_time:
                    cache_file.unlink()
                    removed_files += 1
        
        # Clean memory cache (LRU-style)
        if len(self.memory_cache) > 100:  # Keep top 100 most recent
            sorted_by_access = sorted(
                self.access_times.items(), 
                key=lambda x: x[1], 
                reverse=True
            )
            
            to_remove = [k for k, _ in sorted_by_access[100:]]
            for key in to_remove:
                self.memory_cache.pop(key, None)
                self.access_times.pop(key, None)
                removed_memory += 1
        
        return {"disk_files": removed_files, "memory_entries": removed_memory}
```

### 3.2 Cache-Aware Source Processor

```python
class CachedSourceProcessor:
    """Source processor with intelligent caching"""
    
    def __init__(self, source_config: SourceConfig, cache_config: CacheConfig):
        self.source_config = source_config
        self.cache = LocalCache(cache_config) if cache_config["enabled"] else None
        self.source_processor = self._create_source_processor()
    
    async def process(
        self, 
        input_stream: AsyncGenerator[DataPart, None]
    ) -> AsyncGenerator[DataPart, None]:
        """Process with caching layer"""
        
        async for part in input_stream:
            source_uri = part["source_info"]["uri"]
            transform_hash = part["metadata"].get("transform_hash", "")
            
            # Try cache first
            if self.cache:
                cached_data = await self.cache.get_cached_data(
                    source_uri, 
                    transform_hash, 
                    part["metadata"]
                )
                
                if cached_data is not None:
                    # Cache hit - return cached data
                    yield DataPart(
                        data=cached_data,
                        metadata={**part["metadata"], "cache_hit": True},
                        source_info=part["source_info"],
                        schema=part["schema"]
                    )
                    continue
            
            # Cache miss - process normally
            processed_data = await self.source_processor.process_single(part)
            
            # Store in cache for future use
            if self.cache and processed_data:
                await self.cache.store_cached_data(
                    source_uri,
                    transform_hash,
                    processed_data["data"],
                    part["metadata"]
                )
                
                # Mark as newly cached
                processed_data["metadata"]["cache_stored"] = True
            
            yield processed_data
```

## 4. Kafka Streaming Integration

### 4.1 Kafka Source Processor

```python
import asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.helpers import create_ssl_context
import orjson  # Ultra-fast JSON serialization
import polars as pl
from typing import Optional

class KafkaSource:
    """Kafka streaming source processor"""
    
    def __init__(self, config: KafkaConfig):
        self.config = config
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.running = False
    
    async def initialize(self):
        """Initialize Kafka consumer"""
        self.consumer = AIOKafkaConsumer(
            self.config["topic"],
            bootstrap_servers=self.config["bootstrap_servers"],
            group_id=self.config.get("group_id"),
            auto_offset_reset=self.config["auto_offset_reset"],
            max_poll_records=self.config["max_poll_records"],
            enable_auto_commit=True,
            auto_commit_interval_ms=1000
        )
        await self.consumer.start()
        self.running = True
    
    async def process(
        self, 
        input_stream: AsyncGenerator[DataPart, None]
    ) -> AsyncGenerator[DataPart, None]:
        """Process Kafka messages as stream"""
        
        if not self.consumer:
            await self.initialize()
        
        message_buffer = []
        batch_size = 1000  # Process messages in batches
        
        try:
            while self.running:
                # Consume messages with timeout
                msg_pack = await asyncio.wait_for(
                    self.consumer.getmany(timeout_ms=1000),
                    timeout=2.0
                )
                
                for topic_partition, messages in msg_pack.items():
                    for message in messages:
                        # Deserialize message based on configuration
                        data = await self._deserialize_message(message)
                        message_buffer.append(data)
                        
                        # Process batch when buffer is full
                        if len(message_buffer) >= batch_size:
                            df = await self._create_dataframe(message_buffer)
                            
                            yield DataPart(
                                data=df,
                                metadata={
                                    "source": "kafka",
                                    "topic": self.config["topic"],
                                    "partition": topic_partition.partition,
                                    "offset_range": (
                                        message_buffer[0]["offset"],
                                        message_buffer[-1]["offset"]
                                    ),
                                    "message_count": len(message_buffer)
                                },
                                source_info={
                                    "uri": f"kafka://{self.config['topic']}",
                                    "format": "kafka"
                                },
                                schema={"type": "kafka_messages"}
                            )
                            
                            message_buffer = []
                
                # Yield any remaining messages
                if message_buffer:
                    df = await self._create_dataframe(message_buffer)
                    yield DataPart(
                        data=df,
                        metadata={
                            "source": "kafka",
                            "topic": self.config["topic"],
                            "message_count": len(message_buffer)
                        },
                        source_info={
                            "uri": f"kafka://{self.config['topic']}",
                            "format": "kafka"
                        },
                        schema={"type": "kafka_messages"}
                    )
                    message_buffer = []
        
        except asyncio.TimeoutError:
            # Normal timeout - continue processing
            pass
        except Exception as e:
            logger.error(f"Kafka consumption error: {e}")
            raise
        finally:
            if self.consumer:
                await self.consumer.stop()
    
    async def _deserialize_message(self, message) -> dict:
        """Deserialize Kafka message based on configuration"""
        
        if self.config["serialization"] == "json":
            try:
                value = orjson.loads(message.value)
            except (orjson.JSONDecodeError, UnicodeDecodeError):
                value = message.value.decode('utf-8', errors='ignore')
        
        elif self.config["serialization"] == "avro":
            # TODO: Implement Avro deserialization
            value = message.value
        
        elif self.config["serialization"] == "protobuf":
            # TODO: Implement Protobuf deserialization
            value = message.value
        
        else:  # raw
            value = message.value
        
        return {
            "key": message.key.decode('utf-8') if message.key else None,
            "value": value,
            "topic": message.topic,
            "partition": message.partition,
            "offset": message.offset,
            "timestamp": message.timestamp,
            "headers": dict(message.headers or [])
        }
    
    async def _create_dataframe(self, messages: list[dict]) -> pl.LazyFrame:
        """Create Polars DataFrame from Kafka messages"""
        
        # Convert messages to DataFrame
        df = pl.DataFrame(messages)
        
        # Add derived columns
        df = df.with_columns([
            pl.col("timestamp").cast(pl.Datetime).alias("message_time"),
            pl.lit("kafka").alias("source_type")
        ])
        
        return df.lazy()
```

### 4.2 Kafka Sink Processor

```python
class KafkaSink:
    """Kafka streaming sink processor"""
    
    def __init__(self, config: KafkaConfig):
        self.config = config
        self.producer: Optional[AIOKafkaProducer] = None
    
    async def initialize(self):
        """Initialize Kafka producer"""
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.config["bootstrap_servers"],
            compression_type=self.config["compression_type"],
            batch_size=self.config["batch_size"],
            linger_ms=100,  # Allow batching for better throughput
            acks='all',     # Wait for all replicas
            retries=3
        )
        await self.producer.start()
    
    async def process(
        self, 
        input_stream: AsyncGenerator[DataPart, None]
    ) -> AsyncGenerator[DataPart, None]:
        """Process data and send to Kafka"""
        
        if not self.producer:
            await self.initialize()
        
        try:
            async for part in input_stream:
                # Convert DataFrame to messages
                messages = await self._dataframe_to_messages(part["data"])
                
                # Send messages to Kafka
                sent_count = 0
                for message in messages:
                    try:
                        await self.producer.send(
                            topic=self.config["topic"],
                            key=message.get("key", "").encode('utf-8') if message.get("key") else None,
                            value=await self._serialize_message(message["value"]),
                            partition=message.get("partition"),
                            headers=message.get("headers", {})
                        )
                        sent_count += 1
                    except Exception as e:
                        logger.error(f"Failed to send message: {e}")
                
                # Flush to ensure delivery
                await self.producer.flush()
                
                # Yield result with metadata
                yield DataPart(
                    data=part["data"],
                    metadata={
                        **part["metadata"],
                        "kafka_sent": sent_count,
                        "sink_topic": self.config["topic"]
                    },
                    source_info=part["source_info"],
                    schema=part["schema"]
                )
        
        finally:
            if self.producer:
                await self.producer.stop()
    
    async def _dataframe_to_messages(self, df: pl.LazyFrame) -> list[dict]:
        """Convert DataFrame to Kafka messages"""
        
        # Collect DataFrame to get actual data
        df_collected = df.collect()
        
        messages = []
        
        # Convert each row to a message
        for row in df_collected.iter_rows(named=True):
            # Extract partition key if configured
            partition_key = None
            if self.config.get("partition_key") and self.config["partition_key"] in row:
                partition_key = str(row[self.config["partition_key"]])
            
            message = {
                "key": partition_key,
                "value": dict(row),
                "headers": {
                    "lakepipe_timestamp": str(time.time()),
                    "lakepipe_source": "batch_processing"
                }
            }
            messages.append(message)
        
        return messages
    
    async def _serialize_message(self, value: dict) -> bytes:
        """Serialize message value based on configuration"""
        
        if self.config["serialization"] == "json":
            return orjson.dumps(value, default=str)
        
        elif self.config["serialization"] == "avro":
            # TODO: Implement Avro serialization
            return orjson.dumps(value, default=str)
        
        elif self.config["serialization"] == "protobuf":
            # TODO: Implement Protobuf serialization
            return orjson.dumps(value, default=str)
        
        else:  # raw
            return str(value).encode('utf-8')
```

### 4.3 Kafka Pipeline Examples

```python
# Real-time data processing pipeline
async def create_kafka_streaming_pipeline():
    """Create real-time Kafka processing pipeline"""
    
    # Kafka source configuration
    kafka_source_config = {
        "bootstrap_servers": ["localhost:9092"],
        "topic": "raw_events",
        "group_id": "lakepipe_processor",
        "auto_offset_reset": "latest",
        "serialization": "json",
        "max_poll_records": 1000
    }
    
    # Kafka sink configuration
    kafka_sink_config = {
        "bootstrap_servers": ["localhost:9092"],
        "topic": "processed_events",
        "serialization": "json",
        "compression_type": "snappy",
        "batch_size": 16384,
        "partition_key": "user_id"
    }
    
    # Create pipeline
    kafka_source = KafkaSource(kafka_source_config)
    transform = UserTransform({
        "engine": "polars",
        "operations": [
            {"type": "filter", "condition": "event_type == 'purchase'"},
            {"type": "with_columns", "expressions": [
                "amount * 1.1 as amount_with_tax",
                "current_timestamp() as processed_at"
            ]}
        ]
    })
    kafka_sink = KafkaSink(kafka_sink_config)
    
    # Compose pipeline
    pipeline = kafka_source + transform + kafka_sink
    
    # Execute
    async for result in pipeline(streams.endless_stream()):
        logger.info(f"Processed batch: {result['metadata']}")

# Batch to stream pipeline
async def create_batch_to_kafka_pipeline():
    """Create pipeline from batch data to Kafka stream"""
    
    config = {
        "source": {
            "uri": "s3://data-lake/daily-sales/*.parquet",
            "format": "parquet",
            "cache": {
                "enabled": True,
                "cache_dir": "/tmp/lakepipe_cache",
                "max_size": "10GB",
                "immutable_only": True,
                "ttl_days": 7
            }
        },
        "sink": {
            "uri": "kafka://processed-sales",
            "format": "kafka",
            "kafka": {
                "bootstrap_servers": ["localhost:9092"],
                "topic": "processed-sales",
                "serialization": "json",
                "partition_key": "store_id"
            }
        }
    }
    
    pipeline = create_pipeline(config)
    result = await pipeline.execute()
    return result
```

## 5. Enhanced CLI Interface

### 5.1 Updated CLI Commands

```python
@app.command()
def run(
    source_uri: str = typer.Option(..., "--source-uri", help="Source data URI (supports kafka://topic)"),
    target_uri: str = typer.Option(..., "--target-uri", help="Target data URI (supports kafka://topic)"),
    source_format: str = typer.Option("parquet", "--source-format", help="Source format"),
    target_format: str = typer.Option("parquet", "--target-format", help="Target format"),
    streaming: bool = typer.Option(False, "--streaming", help="Enable streaming mode"),
    batch_size: int = typer.Option(100_000, "--batch-size", help="Batch size for streaming"),
    enable_cache: bool = typer.Option(False, "--cache", help="Enable local caching"),
    cache_dir: str = typer.Option("/tmp/lakepipe_cache", "--cache-dir", help="Cache directory"),
    cache_size: str = typer.Option("10GB", "--cache-size", help="Max cache size"),
    kafka_servers: Optional[str] = typer.Option(None, "--kafka-servers", help="Kafka bootstrap servers (comma-separated)"),
    kafka_group: Optional[str] = typer.Option(None, "--kafka-group", help="Kafka consumer group"),
    config_file: Optional[Path] = typer.Option(None, "--config", help="Configuration file"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output")
):
    """Run lakepipe data pipeline with caching and Kafka support"""
    
    # Build configuration using environment-aware builder + orjson for config files
    if config_file:
        # Load JSON config file with orjson for performance
        with open(config_file, 'rb') as f:
            file_config = orjson.loads(f.read())
    else:
        file_config = {}
    
    # Merge CLI args with file config and environment variables
    cli_overrides = {
        "source": {"uri": source_uri, "format": source_format},
        "sink": {"uri": target_uri, "format": target_format},
        "streaming": {"enabled": streaming, "batch_size": batch_size},
        "cache": {"enabled": enable_cache, "cache_dir": cache_dir, "max_size": cache_size}
    }
    
    if kafka_servers:
        cli_overrides["source"]["kafka"] = {"bootstrap_servers": kafka_servers.split(",")}
        if kafka_group:
            cli_overrides["source"]["kafka"]["group_id"] = kafka_group
    
    # Build type-safe configuration
    config = build_pipeline_config(config_overrides={**file_config, **cli_overrides})
    
    # Execute pipeline with rich progress display
    execute_pipeline_with_display(config)

@app.command()
def cache(
    action: Literal["status", "clear", "cleanup"] = typer.Argument(help="Cache action"),
    cache_dir: str = typer.Option("/tmp/lakepipe_cache", "--cache-dir", help="Cache directory")
):
    """Manage local cache"""
    
    cache_config = {
        "enabled": True,
        "cache_dir": cache_dir,
        "max_size": "10GB",
        "compression": "zstd",
        "ttl_days": 7,
        "immutable_only": True,
        "include_patterns": [],
        "exclude_patterns": []
    }
    
    cache = LocalCache(cache_config)
    
    if action == "status":
        display_cache_status(cache)
    elif action == "clear":
        clear_cache(cache)
    elif action == "cleanup":
        cleanup_results = cache.cleanup_cache()
        console.print(f"✅ Cleaned up {cleanup_results['disk_files']} disk files and {cleanup_results['memory_entries']} memory entries")

@app.command()
def kafka_test(
    servers: str = typer.Option("localhost:9092", "--servers", help="Kafka servers"),
    topic: str = typer.Option(..., "--topic", help="Topic to test"),
    mode: Literal["produce", "consume"] = typer.Option("consume", "--mode", help="Test mode")
):
    """Test Kafka connectivity"""
    
    async def test_kafka():
        if mode == "consume":
            await test_kafka_consumer(servers.split(','), topic)
        else:
            await test_kafka_producer(servers.split(','), topic)
    
    asyncio.run(test_kafka())
```

## 6. Updated Implementation Plan

### Phase 1: Core Infrastructure (Weeks 1-2)
- [ ] Set up project structure with uv and Python 3.13
- [ ] Implement base processor interface
- [ ] Create configuration system with TypedDict
- [ ] Set up logging with loguru and Rich
- [ ] Implement resource monitoring
- [ ] Create basic CLI with typer

### Phase 2: Local Cache System (Weeks 3-4)
- [ ] Implement LocalCache class with disk and memory tiers
- [ ] Add cache validation and TTL management
- [ ] Create cache-aware source processors
- [ ] Implement cache cleanup and management utilities
- [ ] Add cache status monitoring and CLI commands
- [ ] Performance testing and optimization

### Phase 3: Data Sources (Weeks 5-6)
- [ ] Implement Parquet source processor with caching
- [ ] Implement S3 source processor with caching
- [ ] Implement CSV source processor
- [ ] Add streaming Arrow source
- [ ] Implement DuckLake source integration
- [ ] Add Iceberg source support

### Phase 4: Kafka Integration (Weeks 7-8)
- [ ] Implement Kafka source processor with aiokafka
- [ ] Implement Kafka sink processor
- [ ] Add support for multiple serialization formats (JSON, Avro, Protobuf)
- [ ] Create Kafka connectivity testing utilities
- [ ] Add Kafka-specific CLI commands
- [ ] Performance optimization for high-throughput streaming

### Phase 5: Data Sinks (Weeks 9-10)
- [ ] Implement Parquet sink processor
- [ ] Implement S3 sink processor
- [ ] Add Iceberg sink support
- [ ] Add Delta Lake sink support
- [ ] Enhance Kafka sink with partitioning strategies

### Phase 6: Transformation Engine (Weeks 11-12)
- [ ] Implement Polars transformation processor
- [ ] Implement DuckDB transformation processor
- [ ] Add PyArrow compute transformations
- [ ] Create user-defined transformation interface
- [ ] Add zero-copy optimization

### Phase 7: Stream Processing & Integration (Weeks 13-14)
- [ ] Implement streaming processor core
- [ ] Add concurrent execution optimization
- [ ] Implement memory-efficient batching
- [ ] Add backpressure handling
- [ ] Create pipeline composition operators
- [ ] Integrate caching with streaming

### Phase 8: Testing & Documentation (Weeks 15-16)
- [ ] Implement integration tests with real data and Kafka
- [ ] Add cache performance benchmarks
- [ ] Add Kafka streaming performance tests
- [ ] Create comprehensive documentation
- [ ] Add example pipelines with caching and Kafka
- [ ] Performance optimization and tuning

## 7. Enhanced Performance Targets

### 7.1 Cache Performance
- **Cache Hit Ratio**: >90% for immutable datasets
- **Cache Lookup Time**: <10ms for memory cache, <100ms for disk cache
- **Cache Storage**: <5% overhead for storing cached data
- **Memory Efficiency**: Intelligent memory cache with LRU eviction

### 7.2 Kafka Performance
- **Throughput**: Process 100K+ messages/second
- **Latency**: <50ms end-to-end latency for real-time processing
- **Backpressure**: Graceful handling of consumer lag
- **JSON Serialization**: <0.5ms overhead with orjson (2-5x faster than stdlib json)
- **Batch Processing**: Optimal batching for throughput vs latency trade-offs

### 7.3 Configuration & Environment Management
- **Type Safety**: 100% type-safe configuration with TypedDict + python-decouple
- **Environment Flexibility**: Full 12-factor app compliance with .env file support
- **Performance**: Zero runtime overhead for configuration access
- **Validation**: Compile-time type checking with mypy integration

This enhanced system plan now includes intelligent local caching for immutable datasets and comprehensive Kafka streaming capabilities, maintaining the functional programming principles and zero-copy optimizations while adding these critical real-world features. 