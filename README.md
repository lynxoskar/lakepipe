# 🌊 Lakepipe

> **Modern, functional data pipeline library for high-performance batch and streaming transformations**

[![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)
[![Type Safety](https://img.shields.io/badge/type--safety-mypy-green.svg)](http://mypy-lang.org/)
[![Railway Programming](https://img.shields.io/badge/error--handling-railway--oriented-orange.svg)](https://fsharpforfunandprofit.com/rop/)
[![Async/Await](https://img.shields.io/badge/concurrency-async%2Fawait-purple.svg)](https://docs.python.org/3/library/asyncio.html)

Lakepipe provides unified access to **DuckLake**, **S3**, **Iceberg**, **Parquet**, **CSV**, **Kafka streams**, and **streaming Arrow** data sources with intelligent local caching and real-time processing capabilities.

## ✨ Features

🚂 **Railway-Oriented Programming** - Functional error handling with composable `Result` types  
🌊 **Stream-Based Processing** - Composable, async processors inspired by GenAI processors  
🔧 **Type-Safe Configuration** - Environment-aware config with `TypedDict` + `python-decouple`  
⚡ **Zero-Copy Operations** - Seamless integration between Polars, DuckDB, and PyArrow  
🗂️ **Intelligent Caching** - Two-tier local cache for immutable datasets with TTL management  
📡 **Kafka Integration** - High-performance streaming with `aiokafka` and multiple serialization formats  
🎨 **Beautiful CLI** - Rich console interface with progress bars and structured logging  
📊 **Built-in Monitoring** - Performance metrics, memory tracking, and resource monitoring  

## 🚀 Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/your-org/lakepipe.git
cd lakepipe

# Set up virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Basic Usage

```python
from lakepipe import create_pipeline, PipelineConfig

# Create a simple batch pipeline
config = {
    "source": {"uri": "s3://data-lake/sales/*.parquet", "format": "parquet"},
    "sink": {"uri": "s3://processed/sales/", "format": "parquet"},
    "transform": {
        "engine": "polars",
        "operations": [
            {"type": "filter", "condition": "amount > 0"},
            {"type": "with_columns", "expressions": ["amount * 1.1 as amount_with_tax"]}
        ]
    },
    "cache": {"enabled": True, "ttl_days": 7}
}

# Execute pipeline
pipeline = create_pipeline(config)
result = await pipeline.execute()
print(f"✅ Processed {result.processed_count} records in {result.processing_time:.2f}s")
```

### CLI Examples

```bash
# Generate example configuration files
lakepipe config-example

# Run a simple transformation with caching
lakepipe run \
  --source-uri "s3://data-lake/events/*.parquet" \
  --target-uri "s3://processed/events/" \
  --cache \
  --verbose

# Stream processing with Kafka
lakepipe run \
  --source-uri "kafka://raw-events" \
  --target-uri "kafka://processed-events" \
  --kafka-servers "localhost:9092" \
  --streaming \
  --batch-size 10000

# Dry run to validate configuration
lakepipe run \
  --source-uri "s3://bucket/data/*.parquet" \
  --target-uri "file:///output/" \
  --dry-run

# Manage local cache
lakepipe cache status --verbose
lakepipe cache cleanup
lakepipe cache clear

# Test Kafka connectivity  
lakepipe test-kafka --topic "events" --mode consume --timeout 30
```

## 🏗️ Architecture

Lakepipe uses a **stream-based, functional architecture** with composable processors:

```python
# Functional composition with operators
source = S3ParquetSource(config)
cache = LocalCache(config)           # Intelligent caching layer
transform = PolarsTransform(config)
kafka_sink = KafkaSink(config)       # Real-time streaming

# Compose pipeline using + operator (sequential)
pipeline = source + cache + transform + kafka_sink

# Or use | operator for parallel processing
parallel_pipeline = transform_a | transform_b | transform_c

# Execute with automatic error handling
async for result in pipeline(stream_config):
    match result:
        case Success(data): await handle_success(data)
        case Failure(error): await handle_error(error)
```

## 📁 Project Structure

```
lakepipe/
├── 📦 lakepipe/                     # Core package
│   ├── 🔧 config/                  # Configuration system
│   │   ├── types.py                # TypedDict schemas
│   │   └── defaults.py             # Environment-aware builders
│   ├── ⚡ core/                     # Core processing engine
│   │   ├── processors.py           # Base processor interfaces
│   │   ├── streams.py              # Async stream utilities
│   │   ├── results.py              # Railway-oriented programming
│   │   └── logging.py              # Structured logging + monitoring
│   ├── 🗂️ cache/                   # Intelligent caching system
│   │   ├── local.py                # Local filesystem cache
│   │   ├── memory.py               # In-memory cache layer
│   │   ├── policies.py             # Cache validation & TTL
│   │   └── storage.py              # Storage backends
│   ├── 📥 sources/                 # Data source processors
│   │   ├── s3.py                   # S3 integration
│   │   ├── parquet.py              # Parquet file processing
│   │   ├── kafka.py                # Kafka streaming source
│   │   ├── iceberg.py              # Iceberg table source
│   │   └── arrow.py                # Streaming Arrow source
│   ├── 📤 sinks/                   # Data sink processors  
│   │   ├── s3.py                   # S3 output
│   │   ├── parquet.py              # Parquet file output
│   │   ├── kafka.py                # Kafka streaming sink
│   │   ├── iceberg.py              # Iceberg table sink
│   │   └── delta.py                # Delta Lake integration
│   ├── 🔄 transforms/              # Data transformation processors
│   │   ├── polars.py               # Polars-based transforms
│   │   ├── duckdb.py               # DuckDB SQL transforms
│   │   ├── user.py                 # User-defined functions
│   │   └── arrow.py                # Arrow compute transforms
│   ├── 🎨 cli/                     # Command-line interface
│   │   ├── main.py                 # Main CLI entry point
│   │   ├── commands.py             # CLI command implementations
│   │   └── display.py              # Rich console utilities
│   └── 🔌 api/                     # High-level API
│       ├── pipeline.py             # Pipeline orchestration
│       ├── operators.py            # Functional operators
│       └── monitoring.py           # Metrics & monitoring
├── 🧪 tests/                       # Test suite
├── 📚 docs/                        # Documentation
├── 📋 examples/                    # Usage examples
├── ⚙️ pyproject.toml               # Project configuration
├── 🔒 .env.example                 # Environment template
└── 📖 README.md                    # This file
```

## 💡 Use Cases

### 1. **Batch ETL Pipeline**
Transform large datasets with intelligent caching:

```python
# Process daily sales data with caching
config = {
    "source": {
        "uri": "s3://data-lake/sales/date=2024-*/*.parquet",
        "format": "parquet",
        "cache": {
            "enabled": True,
            "immutable_only": True,  # Cache historical data
            "ttl_days": 30
        }
    },
    "transform": {
        "engine": "polars", 
        "operations": [
            {"type": "filter", "condition": "status == 'completed'"},
            {"type": "group_by", "columns": ["product_id"], "agg": "sum(amount)"},
            {"type": "with_columns", "expressions": ["current_date() as processed_date"]}
        ]
    },
    "sink": {"uri": "s3://analytics/sales-summary/", "format": "parquet"}
}
```

### 2. **Real-Time Stream Processing**
Process Kafka streams with low latency:

```python
# Real-time event processing
config = {
    "source": {
        "uri": "kafka://user-events",
        "format": "kafka",
        "kafka": {
            "bootstrap_servers": ["kafka1:9092", "kafka2:9092"],
            "group_id": "analytics-processor",
            "serialization": "json",
            "auto_offset_reset": "latest"
        }
    },
    "transform": {
        "engine": "polars",
        "operations": [
            {"type": "filter", "condition": "event_type == 'purchase'"},
            {"type": "with_columns", "expressions": [
                "amount * exchange_rate as amount_usd",
                "extract_country(ip_address) as country"
            ]}
        ]
    },
    "sink": {
        "uri": "kafka://processed-purchases", 
        "format": "kafka",
        "kafka": {"partition_key": "user_id", "compression_type": "snappy"}
    }
}
```

### 3. **Data Lake to Data Warehouse**
Move data between storage systems:

```bash
# Copy and transform data from S3 to Iceberg
lakepipe run \
  --source-uri "s3://raw-data/logs/2024/01/*/*.parquet" \
  --target-uri "iceberg://warehouse.logs" \
  --source-format parquet \
  --target-format iceberg \
  --cache \
  --verbose
```

### 4. **Multi-Format Pipeline**
Chain different data formats:

```python
# CSV → Parquet → DuckDB → Kafka pipeline
pipeline = (
    CSVSource("data/input.csv") +
    ParquetSink("temp/staging.parquet") +
    DuckDBTransform("SELECT *, ROW_NUMBER() OVER() as id FROM data") +
    KafkaSink("analytics-events")
)

result = await pipeline.execute()
```

## ⚙️ Configuration

Lakepipe supports multiple configuration methods:

### Environment Variables
```bash
# .env file
LAKEPIPE_SOURCE_URI=s3://data-lake/events/*.parquet
LAKEPIPE_SINK_URI=kafka://processed-events
LAKEPIPE_CACHE_ENABLED=true
LAKEPIPE_CACHE_MAX_SIZE=50GB
LAKEPIPE_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
LAKEPIPE_LOG_LEVEL=INFO
```

### JSON Configuration
```json
{
  "source": {
    "uri": "s3://data-lake/sales/*.parquet",
    "format": "parquet",
    "compression": "zstd"
  },
  "sink": {
    "uri": "kafka://processed-sales",
    "format": "kafka",
    "kafka": {
      "serialization": "json",
      "compression_type": "snappy"
    }
  },
  "transform": {
    "engine": "polars",
    "operations": [
      {"type": "filter", "condition": "amount > 0"},
      {"type": "with_columns", "expressions": ["amount * 1.1 as amount_with_tax"]}
    ]
  },
  "cache": {
    "enabled": true,
    "max_size": "10GB",
    "ttl_days": 7
  }
}
```

### CLI Arguments
```bash
lakepipe run \
  --config config.json \
  --source-uri "s3://override-bucket/data/*.parquet" \
  --cache \
  --kafka-servers "prod-kafka1:9092,prod-kafka2:9092" \
  --verbose
```

## 🚀 Performance Features

- **⚡ Zero-Copy**: Direct memory sharing between Polars, DuckDB, and PyArrow
- **🗂️ Smart Caching**: Automatic caching of immutable datasets with configurable TTL
- **📡 Async Streaming**: High-throughput Kafka processing with configurable batching
- **🔄 Backpressure**: Automatic flow control to prevent memory exhaustion
- **📊 Monitoring**: Built-in metrics for throughput, latency, and resource usage
- **🎯 Type Safety**: Compile-time error detection with full mypy support

## 🧪 Testing

```bash
# Run basic functionality tests
python test_basic.py

# Run full test suite (when available)
pytest tests/ -v

# Test CLI functionality
lakepipe --help
lakepipe version
lakepipe config-example
```

## 🤝 Contributing

We welcome contributions! Here's how to get started:

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/amazing-feature`
3. **Make your changes** with tests
4. **Run the test suite**: `python test_basic.py`
5. **Submit a pull request**

### Development Setup

```bash
# Clone and setup
git clone https://github.com/your-org/lakepipe.git
cd lakepipe
python3 -m venv venv
source venv/bin/activate

# Install development dependencies
pip install -e .[dev]

# Run type checking
mypy lakepipe/

# Run linting
ruff check lakepipe/
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **[GenAI Processors](https://github.com/google-gemini/genai-processors)** - Inspiration for stream-based architecture
- **[Polars](https://pola.rs/)** - Lightning-fast DataFrames for Python
- **[DuckDB](https://duckdb.org/)** - Fast analytical database
- **[Rich](https://rich.readthedocs.io/)** - Beautiful terminal output
- **[Typer](https://typer.tiangolo.com/)** - Modern CLI framework

---

<div align="center">

**🌊 Built with ❤️ for the data engineering community**

[Documentation](docs/) • [Examples](examples/) • [Issues](https://github.com/your-org/lakepipe/issues) • [Discussions](https://github.com/your-org/lakepipe/discussions)

</div> 