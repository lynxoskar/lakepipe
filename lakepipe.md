# Lakepipe - Standalone Lakehouse Pipelines

**Lakepipe v0.1.0** is a standalone functional data pipeline library that provides unified access to multiple lakehouse formats (Iceberg, DuckLake, Delta Lake) with railway-oriented programming patterns.

references 
reference for ducklake: https://uithub.com/duckdb/ducklake/tree/main/src
reference for genai:  https://uithub.com/google-gemini/genai-processors


## Features

- **Multi-format Support**: Iceberg, DuckLake, Delta Lake, Parquet, CSV
- **Functional Programming**: Railway-oriented error handling with Result types
- **Zero-copy Operations**: Efficient data transfer between Polars, DuckDB, and PyArrow
- **Time Travel**: Query historical versions of data
- **Schema Evolution**: Automatic schema evolution for compatible formats
- **Streaming Support**: Memory-efficient processing of large datasets

## Installation

```bash
# Install lakepipe standalone
pip install lakepipe

# Or install with all dependencies
pip install "lakepipe[all]"
```

## Quick Start

```bash
# Simple lakehouse pipeline
lakepipe run \
    --source-uri data/input.parquet \
    --target-uri data/output \
    --source-format parquet \
    --target-format iceberg

# Streaming pipeline for large datasets
lakepipe run \
    --source-uri data/large_dataset.parquet \
    --target-uri data/processed \
    --streaming \
    --batch-size 50000
```

## Python API

```python
import lakepipe as lp

# Create pipeline configuration
config = lp.get_default_pipeline_config()
config['source']['uri'] = 'data/input.parquet'
config['target']['uri'] = 'data/output'
config['target']['format'] = 'iceberg'

# Run pipeline
pipeline = lp.create_unified_lakehouse_pipeline(config)
result = pipeline()

if result.is_success():
    print(f"Success: {result.unwrap()}")
else:
    print(f"Error: {result.failure()}")
```

## Project Structure

```
lakepipe/                    # Standalone lakehouse pipelines (v0.1.0)
├── __init__.py              # Lakepipe module exports
├── types.py                 # Lakehouse type definitions
├── cli.py                   # Lakepipe CLI
├── pyproject.toml           # Lakepipe package configuration
├── README.md                # Lakepipe documentation
└── __main__.py              # Lakepipe entry point
```

## Status

**Note**: This is a planned feature that has been documented but not yet implemented. The lakepipe directory and functionality do not currently exist in the codebase. 