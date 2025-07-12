# Datpipe Python Style Guide

## Core Principles

### 1. Railway-Oriented Programming
- Use `Result[T, E]` types for all operations that can fail
- Chain operations using pipe operators
- Handle errors as values, not exceptions

### 2. Immutability First
- Use frozen dataclasses and NamedTuples for data structures
- Never mutate data in-place
- Return new values instead of modifying existing ones

### 3. Pure Functions
- Functions should have no side effects
- Same inputs always produce same outputs
- IO operations should be isolated and explicit

### 4. Typed Dictionaries
- Use TypedDict for configuration and structured data
- No abstract base classes or protocols
- Prefer composition over inheritance

## Code Patterns

### Configuration Pattern
```python
from typing import TypedDict, Optional
from pathlib import Path

class LogConfig(TypedDict):
    level: str
    format: str
    file: Optional[Path]

class StorageConfig(TypedDict):
    uri: str  # "file:///path" or "s3://bucket"
    format: str  # "json", "parquet", etc.

class ModuleConfig(TypedDict):
    log: LogConfig
    storage: StorageConfig
    api_key: Optional[str]

# Default values as functions
def get_default_log_config() -> LogConfig:
    return {
        "level": "INFO",
        "format": "%(time)s | %(level)s | %(message)s",
        "file": None
    }
```

### Railway Function Pattern
```python
from returns.result import Result, Success, Failure
from returns.pipeline import pipe
from typing import Callable

# Pure validation function
def validate_input(data: dict) -> Result[dict, str]:
    if not data.get("required_field"):
        return Failure("Missing required field")
    return Success(data)

# Pure transformation function
def transform_data(data: dict) -> Result[ProcessedData, str]:
    try:
        processed = ProcessedData(**data)
        return Success(processed)
    except Exception as e:
        return Failure(f"Transform failed: {e}")

# Curried function for configuration
def store_with_config(config: StorageConfig) -> Callable[[ProcessedData], Result[Path, str]]:
    def store(data: ProcessedData) -> Result[Path, str]:
        # Implementation here
        pass
    return store

# Compose into pipeline
def process_pipeline(config: ModuleConfig) -> Callable[[dict], Result[Path, str]]:
    return lambda data: pipe(
        data,
        validate_input,
        transform_data,
        store_with_config(config["storage"])
    )
```

### Module CLI Pattern
```python
import click
from functools import reduce
from typing import Dict, Any

@click.command()
@click.option("--log-level", default="INFO")
@click.option("--storage-uri", default="file:///tmp/data")
@click.option("--parent-config", type=click.Path(exists=True))
def main(log_level: str, storage_uri: str, parent_config: Optional[str]):
    # Build config by merging defaults with overrides
    configs = [
        get_default_config(),
        load_parent_config(parent_config) if parent_config else {},
        {"log": {"level": log_level}, "storage": {"uri": storage_uri}}
    ]
    config = reduce(deep_merge, configs)
    
    # Execute pipeline
    result = process_pipeline(config)(input_data)
    
    # Handle result
    result.map(lambda p: logger.info(f"Success: {p}"))
    result.map_failure(lambda e: logger.error(f"Failed: {e}"))
```

### Functional Utilities
```python
from toolz import curry, compose, pipe as toolz_pipe
from typing import TypeVar, Callable

T = TypeVar("T")
U = TypeVar("U")

# Curry for partial application
@curry
def map_dict(key: str, func: Callable[[Any], Any], data: dict) -> dict:
    return {**data, key: func(data.get(key))}

# Function composition
process = compose(
    validate_data,
    transform_data,
    enrich_data
)

# Pipeline with toolz
result = toolz_pipe(
    raw_data,
    clean_data,
    validate_data,
    store_data(config)
)
```

## File Organization

### Module Structure
```
modules/weather/
├── __init__.py      # Module exports
├── __main__.py      # CLI entry point
├── types.py         # TypedDict and dataclass definitions
├── fetch.py         # Pure data fetching functions
├── transform.py     # Pure transformation functions
├── pipeline.py      # Function composition
└── cli.py           # Click CLI implementation
```

### Naming Conventions
- Functions: `snake_case` and descriptive
- Types: `PascalCase` for classes and TypedDict
- Constants: `UPPER_SNAKE_CASE`
- Private functions: Leading underscore `_helper_function`

## Best Practices

### 1. Error Handling
- Always return Result types for fallible operations
- Use Failure to wrap error messages
- Chain error handling with map_failure

### 2. Testing
- Test pure functions in isolation
- Use property-based testing for transformations
- Mock only at IO boundaries

### 3. Dependencies
- Inject all dependencies as parameters
- Use currying for partial application
- No global state or singletons

### 4. Documentation
- Document types, not implementation
- Focus on what, not how
- Include examples in docstrings

## Anti-Patterns to Avoid

### ❌ Don't Use
- Classes with mutable state
- Global variables
- Abstract base classes
- Inheritance hierarchies
- Exception-based error handling
- Side effects in business logic

### ✅ Do Use
- Immutable data structures
- Function parameters
- TypedDict for structure
- Function composition
- Result-based error handling
- Pure functions with explicit IO