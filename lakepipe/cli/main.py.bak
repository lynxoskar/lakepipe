"""
Main CLI entry point for lakepipe using typer.
"""

import typer
from typing import Optional
from pathlib import Path
import asyncio
import time
import orjson
from rich.console import Console
from rich.progress import Progress

from lakepipe.config.defaults import build_pipeline_config
from lakepipe.config.types import PipelineConfig
from lakepipe.core.logging import configure_logging, get_logger
from lakepipe.api.pipeline import create_pipeline, execute_pipeline

# Initialize Rich console and typer app
console = Console()
app = typer.Typer(
    name="lakepipe",
    help="Modern, functional data pipeline library for high-performance batch and streaming transformations",
    add_completion=False,
    rich_markup_mode="rich",
)

# Global logger
logger = get_logger(__name__)

@app.command()
def run(
    source_uri: str = typer.Option(
        ..., 
        "--source-uri", 
        "-s",
        help="Source data URI (supports s3://, file://, kafka://topic)"
    ),
    target_uri: str = typer.Option(
        ..., 
        "--target-uri", 
        "-t",
        help="Target data URI (supports s3://, file://, kafka://topic)"
    ),
    source_format: str = typer.Option(
        "parquet", 
        "--source-format",
        help="Source format (parquet, csv, iceberg, ducklake, arrow, kafka)"
    ),
    target_format: str = typer.Option(
        "parquet", 
        "--target-format",
        help="Target format (parquet, iceberg, delta, csv, kafka)"
    ),
    streaming: bool = typer.Option(
        False, 
        "--streaming", 
        help="Enable streaming mode"
    ),
    batch_size: int = typer.Option(
        100_000, 
        "--batch-size", 
        help="Batch size for streaming"
    ),
    enable_cache: bool = typer.Option(
        False, 
        "--cache", 
        help="Enable local caching"
    ),
    cache_dir: str = typer.Option(
        "/tmp/lakepipe_cache", 
        "--cache-dir", 
        help="Cache directory"
    ),
    cache_size: str = typer.Option(
        "10GB", 
        "--cache-size", 
        help="Max cache size"
    ),
    kafka_servers: Optional[str] = typer.Option(
        None, 
        "--kafka-servers", 
        help="Kafka bootstrap servers (comma-separated)"
    ),
    kafka_group: Optional[str] = typer.Option(
        None, 
        "--kafka-group", 
        help="Kafka consumer group"
    ),
    config_file: Optional[Path] = typer.Option(
        None, 
        "--config", 
        "-c",
        help="Configuration file (JSON)"
    ),
    env_file: str = typer.Option(
        ".env", 
        "--env-file", 
        help="Environment file"
    ),
    verbose: bool = typer.Option(
        False, 
        "--verbose", 
        "-v", 
        help="Verbose output"
    ),
    dry_run: bool = typer.Option(
        False, 
        "--dry-run", 
        help="Dry run mode - validate configuration without executing"
    ),
):
    """
    Run lakepipe data pipeline with caching and Kafka support.
    
    Examples:
        lakepipe run --source-uri s3://bucket/data/*.parquet --target-uri file:///output/
        lakepipe run --source-uri kafka://events --target-uri s3://processed/ --streaming
        lakepipe run --config config.json --verbose
    """
    
    # Build configuration using environment-aware builder + orjson for config files
    file_config = {}
    if config_file:
        try:
            with open(config_file, 'rb') as f:
                file_config = orjson.loads(f.read())
            console.print(f"✅ Loaded configuration from {config_file}")
        except Exception as e:
            console.print(f"❌ Failed to load config file {config_file}: {e}")
            raise typer.Exit(1)
    
    # Merge CLI args with file config and environment variables
    cli_overrides = {
        "source": {"uri": source_uri, "format": source_format},
        "sink": {"uri": target_uri, "format": target_format},
        "streaming": {"enabled": streaming, "batch_size": batch_size},
        "cache": {"enabled": enable_cache, "cache_dir": cache_dir, "max_size": cache_size},
        "log": {"level": "DEBUG" if verbose else "INFO"}
    }
    
    if kafka_servers:
        cli_overrides["source"]["kafka"] = {"bootstrap_servers": kafka_servers.split(",")}
        if kafka_group:
            cli_overrides["source"]["kafka"]["group_id"] = kafka_group
    
    try:
        # Build type-safe configuration
        config = build_pipeline_config(
            config_overrides={**file_config, **cli_overrides},
            env_file=env_file
        )
        
        # Configure logging first
        configure_logging(config["log"])
        
        if dry_run:
            console.print("🔍 [bold blue]Dry run mode - validating configuration[/bold blue]")
            console.print("✅ Configuration is valid")
            console.print(f"📝 Source: {config['source']['uri']} ({config['source']['format']})")
            console.print(f"📝 Target: {config['sink']['uri']} ({config['sink']['format']})")
            console.print(f"📝 Cache: {'Enabled' if config['cache']['enabled'] else 'Disabled'}")
            console.print(f"📝 Streaming: {'Enabled' if config['streaming']['batch_size'] > 0 else 'Disabled'}")
            return
        
        # Execute pipeline with rich progress display
        console.print("🚀 [bold green]Starting lakepipe pipeline[/bold green]")
        asyncio.run(execute_pipeline_with_display(config))
        
    except Exception as e:
        console.print(f"❌ [bold red]Pipeline execution failed: {e}[/bold red]")
        if verbose:
            console.print_exception()
        raise typer.Exit(1)

@app.command()
def cache(
    action: str = typer.Argument(
        help="Cache action to perform (status, clear, cleanup)"
    ),
    cache_dir: str = typer.Option(
        "/tmp/lakepipe_cache", 
        "--cache-dir", 
        help="Cache directory"
    ),
    verbose: bool = typer.Option(
        False, 
        "--verbose", 
        "-v", 
        help="Verbose output"
    ),
):
    """
    Manage local cache.
    
    Examples:
        lakepipe cache status
        lakepipe cache clear --cache-dir /custom/cache
        lakepipe cache cleanup
    """
    
    console.print(f"🗂️  Managing cache in {cache_dir}")
    
    if action == "status":
        _display_cache_status(cache_dir, verbose)
    elif action == "clear":
        _clear_cache(cache_dir)
    elif action == "cleanup":
        _cleanup_cache(cache_dir)

@app.command()
def test_kafka(
    servers: str = typer.Option(
        "localhost:9092", 
        "--servers", 
        help="Kafka bootstrap servers"
    ),
    topic: str = typer.Option(
        ..., 
        "--topic", 
        help="Topic to test"
    ),
    mode: str = typer.Option(
        "consume", 
        "--mode", 
        help="Test mode (produce, consume)"
    ),
    timeout: int = typer.Option(
        30, 
        "--timeout", 
        help="Test timeout in seconds"
    ),
):
    """
    Test Kafka connectivity.
    
    Examples:
        lakepipe test-kafka --topic test-topic --mode consume
        lakepipe test-kafka --servers broker1:9092,broker2:9092 --topic events --mode produce
    """
    
    console.print(f"🔌 Testing Kafka connectivity to {servers}")
    
    async def test_kafka_async():
        if mode == "consume":
            await _test_kafka_consumer(servers.split(','), topic, timeout)
        else:
            await _test_kafka_producer(servers.split(','), topic, timeout)
    
    try:
        asyncio.run(test_kafka_async())
    except Exception as e:
        console.print(f"❌ [bold red]Kafka test failed: {e}[/bold red]")
        raise typer.Exit(1)

@app.command()
def version():
    """Show lakepipe version information."""
    from lakepipe import __version__
    console.print(f"🌊 Lakepipe version: [bold blue]{__version__}[/bold blue]")

@app.command()
def config_example():
    """Generate example configuration files."""
    
    # Generate example .env file
    env_content = """# Lakepipe Environment Configuration

# Logging configuration
LAKEPIPE_LOG_LEVEL=INFO
LAKEPIPE_LOG_FORMAT=%(time)s | %(level)s | %(name)s | %(message)s
LAKEPIPE_LOG_FILE=/var/log/lakepipe.log

# Source configuration
LAKEPIPE_SOURCE_URI=s3://data-lake/raw/*.parquet
LAKEPIPE_SOURCE_FORMAT=parquet
LAKEPIPE_SOURCE_COMPRESSION=zstd

# Sink configuration
LAKEPIPE_SINK_URI=s3://data-lake/processed/
LAKEPIPE_SINK_FORMAT=parquet
LAKEPIPE_SINK_COMPRESSION=zstd

# Cache configuration
LAKEPIPE_CACHE_ENABLED=true
LAKEPIPE_CACHE_DIR=/opt/lakepipe/cache
LAKEPIPE_CACHE_MAX_SIZE=50GB
LAKEPIPE_CACHE_TTL_DAYS=30

# Kafka configuration
LAKEPIPE_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
LAKEPIPE_KAFKA_GROUP_ID=lakepipe-processors
LAKEPIPE_KAFKA_SERIALIZATION=json
"""
    
    # Generate example JSON config
    json_config = {
        "source": {
            "uri": "s3://data-lake/raw/*.parquet",
            "format": "parquet",
            "compression": "zstd"
        },
        "sink": {
            "uri": "s3://data-lake/processed/",
            "format": "parquet",
            "compression": "zstd"
        },
        "transform": {
            "engine": "polars",
            "operations": [
                {
                    "type": "filter",
                    "condition": "amount > 0"
                },
                {
                    "type": "with_columns",
                    "expressions": [
                        "amount * 1.1 as amount_with_tax"
                    ]
                }
            ]
        },
        "cache": {
            "enabled": True,
            "cache_dir": "/opt/lakepipe/cache",
            "max_size": "50GB",
            "ttl_days": 30
        }
    }
    
    # Write example files
    with open(".env.example", "w") as f:
        f.write(env_content)
    
    with open("lakepipe.config.json", "wb") as f:
        f.write(orjson.dumps(json_config, option=orjson.OPT_INDENT_2))
    
    console.print("✅ Generated example configuration files:")
    console.print("  📄 .env.example")
    console.print("  📄 lakepipe.config.json")

# Helper functions

async def execute_pipeline_with_display(config: PipelineConfig):
    """Execute pipeline with rich progress display"""
    
    with Progress(console=console) as progress:
        task = progress.add_task("Processing data...", total=None)
        
        try:
            # Create and execute pipeline
            pipeline = create_pipeline(config)
            result = await pipeline.execute()
            
            progress.update(task, completed=True)
            
            if result:
                console.print("✅ [bold green]Pipeline completed successfully[/bold green]")
                
                # Display metrics if available
                if hasattr(result, 'metrics'):
                    console.print(f"📊 Processed: {result.metrics.get('processed_count', 0)} records")
                    console.print(f"⏱️  Duration: {result.metrics.get('processing_time', 0):.2f}s")
                    console.print(f"🚀 Throughput: {result.metrics.get('throughput', 0):.2f} records/s")
            else:
                console.print("⚠️  [bold yellow]Pipeline completed with no results[/bold yellow]")
                
        except Exception as e:
            progress.update(task, completed=True)
            console.print(f"❌ [bold red]Pipeline failed: {e}[/bold red]")
            raise

def _display_cache_status(cache_dir: str, verbose: bool):
    """Display cache status"""
    import os
    from pathlib import Path
    
    cache_path = Path(cache_dir)
    if not cache_path.exists():
        console.print(f"📁 Cache directory {cache_dir} does not exist")
        return
    
    # Count cache files and total size
    cache_files = list(cache_path.glob("*.parquet"))
    total_size = sum(f.stat().st_size for f in cache_files)
    
    console.print(f"📊 Cache Status:")
    console.print(f"  📁 Directory: {cache_dir}")
    console.print(f"  📄 Files: {len(cache_files)}")
    console.print(f"  💾 Total size: {total_size / 1024 / 1024:.2f} MB")
    
    if verbose and cache_files:
        console.print("  📋 Files:")
        for file in cache_files[:10]:  # Show first 10 files
            size = file.stat().st_size / 1024 / 1024
            console.print(f"    • {file.name} ({size:.2f} MB)")
        
        if len(cache_files) > 10:
            console.print(f"    ... and {len(cache_files) - 10} more files")

def _clear_cache(cache_dir: str):
    """Clear all cache files"""
    import shutil
    from pathlib import Path
    
    cache_path = Path(cache_dir)
    if cache_path.exists():
        shutil.rmtree(cache_path)
        console.print(f"🗑️  Cleared cache directory {cache_dir}")
    else:
        console.print(f"📁 Cache directory {cache_dir} does not exist")

def _cleanup_cache(cache_dir: str):
    """Clean up expired cache files"""
    import time
    from pathlib import Path
    
    cache_path = Path(cache_dir)
    if not cache_path.exists():
        console.print(f"📁 Cache directory {cache_dir} does not exist")
        return
    
    # Remove files older than 7 days
    cutoff_time = time.time() - (7 * 24 * 3600)
    removed_count = 0
    
    for file in cache_path.glob("*.parquet"):
        if file.stat().st_mtime < cutoff_time:
            file.unlink()
            removed_count += 1
    
    console.print(f"🧹 Cleaned up {removed_count} expired cache files")

async def _test_kafka_consumer(servers: list[str], topic: str, timeout: int):
    """Test Kafka consumer connectivity"""
    try:
        from aiokafka import AIOKafkaConsumer
        
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=servers,
            auto_offset_reset='latest',
            group_id='lakepipe-test'
        )
        
        console.print(f"🔌 Connecting to Kafka servers: {', '.join(servers)}")
        await consumer.start()
        
        console.print(f"✅ Successfully connected to Kafka")
        console.print(f"📡 Listening to topic '{topic}' for {timeout} seconds...")
        
        try:
            msg_count = 0
            async with asyncio.timeout(timeout):
                async for msg in consumer:
                    msg_count += 1
                    console.print(f"📨 Received message {msg_count}: {msg.value[:100]}...")
                    
                    if msg_count >= 10:  # Limit to 10 messages
                        break
            
            console.print(f"✅ Successfully received {msg_count} messages")
            
        except asyncio.TimeoutError:
            console.print(f"⏰ Timeout after {timeout} seconds")
            console.print("✅ Kafka consumer test completed")
        
        finally:
            await consumer.stop()
            
    except Exception as e:
        console.print(f"❌ Kafka consumer test failed: {e}")
        raise

async def _test_kafka_producer(servers: list[str], topic: str, timeout: int):
    """Test Kafka producer connectivity"""
    try:
        from aiokafka import AIOKafkaProducer
        import orjson
        
        producer = AIOKafkaProducer(
            bootstrap_servers=servers,
            value_serializer=lambda v: orjson.dumps(v)
        )
        
        console.print(f"🔌 Connecting to Kafka servers: {', '.join(servers)}")
        await producer.start()
        
        console.print(f"✅ Successfully connected to Kafka")
        console.print(f"📤 Sending test messages to topic '{topic}'...")
        
        try:
            # Send 5 test messages
            for i in range(5):
                test_message = {
                    "test_id": i,
                    "message": f"Test message {i}",
                    "timestamp": time.time()
                }
                
                await producer.send(topic, test_message)
                console.print(f"📨 Sent message {i + 1}")
                
                await asyncio.sleep(1)
            
            await producer.flush()
            console.print("✅ Successfully sent all test messages")
            
        finally:
            await producer.stop()
            
    except Exception as e:
        console.print(f"❌ Kafka producer test failed: {e}")
        raise

if __name__ == "__main__":
    app() 