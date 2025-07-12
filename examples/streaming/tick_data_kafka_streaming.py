#!/usr/bin/env python3
"""
🚀 Basic Tick Data Kafka Streaming Example

This example demonstrates how to use lakepipe for real-time tick data processing
from Kafka, showing basic filtering, transformation, and output to Parquet.

Usage:
    python examples/streaming/tick_data_kafka_streaming.py
    python examples/streaming/tick_data_kafka_streaming.py --symbol AAPL
    python examples/streaming/tick_data_kafka_streaming.py --config examples/configs/streaming_config.yaml
"""

import asyncio
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.panel import Panel

# Add lakepipe to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from lakepipe.api.pipeline import create_pipeline
from lakepipe.config.defaults import build_pipeline_config
from lakepipe.config.loaders import load_config_file
from lakepipe.core.logging import configure_logging, get_logger

console = Console()
logger = get_logger(__name__)


def main(
    symbol: Optional[str] = typer.Option(None, "--symbol", "-s", help="Filter by symbol"),
    config_file: Optional[Path] = typer.Option(None, "--config", "-c", help="Configuration file"),
    output_dir: str = typer.Option("output/streaming", "--output", "-o", help="Output directory"),
    batch_size: int = typer.Option(10000, "--batch-size", help="Batch size for processing"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
):
    """
    🚀 Stream tick data from Kafka and process with lakepipe
    
    This example shows:
    - Real-time tick data ingestion from Kafka
    - Basic filtering and data quality checks
    - Calculation of basic metrics (spread, notional value)
    - Output to compressed Parquet files
    """
    
    console.print(Panel(
        "🚀 [bold blue]Tick Data Kafka Streaming[/bold blue]\n"
        "Processing real-time market data with lakepipe",
        title="📡 Streaming Example",
        border_style="blue"
    ))
    
    # Build configuration
    if config_file:
        file_config = load_config_file(config_file)
        console.print(f"✅ Loaded configuration from {config_file}")
    else:
        file_config = {}
        console.print("📋 Using default configuration")
    
    # Override with CLI parameters
    cli_config = {
        "source": {
            "uri": "kafka://market-data-ticks",
            "format": "kafka",
            "kafka": {
                "bootstrap_servers": ["localhost:9092"],
                "group_id": "lakepipe-tick-streaming",
                "auto_offset_reset": "latest"
            }
        },
        "sink": {
            "uri": f"file://{output_dir}/tick-data/",
            "format": "parquet",
            "compression": "zstd",
            "partition_cols": ["symbol", "date"]
        },
        "transform": {
            "engine": "duckdb",
            "operations": [
                {
                    "type": "filter",
                    "condition": "price > 0 AND quantity > 0 AND bid_price > 0 AND ask_price > 0"
                },
                {
                    "type": "with_columns",
                    "expressions": [
                        "extract('date', timestamp) as date",
                        "extract('hour', timestamp) as hour",
                        "extract('minute', timestamp) as minute",
                        "price * quantity as notional",
                        "case when bid_price > 0 then (ask_price - bid_price) / bid_price else null end as spread_pct",
                        "case when ask_price > 0 and bid_price > 0 then (price - (ask_price + bid_price) / 2) / ((ask_price + bid_price) / 2) else null end as mid_price_basis_pct"
                    ]
                }
            ]
        },
        "streaming": {
            "enabled": True,
            "batch_size": batch_size,
            "max_latency_ms": 100
        },
        "log": {
            "level": "DEBUG" if verbose else "INFO"
        }
    }
    
    # Add symbol filter if specified
    if symbol:
        filter_condition = cli_config["transform"]["operations"][0]["condition"]
        cli_config["transform"]["operations"][0]["condition"] = f"({filter_condition}) AND symbol = '{symbol}'"
        console.print(f"🎯 Filtering for symbol: {symbol}")
    
    # Merge configurations
    config = build_pipeline_config(
        config_overrides={**file_config, **cli_config}
    )
    
    # Configure logging
    configure_logging(config["log"])
    
    console.print(f"📤 Output directory: {output_dir}")
    console.print(f"📦 Batch size: {batch_size}")
    
    # Create and run pipeline
    async def run_pipeline():
        try:
            console.print("🚀 [bold green]Starting streaming pipeline...[/bold green]")
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
                transient=True
            ) as progress:
                task = progress.add_task("Processing tick data...", total=None)
                
                pipeline = create_pipeline(config)
                
                # Run pipeline (this will run indefinitely for streaming)
                async for batch_result in pipeline.stream():
                    if batch_result:
                        progress.update(
                            task,
                            description=f"Processed {batch_result.get('record_count', 0)} records"
                        )
                        
                        if verbose:
                            console.print(f"📊 Batch metrics: {batch_result}")
                
        except KeyboardInterrupt:
            console.print("\n⏹️  [yellow]Streaming stopped by user[/yellow]")
        except Exception as e:
            console.print(f"❌ [red]Pipeline error: {e}[/red]")
            if verbose:
                console.print_exception()
            raise
    
    # Run the async pipeline
    asyncio.run(run_pipeline())


if __name__ == "__main__":
    typer.run(main)
