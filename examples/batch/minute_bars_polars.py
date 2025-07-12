#!/usr/bin/env python3
"""
📊 Minute Bars Processing with Polars

This example demonstrates high-performance batch processing of minute bar data
using lakepipe with Polars for efficient OHLCV data transformation.

Usage:
    python examples/batch/minute_bars_polars.py
    python examples/batch/minute_bars_polars.py --input-file examples/data/sample_minute_bars.parquet
    python examples/batch/minute_bars_polars.py --symbols AAPL,GOOGL --date-range 2024-01-01:2024-01-31
"""

import asyncio
import sys
from pathlib import Path
from typing import Optional, List
from datetime import datetime, timedelta

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn
from rich.panel import Panel
from rich.table import Table

# Add lakepipe to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from lakepipe.api.pipeline import create_pipeline
from lakepipe.config.defaults import build_pipeline_config
from lakepipe.config.loaders import load_config_file
from lakepipe.core.logging import configure_logging, get_logger

console = Console()
logger = get_logger(__name__)


def main(
    input_file: str = typer.Option("examples/data/sample_minute_bars.parquet", "--input-file", "-i", help="Input Parquet file"),
    output_dir: str = typer.Option("output/minute-bars", "--output-dir", "-o", help="Output directory"),
    symbols: Optional[str] = typer.Option(None, "--symbols", "-s", help="Comma-separated symbols to process"),
    date_range: Optional[str] = typer.Option(None, "--date-range", help="Date range (YYYY-MM-DD:YYYY-MM-DD)"),
    config_file: Optional[Path] = typer.Option(None, "--config", "-c", help="Configuration file"),
    enable_cache: bool = typer.Option(True, "--cache/--no-cache", help="Enable caching"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
):
    """
    📊 High-performance minute bar processing
    
    This example shows:
    - Efficient OHLCV data processing with Polars
    - Statistical calculations (returns, volatility, ranges)
    - Data quality validation and filtering
    - Optimized Parquet output with partitioning
    """
    
    console.print(Panel(
        f"📊 [bold blue]Minute Bars Processing[/bold blue]\n"
        f"Input: {input_file}\n"
        f"Output: {output_dir}\n"
        f"Symbols: {symbols or 'All'}\n"
        f"Date Range: {date_range or 'All'}",
        title="📈 Batch Processing",
        border_style="blue"
    ))
    
    # Parse parameters
    symbol_list = symbols.split(',') if symbols else None
    start_date, end_date = None, None
    if date_range:
        try:
            start_str, end_str = date_range.split(':')
            start_date = datetime.strptime(start_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_str, '%Y-%m-%d')
        except ValueError:
            console.print("❌ Invalid date range format. Use YYYY-MM-DD:YYYY-MM-DD")
            raise typer.Exit(1)
    
    # Build configuration
    if config_file:
        file_config = load_config_file(config_file)
        console.print(f"✅ Loaded configuration from {config_file}")
    else:
        file_config = {}
    
    # Build filter conditions
    filter_conditions = ["open > 0", "high > 0", "low > 0", "close > 0", "volume > 0"]
    
    if symbol_list:
        symbol_filter = "symbol IN ('" + "', '".join(symbol_list) + "')"
        filter_conditions.append(symbol_filter)
    
    if start_date and end_date:
        date_filter = f"timestamp >= '{start_date.strftime('%Y-%m-%d')}' AND timestamp <= '{end_date.strftime('%Y-%m-%d')}'"
        filter_conditions.append(date_filter)
    
    cli_config = {
        "source": {
            "uri": f"file://{input_file}",
            "format": "parquet"
        },
        "sink": {
            "uri": f"file://{output_dir}/processed/",
            "format": "parquet",
            "compression": "zstd",
            "partition_cols": ["symbol", "date"],
            "write_options": {
                "row_group_size": 100000,
                "use_dictionary": True
            }
        },
        "transform": {
            "engine": "polars",
            "operations": [
                {
                    "type": "filter",
                    "condition": " AND ".join(filter_conditions)
                },
                {
                    "type": "with_columns",
                    "expressions": [
                        "extract('date', timestamp) as date",
                        "extract('hour', timestamp) as hour",
                        "extract('minute', timestamp) as minute",
                        "extract('dayofweek', timestamp) as day_of_week",
                        "(high - low) as range_abs",
                        "(high - low) / low as range_pct",
                        "(close - open) as change_abs",
                        "(close - open) / open as return_pct",
                        "log(close / open) as log_return",
                        "close * volume as dollar_volume",
                        "case when high = low then 0 else (close - low) / (high - low) end as williams_r",
                        "case when open = close then 0 else (close - open) / (high - low) end as true_range"
                    ]
                },
                {
                    "type": "window_function",
                    "window_spec": {
                        "partition_by": ["symbol"],
                        "order_by": ["timestamp"],
                        "frame": "rows between 19 preceding and current row"
                    },
                    "expressions": [
                        "avg(close) over w as sma_20",
                        "avg(volume) over w as avg_volume_20",
                        "avg(dollar_volume) over w as avg_dollar_volume_20",
                        "stddev(close) over w as volatility_20",
                        "stddev(return_pct) over w as return_volatility_20",
                        "min(low) over w as low_20",
                        "max(high) over w as high_20",
                        "sum(volume) over w as volume_20"
                    ]
                },
                {
                    "type": "window_function",
                    "window_spec": {
                        "partition_by": ["symbol"],
                        "order_by": ["timestamp"],
                        "frame": "rows between 49 preceding and current row"
                    },
                    "expressions": [
                        "avg(close) over w as sma_50",
                        "avg(volume) over w as avg_volume_50",
                        "stddev(close) over w as volatility_50",
                        "min(low) over w as low_50",
                        "max(high) over w as high_50"
                    ]
                },
                {
                    "type": "with_columns",
                    "expressions": [
                        "case when sma_20 > sma_50 then 1 else 0 end as trend_signal",
                        "case when volatility_20 > 0 then (close - sma_20) / volatility_20 else 0 end as zscore",
                        "case when avg_volume_20 > 0 then volume / avg_volume_20 else 0 end as volume_ratio",
                        "case when volume > avg_volume_20 * 2 then 1 else 0 end as volume_spike",
                        "case when high_20 > 0 and low_20 > 0 then (close - low_20) / (high_20 - low_20) else 0 end as stoch_k",
                        "case when volatility_20 > 0 then volatility_20 * sqrt(252) else 0 end as annualized_volatility"
                    ]
                },
                {
                    "type": "with_columns",
                    "expressions": [
                        "case when abs(return_pct) > 0.05 then 1 else 0 end as large_move",
                        "case when volume < avg_volume_20 * 0.1 then 1 else 0 end as low_volume_flag",
                        "case when range_pct > 0.02 then 1 else 0 end as high_range_flag",
                        "case when abs(zscore) > 2 then 1 else 0 end as outlier_flag"
                    ]
                }
            ]
        },
        "cache": {
            "enabled": enable_cache,
            "cache_dir": "/tmp/lakepipe_minute_bars_cache",
            "max_size": "10GB",
            "ttl_days": 7
        },
        "log": {
            "level": "DEBUG" if verbose else "INFO"
        }
    }
    
    # Merge configurations
    config = build_pipeline_config(
        config_overrides={**file_config, **cli_config}
    )
    
    # Configure logging
    configure_logging(config["log"])
    
    # Processing statistics
    processing_stats = {
        'total_records': 0,
        'symbols_processed': set(),
        'processing_time': 0,
        'cache_hits': 0,
        'outliers_detected': 0,
        'volume_spikes': 0,
        'large_moves': 0
    }
    
    # Create and run pipeline
    async def run_pipeline():
        nonlocal processing_stats
        
        try:
            console.print("🚀 [bold green]Starting minute bars processing pipeline...[/bold green]")
            
            start_time = datetime.now()
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeRemainingColumn(),
                console=console,
                transient=False
            ) as progress:
                task = progress.add_task("Processing minute bars...", total=None)
                
                pipeline = create_pipeline(config)
                result = await pipeline.execute()
                
                processing_stats['processing_time'] = (datetime.now() - start_time).total_seconds()
                
                if result:
                    processing_stats['total_records'] = result.get('processed_count', 0)
                    
                    # Additional metrics if available
                    if hasattr(result, 'metrics'):
                        metrics = result.metrics
                        processing_stats['cache_hits'] = metrics.get('cache_hits', 0)
                        processing_stats['outliers_detected'] = metrics.get('outliers_detected', 0)
                        processing_stats['volume_spikes'] = metrics.get('volume_spikes', 0)
                        processing_stats['large_moves'] = metrics.get('large_moves', 0)
                
                progress.update(task, completed=True)
                console.print("✅ [bold green]Processing completed successfully![/bold green]")
                
            display_processing_summary()
            
        except Exception as e:
            console.print(f"❌ [red]Pipeline error: {e}[/red]")
            if verbose:
                console.print_exception()
            raise
    
    def display_processing_summary():
        """Display processing summary statistics"""
        
        # Performance metrics table
        perf_table = Table(title="⚡ Performance Metrics")
        perf_table.add_column("Metric", style="bold blue")
        perf_table.add_column("Value", style="green")
        
        perf_table.add_row("Total Records", f"{processing_stats['total_records']:,}")
        perf_table.add_row("Processing Time", f"{processing_stats['processing_time']:.2f} seconds")
        
        if processing_stats['processing_time'] > 0:
            throughput = processing_stats['total_records'] / processing_stats['processing_time']
            perf_table.add_row("Throughput", f"{throughput:,.0f} records/second")
        
        perf_table.add_row("Cache Hits", f"{processing_stats['cache_hits']}")
        
        # Data quality metrics table
        quality_table = Table(title="📊 Data Quality Metrics")
        quality_table.add_column("Metric", style="bold blue")
        quality_table.add_column("Count", style="yellow")
        quality_table.add_column("Percentage", style="magenta")
        
        total = processing_stats['total_records']
        if total > 0:
            quality_table.add_row(
                "Outliers Detected", 
                f"{processing_stats['outliers_detected']}", 
                f"{processing_stats['outliers_detected']/total*100:.2f}%"
            )
            quality_table.add_row(
                "Volume Spikes", 
                f"{processing_stats['volume_spikes']}", 
                f"{processing_stats['volume_spikes']/total*100:.2f}%"
            )
            quality_table.add_row(
                "Large Moves", 
                f"{processing_stats['large_moves']}", 
                f"{processing_stats['large_moves']/total*100:.2f}%"
            )
        
        console.print(perf_table)
        console.print(quality_table)
        
        # Final summary
        console.print(Panel(
            f"📊 [bold blue]Minute Bars Processing Complete[/bold blue]\n\n"
            f"📄 Input File: {input_file}\n"
            f"📂 Output Directory: {output_dir}\n"
            f"📊 Records Processed: {processing_stats['total_records']:,}\n"
            f"⏱️ Processing Time: {processing_stats['processing_time']:.2f}s\n"
            f"🚀 Throughput: {processing_stats['total_records']/processing_stats['processing_time']:,.0f} records/second\n"
            f"🎯 Symbols: {len(processing_stats['symbols_processed'])}\n"
            f"💾 Cache Enabled: {'Yes' if enable_cache else 'No'}",
            title="📈 Processing Summary",
            border_style="green"
        ))
    
    # Run the async pipeline
    asyncio.run(run_pipeline())


if __name__ == "__main__":
    typer.run(main)
