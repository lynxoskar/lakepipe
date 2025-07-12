#!/usr/bin/env python3
"""
📊 Real-time VWAP Calculation Example

This example demonstrates advanced streaming calculations using lakepipe for
real-time Volume Weighted Average Price (VWAP) computation with windowing.

Usage:
    python examples/streaming/real_time_vwap.py
    python examples/streaming/real_time_vwap.py --symbol AAPL --window-minutes 20
    python examples/streaming/real_time_vwap.py --config examples/configs/streaming_config.yaml
"""

import asyncio
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
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
    symbol: str = typer.Option("AAPL", "--symbol", "-s", help="Symbol to track"),
    window_minutes: int = typer.Option(20, "--window-minutes", help="VWAP window in minutes"),
    config_file: Optional[Path] = typer.Option(None, "--config", "-c", help="Configuration file"),
    output_dir: str = typer.Option("output/vwap", "--output", "-o", help="Output directory"),
    alert_threshold: float = typer.Option(0.01, "--alert-threshold", help="Alert threshold (% deviation)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
):
    """
    📊 Real-time VWAP calculation with alerts
    
    This example shows:
    - Real-time VWAP calculation with sliding windows
    - Price deviation alerts from VWAP
    - Advanced window functions in streaming mode
    - Real-time metrics display
    """
    
    console.print(Panel(
        f"📊 [bold blue]Real-time VWAP Calculation[/bold blue]\n"
        f"Symbol: {symbol} | Window: {window_minutes} minutes\n"
        f"Alert threshold: {alert_threshold:.1%} deviation",
        title="📈 VWAP Streaming",
        border_style="blue"
    ))
    
    # Build configuration
    if config_file:
        file_config = load_config_file(config_file)
        console.print(f"✅ Loaded configuration from {config_file}")
    else:
        file_config = {}
        console.print("📋 Using default VWAP configuration")
    
    # Advanced VWAP configuration
    window_seconds = window_minutes * 60
    
    cli_config = {
        "source": {
            "uri": "kafka://market-data-ticks",
            "format": "kafka",
            "kafka": {
                "bootstrap_servers": ["localhost:9092"],
                "group_id": f"lakepipe-vwap-{symbol.lower()}",
                "auto_offset_reset": "latest"
            }
        },
        "sink": {
            "uri": f"file://{output_dir}/vwap-{symbol.lower()}/",
            "format": "parquet",
            "compression": "zstd",
            "partition_cols": ["symbol", "date", "hour"]
        },
        "transform": {
            "engine": "duckdb",
            "operations": [
                {
                    "type": "filter",
                    "condition": f"symbol = '{symbol}' AND price > 0 AND quantity > 0"
                },
                {
                    "type": "with_columns",
                    "expressions": [
                        "extract('date', timestamp) as date",
                        "extract('hour', timestamp) as hour",
                        "extract('minute', timestamp) as minute",
                        "extract('second', timestamp) as second",
                        "extract('epoch', timestamp) as epoch_seconds",
                        "price * quantity as notional",
                        "quantity as volume"
                    ]
                },
                {
                    "type": "window_function",
                    "window_spec": {
                        "partition_by": ["symbol"],
                        "order_by": ["timestamp"],
                        "frame": f"range between {window_seconds} preceding and current row"
                    },
                    "expressions": [
                        "sum(notional) over w as cum_notional",
                        "sum(volume) over w as cum_volume",
                        "sum(notional) over w / sum(volume) over w as vwap",
                        "count(*) over w as tick_count",
                        "min(timestamp) over w as window_start",
                        "max(timestamp) over w as window_end"
                    ]
                },
                {
                    "type": "with_columns",
                    "expressions": [
                        "case when vwap > 0 then (price - vwap) / vwap else null end as vwap_deviation_pct",
                        "case when vwap > 0 and abs((price - vwap) / vwap) > 0.01 then 1 else 0 end as deviation_alert",
                        "case when cum_volume > 0 then cum_notional / cum_volume else null end as vwap_check",
                        "round(vwap, 4) as vwap_rounded",
                        "round(vwap_deviation_pct * 100, 2) as vwap_deviation_bps"
                    ]
                }
            ]
        },
        "streaming": {
            "enabled": True,
            "batch_size": 1000,
            "max_latency_ms": 50,
            "window_size_ms": 1000
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
    
    console.print(f"📤 Output directory: {output_dir}")
    console.print(f"🎯 Alert threshold: {alert_threshold:.1%}")
    
    # State tracking for display
    latest_metrics = {}
    alert_count = 0
    processed_count = 0
    
    # Create and run pipeline
    async def run_pipeline():
        nonlocal latest_metrics, alert_count, processed_count
        
        try:
            console.print("🚀 [bold green]Starting VWAP streaming pipeline...[/bold green]")
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                console=console,
                transient=False
            ) as progress:
                task = progress.add_task("Processing VWAP...", total=None)
                
                pipeline = create_pipeline(config)
                
                # Display table for latest metrics
                async for batch_result in pipeline.stream():
                    if batch_result and batch_result.get('records'):
                        records = batch_result['records']
                        processed_count += len(records)
                        
                        # Update latest metrics from last record
                        if records:
                            latest_record = records[-1]
                            latest_metrics = {
                                'symbol': latest_record.get('symbol'),
                                'price': latest_record.get('price'),
                                'vwap': latest_record.get('vwap_rounded'),
                                'deviation_pct': latest_record.get('vwap_deviation_pct'),
                                'volume': latest_record.get('cum_volume'),
                                'tick_count': latest_record.get('tick_count'),
                                'timestamp': latest_record.get('timestamp')
                            }
                            
                            # Check for alerts
                            if latest_record.get('deviation_alert') == 1:
                                alert_count += 1
                                console.print(f"🚨 [red]VWAP Alert: {symbol} price {latest_record.get('price')} deviates {latest_record.get('vwap_deviation_bps')}bps from VWAP {latest_record.get('vwap_rounded')}[/red]")
                        
                        # Update progress
                        progress.update(
                            task,
                            description=f"Processed {processed_count} ticks | Alerts: {alert_count} | Latest VWAP: {latest_metrics.get('vwap', 'N/A')}"
                        )
                        
                        # Display metrics table periodically
                        if processed_count % 100 == 0:
                            display_metrics_table()
                
        except KeyboardInterrupt:
            console.print("\n⏹️  [yellow]VWAP streaming stopped by user[/yellow]")
            display_final_summary()
        except Exception as e:
            console.print(f"❌ [red]Pipeline error: {e}[/red]")
            if verbose:
                console.print_exception()
            raise
    
    def display_metrics_table():
        """Display current VWAP metrics in a table"""
        if not latest_metrics:
            return
            
        table = Table(title=f"📊 Real-time VWAP Metrics - {symbol}")
        table.add_column("Metric", style="bold blue")
        table.add_column("Value", style="green")
        
        table.add_row("Current Price", f"${latest_metrics.get('price', 'N/A'):.2f}")
        table.add_row("VWAP", f"${latest_metrics.get('vwap', 'N/A'):.4f}")
        table.add_row("Deviation", f"{latest_metrics.get('deviation_pct', 0)*100:.2f}%")
        table.add_row("Cumulative Volume", f"{latest_metrics.get('volume', 0):,}")
        table.add_row("Tick Count", f"{latest_metrics.get('tick_count', 0):,}")
        table.add_row("Total Processed", f"{processed_count:,}")
        table.add_row("Alert Count", f"{alert_count}")
        
        console.print(table)
    
    def display_final_summary():
        """Display final summary statistics"""
        console.print(Panel(
            f"📊 [bold blue]VWAP Session Summary[/bold blue]\n\n"
            f"Symbol: {symbol}\n"
            f"Window: {window_minutes} minutes\n"
            f"Total Ticks Processed: {processed_count:,}\n"
            f"Total Alerts: {alert_count}\n"
            f"Final VWAP: ${latest_metrics.get('vwap', 'N/A')}\n"
            f"Final Price: ${latest_metrics.get('price', 'N/A'):.2f}",
            title="📈 Session Complete",
            border_style="green"
        ))
    
    # Run the async pipeline
    asyncio.run(run_pipeline())


if __name__ == "__main__":
    typer.run(main)
