#!/usr/bin/env python3
"""
🔗 Market Data Enrichment Example

This example demonstrates real-time data enrichment using lakepipe to join
streaming tick data with reference data for enhanced market analysis.

Usage:
    python examples/streaming/market_data_enrichment.py
    python examples/streaming/market_data_enrichment.py --reference-file examples/data/reference_data.json
"""

import asyncio
import sys
from pathlib import Path
from typing import Optional, Dict, Any
import json

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
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


def load_reference_data(file_path: str) -> Dict[str, Any]:
    """Load reference data from JSON file"""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        console.print(f"❌ Failed to load reference data: {e}")
        return {}


def main(
    config_file: Optional[Path] = typer.Option(None, "--config", "-c", help="Configuration file"),
    reference_file: str = typer.Option("examples/data/reference_data.json", "--reference-file", help="Reference data file"),
    output_dir: str = typer.Option("output/enriched", "--output", "-o", help="Output directory"),
    sectors: Optional[str] = typer.Option(None, "--sectors", help="Filter by sectors (comma-separated)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
):
    """
    🔗 Real-time market data enrichment
    
    This example shows:
    - Joining streaming tick data with reference data
    - Real-time sector and market cap classification
    - Data quality scoring and validation
    - Enhanced market analysis metrics
    """
    
    console.print(Panel(
        "🔗 [bold blue]Market Data Enrichment[/bold blue]\n"
        "Enriching streaming tick data with reference information",
        title="�� Data Enrichment",
        border_style="blue"
    ))
    
    # Load reference data
    reference_data = load_reference_data(reference_file)
    if not reference_data:
        console.print("❌ No reference data loaded. Using sample data.")
        reference_data = {
            "symbols": {
                "AAPL": {"name": "Apple Inc.", "sector": "Technology", "market_cap": 2800000000000},
                "GOOGL": {"name": "Alphabet Inc.", "sector": "Technology", "market_cap": 1750000000000},
                "MSFT": {"name": "Microsoft Corporation", "sector": "Technology", "market_cap": 2500000000000}
            }
        }
    
    console.print(f"✅ Loaded reference data for {len(reference_data.get('symbols', {}))} symbols")
    
    # Build configuration
    if config_file:
        file_config = load_config_file(config_file)
        console.print(f"✅ Loaded configuration from {config_file}")
    else:
        file_config = {}
    
    # Prepare sector filter
    sector_filter = ""
    if sectors:
        sector_list = [s.strip() for s in sectors.split(",")]
        sector_filter = f"AND sector IN ({','.join([f"'{s}'" for s in sector_list])})"
        console.print(f"🎯 Filtering for sectors: {', '.join(sector_list)}")
    
    cli_config = {
        "source": {
            "uri": "kafka://market-data-ticks",
            "format": "kafka",
            "kafka": {
                "bootstrap_servers": ["localhost:9092"],
                "group_id": "lakepipe-enrichment",
                "auto_offset_reset": "latest"
            }
        },
        "sink": {
            "uri": f"file://{output_dir}/enriched-data/",
            "format": "parquet",
            "compression": "zstd",
            "partition_cols": ["sector", "date", "hour"]
        },
        "transform": {
            "engine": "duckdb",
            "operations": [
                {
                    "type": "filter",
                    "condition": "price > 0 AND quantity > 0"
                },
                {
                    "type": "with_columns",
                    "expressions": [
                        "extract('date', timestamp) as date",
                        "extract('hour', timestamp) as hour",
                        "extract('minute', timestamp) as minute",
                        "price * quantity as notional",
                        "case when bid_price > 0 then (ask_price - bid_price) / bid_price else null end as spread_pct"
                    ]
                },
                {
                    "type": "join",
                    "join_type": "left",
                    "right_table": "reference_symbols",
                    "join_keys": ["symbol"],
                    "select_columns": ["symbol", "name", "sector", "market_cap", "exchange", "currency"]
                },
                {
                    "type": "with_columns",
                    "expressions": [
                        "case when market_cap >= 200000000000 then 'Large Cap' when market_cap >= 10000000000 then 'Mid Cap' when market_cap >= 2000000000 then 'Small Cap' else 'Micro Cap' end as market_cap_category",
                        "case when spread_pct <= 0.001 then 'Tight' when spread_pct <= 0.005 then 'Normal' when spread_pct <= 0.01 then 'Wide' else 'Very Wide' end as spread_category",
                        "case when notional >= 1000000 then 'Block' when notional >= 100000 then 'Large' when notional >= 10000 then 'Medium' else 'Small' end as trade_size_category",
                        "case when name is not null and sector is not null then 100 when name is not null then 75 when symbol is not null then 50 else 0 end as data_quality_score"
                    ]
                },
                {
                    "type": "window_function",
                    "window_spec": {
                        "partition_by": ["sector"],
                        "order_by": ["timestamp"],
                        "frame": "rows between 99 preceding and current row"
                    },
                    "expressions": [
                        "avg(price) over w as sector_avg_price",
                        "avg(notional) over w as sector_avg_notional",
                        "count(*) over w as sector_tick_count"
                    ]
                },
                {
                    "type": "with_columns",
                    "expressions": [
                        "case when sector_avg_price > 0 then (price - sector_avg_price) / sector_avg_price else null end as sector_relative_performance",
                        "case when sector_avg_notional > 0 then notional / sector_avg_notional else null end as sector_relative_size"
                    ]
                }
            ]
        },
        "streaming": {
            "enabled": True,
            "batch_size": 5000,
            "max_latency_ms": 200
        },
        "log": {
            "level": "DEBUG" if verbose else "INFO"
        }
    }
    
    # Add sector filter if specified
    if sector_filter:
        cli_config["transform"]["operations"].append({
            "type": "filter",
            "condition": f"sector is not null {sector_filter}"
        })
    
    # Merge configurations
    config = build_pipeline_config(
        config_overrides={**file_config, **cli_config}
    )
    
    # Configure logging
    configure_logging(config["log"])
    
    console.print(f"📤 Output directory: {output_dir}")
    
    # State tracking
    enriched_count = 0
    sector_stats = {}
    
    # Create and run pipeline
    async def run_pipeline():
        nonlocal enriched_count, sector_stats
        
        try:
            console.print("🚀 [bold green]Starting market data enrichment pipeline...[/bold green]")
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
                transient=False
            ) as progress:
                task = progress.add_task("Enriching market data...", total=None)
                
                pipeline = create_pipeline(config)
                
                async for batch_result in pipeline.stream():
                    if batch_result and batch_result.get('records'):
                        records = batch_result['records']
                        enriched_count += len(records)
                        
                        # Update sector statistics
                        for record in records:
                            sector = record.get('sector', 'Unknown')
                            if sector not in sector_stats:
                                sector_stats[sector] = {'count': 0, 'total_notional': 0}
                            sector_stats[sector]['count'] += 1
                            sector_stats[sector]['total_notional'] += record.get('notional', 0)
                        
                        progress.update(
                            task,
                            description=f"Enriched {enriched_count} records | Sectors: {len(sector_stats)}"
                        )
                        
                        # Display sector stats periodically
                        if enriched_count % 1000 == 0:
                            display_sector_stats()
                
        except KeyboardInterrupt:
            console.print("\n⏹️  [yellow]Enrichment stopped by user[/yellow]")
            display_final_summary()
        except Exception as e:
            console.print(f"❌ [red]Pipeline error: {e}[/red]")
            if verbose:
                console.print_exception()
            raise
    
    def display_sector_stats():
        """Display sector statistics"""
        if not sector_stats:
            return
            
        table = Table(title="📊 Sector Statistics")
        table.add_column("Sector", style="bold blue")
        table.add_column("Tick Count", style="green")
        table.add_column("Total Notional", style="yellow")
        table.add_column("Avg Notional", style="magenta")
        
        for sector, stats in sorted(sector_stats.items()):
            avg_notional = stats['total_notional'] / stats['count'] if stats['count'] > 0 else 0
            table.add_row(
                sector,
                f"{stats['count']:,}",
                f"${stats['total_notional']:,.0f}",
                f"${avg_notional:,.0f}"
            )
        
        console.print(table)
    
    def display_final_summary():
        """Display final summary"""
        console.print(Panel(
            f"🔗 [bold blue]Enrichment Session Summary[/bold blue]\n\n"
            f"Total Records Enriched: {enriched_count:,}\n"
            f"Sectors Processed: {len(sector_stats)}\n"
            f"Reference Data Quality: {len(reference_data.get('symbols', {}))} symbols",
            title="📊 Session Complete",
            border_style="green"
        ))
        
        if sector_stats:
            display_sector_stats()
    
    # Run the async pipeline
    asyncio.run(run_pipeline())


if __name__ == "__main__":
    typer.run(main)
