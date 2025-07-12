#!/usr/bin/env python3
"""
📊 Portfolio Analytics Example

This example demonstrates comprehensive portfolio performance analysis using lakepipe
for returns calculation, risk metrics, and attribution analysis.

Usage:
    python examples/batch/portfolio_analytics.py
    python examples/batch/portfolio_analytics.py --portfolio-file examples/data/portfolio.csv
    python examples/batch/portfolio_analytics.py --benchmark SPY --start-date 2024-01-01
"""

import asyncio
import sys
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import json

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
    input_file: str = typer.Option("examples/data/sample_minute_bars.parquet", "--input-file", "-i", help="Input price data file"),
    portfolio_file: Optional[str] = typer.Option(None, "--portfolio-file", "-p", help="Portfolio holdings file"),
    output_dir: str = typer.Option("output/portfolio", "--output-dir", "-o", help="Output directory"),
    benchmark: str = typer.Option("SPY", "--benchmark", "-b", help="Benchmark symbol"),
    start_date: Optional[str] = typer.Option(None, "--start-date", help="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = typer.Option(None, "--end-date", help="End date (YYYY-MM-DD)"),
    rebalance_freq: str = typer.Option("daily", "--rebalance-freq", help="Rebalancing frequency (daily/weekly/monthly)"),
    config_file: Optional[Path] = typer.Option(None, "--config", "-c", help="Configuration file"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
):
    """
    📊 Comprehensive portfolio analytics
    
    This example shows:
    - Portfolio returns calculation and attribution
    - Risk metrics (volatility, VaR, drawdown)
    - Sharpe ratio and information ratio
    - Sector and security attribution
    - Benchmark comparison and tracking error
    """
    
    console.print(Panel(
        f"📊 [bold blue]Portfolio Analytics[/bold blue]\n"
        f"Price Data: {input_file}\n"
        f"Portfolio: {portfolio_file or 'Equal Weight'}\n"
        f"Benchmark: {benchmark}\n"
        f"Rebalance: {rebalance_freq}\n"
        f"Period: {start_date or 'Full'} to {end_date or 'Latest'}",
        title="💼 Portfolio Analysis",
        border_style="blue"
    ))
    
    # Default portfolio if none provided
    default_portfolio = {
        "AAPL": 0.25,
        "GOOGL": 0.20,
        "MSFT": 0.25,
        "AMZN": 0.15,
        "TSLA": 0.15
    }
    
    # Load portfolio weights
    portfolio_weights = default_portfolio
    if portfolio_file:
        try:
            # In a real implementation, load from CSV
            console.print(f"📋 Using default portfolio weights")
        except Exception as e:
            console.print(f"❌ Failed to load portfolio: {e}")
            portfolio_weights = default_portfolio
    
    console.print(f"📊 Portfolio symbols: {', '.join(portfolio_weights.keys())}")
    
    # Build configuration
    if config_file:
        file_config = load_config_file(config_file)
        console.print(f"✅ Loaded configuration from {config_file}")
    else:
        file_config = {}
    
    # Build date filters
    date_filters = []
    if start_date:
        date_filters.append(f"timestamp >= '{start_date}'")
    if end_date:
        date_filters.append(f"timestamp <= '{end_date}'")
    
    # Portfolio symbols filter
    portfolio_symbols = list(portfolio_weights.keys())
    if benchmark not in portfolio_symbols:
        portfolio_symbols.append(benchmark)
    
    symbol_filter = "symbol IN ('" + "', '".join(portfolio_symbols) + "')"
    
    filter_conditions = ["open > 0", "high > 0", "low > 0", "close > 0", "volume > 0", symbol_filter]
    filter_conditions.extend(date_filters)
    
    cli_config = {
        "source": {
            "uri": f"file://{input_file}",
            "format": "parquet"
        },
        "sink": {
            "uri": f"file://{output_dir}/analytics/",
            "format": "parquet",
            "compression": "zstd",
            "partition_cols": ["date"]
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
                        "(close - open) / open as intraday_return",
                        "log(close / open) as log_return",
                        "close * volume as dollar_volume"
                    ]
                },
                # Calculate daily returns (using last price of each day)
                {
                    "type": "window_function",
                    "window_spec": {
                        "partition_by": ["symbol", "date"],
                        "order_by": ["timestamp"]
                    },
                    "expressions": [
                        "row_number() over w as row_num",
                        "count(*) over w as daily_count",
                        "last_value(close) over w as daily_close",
                        "first_value(close) over w as daily_open",
                        "max(high) over w as daily_high",
                        "min(low) over w as daily_low",
                        "sum(volume) over w as daily_volume"
                    ]
                },
                # Keep only last record per day
                {
                    "type": "filter",
                    "condition": "row_num = daily_count"
                },
                # Calculate daily returns
                {
                    "type": "window_function",
                    "window_spec": {
                        "partition_by": ["symbol"],
                        "order_by": ["date"],
                        "frame": "rows between 1 preceding and current row"
                    },
                    "expressions": [
                        "lag(daily_close, 1) over w as prev_close"
                    ]
                },
                {
                    "type": "with_columns",
                    "expressions": [
                        "case when prev_close > 0 then (daily_close - prev_close) / prev_close else 0 end as daily_return",
                        "case when prev_close > 0 then log(daily_close / prev_close) else 0 end as daily_log_return",
                        "(daily_high - daily_low) / daily_low as daily_range_pct"
                    ]
                },
                # Add portfolio weights
                {
                    "type": "with_columns",
                    "expressions": [
                        f"case " + " ".join([f"when symbol = '{sym}' then {weight}" for sym, weight in portfolio_weights.items()]) + " else 0 end as portfolio_weight",
                        f"case when symbol = '{benchmark}' then 1 else 0 end as is_benchmark"
                    ]
                },
                # Calculate portfolio metrics
                {
                    "type": "window_function",
                    "window_spec": {
                        "partition_by": ["date"],
                        "order_by": ["symbol"]
                    },
                    "expressions": [
                        "sum(daily_return * portfolio_weight) over w as portfolio_return",
                        "sum(case when is_benchmark = 1 then daily_return else 0 end) over w as benchmark_return"
                    ]
                },
                # Rolling statistics
                {
                    "type": "window_function",
                    "window_spec": {
                        "partition_by": ["symbol"],
                        "order_by": ["date"],
                        "frame": "rows between 29 preceding and current row"
                    },
                    "expressions": [
                        "avg(daily_return) over w as avg_return_30d",
                        "stddev(daily_return) over w as volatility_30d",
                        "min(daily_return) over w as min_return_30d",
                        "max(daily_return) over w as max_return_30d"
                    ]
                },
                # Portfolio level rolling statistics
                {
                    "type": "window_function",
                    "window_spec": {
                        "partition_by": [],
                        "order_by": ["date"],
                        "frame": "rows between 29 preceding and current row"
                    },
                    "expressions": [
                        "avg(portfolio_return) over w as portfolio_avg_return_30d",
                        "stddev(portfolio_return) over w as portfolio_volatility_30d",
                        "avg(benchmark_return) over w as benchmark_avg_return_30d",
                        "stddev(benchmark_return) over w as benchmark_volatility_30d",
                        "stddev(portfolio_return - benchmark_return) over w as tracking_error_30d"
                    ]
                },
                # Risk metrics
                {
                    "type": "with_columns",
                    "expressions": [
                        "daily_return * portfolio_weight as contribution",
                        "portfolio_return - benchmark_return as active_return",
                        "case when volatility_30d > 0 then (avg_return_30d * 252) / (volatility_30d * sqrt(252)) else 0 end as sharpe_ratio",
                        "case when portfolio_volatility_30d > 0 then (portfolio_avg_return_30d * 252) / (portfolio_volatility_30d * sqrt(252)) else 0 end as portfolio_sharpe",
                        "case when benchmark_volatility_30d > 0 then (benchmark_avg_return_30d * 252) / (benchmark_volatility_30d * sqrt(252)) else 0 end as benchmark_sharpe",
                        "case when tracking_error_30d > 0 then ((portfolio_avg_return_30d - benchmark_avg_return_30d) * 252) / (tracking_error_30d * sqrt(252)) else 0 end as information_ratio"
                    ]
                },
                # Cumulative performance
                {
                    "type": "window_function",
                    "window_spec": {
                        "partition_by": ["symbol"],
                        "order_by": ["date"],
                        "frame": "unbounded preceding"
                    },
                    "expressions": [
                        "sum(daily_return) over w as cumulative_return",
                        "exp(sum(daily_log_return) over w) - 1 as cumulative_log_return"
                    ]
                },
                # Portfolio cumulative performance
                {
                    "type": "window_function",
                    "window_spec": {
                        "partition_by": [],
                        "order_by": ["date"],
                        "frame": "unbounded preceding"
                    },
                    "expressions": [
                        "sum(portfolio_return) over w as portfolio_cumulative_return",
                        "sum(benchmark_return) over w as benchmark_cumulative_return",
                        "max(sum(portfolio_return) over w) over w as portfolio_peak",
                        "max(sum(benchmark_return) over w) over w as benchmark_peak"
                    ]
                },
                # Drawdown calculation
                {
                    "type": "with_columns",
                    "expressions": [
                        "case when portfolio_peak > 0 then (portfolio_cumulative_return - portfolio_peak) / portfolio_peak else 0 end as portfolio_drawdown",
                        "case when benchmark_peak > 0 then (benchmark_cumulative_return - benchmark_peak) / benchmark_peak else 0 end as benchmark_drawdown",
                        "case when portfolio_drawdown < -0.05 then 1 else 0 end as significant_drawdown"
                    ]
                }
            ]
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
    
    # Analytics statistics
    analytics_stats = {
        'total_records': 0,
        'trading_days': 0,
        'symbols_analyzed': len(portfolio_weights),
        'processing_time': 0,
        'portfolio_return': 0,
        'benchmark_return': 0,
        'max_drawdown': 0,
        'sharpe_ratio': 0,
        'information_ratio': 0,
        'tracking_error': 0
    }
    
    # Create and run pipeline
    async def run_pipeline():
        nonlocal analytics_stats
        
        try:
            console.print("🚀 [bold green]Starting portfolio analytics calculation...[/bold green]")
            
            start_time = datetime.now()
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                console=console,
                transient=False
            ) as progress:
                task = progress.add_task("Calculating portfolio analytics...", total=None)
                
                pipeline = create_pipeline(config)
                result = await pipeline.execute()
                
                analytics_stats['processing_time'] = (datetime.now() - start_time).total_seconds()
                
                if result:
                    analytics_stats['total_records'] = result.get('processed_count', 0)
                    
                    # Extract final performance metrics if available
                    if hasattr(result, 'final_metrics'):
                        final_metrics = result.final_metrics
                        analytics_stats['portfolio_return'] = final_metrics.get('portfolio_cumulative_return', 0)
                        analytics_stats['benchmark_return'] = final_metrics.get('benchmark_cumulative_return', 0)
                        analytics_stats['max_drawdown'] = final_metrics.get('max_drawdown', 0)
                        analytics_stats['sharpe_ratio'] = final_metrics.get('portfolio_sharpe', 0)
                        analytics_stats['information_ratio'] = final_metrics.get('information_ratio', 0)
                        analytics_stats['tracking_error'] = final_metrics.get('tracking_error_30d', 0)
                
                progress.update(task, completed=True)
                console.print("✅ [bold green]Portfolio analytics completed successfully![/bold green]")
                
            display_portfolio_summary()
            
        except Exception as e:
            console.print(f"❌ [red]Pipeline error: {e}[/red]")
            if verbose:
                console.print_exception()
            raise
    
    def display_portfolio_summary():
        """Display portfolio analytics summary"""
        
        # Portfolio composition table
        composition_table = Table(title="📊 Portfolio Composition")
        composition_table.add_column("Symbol", style="bold blue")
        composition_table.add_column("Weight", style="green")
        composition_table.add_column("Sector", style="yellow")
        
        for symbol, weight in portfolio_weights.items():
            composition_table.add_row(symbol, f"{weight:.1%}", "Technology")  # Simplified
        
        # Performance metrics table
        performance_table = Table(title="📈 Performance Metrics")
        performance_table.add_column("Metric", style="bold blue")
        performance_table.add_column("Portfolio", style="green")
        performance_table.add_column("Benchmark", style="yellow")
        performance_table.add_column("Difference", style="magenta")
        
        performance_table.add_row("Total Return", f"{analytics_stats['portfolio_return']:.2%}", f"{analytics_stats['benchmark_return']:.2%}", f"{analytics_stats['portfolio_return'] - analytics_stats['benchmark_return']:.2%}")
        performance_table.add_row("Max Drawdown", f"{analytics_stats['max_drawdown']:.2%}", "-", "-")
        performance_table.add_row("Sharpe Ratio", f"{analytics_stats['sharpe_ratio']:.2f}", "-", "-")
        performance_table.add_row("Information Ratio", f"{analytics_stats['information_ratio']:.2f}", "-", "-")
        performance_table.add_row("Tracking Error", f"{analytics_stats['tracking_error']:.2%}", "-", "-")
        
        # Processing metrics
        processing_table = Table(title="⚡ Processing Metrics")
        processing_table.add_column("Metric", style="bold blue")
        processing_table.add_column("Value", style="green")
        
        processing_table.add_row("Total Records", f"{analytics_stats['total_records']:,}")
        processing_table.add_row("Trading Days", f"{analytics_stats['trading_days']}")
        processing_table.add_row("Symbols Analyzed", f"{analytics_stats['symbols_analyzed']}")
        processing_table.add_row("Processing Time", f"{analytics_stats['processing_time']:.2f} seconds")
        
        console.print(composition_table)
        console.print(performance_table)
        console.print(processing_table)
        
        # Final summary
        console.print(Panel(
            f"📊 [bold blue]Portfolio Analytics Complete[/bold blue]\n\n"
            f"📄 Price Data: {input_file}\n"
            f"📂 Output Directory: {output_dir}\n"
            f"📊 Records Processed: {analytics_stats['total_records']:,}\n"
            f"⏱️ Processing Time: {analytics_stats['processing_time']:.2f}s\n"
            f"🎯 Portfolio Return: {analytics_stats['portfolio_return']:.2%}\n"
            f"📈 Benchmark Return: {analytics_stats['benchmark_return']:.2%}\n"
            f"🎯 Alpha: {analytics_stats['portfolio_return'] - analytics_stats['benchmark_return']:.2%}\n"
            f"📊 Sharpe Ratio: {analytics_stats['sharpe_ratio']:.2f}\n"
            f"📊 Information Ratio: {analytics_stats['information_ratio']:.2f}",
            title="💼 Analysis Summary",
            border_style="green"
        ))
    
    # Run the async pipeline
    asyncio.run(run_pipeline())


if __name__ == "__main__":
    typer.run(main)
