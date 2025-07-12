#!/usr/bin/env python3
"""
📈 Technical Indicators Calculation Example

This example demonstrates comprehensive technical indicator calculation using lakepipe
for RSI, MACD, Bollinger Bands, and other popular trading indicators.

Usage:
    python examples/batch/technical_indicators.py
    python examples/batch/technical_indicators.py --symbols AAPL,GOOGL --rsi-period 14
    python examples/batch/technical_indicators.py --config examples/configs/batch_config.yaml
"""

import asyncio
import sys
from pathlib import Path
from typing import Optional, List
from datetime import datetime

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
    input_file: str = typer.Option("examples/data/sample_minute_bars.parquet", "--input-file", "-i", help="Input Parquet file"),
    output_dir: str = typer.Option("output/indicators", "--output-dir", "-o", help="Output directory"),
    symbols: Optional[str] = typer.Option(None, "--symbols", "-s", help="Comma-separated symbols"),
    rsi_period: int = typer.Option(14, "--rsi-period", help="RSI period"),
    macd_fast: int = typer.Option(12, "--macd-fast", help="MACD fast period"),
    macd_slow: int = typer.Option(26, "--macd-slow", help="MACD slow period"),
    macd_signal: int = typer.Option(9, "--macd-signal", help="MACD signal period"),
    bollinger_period: int = typer.Option(20, "--bollinger-period", help="Bollinger Bands period"),
    bollinger_std: float = typer.Option(2.0, "--bollinger-std", help="Bollinger Bands standard deviation"),
    config_file: Optional[Path] = typer.Option(None, "--config", "-c", help="Configuration file"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
):
    """
    📈 Calculate comprehensive technical indicators
    
    This example shows:
    - RSI (Relative Strength Index) calculation
    - MACD (Moving Average Convergence Divergence)
    - Bollinger Bands with dynamic bands
    - Stochastic Oscillator
    - Various moving averages (SMA, EMA)
    - Volume-based indicators
    """
    
    console.print(Panel(
        f"📈 [bold blue]Technical Indicators Calculation[/bold blue]\n"
        f"Input: {input_file}\n"
        f"Output: {output_dir}\n"
        f"Symbols: {symbols or 'All'}\n"
        f"RSI Period: {rsi_period}\n"
        f"MACD: {macd_fast}/{macd_slow}/{macd_signal}\n"
        f"Bollinger: {bollinger_period} ± {bollinger_std}σ",
        title="📊 Technical Analysis",
        border_style="blue"
    ))
    
    # Parse symbols
    symbol_list = symbols.split(',') if symbols else None
    
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
    
    cli_config = {
        "source": {
            "uri": f"file://{input_file}",
            "format": "parquet"
        },
        "sink": {
            "uri": f"file://{output_dir}/indicators/",
            "format": "parquet",
            "compression": "zstd",
            "partition_cols": ["symbol", "date"]
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
                        "(high - low) as true_range",
                        "(close - open) as change",
                        "(close - open) / open as return_pct",
                        "case when close > open then 1 else 0 end as up_day",
                        "case when close > open then close - open else 0 end as gain",
                        "case when close < open then open - close else 0 end as loss",
                        "close * volume as dollar_volume"
                    ]
                },
                # Simple Moving Averages
                {
                    "type": "window_function",
                    "window_spec": {
                        "partition_by": ["symbol"],
                        "order_by": ["timestamp"],
                        "frame": "rows between 9 preceding and current row"
                    },
                    "expressions": [
                        "avg(close) over w as sma_10",
                        "avg(volume) over w as sma_volume_10"
                    ]
                },
                {
                    "type": "window_function",
                    "window_spec": {
                        "partition_by": ["symbol"],
                        "order_by": ["timestamp"],
                        "frame": f"rows between {bollinger_period-1} preceding and current row"
                    },
                    "expressions": [
                        f"avg(close) over w as sma_{bollinger_period}",
                        f"stddev(close) over w as std_{bollinger_period}",
                        f"avg(volume) over w as sma_volume_{bollinger_period}"
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
                        "avg(volume) over w as sma_volume_50"
                    ]
                },
                {
                    "type": "window_function",
                    "window_spec": {
                        "partition_by": ["symbol"],
                        "order_by": ["timestamp"],
                        "frame": "rows between 199 preceding and current row"
                    },
                    "expressions": [
                        "avg(close) over w as sma_200"
                    ]
                },
                # RSI Components
                {
                    "type": "window_function",
                    "window_spec": {
                        "partition_by": ["symbol"],
                        "order_by": ["timestamp"],
                        "frame": f"rows between {rsi_period-1} preceding and current row"
                    },
                    "expressions": [
                        f"avg(gain) over w as avg_gain_{rsi_period}",
                        f"avg(loss) over w as avg_loss_{rsi_period}"
                    ]
                },
                # MACD Components
                {
                    "type": "window_function",
                    "window_spec": {
                        "partition_by": ["symbol"],
                        "order_by": ["timestamp"],
                        "frame": f"rows between {macd_fast-1} preceding and current row"
                    },
                    "expressions": [
                        f"avg(close) over w as ema_{macd_fast}"
                    ]
                },
                {
                    "type": "window_function",
                    "window_spec": {
                        "partition_by": ["symbol"],
                        "order_by": ["timestamp"],
                        "frame": f"rows between {macd_slow-1} preceding and current row"
                    },
                    "expressions": [
                        f"avg(close) over w as ema_{macd_slow}"
                    ]
                },
                # Stochastic Components
                {
                    "type": "window_function",
                    "window_spec": {
                        "partition_by": ["symbol"],
                        "order_by": ["timestamp"],
                        "frame": "rows between 13 preceding and current row"
                    },
                    "expressions": [
                        "min(low) over w as low_14",
                        "max(high) over w as high_14"
                    ]
                },
                # Calculate Technical Indicators
                {
                    "type": "with_columns",
                    "expressions": [
                        # RSI
                        f"case when avg_loss_{rsi_period} > 0 then 100 - (100 / (1 + avg_gain_{rsi_period} / avg_loss_{rsi_period})) else 100 end as rsi",
                        
                        # MACD
                        f"ema_{macd_fast} - ema_{macd_slow} as macd_line",
                        
                        # Bollinger Bands
                        f"sma_{bollinger_period} + ({bollinger_std} * std_{bollinger_period}) as bb_upper",
                        f"sma_{bollinger_period} - ({bollinger_std} * std_{bollinger_period}) as bb_lower",
                        f"sma_{bollinger_period} as bb_middle",
                        
                        # Stochastic
                        "case when high_14 > low_14 then ((close - low_14) / (high_14 - low_14)) * 100 else 50 end as stoch_k",
                        
                        # Price Position
                        f"case when std_{bollinger_period} > 0 then (close - sma_{bollinger_period}) / std_{bollinger_period} else 0 end as bb_position",
                        
                        # Volume indicators
                        f"case when sma_volume_{bollinger_period} > 0 then volume / sma_volume_{bollinger_period} else 1 end as volume_ratio",
                        
                        # Trend indicators
                        "case when sma_10 > sma_50 and sma_50 > sma_200 then 1 else 0 end as bullish_trend",
                        "case when sma_10 < sma_50 and sma_50 < sma_200 then 1 else 0 end as bearish_trend"
                    ]
                },
                # MACD Signal Line
                {
                    "type": "window_function",
                    "window_spec": {
                        "partition_by": ["symbol"],
                        "order_by": ["timestamp"],
                        "frame": f"rows between {macd_signal-1} preceding and current row"
                    },
                    "expressions": [
                        "avg(macd_line) over w as macd_signal"
                    ]
                },
                # Stochastic %D
                {
                    "type": "window_function",
                    "window_spec": {
                        "partition_by": ["symbol"],
                        "order_by": ["timestamp"],
                        "frame": "rows between 2 preceding and current row"
                    },
                    "expressions": [
                        "avg(stoch_k) over w as stoch_d"
                    ]
                },
                # Final Indicator Calculations
                {
                    "type": "with_columns",
                    "expressions": [
                        # MACD Histogram
                        "macd_line - macd_signal as macd_histogram",
                        
                        # Bollinger Band Width
                        f"(bb_upper - bb_lower) / bb_middle as bb_width",
                        
                        # Signal Generation
                        "case when rsi < 30 then 'Oversold' when rsi > 70 then 'Overbought' else 'Neutral' end as rsi_signal",
                        "case when macd_line > macd_signal then 'Bullish' else 'Bearish' end as macd_signal_trend",
                        "case when close > bb_upper then 'Overbought' when close < bb_lower then 'Oversold' else 'Normal' end as bb_signal",
                        "case when stoch_k < 20 then 'Oversold' when stoch_k > 80 then 'Overbought' else 'Neutral' end as stoch_signal",
                        
                        # Volatility measures
                        f"case when bb_width > 0.1 then 'High' when bb_width > 0.05 then 'Medium' else 'Low' end as volatility_regime",
                        
                        # Volume confirmation
                        "case when volume_ratio > 1.5 then 'High Volume' when volume_ratio > 1.2 then 'Above Average' else 'Normal' end as volume_signal"
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
    
    # Processing statistics
    processing_stats = {
        'total_records': 0,
        'symbols_processed': set(),
        'processing_time': 0,
        'oversold_signals': 0,
        'overbought_signals': 0,
        'bullish_macd': 0,
        'bearish_macd': 0
    }
    
    # Create and run pipeline
    async def run_pipeline():
        nonlocal processing_stats
        
        try:
            console.print("🚀 [bold green]Starting technical indicators calculation...[/bold green]")
            
            start_time = datetime.now()
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                console=console,
                transient=False
            ) as progress:
                task = progress.add_task("Calculating indicators...", total=None)
                
                pipeline = create_pipeline(config)
                result = await pipeline.execute()
                
                processing_stats['processing_time'] = (datetime.now() - start_time).total_seconds()
                
                if result:
                    processing_stats['total_records'] = result.get('processed_count', 0)
                    
                    # Count signals if available
                    if hasattr(result, 'metrics'):
                        metrics = result.metrics
                        processing_stats['oversold_signals'] = metrics.get('oversold_signals', 0)
                        processing_stats['overbought_signals'] = metrics.get('overbought_signals', 0)
                        processing_stats['bullish_macd'] = metrics.get('bullish_macd', 0)
                        processing_stats['bearish_macd'] = metrics.get('bearish_macd', 0)
                
                progress.update(task, completed=True)
                console.print("✅ [bold green]Technical indicators calculated successfully![/bold green]")
                
            display_indicators_summary()
            
        except Exception as e:
            console.print(f"❌ [red]Pipeline error: {e}[/red]")
            if verbose:
                console.print_exception()
            raise
    
    def display_indicators_summary():
        """Display technical indicators summary"""
        
        # Indicators table
        indicators_table = Table(title="📊 Technical Indicators Summary")
        indicators_table.add_column("Indicator", style="bold blue")
        indicators_table.add_column("Configuration", style="green")
        indicators_table.add_column("Signals Generated", style="yellow")
        
        indicators_table.add_row("RSI", f"Period: {rsi_period}", f"Oversold: {processing_stats['oversold_signals']}, Overbought: {processing_stats['overbought_signals']}")
        indicators_table.add_row("MACD", f"{macd_fast}/{macd_slow}/{macd_signal}", f"Bullish: {processing_stats['bullish_macd']}, Bearish: {processing_stats['bearish_macd']}")
        indicators_table.add_row("Bollinger Bands", f"Period: {bollinger_period}, Std: {bollinger_std}", "Calculated")
        indicators_table.add_row("Stochastic", "14/3", "Calculated")
        indicators_table.add_row("Moving Averages", "10/20/50/200", "Calculated")
        indicators_table.add_row("Volume Indicators", "Ratio & Signals", "Calculated")
        
        # Performance metrics
        perf_table = Table(title="⚡ Performance Metrics")
        perf_table.add_column("Metric", style="bold blue")
        perf_table.add_column("Value", style="green")
        
        perf_table.add_row("Total Records", f"{processing_stats['total_records']:,}")
        perf_table.add_row("Processing Time", f"{processing_stats['processing_time']:.2f} seconds")
        
        if processing_stats['processing_time'] > 0:
            throughput = processing_stats['total_records'] / processing_stats['processing_time']
            perf_table.add_row("Throughput", f"{throughput:,.0f} records/second")
        
        console.print(indicators_table)
        console.print(perf_table)
        
        # Final summary
        console.print(Panel(
            f"📈 [bold blue]Technical Indicators Complete[/bold blue]\n\n"
            f"📄 Input File: {input_file}\n"
            f"📂 Output Directory: {output_dir}\n"
            f"📊 Records Processed: {processing_stats['total_records']:,}\n"
            f"⏱️ Processing Time: {processing_stats['processing_time']:.2f}s\n"
            f"🎯 Symbols: {len(processing_stats['symbols_processed'])}\n"
            f"📈 Indicators: RSI, MACD, Bollinger Bands, Stochastic, Moving Averages\n"
            f"🎯 Signals: {processing_stats['oversold_signals'] + processing_stats['overbought_signals'] + processing_stats['bullish_macd'] + processing_stats['bearish_macd']} total",
            title="📊 Calculation Summary",
            border_style="green"
        ))
    
    # Run the async pipeline
    asyncio.run(run_pipeline())


if __name__ == "__main__":
    typer.run(main)
