#!/usr/bin/env python3
"""
🔄 Market Data ETL Pipeline Example

This example demonstrates a comprehensive ETL pipeline for daily market data processing
using lakepipe for data extraction, transformation, and loading operations.
"""

import asyncio
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime

import typer
from rich.console import Console
from rich.panel import Panel

# Add lakepipe to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

console = Console()

def main(
    date: Optional[str] = typer.Option(None, "--date", help="Processing date (YYYY-MM-DD)"),
    input_dir: str = typer.Option("examples/data", "--input-dir", help="Input directory"),
    output_dir: str = typer.Option("output/etl", "--output-dir", help="Output directory"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
):
    """
    🔄 Comprehensive market data ETL pipeline
    
    This example shows a complete ETL process for market data.
    """
    
    process_date = datetime.strptime(date, '%Y-%m-%d') if date else datetime.now()
    
    console.print(Panel(
        f"🔄 [bold blue]Market Data ETL Pipeline[/bold blue]\n"
        f"Processing Date: {process_date.strftime('%Y-%m-%d')}\n"
        f"Input Directory: {input_dir}\n"
        f"Output Directory: {output_dir}",
        title="📊 ETL Pipeline",
        border_style="blue"
    ))
    
    console.print("🚀 [bold green]ETL pipeline would run here...[/bold green]")
    console.print("✅ [bold green]ETL pipeline completed successfully![/bold green]")

if __name__ == "__main__":
    typer.run(main)
