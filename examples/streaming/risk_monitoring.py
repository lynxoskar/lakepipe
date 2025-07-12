#!/usr/bin/env python3
"""
🚨 Real-time Risk Monitoring Example

This example demonstrates real-time risk monitoring using lakepipe for
position tracking, exposure calculation, and automated risk alerts.

Usage:
    python examples/streaming/risk_monitoring.py
    python examples/streaming/risk_monitoring.py --max-exposure 1000000 --alert-webhook http://localhost:8000/alerts
"""

import asyncio
import sys
from pathlib import Path
from typing import Optional, Dict, Any
import json
from datetime import datetime, timedelta

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.panel import Panel
from rich.table import Table
from rich.live import Live

# Add lakepipe to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from lakepipe.api.pipeline import create_pipeline
from lakepipe.config.defaults import build_pipeline_config
from lakepipe.config.loaders import load_config_file
from lakepipe.core.logging import configure_logging, get_logger

console = Console()
logger = get_logger(__name__)


def main(
    config_file: Optional[Path] = typer.Option(None, "--config", "-c", help="Configuration file"),
    max_exposure: float = typer.Option(1000000.0, "--max-exposure", help="Maximum exposure limit"),
    max_position: float = typer.Option(500000.0, "--max-position", help="Maximum position size"),
    var_limit: float = typer.Option(100000.0, "--var-limit", help="VaR limit"),
    alert_webhook: Optional[str] = typer.Option(None, "--alert-webhook", help="Alert webhook URL"),
    output_dir: str = typer.Option("output/risk", "--output", "-o", help="Output directory"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
):
    """
    🚨 Real-time risk monitoring and alerting
    
    This example shows:
    - Real-time position tracking and exposure calculation
    - Automated risk limit monitoring
    - VaR calculation and breach detection
    - Risk alert generation and notifications
    """
    
    console.print(Panel(
        f"🚨 [bold red]Real-time Risk Monitoring[/bold red]\n"
        f"Max Exposure: ${max_exposure:,.0f}\n"
        f"Max Position: ${max_position:,.0f}\n"
        f"VaR Limit: ${var_limit:,.0f}",
        title="🛡️ Risk Management",
        border_style="red"
    ))
    
    # Build configuration
    if config_file:
        file_config = load_config_file(config_file)
        console.print(f"✅ Loaded configuration from {config_file}")
    else:
        file_config = {}
    
    cli_config = {
        "source": {
            "uri": "kafka://market-data-ticks",
            "format": "kafka",
            "kafka": {
                "bootstrap_servers": ["localhost:9092"],
                "group_id": "lakepipe-risk-monitoring",
                "auto_offset_reset": "latest"
            }
        },
        "sink": {
            "uri": f"file://{output_dir}/risk-data/",
            "format": "parquet",
            "compression": "zstd",
            "partition_cols": ["date", "hour"]
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
                        "case when trade_type = 'BUY' then quantity when trade_type = 'SELL' then -quantity else 0 end as signed_quantity",
                        "case when trade_type = 'BUY' then notional when trade_type = 'SELL' then -notional else 0 end as signed_notional"
                    ]
                },
                {
                    "type": "window_function",
                    "window_spec": {
                        "partition_by": ["symbol"],
                        "order_by": ["timestamp"],
                        "frame": "unbounded preceding"
                    },
                    "expressions": [
                        "sum(signed_quantity) over w as cumulative_position",
                        "sum(signed_notional) over w as cumulative_exposure",
                        "avg(price) over w as avg_price"
                    ]
                },
                {
                    "type": "window_function",
                    "window_spec": {
                        "partition_by": ["symbol"],
                        "order_by": ["timestamp"],
                        "frame": "rows between 99 preceding and current row"
                    },
                    "expressions": [
                        "stddev(price) over w as price_volatility",
                        "avg(abs(signed_notional)) over w as avg_trade_size",
                        "count(*) over w as trade_count_100"
                    ]
                },
                {
                    "type": "with_columns",
                    "expressions": [
                        "abs(cumulative_position * price) as position_value",
                        "abs(cumulative_exposure) as total_exposure",
                        "case when price_volatility > 0 then price_volatility * sqrt(252) else 0 end as annualized_volatility",
                        "case when avg_price > 0 then (price - avg_price) / avg_price else 0 end as price_deviation_pct"
                    ]
                },
                {
                    "type": "with_columns",
                    "expressions": [
                        f"case when position_value > {max_position} then 1 else 0 end as position_limit_breach",
                        f"case when total_exposure > {max_exposure} then 1 else 0 end as exposure_limit_breach",
                        f"case when position_value * annualized_volatility * 1.96 > {var_limit} then 1 else 0 end as var_limit_breach",
                        "case when abs(price_deviation_pct) > 0.05 then 1 else 0 end as price_shock_alert",
                        "position_value * annualized_volatility * 1.96 as estimated_var"
                    ]
                },
                {
                    "type": "with_columns",
                    "expressions": [
                        "case when position_limit_breach + exposure_limit_breach + var_limit_breach + price_shock_alert > 0 then 1 else 0 end as risk_alert",
                        "case when position_limit_breach = 1 then 'Position Limit' when exposure_limit_breach = 1 then 'Exposure Limit' when var_limit_breach = 1 then 'VaR Limit' when price_shock_alert = 1 then 'Price Shock' else 'No Alert' end as alert_type"
                    ]
                }
            ]
        },
        "streaming": {
            "enabled": True,
            "batch_size": 1000,
            "max_latency_ms": 100
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
    if alert_webhook:
        console.print(f"🔔 Alert webhook: {alert_webhook}")
    
    # State tracking
    risk_stats = {
        'total_alerts': 0,
        'position_breaches': 0,
        'exposure_breaches': 0,
        'var_breaches': 0,
        'price_shocks': 0,
        'processed_count': 0,
        'max_position_value': 0,
        'max_exposure': 0,
        'max_var': 0
    }
    
    position_summary = {}
    
    # Create and run pipeline
    async def run_pipeline():
        nonlocal risk_stats, position_summary
        
        try:
            console.print("🚀 [bold green]Starting risk monitoring pipeline...[/bold green]")
            
            with Live(generate_risk_dashboard(), refresh_per_second=2, console=console) as live:
                pipeline = create_pipeline(config)
                
                async for batch_result in pipeline.stream():
                    if batch_result and batch_result.get('records'):
                        records = batch_result['records']
                        risk_stats['processed_count'] += len(records)
                        
                        # Process records for risk analysis
                        for record in records:
                            symbol = record.get('symbol')
                            
                            # Update position summary
                            position_summary[symbol] = {
                                'position_value': record.get('position_value', 0),
                                'total_exposure': record.get('total_exposure', 0),
                                'estimated_var': record.get('estimated_var', 0),
                                'price': record.get('price', 0),
                                'cumulative_position': record.get('cumulative_position', 0),
                                'alert_type': record.get('alert_type', 'No Alert'),
                                'timestamp': record.get('timestamp')
                            }
                            
                            # Update risk statistics
                            risk_stats['max_position_value'] = max(risk_stats['max_position_value'], record.get('position_value', 0))
                            risk_stats['max_exposure'] = max(risk_stats['max_exposure'], record.get('total_exposure', 0))
                            risk_stats['max_var'] = max(risk_stats['max_var'], record.get('estimated_var', 0))
                            
                            # Count alerts
                            if record.get('risk_alert') == 1:
                                risk_stats['total_alerts'] += 1
                                
                                alert_type = record.get('alert_type', 'Unknown')
                                if alert_type == 'Position Limit':
                                    risk_stats['position_breaches'] += 1
                                elif alert_type == 'Exposure Limit':
                                    risk_stats['exposure_breaches'] += 1
                                elif alert_type == 'VaR Limit':
                                    risk_stats['var_breaches'] += 1
                                elif alert_type == 'Price Shock':
                                    risk_stats['price_shocks'] += 1
                                
                                # Send alert if webhook configured
                                if alert_webhook:
                                    await send_risk_alert(record, alert_webhook)
                        
                        # Update live dashboard
                        live.update(generate_risk_dashboard())
                
        except KeyboardInterrupt:
            console.print("\n⏹️  [yellow]Risk monitoring stopped by user[/yellow]")
            display_final_summary()
        except Exception as e:
            console.print(f"❌ [red]Pipeline error: {e}[/red]")
            if verbose:
                console.print_exception()
            raise
    
    def generate_risk_dashboard():
        """Generate the live risk dashboard"""
        # Risk summary table
        risk_table = Table(title="🚨 Risk Summary")
        risk_table.add_column("Metric", style="bold blue")
        risk_table.add_column("Value", style="green")
        risk_table.add_column("Limit", style="yellow")
        risk_table.add_column("Status", style="red")
        
        risk_table.add_row("Total Alerts", f"{risk_stats['total_alerts']}", "-", "🔍")
        risk_table.add_row("Position Breaches", f"{risk_stats['position_breaches']}", "-", "⚠️" if risk_stats['position_breaches'] > 0 else "✅")
        risk_table.add_row("Exposure Breaches", f"{risk_stats['exposure_breaches']}", "-", "⚠️" if risk_stats['exposure_breaches'] > 0 else "✅")
        risk_table.add_row("VaR Breaches", f"{risk_stats['var_breaches']}", "-", "⚠️" if risk_stats['var_breaches'] > 0 else "✅")
        risk_table.add_row("Price Shocks", f"{risk_stats['price_shocks']}", "-", "⚠️" if risk_stats['price_shocks'] > 0 else "✅")
        
        # Position summary table
        position_table = Table(title="📊 Position Summary")
        position_table.add_column("Symbol", style="bold blue")
        position_table.add_column("Position", style="green")
        position_table.add_column("Value", style="yellow")
        position_table.add_column("VaR", style="magenta")
        position_table.add_column("Alert", style="red")
        
        for symbol, pos in sorted(position_summary.items()):
            position_table.add_row(
                symbol,
                f"{pos['cumulative_position']:,.0f}",
                f"${pos['position_value']:,.0f}",
                f"${pos['estimated_var']:,.0f}",
                pos['alert_type']
            )
        
        return Panel(
            f"{risk_table}\n\n{position_table}\n\n"
            f"📈 Processed: {risk_stats['processed_count']:,} records\n"
            f"💰 Max Position: ${risk_stats['max_position_value']:,.0f}\n"
            f"🏦 Max Exposure: ${risk_stats['max_exposure']:,.0f}\n"
            f"⚠️ Max VaR: ${risk_stats['max_var']:,.0f}",
            title="🛡️ Real-time Risk Dashboard",
            border_style="red"
        )
    
    async def send_risk_alert(record: Dict[str, Any], webhook_url: str):
        """Send risk alert to webhook"""
        try:
            alert_data = {
                'timestamp': record.get('timestamp'),
                'symbol': record.get('symbol'),
                'alert_type': record.get('alert_type'),
                'position_value': record.get('position_value'),
                'total_exposure': record.get('total_exposure'),
                'estimated_var': record.get('estimated_var'),
                'price': record.get('price')
            }
            
            # In a real implementation, you would send HTTP POST to webhook
            console.print(f"🔔 [yellow]ALERT: {alert_data['alert_type']} for {alert_data['symbol']}[/yellow]")
            
        except Exception as e:
            logger.error(f"Failed to send alert: {e}")
    
    def display_final_summary():
        """Display final risk summary"""
        console.print(Panel(
            f"🚨 [bold red]Risk Monitoring Summary[/bold red]\n\n"
            f"Total Records Processed: {risk_stats['processed_count']:,}\n"
            f"Total Risk Alerts: {risk_stats['total_alerts']}\n"
            f"Position Limit Breaches: {risk_stats['position_breaches']}\n"
            f"Exposure Limit Breaches: {risk_stats['exposure_breaches']}\n"
            f"VaR Limit Breaches: {risk_stats['var_breaches']}\n"
            f"Price Shock Alerts: {risk_stats['price_shocks']}\n\n"
            f"Maximum Position Value: ${risk_stats['max_position_value']:,.0f}\n"
            f"Maximum Exposure: ${risk_stats['max_exposure']:,.0f}\n"
            f"Maximum VaR: ${risk_stats['max_var']:,.0f}",
            title="📊 Session Complete",
            border_style="green"
        ))
    
    # Run the async pipeline
    asyncio.run(run_pipeline())


if __name__ == "__main__":
    typer.run(main)
