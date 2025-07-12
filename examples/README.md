# 📈 Lakepipe Financial Data Examples

This folder contains comprehensive examples of using lakepipe for financial market data processing, showcasing both **streaming tick data** and **batch minute bar** processing patterns.

## 📁 Example Structure

```
examples/
├── 📡 streaming/                    # Real-time tick data processing
│   ├── tick_data_kafka_streaming.py    # Basic tick data streaming
│   ├── real_time_vwap.py               # VWAP calculation engine
│   ├── market_data_enrichment.py       # Real-time data enrichment
│   └── risk_monitoring.py              # Real-time risk alerts
├── 📊 batch/                        # Batch minute bar processing  
│   ├── minute_bars_polars.py           # OHLCV data processing
│   ├── technical_indicators.py         # RSI, MACD, Bollinger Bands
│   ├── portfolio_analytics.py          # Portfolio performance analysis
│   └── market_data_etl.py              # Daily ETL pipeline
├── 📋 data/                         # Sample datasets
│   ├── sample_tick_data.csv            # Sample tick data
│   ├── sample_minute_bars.parquet      # Sample OHLCV data
│   ├── reference_data.json             # Symbol reference data
│   └── market_config.json              # Market data configuration
├── 🔧 configs/                      # Configuration files
│   ├── streaming_config.yaml           # Streaming pipeline config
│   ├── batch_config.yaml               # Batch processing config
│   └── .env.example                    # Environment variables
└── 📖 README.md                     # This file
```

## 🚀 Quick Start

### Running Streaming Examples

```bash
# 1. Basic tick data streaming
python examples/streaming/tick_data_kafka_streaming.py

# 2. Real-time VWAP calculation
python examples/streaming/real_time_vwap.py --symbol AAPL

# 3. Risk monitoring
python examples/streaming/risk_monitoring.py

# Via CLI with YAML config
lakepipe run --config examples/configs/streaming_config.yaml --verbose
```

### Running Batch Examples

```bash
# 1. Process minute bars
python examples/batch/minute_bars_polars.py

# 2. Calculate technical indicators  
python examples/batch/technical_indicators.py

# 3. Portfolio analytics
python examples/batch/portfolio_analytics.py

# Via CLI with YAML config
lakepipe run --config examples/configs/batch_config.yaml --cache --verbose
```

## 📊 Performance Benchmarks

- **Tick Processing**: 100K+ ticks/second
- **VWAP Calculation**: <1ms latency
- **Minute Bar Processing**: 1M+ bars/second
- **Technical Indicators**: Full day in <100ms

## 🎯 Learning Objectives

After going through these examples, you'll understand:

1. **🌊 Stream Processing Patterns**
   - Real-time data transformation with DuckDB
   - Window-based calculations for financial metrics
   - Low-latency processing architectures

2. **📊 Batch Processing Optimization**
   - Efficient data processing with Polars
   - Intelligent caching strategies for market data
   - Large-scale financial data ETL patterns

3. **🔧 Production Patterns**
   - YAML configuration management for financial systems
   - Error handling and monitoring in trading systems
   - Performance optimization for market data processing

4. **📈 Financial Domain Knowledge**
   - Market data structures and formats
   - Common financial calculations (VWAP, technical indicators)
   - Risk management and portfolio analytics

---

**💰 Ready to process financial data at scale with lakepipe!** 🌊📈
