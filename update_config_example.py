#!/usr/bin/env python3
"""Update the config_example command to generate YAML files"""

# Read the current CLI file
with open('lakepipe/cli/main.py', 'r') as f:
    content = f.read()

# Find and replace the config_example function
old_config_example = '''@app.command()
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
    console.print("  📄 lakepipe.config.json")'''

new_config_example = '''@app.command()
def config_example():
    """Generate example configuration files in both JSON and YAML formats."""
    
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
    
    # Generate example configuration structure
    example_config = {
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
    
    # Save in both JSON and YAML formats
    save_config_file(example_config, "lakepipe.config.json", "json")
    save_config_file(example_config, "lakepipe.config.yaml", "yaml")
    
    console.print("✅ Generated example configuration files:")
    console.print("  📄 .env.example")
    console.print("  📄 lakepipe.config.json")
    console.print("  📄 lakepipe.config.yaml")'''

# Replace the config_example function
content = content.replace(old_config_example, new_config_example)

# Write the updated content
with open('lakepipe/cli/main.py', 'w') as f:
    f.write(content)

print("✅ Updated config_example command to generate both JSON and YAML files")
