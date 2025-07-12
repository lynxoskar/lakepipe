#!/usr/bin/env python3
"""Update CLI to support YAML configuration"""

# Read the current CLI file
with open('lakepipe/cli/main.py', 'r') as f:
    content = f.read()

# Add import for config loader
import_section = """import typer
from typing import Optional
from pathlib import Path
import asyncio
import time
import orjson
from rich.console import Console
from rich.progress import Progress

from lakepipe.config.defaults import build_pipeline_config
from lakepipe.config.types import PipelineConfig
from lakepipe.config.loaders import load_config_file, save_config_file
from lakepipe.core.logging import configure_logging, get_logger
from lakepipe.api.pipeline import create_pipeline, execute_pipeline"""

# Replace the import section
old_import = """import typer
from typing import Optional
from pathlib import Path
import asyncio
import time
import orjson
from rich.console import Console
from rich.progress import Progress

from lakepipe.config.defaults import build_pipeline_config
from lakepipe.config.types import PipelineConfig
from lakepipe.core.logging import configure_logging, get_logger
from lakepipe.api.pipeline import create_pipeline, execute_pipeline"""

content = content.replace(old_import, import_section)

# Update the help text for config file parameter
content = content.replace(
    'help="Configuration file (JSON)"',
    'help="Configuration file (JSON or YAML)"'
)

# Update the config loading section
old_config_loading = """    # Build configuration using environment-aware builder + orjson for config files
    file_config = {}
    if config_file:
        try:
            with open(config_file, 'rb') as f:
                file_config = orjson.loads(f.read())
            console.print(f"✅ Loaded configuration from {config_file}")
        except Exception as e:
            console.print(f"❌ Failed to load config file {config_file}: {e}")
            raise typer.Exit(1)"""

new_config_loading = """    # Build configuration using environment-aware builder + support for JSON/YAML
    file_config = {}
    if config_file:
        try:
            file_config = load_config_file(config_file)
            file_format = "YAML" if str(config_file).endswith(('.yaml', '.yml')) else "JSON"
            console.print(f"✅ Loaded {file_format} configuration from {config_file}")
        except Exception as e:
            console.print(f"❌ Failed to load config file {config_file}: {e}")
            raise typer.Exit(1)"""

content = content.replace(old_config_loading, new_config_loading)

# Update the examples in the docstring
content = content.replace(
    'lakepipe run --config config.json --verbose',
    'lakepipe run --config config.yaml --verbose'
)

# Write the updated content
with open('lakepipe/cli/main.py', 'w') as f:
    f.write(content)

print("✅ Updated CLI to support YAML configurations")
