"""
Configuration file loaders supporting JSON and YAML formats.
"""

import json
from pathlib import Path
from typing import Dict, Any, Union
import orjson
import yaml


def load_config_file(file_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Load configuration from JSON or YAML file.
    
    Args:
        file_path: Path to configuration file
        
    Returns:
        Dictionary containing configuration
        
    Raises:
        ValueError: If file format is not supported
        FileNotFoundError: If file doesn't exist
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {file_path}")
    
    suffix = file_path.suffix.lower()
    
    if suffix in ['.json']:
        return _load_json_config(file_path)
    elif suffix in ['.yaml', '.yml']:
        return _load_yaml_config(file_path)
    else:
        # Try to detect format based on content
        try:
            return _load_json_config(file_path)
        except:
            try:
                return _load_yaml_config(file_path)
            except:
                raise ValueError(f"Unsupported configuration file format: {suffix}")


def _load_json_config(file_path: Path) -> Dict[str, Any]:
    """Load JSON configuration file using orjson for performance"""
    try:
        with open(file_path, 'rb') as f:
            return orjson.loads(f.read())
    except orjson.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in {file_path}: {e}")


def _load_yaml_config(file_path: Path) -> Dict[str, Any]:
    """Load YAML configuration file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML in {file_path}: {e}")


def save_config_file(config: Dict[str, Any], file_path: Union[str, Path], format: str = 'auto') -> None:
    """
    Save configuration to JSON or YAML file.
    
    Args:
        config: Configuration dictionary to save
        file_path: Path to save configuration to
        format: Format to save as ('json', 'yaml', or 'auto' to detect from extension)
    """
    file_path = Path(file_path)
    
    if format == 'auto':
        suffix = file_path.suffix.lower()
        if suffix in ['.yaml', '.yml']:
            format = 'yaml'
        else:
            format = 'json'
    
    if format == 'yaml':
        _save_yaml_config(config, file_path)
    else:
        _save_json_config(config, file_path)


def _save_json_config(config: Dict[str, Any], file_path: Path) -> None:
    """Save configuration as JSON using orjson"""
    with open(file_path, 'wb') as f:
        f.write(orjson.dumps(config, option=orjson.OPT_INDENT_2))


def _save_yaml_config(config: Dict[str, Any], file_path: Path) -> None:
    """Save configuration as YAML"""
    with open(file_path, 'w', encoding='utf-8') as f:
        yaml.dump(config, f, default_flow_style=False, indent=2, sort_keys=False)
