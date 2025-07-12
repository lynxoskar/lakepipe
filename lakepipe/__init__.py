"""
Lakepipe: Modern, functional data pipeline library for high-performance
batch and streaming transformations across multiple lakehouse formats.

Built on railway-oriented programming principles with zero-copy operations.
"""

__version__ = "1.0.0"

from lakepipe.api.pipeline import create_pipeline
from lakepipe.config.types import PipelineConfig
from lakepipe.core.results import Result, Success, Failure

__all__ = [
    "create_pipeline",
    "PipelineConfig", 
    "Result",
    "Success",
    "Failure",
] 