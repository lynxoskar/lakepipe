"""
Logging configuration for lakepipe using loguru with Rich integration.
"""

import sys
from typing import Optional, Dict, Any
from pathlib import Path
from loguru import logger
from rich.logging import RichHandler
from rich.console import Console

from lakepipe.config.types import LogConfig

# Global console instance for Rich integration
console = Console()

# Store original logger configuration
_original_logger_config = None
_is_configured = False

def configure_logging(config: LogConfig) -> None:
    """
    Configure logging with loguru and Rich integration.
    
    Args:
        config: Logging configuration
    """
    global _is_configured
    
    if _is_configured:
        return
    
    # Remove default handler
    logger.remove()
    
    # Configure console logging with Rich
    logger.add(
        RichHandler(
            console=console,
            show_time=True,
            show_path=True,
            markup=True,
            rich_tracebacks=True,
            tracebacks_show_locals=True,
        ),
        level=config["level"],
        format=config["format"],
        colorize=True,
        enqueue=True,  # Thread-safe logging
    )
    
    # Add file logging if specified
    if config.get("file"):
        file_path = Path(config["file"])
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.add(
            str(file_path),
            level=config["level"],
            format=config["format"],
            rotation="10 MB",
            retention="7 days",
            compression="gz",
            enqueue=True,
        )
    
    # Set up structured logging for JSON output
    logger.add(
        sys.stderr,
        level="ERROR",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name} | {message}",
        serialize=True,  # JSON output
        enqueue=True,
    )
    
    _is_configured = True

def get_logger(name: str) -> Any:
    """
    Get a logger instance with the specified name.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Logger instance
    """
    return logger.bind(name=name)

def log_processor_metrics(
    processor_name: str,
    metrics: Dict[str, Any],
    level: str = "INFO"
) -> None:
    """
    Log processor metrics in a structured format.
    
    Args:
        processor_name: Name of the processor
        metrics: Metrics dictionary
        level: Log level
    """
    logger.bind(
        processor=processor_name,
        metrics=metrics
    ).log(level, f"Processor {processor_name} metrics: {metrics}")

def log_pipeline_event(
    event_type: str,
    pipeline_name: str,
    details: Optional[Dict[str, Any]] = None,
    level: str = "INFO"
) -> None:
    """
    Log pipeline events in a structured format.
    
    Args:
        event_type: Type of event (start, complete, error, etc.)
        pipeline_name: Name of the pipeline
        details: Additional event details
        level: Log level
    """
    details = details or {}
    
    logger.bind(
        event_type=event_type,
        pipeline=pipeline_name,
        details=details
    ).log(level, f"Pipeline {pipeline_name} {event_type}: {details}")

def log_cache_event(
    event_type: str,
    cache_key: str,
    details: Optional[Dict[str, Any]] = None,
    level: str = "DEBUG"
) -> None:
    """
    Log cache events in a structured format.
    
    Args:
        event_type: Type of event (hit, miss, store, evict, etc.)
        cache_key: Cache key
        details: Additional event details
        level: Log level
    """
    details = details or {}
    
    logger.bind(
        event_type=event_type,
        cache_key=cache_key,
        details=details
    ).log(level, f"Cache {event_type} for key {cache_key}: {details}")

def log_streaming_event(
    event_type: str,
    topic: str,
    details: Optional[Dict[str, Any]] = None,
    level: str = "DEBUG"
) -> None:
    """
    Log streaming events in a structured format.
    
    Args:
        event_type: Type of event (produce, consume, error, etc.)
        topic: Kafka topic or stream identifier
        details: Additional event details
        level: Log level
    """
    details = details or {}
    
    logger.bind(
        event_type=event_type,
        topic=topic,
        details=details
    ).log(level, f"Streaming {event_type} for topic {topic}: {details}")

# Context managers for logging

class LogContext:
    """Context manager for adding context to logs"""
    
    def __init__(self, **context: Any):
        self.context = context
        self.logger = logger.bind(**context)
    
    def __enter__(self):
        return self.logger
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.logger.error(f"Exception in context: {exc_val}")
        return False

def pipeline_context(pipeline_name: str) -> LogContext:
    """Create a logging context for a pipeline"""
    return LogContext(pipeline=pipeline_name)

def processor_context(processor_name: str) -> LogContext:
    """Create a logging context for a processor"""
    return LogContext(processor=processor_name)

def cache_context(cache_key: str) -> LogContext:
    """Create a logging context for cache operations"""
    return LogContext(cache_key=cache_key)

# Performance logging utilities

import time
import functools
from typing import Callable, TypeVar, Any

F = TypeVar('F', bound=Callable[..., Any])

def log_performance(func: F) -> F:
    """
    Decorator to log function performance.
    
    Args:
        func: Function to decorate
        
    Returns:
        Decorated function
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        func_logger = get_logger(func.__module__)
        
        try:
            result = func(*args, **kwargs)
            end_time = time.time()
            duration = end_time - start_time
            
            func_logger.debug(
                f"Function {func.__name__} completed in {duration:.4f}s"
            )
            return result
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            
            func_logger.error(
                f"Function {func.__name__} failed after {duration:.4f}s: {e}"
            )
            raise
    
    return wrapper

def log_async_performance(func: F) -> F:
    """
    Decorator to log async function performance.
    
    Args:
        func: Async function to decorate
        
    Returns:
        Decorated function
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        func_logger = get_logger(func.__module__)
        
        try:
            result = await func(*args, **kwargs)
            end_time = time.time()
            duration = end_time - start_time
            
            func_logger.debug(
                f"Async function {func.__name__} completed in {duration:.4f}s"
            )
            return result
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            
            func_logger.error(
                f"Async function {func.__name__} failed after {duration:.4f}s: {e}"
            )
            raise
    
    return wrapper

# Memory usage logging

import psutil
import os

def log_memory_usage(context: str = "general") -> None:
    """
    Log current memory usage.
    
    Args:
        context: Context for the memory usage log
    """
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    
    logger.bind(
        context=context,
        memory_rss=memory_info.rss,
        memory_vms=memory_info.vms,
        memory_percent=process.memory_percent()
    ).info(f"Memory usage in {context}: RSS={memory_info.rss / 1024 / 1024:.2f}MB, "
           f"VMS={memory_info.vms / 1024 / 1024:.2f}MB, "
           f"Percent={process.memory_percent():.2f}%")

# Error handling with logging

def log_and_reraise(
    exception: Exception,
    context: str = "general",
    level: str = "ERROR"
) -> None:
    """
    Log an exception and re-raise it.
    
    Args:
        exception: Exception to log
        context: Context for the error
        level: Log level
    """
    logger.bind(
        context=context,
        exception_type=type(exception).__name__,
        exception_message=str(exception)
    ).log(level, f"Exception in {context}: {exception}")
    
    raise exception

def log_and_suppress(
    exception: Exception,
    context: str = "general",
    level: str = "WARNING"
) -> None:
    """
    Log an exception and suppress it.
    
    Args:
        exception: Exception to log
        context: Context for the error
        level: Log level
    """
    logger.bind(
        context=context,
        exception_type=type(exception).__name__,
        exception_message=str(exception)
    ).log(level, f"Suppressed exception in {context}: {exception}")

# Testing utilities

def capture_logs():
    """
    Context manager to capture logs for testing.
    
    Returns:
        List of log records
    """
    import io
    from contextlib import redirect_stderr
    
    log_capture = io.StringIO()
    
    class LogCapture:
        def __init__(self, stream):
            self.stream = stream
            self.logs = []
        
        def __enter__(self):
            # Add a handler that captures to our list
            logger.add(
                self.stream,
                level="DEBUG",
                format="{time} | {level} | {name} | {message}",
                enqueue=False,
            )
            return self.logs
        
        def __exit__(self, exc_type, exc_val, exc_tb):
            # Remove the capture handler
            logger.remove()
            self.logs.extend(self.stream.getvalue().split('\n'))
    
    return LogCapture(log_capture) 