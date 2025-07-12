"""
Result types for railway-oriented programming.
Using the returns library for functional error handling.
"""

from typing import TypeVar, Generic, Union, Callable, Any
from returns.result import Result as ReturnsResult, Success as ReturnsSuccess, Failure as ReturnsFailure

# Type variables for generic type safety
T = TypeVar('T')  # Success value type
E = TypeVar('E')  # Error value type

# Re-export common types from returns library
Result = ReturnsResult[T, E]
Success = ReturnsSuccess[T]
Failure = ReturnsFailure[E]

# Common error types for lakepipe
class LakepipeError(Exception):
    """Base exception for all lakepipe errors"""
    pass

class ConfigurationError(LakepipeError):
    """Configuration-related errors"""
    pass

class ProcessingError(LakepipeError):
    """Data processing errors"""
    pass

class CacheError(LakepipeError):
    """Cache-related errors"""
    pass

class StreamingError(LakepipeError):
    """Streaming processing errors"""
    pass

class KafkaError(LakepipeError):
    """Kafka-related errors"""
    pass

# Utility functions for working with Results

def safe_execute(func: Callable[..., T], *args: Any, **kwargs: Any) -> Result[T, Exception]:
    """
    Safely execute a function and return a Result.
    
    Args:
        func: Function to execute
        *args: Positional arguments for the function
        **kwargs: Keyword arguments for the function
        
    Returns:
        Success with the result or Failure with the exception
    """
    try:
        result = func(*args, **kwargs)
        return Success(result)
    except Exception as e:
        return Failure(e)

async def async_safe_execute(
    func: Callable[..., T], 
    *args: Any, 
    **kwargs: Any
) -> Result[T, Exception]:
    """
    Safely execute an async function and return a Result.
    
    Args:
        func: Async function to execute
        *args: Positional arguments for the function
        **kwargs: Keyword arguments for the function
        
    Returns:
        Success with the result or Failure with the exception
    """
    try:
        result = await func(*args, **kwargs)
        return Success(result)
    except Exception as e:
        return Failure(e)

def chain_results(*results: Result[Any, E]) -> Result[list[Any], E]:
    """
    Chain multiple Results together. If any fails, return the first failure.
    
    Args:
        *results: Results to chain together
        
    Returns:
        Success with list of all values or first Failure encountered
    """
    values = []
    for result in results:
        if isinstance(result, Failure):
            return result
        values.append(result.unwrap())
    return Success(values)

def collect_results(results: list[Result[T, E]]) -> Result[list[T], list[E]]:
    """
    Collect all Results, returning either all successes or all failures.
    
    Args:
        results: List of Results to collect
        
    Returns:
        Success with all values or Failure with all errors
    """
    successes = []
    failures = []
    
    for result in results:
        if isinstance(result, Success):
            successes.append(result.unwrap())
        else:
            failures.append(result.failure())
    
    if failures:
        return Failure(failures)
    return Success(successes)

# Context managers for Result handling

class ResultContext:
    """Context manager for handling Results with automatic error propagation"""
    
    def __init__(self):
        self.errors: list[Exception] = []
        self.results: list[Any] = []
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.errors.append(exc_val)
        return False  # Don't suppress exceptions
    
    def add_result(self, result: Result[T, E]) -> T:
        """Add a result and return the unwrapped value if successful"""
        if isinstance(result, Success):
            value = result.unwrap()
            self.results.append(value)
            return value
        else:
            error = result.failure()
            self.errors.append(error)
            raise error
    
    def get_result(self) -> Result[list[Any], list[Exception]]:
        """Get the final result with all collected values or errors"""
        if self.errors:
            return Failure(self.errors)
        return Success(self.results)

# Decorators for automatic Result wrapping

def result_wrapper(func: Callable[..., T]) -> Callable[..., Result[T, Exception]]:
    """Decorator to automatically wrap function results in Result types"""
    def wrapper(*args, **kwargs) -> Result[T, Exception]:
        return safe_execute(func, *args, **kwargs)
    return wrapper

def async_result_wrapper(func: Callable[..., T]) -> Callable[..., Result[T, Exception]]:
    """Decorator to automatically wrap async function results in Result types"""
    async def wrapper(*args, **kwargs) -> Result[T, Exception]:
        return await async_safe_execute(func, *args, **kwargs)
    return wrapper 