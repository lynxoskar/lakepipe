"""
Stream manipulation utilities for lakepipe processors.
Provides functional stream operations and utilities.
"""

import asyncio
from typing import (
    TypeVar, AsyncGenerator, Callable, Optional, Any, 
    Union, Awaitable, Iterable, AsyncIterable
)
from contextlib import asynccontextmanager
import time

from lakepipe.core.results import Result, Success, Failure

T = TypeVar('T')
U = TypeVar('U')

# Core stream utilities

async def empty_stream() -> AsyncGenerator[Any, None]:
    """Create an empty async stream"""
    return
    yield  # Make it a generator

async def single_item_stream(item: T) -> AsyncGenerator[T, None]:
    """Create a stream with a single item"""
    yield item

async def from_iterable(items: Iterable[T]) -> AsyncGenerator[T, None]:
    """Create a stream from an iterable"""
    for item in items:
        yield item

async def from_async_iterable(items: AsyncIterable[T]) -> AsyncGenerator[T, None]:
    """Create a stream from an async iterable"""
    async for item in items:
        yield item

async def endless_stream() -> AsyncGenerator[None, None]:
    """Create an endless stream of None values (for source processors)"""
    while True:
        yield None

# Stream transformation utilities

async def map_stream(
    stream: AsyncGenerator[T, None], 
    func: Callable[[T], U]
) -> AsyncGenerator[U, None]:
    """Map a function over stream items"""
    async for item in stream:
        yield func(item)

async def async_map_stream(
    stream: AsyncGenerator[T, None], 
    func: Callable[[T], Awaitable[U]]
) -> AsyncGenerator[U, None]:
    """Map an async function over stream items"""
    async for item in stream:
        yield await func(item)

async def filter_stream(
    stream: AsyncGenerator[T, None], 
    predicate: Callable[[T], bool]
) -> AsyncGenerator[T, None]:
    """Filter stream items based on predicate"""
    async for item in stream:
        if predicate(item):
            yield item

async def async_filter_stream(
    stream: AsyncGenerator[T, None], 
    predicate: Callable[[T], Awaitable[bool]]
) -> AsyncGenerator[T, None]:
    """Filter stream items based on async predicate"""
    async for item in stream:
        if await predicate(item):
            yield item

async def take_stream(
    stream: AsyncGenerator[T, None], 
    count: int
) -> AsyncGenerator[T, None]:
    """Take only the first n items from stream"""
    taken = 0
    async for item in stream:
        if taken >= count:
            break
        yield item
        taken += 1

async def skip_stream(
    stream: AsyncGenerator[T, None], 
    count: int
) -> AsyncGenerator[T, None]:
    """Skip the first n items from stream"""
    skipped = 0
    async for item in stream:
        if skipped < count:
            skipped += 1
            continue
        yield item

async def batch_stream(
    stream: AsyncGenerator[T, None], 
    batch_size: int
) -> AsyncGenerator[list[T], None]:
    """Batch stream items together"""
    batch = []
    async for item in stream:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    
    # Yield remaining items
    if batch:
        yield batch

async def throttle_stream(
    stream: AsyncGenerator[T, None], 
    rate_limit: float
) -> AsyncGenerator[T, None]:
    """Throttle stream to a maximum rate (items per second)"""
    last_time = time.time()
    min_interval = 1.0 / rate_limit
    
    async for item in stream:
        current_time = time.time()
        elapsed = current_time - last_time
        
        if elapsed < min_interval:
            await asyncio.sleep(min_interval - elapsed)
        
        yield item
        last_time = time.time()

async def buffer_stream(
    stream: AsyncGenerator[T, None], 
    buffer_size: int
) -> AsyncGenerator[T, None]:
    """Buffer stream items for smoother processing"""
    buffer = []
    
    async for item in stream:
        buffer.append(item)
        
        # Yield items from buffer once it's full
        if len(buffer) >= buffer_size:
            for buffered_item in buffer:
                yield buffered_item
            buffer = []
    
    # Yield remaining buffered items
    for buffered_item in buffer:
        yield buffered_item

# Stream combination utilities

async def concat_streams(
    *streams: AsyncGenerator[T, None]
) -> AsyncGenerator[T, None]:
    """Concatenate multiple streams"""
    for stream in streams:
        async for item in stream:
            yield item

async def merge_streams(
    *streams: AsyncGenerator[T, None]
) -> AsyncGenerator[T, None]:
    """Merge multiple streams concurrently"""
    async def stream_to_queue(stream: AsyncGenerator[T, None], queue: asyncio.Queue):
        """Helper to put stream items into a queue"""
        try:
            async for item in stream:
                await queue.put(item)
        except Exception as e:
            await queue.put(e)
        finally:
            await queue.put(None)  # Signal end of stream
    
    queue = asyncio.Queue()
    tasks = []
    
    # Start tasks for each stream
    for stream in streams:
        task = asyncio.create_task(stream_to_queue(stream, queue))
        tasks.append(task)
    
    # Collect items from all streams
    completed_streams = 0
    while completed_streams < len(streams):
        item = await queue.get()
        
        if item is None:
            completed_streams += 1
        elif isinstance(item, Exception):
            # Handle stream errors
            raise item
        else:
            yield item
    
    # Wait for all tasks to complete
    await asyncio.gather(*tasks)

async def zip_streams(
    *streams: AsyncGenerator[T, None]
) -> AsyncGenerator[tuple[T, ...], None]:
    """Zip multiple streams together"""
    iterators = [stream.__aiter__() for stream in streams]
    
    while True:
        try:
            # Get next item from each stream
            items = []
            for iterator in iterators:
                item = await iterator.__anext__()
                items.append(item)
            
            yield tuple(items)
        except StopAsyncIteration:
            # Stop when any stream ends
            break

# Stream monitoring utilities

class StreamMonitor:
    """Monitor stream processing metrics"""
    
    def __init__(self, name: str):
        self.name = name
        self.item_count = 0
        self.error_count = 0
        self.start_time = time.time()
        self.last_item_time = time.time()
    
    def record_item(self):
        """Record processing of an item"""
        self.item_count += 1
        self.last_item_time = time.time()
    
    def record_error(self):
        """Record an error"""
        self.error_count += 1
    
    def get_metrics(self) -> dict[str, Any]:
        """Get current metrics"""
        current_time = time.time()
        elapsed_time = current_time - self.start_time
        
        return {
            "name": self.name,
            "item_count": self.item_count,
            "error_count": self.error_count,
            "elapsed_time": elapsed_time,
            "throughput": self.item_count / elapsed_time if elapsed_time > 0 else 0,
            "error_rate": self.error_count / self.item_count if self.item_count > 0 else 0,
            "last_item_time": self.last_item_time
        }

async def monitor_stream(
    stream: AsyncGenerator[T, None], 
    monitor: StreamMonitor
) -> AsyncGenerator[T, None]:
    """Add monitoring to a stream"""
    async for item in stream:
        monitor.record_item()
        yield item

# Stream error handling

async def safe_stream(
    stream: AsyncGenerator[T, None], 
    error_handler: Optional[Callable[[Exception], None]] = None
) -> AsyncGenerator[Union[T, Exception], None]:
    """Make a stream safe by catching and yielding exceptions"""
    try:
        async for item in stream:
            yield item
    except Exception as e:
        if error_handler:
            error_handler(e)
        yield e

async def retry_stream(
    stream_factory: Callable[[], AsyncGenerator[T, None]], 
    max_retries: int = 3,
    delay: float = 1.0
) -> AsyncGenerator[T, None]:
    """Retry stream creation on failure"""
    retries = 0
    
    while retries <= max_retries:
        try:
            stream = stream_factory()
            async for item in stream:
                yield item
            break  # Success, exit retry loop
        except Exception as e:
            retries += 1
            if retries > max_retries:
                raise e
            await asyncio.sleep(delay * retries)  # Exponential backoff

# Stream utilities for Result types

async def result_stream(
    stream: AsyncGenerator[T, None], 
    processor: Callable[[T], Result[U, Exception]]
) -> AsyncGenerator[Result[U, Exception], None]:
    """Process stream items and yield Results"""
    async for item in stream:
        result = processor(item)
        yield result

async def unwrap_result_stream(
    stream: AsyncGenerator[Result[T, Exception], None]
) -> AsyncGenerator[T, None]:
    """Unwrap successful Results from stream, skip failures"""
    async for result in stream:
        if isinstance(result, Success):
            yield result.unwrap()
        # Skip failures

async def collect_stream_results(
    stream: AsyncGenerator[Result[T, Exception], None]
) -> Result[list[T], list[Exception]]:
    """Collect all Results from stream"""
    successes = []
    failures = []
    
    async for result in stream:
        if isinstance(result, Success):
            successes.append(result.unwrap())
        else:
            failures.append(result.failure())
    
    if failures:
        return Failure(failures)
    return Success(successes)

# Context managers for stream processing

@asynccontextmanager
async def stream_context():
    """Context manager for stream processing setup/cleanup"""
    # Setup
    monitors = []
    start_time = time.time()
    
    try:
        yield monitors
    finally:
        # Cleanup
        end_time = time.time()
        total_time = end_time - start_time
        
        # Could log final metrics here
        for monitor in monitors:
            metrics = monitor.get_metrics()
            # Log or store metrics as needed 