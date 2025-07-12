"""
Base processor interface for stream-based processing.
Inspired by genai-processors for composable, functional stream processing.
"""

from abc import ABC, abstractmethod
from typing import (
    TypeVar, Generic, AsyncGenerator, Any, Protocol, 
    Optional, Callable, Union, TypedDict
)
import asyncio
from contextlib import asynccontextmanager
import polars as pl

from lakepipe.core.results import Result, Success, Failure, async_safe_execute
from lakepipe.config.types import PipelineConfig

# Type definitions for data flow
T = TypeVar('T')
U = TypeVar('U')

class DataPart(TypedDict):
    """Standard data part structure for streaming"""
    data: pl.LazyFrame
    metadata: dict[str, Any]
    source_info: dict[str, Any]
    schema: dict[str, Any]

class ProcessorMetrics(TypedDict):
    """Metrics collected during processing"""
    processed_count: int
    error_count: int
    processing_time: float
    memory_usage: int
    throughput: float

# Base processor interface

class BaseProcessor(ABC, Generic[T, U]):
    """
    Base processor interface for stream-based data processing.
    
    All processors in lakepipe inherit from this base class and implement
    the process method for composable stream processing.
    """
    
    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.metrics: ProcessorMetrics = {
            "processed_count": 0,
            "error_count": 0,
            "processing_time": 0.0,
            "memory_usage": 0,
            "throughput": 0.0
        }
    
    @abstractmethod
    async def process(
        self, 
        input_stream: AsyncGenerator[T, None]
    ) -> AsyncGenerator[U, None]:
        """
        Process the input stream and yield results.
        
        Args:
            input_stream: Async generator of input data
            
        Yields:
            Processed data of type U
        """
        pass
    
    async def initialize(self) -> Result[None, Exception]:
        """Initialize the processor (optional override)"""
        return Success(None)
    
    async def finalize(self) -> Result[None, Exception]:
        """Finalize the processor (optional override)"""
        return Success(None)
    
    def __add__(self, other: 'BaseProcessor') -> 'CompositeProcessor':
        """Allow processor composition using + operator"""
        return CompositeProcessor([self, other])
    
    def __or__(self, other: 'BaseProcessor') -> 'ParallelProcessor':
        """Allow parallel processing using | operator"""
        return ParallelProcessor([self, other])
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.finalize()
        return False

class CompositeProcessor(BaseProcessor[T, U]):
    """Processor that chains multiple processors together"""
    
    def __init__(self, processors: list[BaseProcessor]):
        super().__init__({})
        self.processors = processors
    
    async def process(
        self, 
        input_stream: AsyncGenerator[T, None]
    ) -> AsyncGenerator[U, None]:
        """Chain processors together"""
        current_stream = input_stream
        
        for processor in self.processors:
            current_stream = processor.process(current_stream)
        
        async for item in current_stream:
            yield item
    
    async def initialize(self) -> Result[None, Exception]:
        """Initialize all processors"""
        results = []
        for processor in self.processors:
            result = await processor.initialize()
            results.append(result)
        
        # Check if any initialization failed
        for result in results:
            if isinstance(result, Failure):
                return result
        
        return Success(None)
    
    async def finalize(self) -> Result[None, Exception]:
        """Finalize all processors"""
        results = []
        for processor in self.processors:
            result = await processor.finalize()
            results.append(result)
        
        # Return first failure if any
        for result in results:
            if isinstance(result, Failure):
                return result
        
        return Success(None)

class ParallelProcessor(BaseProcessor[T, U]):
    """Processor that runs multiple processors in parallel"""
    
    def __init__(self, processors: list[BaseProcessor]):
        super().__init__({})
        self.processors = processors
    
    async def process(
        self, 
        input_stream: AsyncGenerator[T, None]
    ) -> AsyncGenerator[U, None]:
        """Run processors in parallel and merge results"""
        
        # Create separate streams for each processor
        input_items = []
        async for item in input_stream:
            input_items.append(item)
        
        # Process in parallel
        tasks = []
        for processor in self.processors:
            task = asyncio.create_task(
                self._process_with_processor(processor, input_items)
            )
            tasks.append(task)
        
        # Collect results from all processors
        results = await asyncio.gather(*tasks)
        
        # Merge and yield results
        for processor_results in results:
            for item in processor_results:
                yield item
    
    async def _process_with_processor(
        self, 
        processor: BaseProcessor, 
        items: list[T]
    ) -> list[U]:
        """Process items with a single processor"""
        async def item_stream():
            for item in items:
                yield item
        
        results = []
        async for result in processor.process(item_stream()):
            results.append(result)
        
        return results

# Specialized processor types

class SourceProcessor(BaseProcessor[None, DataPart]):
    """Base class for data source processors"""
    
    async def process(
        self, 
        input_stream: AsyncGenerator[None, None]
    ) -> AsyncGenerator[DataPart, None]:
        """Source processors generate data from external sources"""
        async for data_part in self._generate_data():
            yield data_part
    
    @abstractmethod
    async def _generate_data(self) -> AsyncGenerator[DataPart, None]:
        """Generate data from the source"""
        pass

class SinkProcessor(BaseProcessor[DataPart, DataPart]):
    """Base class for data sink processors"""
    
    async def process(
        self, 
        input_stream: AsyncGenerator[DataPart, None]
    ) -> AsyncGenerator[DataPart, None]:
        """Sink processors consume data and optionally pass it through"""
        async for data_part in input_stream:
            result = await self._consume_data(data_part)
            if isinstance(result, Success):
                yield data_part
            else:
                # Handle error appropriately
                self.metrics["error_count"] += 1
                # Could optionally yield error info or skip
                continue
    
    @abstractmethod
    async def _consume_data(self, data_part: DataPart) -> Result[None, Exception]:
        """Consume data to the sink"""
        pass

class TransformProcessor(BaseProcessor[DataPart, DataPart]):
    """Base class for data transformation processors"""
    
    async def process(
        self, 
        input_stream: AsyncGenerator[DataPart, None]
    ) -> AsyncGenerator[DataPart, None]:
        """Transform processors modify data in place"""
        async for data_part in input_stream:
            result = await self._transform_data(data_part)
            if isinstance(result, Success):
                transformed_part = result.unwrap()
                self.metrics["processed_count"] += 1
                yield transformed_part
            else:
                # Handle transformation error
                self.metrics["error_count"] += 1
                # Could optionally yield original data or error info
                continue
    
    @abstractmethod
    async def _transform_data(self, data_part: DataPart) -> Result[DataPart, Exception]:
        """Transform the data part"""
        pass

# Utility processors

class FilterProcessor(BaseProcessor[T, T]):
    """Processor that filters items based on a predicate"""
    
    def __init__(self, predicate: Callable[[T], bool]):
        super().__init__({})
        self.predicate = predicate
    
    async def process(
        self, 
        input_stream: AsyncGenerator[T, None]
    ) -> AsyncGenerator[T, None]:
        """Filter items based on predicate"""
        async for item in input_stream:
            if self.predicate(item):
                yield item

class MapProcessor(BaseProcessor[T, U]):
    """Processor that maps items using a function"""
    
    def __init__(self, map_func: Callable[[T], U]):
        super().__init__({})
        self.map_func = map_func
    
    async def process(
        self, 
        input_stream: AsyncGenerator[T, None]
    ) -> AsyncGenerator[U, None]:
        """Map items using the provided function"""
        async for item in input_stream:
            try:
                mapped_item = self.map_func(item)
                yield mapped_item
            except Exception as e:
                self.metrics["error_count"] += 1
                # Skip items that fail mapping
                continue

class BatchProcessor(BaseProcessor[T, list[T]]):
    """Processor that batches items together"""
    
    def __init__(self, batch_size: int):
        super().__init__({"batch_size": batch_size})
        self.batch_size = batch_size
    
    async def process(
        self, 
        input_stream: AsyncGenerator[T, None]
    ) -> AsyncGenerator[list[T], None]:
        """Batch items together"""
        batch = []
        async for item in input_stream:
            batch.append(item)
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        
        # Yield remaining items
        if batch:
            yield batch

# Monitoring and metrics

class MetricsCollector:
    """Collects and aggregates metrics from processors"""
    
    def __init__(self):
        self.processor_metrics: dict[str, ProcessorMetrics] = {}
    
    def collect_metrics(self, processor_name: str, metrics: ProcessorMetrics):
        """Collect metrics from a processor"""
        self.processor_metrics[processor_name] = metrics
    
    def get_aggregated_metrics(self) -> dict[str, Any]:
        """Get aggregated metrics across all processors"""
        total_processed = sum(m["processed_count"] for m in self.processor_metrics.values())
        total_errors = sum(m["error_count"] for m in self.processor_metrics.values())
        total_time = sum(m["processing_time"] for m in self.processor_metrics.values())
        
        return {
            "total_processed": total_processed,
            "total_errors": total_errors,
            "total_processing_time": total_time,
            "average_throughput": total_processed / total_time if total_time > 0 else 0,
            "error_rate": total_errors / total_processed if total_processed > 0 else 0,
            "processors": self.processor_metrics
        } 