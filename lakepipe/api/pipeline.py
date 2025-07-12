"""
High-level pipeline API for creating and executing data pipelines.
"""

import asyncio
from typing import Optional, Any, Dict
from dataclasses import dataclass
import time

from lakepipe.config.types import PipelineConfig
from lakepipe.core.processors import (
    BaseProcessor, DataPart, CompositeProcessor, 
    SourceProcessor, SinkProcessor, TransformProcessor
)
from lakepipe.core.results import Result, Success, Failure
from lakepipe.core.streams import endless_stream
from lakepipe.core.logging import (
    get_logger, log_pipeline_event, log_processor_metrics,
    pipeline_context, log_memory_usage
)

logger = get_logger(__name__)

@dataclass
class PipelineResult:
    """Result of pipeline execution"""
    success: bool
    processed_count: int
    error_count: int
    processing_time: float
    throughput: float
    metrics: Dict[str, Any]
    errors: list[Exception]

class Pipeline:
    """High-level pipeline orchestrator"""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.name = f"pipeline-{int(time.time())}"
        self.processors: list[BaseProcessor] = []
        self.metrics: Dict[str, Any] = {}
        
    def add_processor(self, processor: BaseProcessor) -> 'Pipeline':
        """Add a processor to the pipeline"""
        self.processors.append(processor)
        return self
    
    def compose_processors(self) -> BaseProcessor:
        """Compose all processors into a single pipeline"""
        if not self.processors:
            raise ValueError("No processors added to pipeline")
        
        if len(self.processors) == 1:
            return self.processors[0]
        
        # Chain processors together
        return CompositeProcessor(self.processors)
    
    async def execute(self) -> PipelineResult:
        """Execute the pipeline"""
        
        start_time = time.time()
        processed_count = 0
        error_count = 0
        errors = []
        
        with pipeline_context(self.name) as ctx_logger:
            try:
                ctx_logger.info(f"Starting pipeline execution")
                log_pipeline_event("start", self.name)
                log_memory_usage("pipeline_start")
                
                # Create composed processor
                composed_processor = self.compose_processors()
                
                # Initialize all processors
                init_result = await composed_processor.initialize()
                if isinstance(init_result, Failure):
                    raise init_result.failure()
                
                # Execute pipeline
                results = []
                async for result in composed_processor.process(endless_stream()):
                    if isinstance(result, DataPart):
                        results.append(result)
                        processed_count += 1
                        
                        # Log progress periodically
                        if processed_count % 10000 == 0:
                            elapsed = time.time() - start_time
                            throughput = processed_count / elapsed if elapsed > 0 else 0
                            ctx_logger.info(f"Processed {processed_count} records, throughput: {throughput:.2f} records/s")
                    else:
                        # Handle errors
                        error_count += 1
                        if hasattr(result, 'error'):
                            errors.append(result.error)
                    
                    # Stop after processing some data (for now)
                    # TODO: Add proper stopping conditions
                    if processed_count >= 1000:  # Temporary limit
                        break
                
                # Finalize processors
                finalize_result = await composed_processor.finalize()
                if isinstance(finalize_result, Failure):
                    ctx_logger.warning(f"Finalization warning: {finalize_result.failure()}")
                
                end_time = time.time()
                processing_time = end_time - start_time
                throughput = processed_count / processing_time if processing_time > 0 else 0
                
                # Collect metrics
                self.metrics = {
                    "processed_count": processed_count,
                    "error_count": error_count,
                    "processing_time": processing_time,
                    "throughput": throughput,
                    "start_time": start_time,
                    "end_time": end_time
                }
                
                # Log final metrics
                log_processor_metrics(self.name, self.metrics)
                log_pipeline_event("complete", self.name, self.metrics)
                log_memory_usage("pipeline_end")
                
                return PipelineResult(
                    success=True,
                    processed_count=processed_count,
                    error_count=error_count,
                    processing_time=processing_time,
                    throughput=throughput,
                    metrics=self.metrics,
                    errors=errors
                )
                
            except Exception as e:
                error_count += 1
                errors.append(e)
                
                end_time = time.time()
                processing_time = end_time - start_time
                
                self.metrics = {
                    "processed_count": processed_count,
                    "error_count": error_count,
                    "processing_time": processing_time,
                    "throughput": 0,
                    "start_time": start_time,
                    "end_time": end_time,
                    "error": str(e)
                }
                
                log_pipeline_event("error", self.name, self.metrics, "ERROR")
                ctx_logger.error(f"Pipeline execution failed: {e}")
                
                return PipelineResult(
                    success=False,
                    processed_count=processed_count,
                    error_count=error_count,
                    processing_time=processing_time,
                    throughput=0,
                    metrics=self.metrics,
                    errors=errors
                )

# Simple mock processors for initial testing

class MockSourceProcessor(SourceProcessor):
    """Mock source processor for testing"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.record_count = config.get("record_count", 100)
    
    async def _generate_data(self):
        """Generate mock data"""
        import polars as pl
        
        # Generate simple test data
        data = pl.DataFrame({
            "id": list(range(self.record_count)),
            "value": [f"test_value_{i}" for i in range(self.record_count)],
            "amount": [100.0 + i for i in range(self.record_count)]
        })
        
        yield DataPart(
            data=data.lazy(),
            metadata={"source": "mock", "record_count": self.record_count},
            source_info={"uri": "mock://test", "format": "mock"},
            schema={"columns": ["id", "value", "amount"]}
        )

class MockSinkProcessor(SinkProcessor):
    """Mock sink processor for testing"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.output_path = config.get("output_path", "/tmp/lakepipe_output")
    
    async def _consume_data(self, data_part: DataPart) -> Result[None, Exception]:
        """Mock consume data"""
        try:
            # Just log that we received data
            record_count = data_part["metadata"].get("record_count", 0)
            logger.info(f"Mock sink received {record_count} records")
            return Success(None)
        except Exception as e:
            return Failure(e)

class MockTransformProcessor(TransformProcessor):
    """Mock transform processor for testing"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.operations = config.get("operations", [])
    
    async def _transform_data(self, data_part: DataPart) -> Result[DataPart, Exception]:
        """Mock transform data"""
        try:
            # Simple transformation: add a computed column
            df = data_part["data"]
            
            # Add computed column
            transformed_df = df.with_columns([
                (pl.col("amount") * 1.1).alias("amount_with_tax")
            ])
            
            # Return transformed data part
            return Success(DataPart(
                data=transformed_df,
                metadata={**data_part["metadata"], "transformed": True},
                source_info=data_part["source_info"],
                schema=data_part["schema"]
            ))
        except Exception as e:
            return Failure(e)

# Factory functions

def create_source_processor(config: PipelineConfig) -> BaseProcessor:
    """Create a source processor based on configuration"""
    source_config = config["source"]
    
    # For now, just create a mock source
    # TODO: Implement actual source processors
    return MockSourceProcessor({
        "record_count": 1000,
        "uri": source_config["uri"],
        "format": source_config["format"]
    })

def create_sink_processor(config: PipelineConfig) -> BaseProcessor:
    """Create a sink processor based on configuration"""
    sink_config = config["sink"]
    
    # For now, just create a mock sink
    # TODO: Implement actual sink processors
    return MockSinkProcessor({
        "output_path": "/tmp/lakepipe_output",
        "uri": sink_config["uri"],
        "format": sink_config["format"]
    })

def create_transform_processor(config: PipelineConfig) -> BaseProcessor:
    """Create a transform processor based on configuration"""
    transform_config = config["transform"]
    
    # For now, just create a mock transform
    # TODO: Implement actual transform processors
    return MockTransformProcessor({
        "operations": transform_config["operations"]
    })

def create_pipeline(config: PipelineConfig) -> Pipeline:
    """
    Create a complete pipeline from configuration.
    
    Args:
        config: Pipeline configuration
        
    Returns:
        Configured pipeline ready for execution
    """
    
    pipeline = Pipeline(config)
    
    # Add source processor
    source_processor = create_source_processor(config)
    pipeline.add_processor(source_processor)
    
    # Add transform processor if configured
    if config["transform"]["operations"]:
        transform_processor = create_transform_processor(config)
        pipeline.add_processor(transform_processor)
    
    # Add sink processor
    sink_processor = create_sink_processor(config)
    pipeline.add_processor(sink_processor)
    
    return pipeline

# Convenience function for CLI
async def execute_pipeline(config: PipelineConfig) -> PipelineResult:
    """
    Create and execute a pipeline from configuration.
    
    Args:
        config: Pipeline configuration
        
    Returns:
        Pipeline execution result
    """
    pipeline = create_pipeline(config)
    return await pipeline.execute()

# Pipeline composition utilities

def compose_pipelines(*pipelines: Pipeline) -> Pipeline:
    """Compose multiple pipelines into one"""
    if not pipelines:
        raise ValueError("No pipelines provided")
    
    if len(pipelines) == 1:
        return pipelines[0]
    
    # Create new pipeline with combined processors
    combined_config = pipelines[0].config  # Use first pipeline's config
    combined_pipeline = Pipeline(combined_config)
    
    for pipeline in pipelines:
        combined_pipeline.processors.extend(pipeline.processors)
    
    return combined_pipeline

def parallel_pipelines(*pipelines: Pipeline) -> Pipeline:
    """Run multiple pipelines in parallel"""
    # TODO: Implement parallel execution
    # For now, just compose them sequentially
    return compose_pipelines(*pipelines) 