"""
Polars-based transform processor for high-performance data transformations.
"""

from typing import Dict, Any, List
import polars as pl

from lakepipe.core.processors import TransformProcessor, DataPart
from lakepipe.core.results import Result, Success, Failure
from lakepipe.core.logging import get_logger

logger = get_logger(__name__)


class PolarsTransformProcessor(TransformProcessor):
    """Transform processor using Polars for high-performance operations"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.operations = config.get("operations", [])
        self.user_functions = config.get("user_functions", [])
        
    async def _transform_data(self, data_part: DataPart) -> Result[DataPart, Exception]:
        """Transform data using Polars LazyFrame operations"""
        try:
            df = data_part["data"]
            
            # Apply each operation in sequence
            for operation in self.operations:
                df = await self._apply_operation(df, operation)
                
            # Return transformed data part
            return Success(DataPart(
                data=df,
                metadata={**data_part["metadata"], "engine": "polars", "transformed": True},
                source_info=data_part["source_info"],
                schema=data_part["schema"]
            ))
            
        except Exception as e:
            logger.error(f"Polars transformation failed: {e}")
            return Failure(e)
    
    async def _apply_operation(self, df: pl.LazyFrame, operation: Dict[str, Any]) -> pl.LazyFrame:
        """Apply a single operation to the LazyFrame"""
        op_type = operation.get("type")
        
        if op_type == "filter":
            return self._apply_filter(df, operation)
        elif op_type == "with_columns":
            return self._apply_with_columns(df, operation)
        elif op_type == "window_function":
            return self._apply_window_function(df, operation)
        elif op_type == "group_by":
            return self._apply_group_by(df, operation)
        elif op_type == "select":
            return self._apply_select(df, operation)
        elif op_type == "sort":
            return self._apply_sort(df, operation)
        else:
            logger.warning(f"Unknown operation type: {op_type}")
            return df
    
    def _apply_filter(self, df: pl.LazyFrame, operation: Dict[str, Any]) -> pl.LazyFrame:
        """Apply filter operation"""
        condition = operation.get("condition")
        if not condition:
            return df
        
        # Parse the condition string into a Polars expression
        # For now, use the condition as-is (Polars can parse SQL-like expressions)
        try:
            return df.filter(pl.sql_expr(condition))
        except Exception:
            # Fallback: try to evaluate as Python expression
            logger.warning(f"SQL expression failed, trying Python eval: {condition}")
            # This is unsafe but for demo purposes
            # In production, would need proper expression parser
            return df.filter(eval(f"pl.{condition}"))
    
    def _apply_with_columns(self, df: pl.LazyFrame, operation: Dict[str, Any]) -> pl.LazyFrame:
        """Apply with_columns operation"""
        expressions = operation.get("expressions", [])
        if not expressions:
            return df
        
        # Convert string expressions to Polars expressions
        polars_exprs = []
        for expr_str in expressions:
            try:
                # Try SQL-style expression first
                polars_exprs.append(pl.sql_expr(expr_str))
            except Exception:
                # Fallback to simple column operations
                logger.warning(f"Complex expression parsing not implemented: {expr_str}")
                # For now, just add a placeholder
                polars_exprs.append(pl.lit(None).alias("computed_column"))
        
        return df.with_columns(polars_exprs)
    
    def _apply_window_function(self, df: pl.LazyFrame, operation: Dict[str, Any]) -> pl.LazyFrame:
        """Apply window function operation"""
        window_spec = operation.get("window_spec", {})
        expressions = operation.get("expressions", [])
        
        if not expressions:
            return df
        
        # Build window specification
        partition_by = window_spec.get("partition_by", [])
        order_by = window_spec.get("order_by", [])
        frame = window_spec.get("frame", "unbounded preceding")
        
        # Convert expressions to Polars window functions
        window_exprs = []
        for expr_str in expressions:
            try:
                # Parse window expression (simplified)
                if "over w" in expr_str:
                    # Remove "over w" and create window expression
                    base_expr = expr_str.replace(" over w", "").strip()
                    
                    # Simple mapping for common window functions
                    if base_expr.startswith("avg("):
                        col = base_expr[4:-1]  # Extract column name
                        window_expr = pl.col(col).mean().over(
                            partition_by=partition_by,
                            order_by=order_by
                        )
                    elif base_expr.startswith("sum("):
                        col = base_expr[4:-1]
                        window_expr = pl.col(col).sum().over(
                            partition_by=partition_by,
                            order_by=order_by
                        )
                    elif base_expr.startswith("count("):
                        col = base_expr[6:-1]
                        window_expr = pl.col(col).count().over(
                            partition_by=partition_by,
                            order_by=order_by
                        )
                    else:
                        # Default to current value
                        window_expr = pl.lit(None)
                    
                    # Extract alias
                    if " as " in expr_str:
                        alias = expr_str.split(" as ")[-1].strip()
                        window_expr = window_expr.alias(alias)
                    
                    window_exprs.append(window_expr)
                else:
                    # Regular expression
                    window_exprs.append(pl.sql_expr(expr_str))
                    
            except Exception as e:
                logger.warning(f"Window function parsing failed: {expr_str}, error: {e}")
                window_exprs.append(pl.lit(None).alias("window_result"))
        
        return df.with_columns(window_exprs)
    
    def _apply_group_by(self, df: pl.LazyFrame, operation: Dict[str, Any]) -> pl.LazyFrame:
        """Apply group_by operation"""
        columns = operation.get("columns", [])
        agg = operation.get("agg", "count")
        
        if not columns:
            return df
        
        # Simple aggregation mapping
        if agg == "count":
            return df.group_by(columns).agg(pl.len().alias("count"))
        elif agg.startswith("sum("):
            col = agg[4:-1]
            return df.group_by(columns).agg(pl.col(col).sum())
        elif agg.startswith("avg("):
            col = agg[4:-1]
            return df.group_by(columns).agg(pl.col(col).mean())
        else:
            return df.group_by(columns).agg(pl.len().alias("count"))
    
    def _apply_select(self, df: pl.LazyFrame, operation: Dict[str, Any]) -> pl.LazyFrame:
        """Apply select operation"""
        columns = operation.get("columns", [])
        if not columns:
            return df
        
        return df.select(columns)
    
    def _apply_sort(self, df: pl.LazyFrame, operation: Dict[str, Any]) -> pl.LazyFrame:
        """Apply sort operation"""
        columns = operation.get("columns", [])
        descending = operation.get("descending", False)
        
        if not columns:
            return df
        
        return df.sort(columns, descending=descending) 