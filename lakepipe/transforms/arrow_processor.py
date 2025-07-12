"""
PyArrow-based transform processor for compute operations with zero-copy efficiency.
"""

from typing import Dict, Any, List
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc

from lakepipe.core.processors import TransformProcessor, DataPart
from lakepipe.core.results import Result, Success, Failure
from lakepipe.core.logging import get_logger

logger = get_logger(__name__)


class ArrowTransformProcessor(TransformProcessor):
    """Transform processor using PyArrow compute functions"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.operations = config.get("operations", [])
        self.user_functions = config.get("user_functions", [])
        
    async def _transform_data(self, data_part: DataPart) -> Result[DataPart, Exception]:
        """Transform data using PyArrow compute operations"""
        try:
            # Convert Polars LazyFrame to Arrow Table for processing
            polars_df = data_part["data"]
            arrow_table = polars_df.collect().to_arrow()
            
            # Apply each operation in sequence
            for operation in self.operations:
                arrow_table = self._apply_arrow_operation(arrow_table, operation)
                
            # Convert back to Polars LazyFrame
            result_df = pl.from_arrow(arrow_table).lazy()
            
            # Return transformed data part
            return Success(DataPart(
                data=result_df,
                metadata={**data_part["metadata"], "engine": "arrow", "transformed": True},
                source_info=data_part["source_info"],
                schema=data_part["schema"]
            ))
            
        except Exception as e:
            logger.error(f"Arrow transformation failed: {e}")
            return Failure(e)
    
    def _apply_arrow_operation(self, table: pa.Table, operation: Dict[str, Any]) -> pa.Table:
        """Apply a single operation to the Arrow Table"""
        op_type = operation.get("type")
        
        if op_type == "filter":
            return self._apply_arrow_filter(table, operation)
        elif op_type == "with_columns":
            return self._apply_arrow_with_columns(table, operation)
        elif op_type == "select":
            return self._apply_arrow_select(table, operation)
        elif op_type == "sort":
            return self._apply_arrow_sort(table, operation)
        elif op_type == "group_by":
            return self._apply_arrow_group_by(table, operation)
        else:
            logger.warning(f"Operation type '{op_type}' not fully supported in Arrow processor")
            return table
    
    def _apply_arrow_filter(self, table: pa.Table, operation: Dict[str, Any]) -> pa.Table:
        """Apply filter operation using Arrow compute"""
        condition = operation.get("condition")
        if not condition:
            return table
        
        try:
            # Parse simple conditions
            # This is a simplified parser - in production would need more robust parsing
            filter_expr = self._parse_filter_condition(table, condition)
            if filter_expr is not None:
                return pc.filter(table, filter_expr)
            else:
                logger.warning(f"Could not parse filter condition: {condition}")
                return table
                
        except Exception as e:
            logger.warning(f"Arrow filter failed: {e}")
            return table
    
    def _apply_arrow_with_columns(self, table: pa.Table, operation: Dict[str, Any]) -> pa.Table:
        """Apply with_columns operation using Arrow compute"""
        expressions = operation.get("expressions", [])
        if not expressions:
            return table
        
        # Add computed columns to the table
        new_columns = []
        new_names = []
        
        # Keep existing columns
        for i, column in enumerate(table.columns):
            new_columns.append(column)
            new_names.append(table.column_names[i])
        
        # Add new computed columns
        for expr in expressions:
            try:
                computed_column, column_name = self._compute_arrow_expression(table, expr)
                if computed_column is not None:
                    new_columns.append(computed_column)
                    new_names.append(column_name)
            except Exception as e:
                logger.warning(f"Failed to compute Arrow expression '{expr}': {e}")
        
        return pa.table(new_columns, names=new_names)
    
    def _apply_arrow_select(self, table: pa.Table, operation: Dict[str, Any]) -> pa.Table:
        """Apply select operation"""
        columns = operation.get("columns", [])
        if not columns:
            return table
        
        return table.select(columns)
    
    def _apply_arrow_sort(self, table: pa.Table, operation: Dict[str, Any]) -> pa.Table:
        """Apply sort operation using Arrow compute"""
        columns = operation.get("columns", [])
        descending = operation.get("descending", False)
        
        if not columns:
            return table
        
        # Create sort keys
        sort_keys = [(col, "descending" if descending else "ascending") for col in columns]
        
        return pc.sort_table(table, sort_keys)
    
    def _apply_arrow_group_by(self, table: pa.Table, operation: Dict[str, Any]) -> pa.Table:
        """Apply group_by operation using Arrow compute"""
        columns = operation.get("columns", [])
        agg = operation.get("agg", "count")
        
        if not columns:
            return table
        
        try:
            # Use Arrow's group_by functionality
            # This is simplified - full implementation would handle more aggregations
            if agg == "count":
                # Group by and count
                result = pc.group_by(table, columns).aggregate([("*", "count")])
                return result
            else:
                logger.warning(f"Aggregation '{agg}' not implemented for Arrow processor")
                return table
        except Exception as e:
            logger.warning(f"Arrow group_by failed: {e}")
            return table
    
    def _parse_filter_condition(self, table: pa.Table, condition: str) -> pa.compute.Expression:
        """Parse filter condition into Arrow expression"""
        # Simplified condition parsing
        # In production, would need a proper expression parser
        
        try:
            # Handle simple comparisons
            if " > " in condition:
                parts = condition.split(" > ")
                if len(parts) == 2:
                    col_name = parts[0].strip()
                    value = self._parse_value(parts[1].strip())
                    if col_name in table.column_names:
                        return pc.greater(pc.field(col_name), pa.scalar(value))
            
            elif " < " in condition:
                parts = condition.split(" < ")
                if len(parts) == 2:
                    col_name = parts[0].strip()
                    value = self._parse_value(parts[1].strip())
                    if col_name in table.column_names:
                        return pc.less(pc.field(col_name), pa.scalar(value))
            
            elif " = " in condition:
                parts = condition.split(" = ")
                if len(parts) == 2:
                    col_name = parts[0].strip()
                    value = self._parse_value(parts[1].strip())
                    if col_name in table.column_names:
                        return pc.equal(pc.field(col_name), pa.scalar(value))
            
            elif " AND " in condition:
                # Handle simple AND conditions
                parts = condition.split(" AND ")
                left_expr = self._parse_filter_condition(table, parts[0].strip())
                right_expr = self._parse_filter_condition(table, parts[1].strip())
                if left_expr is not None and right_expr is not None:
                    return pc.and_(left_expr, right_expr)
            
            # Default fallback
            return None
            
        except Exception:
            return None
    
    def _compute_arrow_expression(self, table: pa.Table, expr: str) -> tuple[pa.Array, str]:
        """Compute Arrow expression and return column with name"""
        # Simplified expression computing
        # In production, would need a proper expression parser
        
        try:
            # Extract alias if present
            if " as " in expr:
                computation, alias = expr.split(" as ", 1)
                column_name = alias.strip()
            else:
                computation = expr
                column_name = "computed_column"
            
            computation = computation.strip()
            
            # Handle simple arithmetic operations
            if " * " in computation:
                parts = computation.split(" * ")
                if len(parts) == 2:
                    left = self._get_column_or_value(table, parts[0].strip())
                    right = self._get_column_or_value(table, parts[1].strip())
                    if left is not None and right is not None:
                        result = pc.multiply(left, right)
                        return result, column_name
            
            elif " + " in computation:
                parts = computation.split(" + ")
                if len(parts) == 2:
                    left = self._get_column_or_value(table, parts[0].strip())
                    right = self._get_column_or_value(table, parts[1].strip())
                    if left is not None and right is not None:
                        result = pc.add(left, right)
                        return result, column_name
            
            elif " - " in computation:
                parts = computation.split(" - ")
                if len(parts) == 2:
                    left = self._get_column_or_value(table, parts[0].strip())
                    right = self._get_column_or_value(table, parts[1].strip())
                    if left is not None and right is not None:
                        result = pc.subtract(left, right)
                        return result, column_name
            
            elif " / " in computation:
                parts = computation.split(" / ")
                if len(parts) == 2:
                    left = self._get_column_or_value(table, parts[0].strip())
                    right = self._get_column_or_value(table, parts[1].strip())
                    if left is not None and right is not None:
                        result = pc.divide(left, right)
                        return result, column_name
            
            # Handle simple functions
            elif computation.startswith("log(") and computation.endswith(")"):
                col_name = computation[4:-1].strip()
                if col_name in table.column_names:
                    result = pc.ln(table.column(col_name))
                    return result, column_name
            
            # Handle column copy
            elif computation in table.column_names:
                return table.column(computation), column_name
            
            # Default: return null column
            null_array = pa.nulls(len(table), pa.float64())
            return null_array, column_name
            
        except Exception as e:
            logger.warning(f"Failed to compute expression '{expr}': {e}")
            null_array = pa.nulls(len(table), pa.float64())
            return null_array, "error_column"
    
    def _get_column_or_value(self, table: pa.Table, item: str):
        """Get column from table or parse as literal value"""
        item = item.strip()
        
        # Check if it's a column name
        if item in table.column_names:
            return table.column(item)
        
        # Try to parse as numeric value
        try:
            if "." in item:
                return pa.scalar(float(item))
            else:
                return pa.scalar(int(item))
        except ValueError:
            # Try as string literal
            if item.startswith("'") and item.endswith("'"):
                return pa.scalar(item[1:-1])
            elif item.startswith('"') and item.endswith('"'):
                return pa.scalar(item[1:-1])
        
        return None
    
    def _parse_value(self, value_str: str):
        """Parse string value to appropriate type"""
        value_str = value_str.strip()
        
        # Try numeric first
        try:
            if "." in value_str:
                return float(value_str)
            else:
                return int(value_str)
        except ValueError:
            pass
        
        # Handle string literals
        if value_str.startswith("'") and value_str.endswith("'"):
            return value_str[1:-1]
        elif value_str.startswith('"') and value_str.endswith('"'):
            return value_str[1:-1]
        
        # Return as-is
        return value_str 