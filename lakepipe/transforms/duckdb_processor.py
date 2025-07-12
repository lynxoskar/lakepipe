"""
DuckDB-based transform processor for SQL analytics with zero-copy Arrow integration.
"""

from typing import Dict, Any, List
import polars as pl
import duckdb
import pyarrow as pa

from lakepipe.core.processors import TransformProcessor, DataPart
from lakepipe.core.results import Result, Success, Failure
from lakepipe.core.logging import get_logger

logger = get_logger(__name__)


class DuckDBTransformProcessor(TransformProcessor):
    """Transform processor using DuckDB for SQL analytics"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.operations = config.get("operations", [])
        self.user_functions = config.get("user_functions", [])
        self.conn = None
        
    async def initialize(self) -> Result[None, Exception]:
        """Initialize DuckDB connection"""
        try:
            self.conn = duckdb.connect()
            # Enable Arrow extension for zero-copy operations
            self.conn.execute("INSTALL arrow; LOAD arrow;")
            return Success(None)
        except Exception as e:
            logger.error(f"Failed to initialize DuckDB connection: {e}")
            return Failure(e)
    
    async def finalize(self) -> Result[None, Exception]:
        """Close DuckDB connection"""
        try:
            if self.conn:
                self.conn.close()
            return Success(None)
        except Exception as e:
            logger.error(f"Failed to close DuckDB connection: {e}")
            return Failure(e)
        
    async def _transform_data(self, data_part: DataPart) -> Result[DataPart, Exception]:
        """Transform data using DuckDB SQL operations"""
        try:
            if not self.conn:
                await self.initialize()
            
            # Convert Polars LazyFrame to Arrow Table for zero-copy
            polars_df = data_part["data"]
            arrow_table = polars_df.collect().to_arrow()
            
            # Register Arrow table in DuckDB
            self.conn.register("input_table", arrow_table)
            
            # Apply each operation in sequence
            current_table = "input_table"
            for i, operation in enumerate(self.operations):
                result_table = f"result_table_{i}"
                sql_query = self._operation_to_sql(operation, current_table)
                
                if sql_query:
                    logger.debug(f"Executing DuckDB query: {sql_query}")
                    self.conn.execute(f"CREATE OR REPLACE TABLE {result_table} AS {sql_query}")
                    current_table = result_table
                
            # Get final result as Arrow table
            final_arrow = self.conn.execute(f"SELECT * FROM {current_table}").arrow()
            
            # Convert back to Polars LazyFrame
            result_df = pl.from_arrow(final_arrow).lazy()
            
            # Clean up temporary tables
            for i in range(len(self.operations)):
                try:
                    self.conn.execute(f"DROP TABLE IF EXISTS result_table_{i}")
                except:
                    pass
            self.conn.execute("DROP TABLE IF EXISTS input_table")
            
            # Return transformed data part
            return Success(DataPart(
                data=result_df,
                metadata={**data_part["metadata"], "engine": "duckdb", "transformed": True},
                source_info=data_part["source_info"],
                schema=data_part["schema"]
            ))
            
        except Exception as e:
            logger.error(f"DuckDB transformation failed: {e}")
            return Failure(e)
    
    def _operation_to_sql(self, operation: Dict[str, Any], table_name: str) -> str:
        """Convert operation config to SQL query"""
        op_type = operation.get("type")
        
        if op_type == "filter":
            return self._filter_to_sql(operation, table_name)
        elif op_type == "with_columns":
            return self._with_columns_to_sql(operation, table_name)
        elif op_type == "window_function":
            return self._window_function_to_sql(operation, table_name)
        elif op_type == "group_by":
            return self._group_by_to_sql(operation, table_name)
        elif op_type == "select":
            return self._select_to_sql(operation, table_name)
        elif op_type == "sort":
            return self._sort_to_sql(operation, table_name)
        else:
            logger.warning(f"Unknown operation type for DuckDB: {op_type}")
            return f"SELECT * FROM {table_name}"
    
    def _filter_to_sql(self, operation: Dict[str, Any], table_name: str) -> str:
        """Convert filter operation to SQL"""
        condition = operation.get("condition", "TRUE")
        return f"SELECT * FROM {table_name} WHERE {condition}"
    
    def _with_columns_to_sql(self, operation: Dict[str, Any], table_name: str) -> str:
        """Convert with_columns operation to SQL"""
        expressions = operation.get("expressions", [])
        
        if not expressions:
            return f"SELECT * FROM {table_name}"
        
        # Build SELECT with existing columns plus new computed columns
        select_parts = ["*"]  # Include all existing columns
        
        for expr in expressions:
            # Clean up expression for SQL
            sql_expr = expr.strip()
            
            # Handle common transformations
            sql_expr = self._convert_polars_to_sql_expr(sql_expr)
            select_parts.append(sql_expr)
        
        select_clause = ", ".join(select_parts)
        return f"SELECT {select_clause} FROM {table_name}"
    
    def _window_function_to_sql(self, operation: Dict[str, Any], table_name: str) -> str:
        """Convert window function operation to SQL"""
        window_spec = operation.get("window_spec", {})
        expressions = operation.get("expressions", [])
        
        if not expressions:
            return f"SELECT * FROM {table_name}"
        
        # Build window specification
        partition_by = window_spec.get("partition_by", [])
        order_by = window_spec.get("order_by", [])
        frame = window_spec.get("frame", "unbounded preceding")
        
        # Build OVER clause
        over_parts = []
        if partition_by:
            over_parts.append(f"PARTITION BY {', '.join(partition_by)}")
        if order_by:
            over_parts.append(f"ORDER BY {', '.join(order_by)}")
        if frame and frame != "unbounded preceding":
            # Convert frame specification to SQL
            if "rows between" in frame.lower():
                over_parts.append(f"ROWS {frame.upper()}")
        
        over_clause = f"OVER ({' '.join(over_parts)})" if over_parts else "OVER ()"
        
        # Build SELECT with existing columns plus window functions
        select_parts = ["*"]
        
        for expr in expressions:
            # Replace "over w" with actual OVER clause
            sql_expr = expr.replace(" over w", f" {over_clause}")
            sql_expr = self._convert_polars_to_sql_expr(sql_expr)
            select_parts.append(sql_expr)
        
        select_clause = ", ".join(select_parts)
        return f"SELECT {select_clause} FROM {table_name}"
    
    def _group_by_to_sql(self, operation: Dict[str, Any], table_name: str) -> str:
        """Convert group_by operation to SQL"""
        columns = operation.get("columns", [])
        agg = operation.get("agg", "count")
        
        if not columns:
            return f"SELECT * FROM {table_name}"
        
        group_by_clause = ", ".join(columns)
        
        # Convert aggregation to SQL
        if agg == "count":
            select_clause = f"{group_by_clause}, COUNT(*) as count"
        elif agg.startswith("sum(") and agg.endswith(")"):
            col = agg[4:-1]
            select_clause = f"{group_by_clause}, SUM({col}) as sum_{col}"
        elif agg.startswith("avg(") and agg.endswith(")"):
            col = agg[4:-1]
            select_clause = f"{group_by_clause}, AVG({col}) as avg_{col}"
        else:
            select_clause = f"{group_by_clause}, COUNT(*) as count"
        
        return f"SELECT {select_clause} FROM {table_name} GROUP BY {group_by_clause}"
    
    def _select_to_sql(self, operation: Dict[str, Any], table_name: str) -> str:
        """Convert select operation to SQL"""
        columns = operation.get("columns", [])
        
        if not columns:
            return f"SELECT * FROM {table_name}"
        
        select_clause = ", ".join(columns)
        return f"SELECT {select_clause} FROM {table_name}"
    
    def _sort_to_sql(self, operation: Dict[str, Any], table_name: str) -> str:
        """Convert sort operation to SQL"""
        columns = operation.get("columns", [])
        descending = operation.get("descending", False)
        
        if not columns:
            return f"SELECT * FROM {table_name}"
        
        order_clause = ", ".join(columns)
        if descending:
            order_clause += " DESC"
        
        return f"SELECT * FROM {table_name} ORDER BY {order_clause}"
    
    def _convert_polars_to_sql_expr(self, expr: str) -> str:
        """Convert Polars-style expressions to SQL"""
        # Handle common Polars functions that need conversion to SQL
        conversions = {
            "extract('date'": "DATE(",
            "extract('hour'": "HOUR(",
            "extract('minute'": "MINUTE(",
            "extract('dayofweek'": "DAYOFWEEK(",
            "extract('epoch'": "EPOCH(",
            "current_timestamp()": "CURRENT_TIMESTAMP",
            "current_date()": "CURRENT_DATE",
        }
        
        sql_expr = expr
        for polars_func, sql_func in conversions.items():
            if polars_func in sql_expr:
                # Handle parentheses correctly
                if polars_func.endswith("(") and not sql_func.endswith("("):
                    sql_expr = sql_expr.replace(polars_func, sql_func + "(")
                else:
                    sql_expr = sql_expr.replace(polars_func, sql_func)
        
        return sql_expr 