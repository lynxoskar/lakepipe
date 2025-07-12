"""
User-defined transform processor for custom transformation functions.
"""

from typing import Dict, Any, List, Callable
import polars as pl
import importlib
import inspect

from lakepipe.core.processors import TransformProcessor, DataPart
from lakepipe.core.results import Result, Success, Failure
from lakepipe.core.logging import get_logger

logger = get_logger(__name__)


class UserTransformProcessor(TransformProcessor):
    """Transform processor using user-defined functions"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.operations = config.get("operations", [])
        self.user_functions = config.get("user_functions", [])
        self.loaded_functions: Dict[str, Callable] = {}
        
    async def initialize(self) -> Result[None, Exception]:
        """Initialize and load user functions"""
        try:
            await self._load_user_functions()
            return Success(None)
        except Exception as e:
            logger.error(f"Failed to load user functions: {e}")
            return Failure(e)
    
    async def _load_user_functions(self):
        """Load user-defined functions from module paths"""
        for func_path in self.user_functions or []:
            try:
                # Parse module.function format
                if "." not in func_path:
                    logger.warning(f"Invalid function path format: {func_path}")
                    continue
                
                module_path, function_name = func_path.rsplit(".", 1)
                
                # Import the module
                module = importlib.import_module(module_path)
                
                # Get the function
                if hasattr(module, function_name):
                    func = getattr(module, function_name)
                    
                    # Validate function signature
                    if callable(func):
                        self.loaded_functions[function_name] = func
                        logger.info(f"Loaded user function: {function_name}")
                    else:
                        logger.warning(f"Object {function_name} is not callable")
                else:
                    logger.warning(f"Function {function_name} not found in module {module_path}")
                    
            except Exception as e:
                logger.error(f"Failed to load function {func_path}: {e}")
        
    async def _transform_data(self, data_part: DataPart) -> Result[DataPart, Exception]:
        """Transform data using user-defined operations"""
        try:
            df = data_part["data"]
            
            # Apply each operation in sequence
            for operation in self.operations:
                df = await self._apply_user_operation(df, operation, data_part)
                
            # Return transformed data part
            return Success(DataPart(
                data=df,
                metadata={**data_part["metadata"], "engine": "user", "transformed": True},
                source_info=data_part["source_info"],
                schema=data_part["schema"]
            ))
            
        except Exception as e:
            logger.error(f"User transformation failed: {e}")
            return Failure(e)
    
    async def _apply_user_operation(
        self, 
        df: pl.LazyFrame, 
        operation: Dict[str, Any],
        data_part: DataPart
    ) -> pl.LazyFrame:
        """Apply a user-defined operation"""
        op_type = operation.get("type")
        
        if op_type == "function":
            return await self._apply_user_function(df, operation, data_part)
        elif op_type == "lambda":
            return await self._apply_lambda_function(df, operation)
        elif op_type == "python":
            return await self._apply_python_code(df, operation, data_part)
        elif op_type == "filter":
            return self._apply_simple_filter(df, operation)
        elif op_type == "with_columns":
            return self._apply_simple_with_columns(df, operation)
        else:
            logger.warning(f"Unknown user operation type: {op_type}")
            return df
    
    async def _apply_user_function(
        self, 
        df: pl.LazyFrame, 
        operation: Dict[str, Any],
        data_part: DataPart
    ) -> pl.LazyFrame:
        """Apply a user-defined function"""
        function_name = operation.get("function")
        args = operation.get("args", [])
        kwargs = operation.get("kwargs", {})
        
        if not function_name:
            logger.warning("No function name specified in user operation")
            return df
        
        if function_name not in self.loaded_functions:
            logger.warning(f"Function {function_name} not loaded")
            return df
        
        try:
            func = self.loaded_functions[function_name]
            
            # Inspect function signature to determine how to call it
            sig = inspect.signature(func)
            params = list(sig.parameters.keys())
            
            # Call function based on its signature
            if len(params) == 1:
                # Function takes only DataFrame
                result = func(df.collect())
            elif len(params) == 2:
                # Function takes DataFrame and metadata
                result = func(df.collect(), data_part["metadata"])
            elif len(params) >= 3:
                # Function takes DataFrame, metadata, and additional args
                result = func(df.collect(), data_part["metadata"], *args, **kwargs)
            else:
                # No parameters - just call it
                result = func()
            
            # Convert result back to LazyFrame
            if isinstance(result, pl.DataFrame):
                return result.lazy()
            elif isinstance(result, pl.LazyFrame):
                return result
            else:
                logger.warning(f"Function {function_name} returned unexpected type: {type(result)}")
                return df
                
        except Exception as e:
            logger.error(f"User function {function_name} failed: {e}")
            return df
    
    async def _apply_lambda_function(
        self, 
        df: pl.LazyFrame, 
        operation: Dict[str, Any]
    ) -> pl.LazyFrame:
        """Apply a lambda function defined inline"""
        lambda_code = operation.get("code")
        
        if not lambda_code:
            logger.warning("No lambda code specified")
            return df
        
        try:
            # Create a safe environment for lambda evaluation
            safe_globals = {
                "pl": pl,
                "__builtins__": {},
            }
            
            # Evaluate the lambda
            lambda_func = eval(lambda_code, safe_globals)
            
            if not callable(lambda_func):
                logger.warning("Lambda code did not produce a callable")
                return df
            
            # Apply the lambda to the collected DataFrame
            result = lambda_func(df.collect())
            
            # Convert result back to LazyFrame
            if isinstance(result, pl.DataFrame):
                return result.lazy()
            elif isinstance(result, pl.LazyFrame):
                return result
            else:
                logger.warning(f"Lambda returned unexpected type: {type(result)}")
                return df
                
        except Exception as e:
            logger.error(f"Lambda function failed: {e}")
            return df
    
    async def _apply_python_code(
        self, 
        df: pl.LazyFrame, 
        operation: Dict[str, Any],
        data_part: DataPart
    ) -> pl.LazyFrame:
        """Execute arbitrary Python code for transformation"""
        code = operation.get("code")
        
        if not code:
            logger.warning("No Python code specified")
            return df
        
        try:
            # Create a safe execution environment
            exec_globals = {
                "pl": pl,
                "df": df.collect(),  # Provide the DataFrame as 'df'
                "metadata": data_part["metadata"],
                "source_info": data_part["source_info"],
                "__builtins__": {
                    "len": len,
                    "str": str,
                    "int": int,
                    "float": float,
                    "bool": bool,
                    "list": list,
                    "dict": dict,
                    "tuple": tuple,
                    "set": set,
                    "max": max,
                    "min": min,
                    "sum": sum,
                    "abs": abs,
                    "round": round,
                }
            }
            
            exec_locals = {}
            
            # Execute the code
            exec(code, exec_globals, exec_locals)
            
            # Look for 'result' in locals, or use modified 'df'
            if "result" in exec_locals:
                result = exec_locals["result"]
            elif "df" in exec_globals:
                result = exec_globals["df"]
            else:
                logger.warning("Python code did not produce a result")
                return df
            
            # Convert result back to LazyFrame
            if isinstance(result, pl.DataFrame):
                return result.lazy()
            elif isinstance(result, pl.LazyFrame):
                return result
            else:
                logger.warning(f"Python code returned unexpected type: {type(result)}")
                return df
                
        except Exception as e:
            logger.error(f"Python code execution failed: {e}")
            return df
    
    def _apply_simple_filter(self, df: pl.LazyFrame, operation: Dict[str, Any]) -> pl.LazyFrame:
        """Apply simple filter for user operations"""
        condition = operation.get("condition")
        if not condition:
            return df
        
        try:
            # Simple condition parsing
            if "==" in condition:
                parts = condition.split("==")
                if len(parts) == 2:
                    col = parts[0].strip()
                    val = parts[1].strip().strip('"\'')
                    return df.filter(pl.col(col) == val)
            elif ">" in condition:
                parts = condition.split(">")
                if len(parts) == 2:
                    col = parts[0].strip()
                    val = float(parts[1].strip())
                    return df.filter(pl.col(col) > val)
            elif "<" in condition:
                parts = condition.split("<")
                if len(parts) == 2:
                    col = parts[0].strip()
                    val = float(parts[1].strip())
                    return df.filter(pl.col(col) < val)
            
            # Fallback: try to evaluate as Polars expression
            return df.filter(eval(f"pl.{condition}"))
            
        except Exception as e:
            logger.warning(f"Filter condition failed: {condition}, error: {e}")
            return df
    
    def _apply_simple_with_columns(self, df: pl.LazyFrame, operation: Dict[str, Any]) -> pl.LazyFrame:
        """Apply simple with_columns for user operations"""
        expressions = operation.get("expressions", [])
        if not expressions:
            return df
        
        try:
            polars_exprs = []
            for expr in expressions:
                # Simple expression parsing
                if " as " in expr:
                    computation, alias = expr.split(" as ")
                    computation = computation.strip()
                    alias = alias.strip()
                    
                    # Handle simple arithmetic
                    if "*" in computation:
                        parts = computation.split("*")
                        if len(parts) == 2:
                            left = parts[0].strip()
                            right = float(parts[1].strip())
                            polars_exprs.append((pl.col(left) * right).alias(alias))
                    else:
                        # Default to literal
                        polars_exprs.append(pl.lit(computation).alias(alias))
                        
            return df.with_columns(polars_exprs)
            
        except Exception as e:
            logger.warning(f"with_columns failed: {e}")
            return df 