#!/usr/bin/env python3
"""
Test script for engine-specific transform processors.
"""

import asyncio
import polars as pl
from typing import Dict, Any

def test_imports():
    """Test that all engine processors can be imported"""
    print("\n🔍 Testing engine imports...")
    
    try:
        from lakepipe.transforms import (
            PolarsTransformProcessor,
            DuckDBTransformProcessor,
            ArrowTransformProcessor,
            UserTransformProcessor
        )
        print("✅ All engine processors imported successfully!")
        return True
    except Exception as e:
        print(f"❌ Engine import failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def create_test_data():
    """Create test data for engine testing"""
    from lakepipe.core.processors import DataPart
    
    # Create sample data
    df = pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "amount": [100.0, 200.0, 150.0, 300.0, 250.0],
        "category": ["A", "B", "A", "C", "B"]
    })
    
    return DataPart(
        data=df.lazy(),
        metadata={"source": "test", "record_count": 5},
        source_info={"uri": "test://data", "format": "test"},
        schema={"columns": ["id", "name", "amount", "category"]}
    )

async def test_polars_engine():
    """Test Polars transform processor"""
    print("\n🔍 Testing Polars engine...")
    
    try:
        from lakepipe.transforms import PolarsTransformProcessor
        
        # Create processor with test operations
        config = {
            "operations": [
                {
                    "type": "filter",
                    "condition": "amount > 150"
                },
                {
                    "type": "with_columns",
                    "expressions": [
                        "amount * 1.1 as amount_with_tax"
                    ]
                }
            ]
        }
        
        processor = PolarsTransformProcessor(config)
        test_data = create_test_data()
        
        # Test transformation
        result = await processor._transform_data(test_data)
        
        if result.is_success():
            transformed_data = result.unwrap()
            df = transformed_data["data"].collect()
            print(f"✅ Polars engine works! Filtered to {len(df)} rows")
            print(f"   📊 Columns: {df.columns}")
            return True
        else:
            print(f"❌ Polars transformation failed: {result.failure()}")
            return False
            
    except Exception as e:
        print(f"❌ Polars engine test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_duckdb_engine():
    """Test DuckDB transform processor"""
    print("\n🔍 Testing DuckDB engine...")
    
    try:
        from lakepipe.transforms import DuckDBTransformProcessor
        
        # Create processor with test operations
        config = {
            "operations": [
                {
                    "type": "filter",
                    "condition": "amount > 150"
                },
                {
                    "type": "with_columns",
                    "expressions": [
                        "amount * 1.1 as amount_with_tax"
                    ]
                }
            ]
        }
        
        processor = DuckDBTransformProcessor(config)
        
        # Initialize the processor
        init_result = await processor.initialize()
        if not init_result.is_success():
            print(f"❌ DuckDB initialization failed: {init_result.failure()}")
            return False
        
        test_data = create_test_data()
        
        # Test transformation
        result = await processor._transform_data(test_data)
        
        # Clean up
        await processor.finalize()
        
        if result.is_success():
            transformed_data = result.unwrap()
            df = transformed_data["data"].collect()
            print(f"✅ DuckDB engine works! Filtered to {len(df)} rows")
            print(f"   📊 Columns: {df.columns}")
            return True
        else:
            print(f"❌ DuckDB transformation failed: {result.failure()}")
            return False
            
    except Exception as e:
        print(f"❌ DuckDB engine test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_arrow_engine():
    """Test Arrow transform processor"""
    print("\n🔍 Testing Arrow engine...")
    
    try:
        from lakepipe.transforms import ArrowTransformProcessor
        
        # Create processor with test operations
        config = {
            "operations": [
                {
                    "type": "filter",
                    "condition": "amount > 150"
                },
                {
                    "type": "with_columns",
                    "expressions": [
                        "amount * 1.1 as amount_with_tax"
                    ]
                }
            ]
        }
        
        processor = ArrowTransformProcessor(config)
        test_data = create_test_data()
        
        # Test transformation
        result = await processor._transform_data(test_data)
        
        if result.is_success():
            transformed_data = result.unwrap()
            df = transformed_data["data"].collect()
            print(f"✅ Arrow engine works! Processed {len(df)} rows")
            print(f"   📊 Columns: {df.columns}")
            return True
        else:
            print(f"❌ Arrow transformation failed: {result.failure()}")
            return False
            
    except Exception as e:
        print(f"❌ Arrow engine test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_user_engine():
    """Test User transform processor"""
    print("\n🔍 Testing User engine...")
    
    try:
        from lakepipe.transforms import UserTransformProcessor
        
        # Create processor with test operations
        config = {
            "operations": [
                {
                    "type": "filter",
                    "condition": "amount > 150"
                },
                {
                    "type": "with_columns",
                    "expressions": [
                        "amount * 1.1 as amount_with_tax"
                    ]
                }
            ]
        }
        
        processor = UserTransformProcessor(config)
        
        # Initialize the processor
        init_result = await processor.initialize()
        if not init_result.is_success():
            print(f"❌ User processor initialization failed: {init_result.failure()}")
            return False
        
        test_data = create_test_data()
        
        # Test transformation
        result = await processor._transform_data(test_data)
        
        if result.is_success():
            transformed_data = result.unwrap()
            df = transformed_data["data"].collect()
            print(f"✅ User engine works! Processed {len(df)} rows")
            print(f"   📊 Columns: {df.columns}")
            return True
        else:
            print(f"❌ User transformation failed: {result.failure()}")
            return False
            
    except Exception as e:
        print(f"❌ User engine test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_pipeline_engine_dispatch():
    """Test that pipeline creation correctly dispatches to engines"""
    print("\n🔍 Testing pipeline engine dispatch...")
    
    try:
        from lakepipe.config.defaults import build_pipeline_config
        from lakepipe.api.pipeline import create_pipeline
        
        # Test each engine
        engines = ["polars", "duckdb", "arrow", "user"]
        success_count = 0
        
        for engine in engines:
            try:
                config = build_pipeline_config({
                    "source": {"uri": "mock://test", "format": "mock"},
                    "sink": {"uri": "mock://output", "format": "mock"},
                    "transform": {
                        "engine": engine,
                        "operations": [
                            {"type": "filter", "condition": "amount > 0"}
                        ]
                    }
                })
                
                pipeline = create_pipeline(config)
                
                # Check that we have processors
                if len(pipeline.processors) >= 2:  # source + sink at minimum
                    print(f"   ✅ {engine} engine dispatch works")
                    success_count += 1
                else:
                    print(f"   ❌ {engine} engine dispatch failed - no processors")
                    
            except Exception as e:
                print(f"   ❌ {engine} engine dispatch failed: {e}")
        
        if success_count == len(engines):
            print("✅ All engine dispatches work!")
            return True
        else:
            print(f"❌ Only {success_count}/{len(engines)} engines dispatched correctly")
            return False
            
    except Exception as e:
        print(f"❌ Pipeline engine dispatch test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Run all engine tests"""
    print("🚀 Starting lakepipe engine tests...")
    
    tests = [
        ("Import test", test_imports),
        ("Polars engine", test_polars_engine),
        ("DuckDB engine", test_duckdb_engine),
        ("Arrow engine", test_arrow_engine),
        ("User engine", test_user_engine),
        ("Pipeline dispatch", test_pipeline_engine_dispatch),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        try:
            if asyncio.iscoroutinefunction(test_func):
                result = await test_func()
            else:
                result = test_func()
            if result:
                passed += 1
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
    
    print(f"\n📊 Results: {passed}/{total} tests passed")
    if passed == total:
        print("🎉 All engine tests passed! Engine implementation is working! 🎉")
    else:
        print("⚠️ Some engine tests failed. Check the output above for details.")
    
    return passed == total

if __name__ == "__main__":
    asyncio.run(main()) 