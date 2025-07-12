#!/usr/bin/env python3
"""
Simple test script to validate lakepipe core functionality.
"""

def test_imports():
    """Test that all core imports work"""
    print("🔍 Testing imports...")
    
    try:
        from lakepipe.config.defaults import build_pipeline_config
        print("✅ Config system imported successfully")
        
        from lakepipe.core.processors import BaseProcessor
        print("✅ Processors imported successfully")
        
        from lakepipe.core.results import Result, Success, Failure
        print("✅ Results imported successfully")
        
        from lakepipe.api.pipeline import create_pipeline
        print("✅ Pipeline API imported successfully")
        
        return True
    except Exception as e:
        print(f"❌ Import failed: {e}")
        return False

def test_configuration():
    """Test configuration system"""
    print("\n🔍 Testing configuration system...")
    
    try:
        from lakepipe.config.defaults import build_pipeline_config
        
        # Test with default configuration
        config = build_pipeline_config()
        
        # Validate required sections exist
        required_sections = ["source", "sink", "transform", "cache", "log", "monitoring", "streaming"]
        for section in required_sections:
            if section not in config:
                print(f"❌ Missing config section: {section}")
                return False
        
        print("✅ Configuration system works!")
        print(f"   📁 Cache enabled: {config['cache']['enabled']}")
        print(f"   📝 Log level: {config['log']['level']}")
        print(f"   🔄 Transform engine: {config['transform']['engine']}")
        
        return True
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_pipeline_creation():
    """Test basic pipeline creation"""
    print("\n🔍 Testing pipeline creation...")
    
    try:
        from lakepipe.config.defaults import build_pipeline_config
        from lakepipe.api.pipeline import create_pipeline
        
        # Create test configuration
        config = build_pipeline_config({
            "source": {"uri": "mock://test", "format": "mock"},
            "sink": {"uri": "mock://output", "format": "mock"},
            "transform": {"operations": []}
        })
        
        # Create pipeline
        pipeline = create_pipeline(config)
        
        print("✅ Pipeline created successfully!")
        print(f"   📊 Pipeline name: {pipeline.name}")
        print(f"   🔗 Processors: {len(pipeline.processors)}")
        
        return True
    except Exception as e:
        print(f"❌ Pipeline creation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_result_types():
    """Test Result types"""
    print("\n🔍 Testing Result types...")
    
    try:
        from lakepipe.core.results import Success, Failure, safe_execute
        
        # Test Success
        success_result = Success("test_value")
        assert success_result.unwrap() == "test_value"
        
        # Test Failure
        test_error = Exception("test_error")
        failure_result = Failure(test_error)
        assert failure_result.failure() == test_error
        
        # Test safe_execute
        def test_func(x):
            return x * 2
        
        result = safe_execute(test_func, 5)
        assert result.unwrap() == 10  # If it unwraps successfully, it's a Success
        
        print("✅ Result types work correctly!")
        
        return True
    except Exception as e:
        print(f"❌ Result types test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🚀 Starting lakepipe basic tests...\n")
    
    tests = [
        test_imports,
        test_configuration,
        test_result_types,
        test_pipeline_creation,
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
        print()
    
    print(f"📊 Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Lakepipe core infrastructure is working! 🎉")
    else:
        print("⚠️  Some tests failed. Please check the output above.")
        exit(1) 