#!/usr/bin/env python3
"""Test script to verify logging imports work correctly"""
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

try:
    from custom_logging.structured_logger import get_structured_logger
    from custom_logging.log_analyzer import get_log_analyzer
    
    print("✅ All imports successful!")
    print(f"  - get_structured_logger: {get_structured_logger}")
    print(f"  - get_log_analyzer: {get_log_analyzer}")
    
    # Test creating instances
    logger = get_structured_logger()
    analyzer = get_log_analyzer()
    
    print("✅ Instances created successfully!")
    print(f"  - Logger type: {type(logger)}")
    print(f"  - Analyzer type: {type(analyzer)}")
    
except Exception as e:
    print(f"❌ Import failed: {e}")
    import traceback
    traceback.print_exc()