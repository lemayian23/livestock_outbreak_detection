"""
Tests for enhanced logging system
"""
import pytest
import tempfile
import json
import logging
import os
import sys
from pathlib import Path
from datetime import datetime
from unittest.mock import patch, MagicMock

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from logging.structured_logger import (
    StructuredLogger,
    JSONFormatter,
    LogContext,
    get_structured_logger,
    setup_logging,
    log_context_var
)
from logging.log_analyzer import LogAnalyzer, get_log_analyzer


class TestJSONFormatter:
    def test_format_basic(self):
        """Test JSON formatter with basic log record"""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="Test message",
            args=(),
            exc_info=None
        )
        
        result = formatter.format(record)
        data = json.loads(result)
        
        assert data['level'] == 'INFO'
        assert data['message'] == 'Test message'
        assert data['logger'] == 'test_logger'
        assert 'timestamp' in data
    
    def test_format_with_exception(self):
        """Test JSON formatter with exception"""
        formatter = JSONFormatter()
        
        try:
            raise ValueError("Test error")
        except ValueError as e:
            record = logging.LogRecord(
                name="test_logger",
                level=logging.ERROR,
                pathname="test.py",
                lineno=10,
                msg="Test error message",
                args=(),
                exc_info=sys.exc_info()
            )
            
            result = formatter.format(record)
            data = json.loads(result)
            
            assert data['level'] == 'ERROR'
            assert 'exception' in data
            assert data['exception']['type'] == 'ValueError'
            assert 'Test error' in data['exception']['message']
            assert 'traceback' in data['exception']


class TestStructuredLogger:
    def setup_method(self):
        """Reset context before each test"""
        log_context_var.set({})
    
    def test_basic_logging(self):
        """Test basic logging functionality"""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                'level': 'DEBUG',
                'json_format': True,
                'file_enabled': True,
                'log_dir': tmpdir,
                'max_file_size': 1024 * 1024,
                'backup_count': 1
            }
            
            logger = StructuredLogger('test_logger', config)
            
            # Test different log levels
            logger.debug("Debug message", test_field="debug")
            logger.info("Info message", test_field="info")
            logger.warning("Warning message", test_field="warning")
            logger.error("Error message", test_field="error")
            
            # Check that log file was created
            log_file = Path(tmpdir) / "test_logger.log"
            assert log_file.exists()
            
            # Read and verify logs
            with open(log_file, 'r') as f:
                lines = f.readlines()
            
            assert len(lines) >= 4
            for line in lines:
                data = json.loads(line.strip())
                assert 'timestamp' in data
                assert 'message' in data
                assert 'level' in data
                assert 'test_field' in data['extra']
    
    def test_context_logging(self):
        """Test logging with context"""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                'level': 'INFO',
                'json_format': True,
                'file_enabled': True,
                'log_dir': tmpdir
            }
            
            logger = StructuredLogger('test_logger', config)
            
            # Set context
            logger.set_context(request_id='123', user_id='456')
            
            # Log with context
            logger.info("Test message", additional="data")
            
            # Read log
            log_file = Path(tmpdir) / "test_logger.log"
            with open(log_file, 'r') as f:
                data = json.loads(f.readline().strip())
            
            assert 'context' in data
            assert data['context']['request_id'] == '123'
            assert data['context']['user_id'] == '456'
            assert data['extra']['additional'] == 'data'
    
    def test_context_manager(self):
        """Test logging with context manager"""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                'level': 'INFO',
                'json_format': True,
                'file_enabled': True,
                'log_dir': tmpdir
            }
            
            logger = StructuredLogger('test_logger', config)
            
            # Use context manager
            with logger.with_context(operation='test_op', stage='start'):
                logger.info("Inside context")
            
            logger.info("Outside context")
            
            # Read logs
            log_file = Path(tmpdir) / "test_logger.log"
            with open(log_file, 'r') as f:
                lines = f.readlines()
            
            # First log should have context
            data1 = json.loads(lines[0].strip())
            assert 'context' in data1
            assert data1['context']['operation'] == 'test_op'
            
            # Second log should not have context
            data2 = json.loads(lines[1].strip())
            assert 'context' not in data2 or 'operation' not in data2.get('context', {})
    
    def test_timer_context(self):
        """Test timer context manager"""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                'level': 'INFO',
                'json_format': True,
                'file_enabled': True,
                'log_dir': tmpdir
            }
            
            logger = StructuredLogger('test_logger', config)
            
            # Use timer
            with logger.timer("test_operation"):
                pass  # Do nothing, just sleep a tiny bit
            
            # Read log
            log_file = Path(tmpdir) / "test_logger.log"
            with open(log_file, 'r') as f:
                data = json.loads(f.readline().strip())
            
            assert data['message'] == 'Starting operation: test_operation'
            
            # Second log should have duration
            data2 = json.loads(f.readline().strip())
            assert 'duration_ms' in data2
            assert data2['operation'] == 'test_operation'
            assert data2['status'] == 'success'
    
    def test_error_logging_with_exception(self):
        """Test error logging with exception"""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                'level': 'ERROR',
                'json_format': True,
                'file_enabled': True,
                'log_dir': tmpdir,
                'separate_error_log': True
            }
            
            logger = StructuredLogger('test_logger', config)
            
            try:
                raise RuntimeError("Test runtime error")
            except RuntimeError as e:
                logger.error("Failed operation", exc_info=e)
            
            # Check error log file
            error_file = Path(tmpdir) / "test_logger.error.log"
            assert error_file.exists()
            
            with open(error_file, 'r') as f:
                data = json.loads(f.readline().strip())
            
            assert data['level'] == 'ERROR'
            assert 'exception' in data
            assert data['exception']['type'] == 'RuntimeError'
    
    def test_log_rotation(self):
        """Test log rotation"""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                'level': 'INFO',
                'json_format': False,  # Text format for easier testing
                'file_enabled': True,
                'log_dir': tmpdir,
                'max_file_size': 100,  # Very small to trigger rotation
                'backup_count': 2
            }
            
            logger = StructuredLogger('test_logger', config)
            
            # Write enough logs to trigger rotation
            for i in range(50):
                logger.info(f"Test message {i}")
            
            # Check that rotation occurred
            log_files = list(Path(tmpdir).glob("test_logger.log*"))
            assert len(log_files) >= 2  # Original + at least one backup
            
            # Rotate manually
            logger.rotate_logs()
            log_files_after = list(Path(tmpdir).glob("test_logger.log*"))
            assert len(log_files_after) >= len(log_files)  # Should have more or same
    
    def test_get_stats(self):
        """Test getting logging statistics"""
        logger = StructuredLogger('test_logger', {})
        
        logger.info("Message 1")
        logger.error("Message 2")
        logger.info("Message 3")
        
        stats = logger.get_stats()
        
        assert stats['total_logs'] == 3
        assert stats['errors'] == 1
        assert stats['handlers'] > 0


class TestLogAnalyzer:
    def test_parse_log_line(self):
        """Test parsing log lines"""
        analyzer = LogAnalyzer()
        
        # Valid JSON log
        valid_line = '{"timestamp": "2024-01-01T00:00:00Z", "level": "INFO", "message": "Test"}'
        result = analyzer.parse_log_line(valid_line)
        assert result['timestamp'] == '2024-01-01T00:00:00Z'
        assert result['level'] == 'INFO'
        assert result['message'] == 'Test'
        
        # Invalid JSON
        invalid_line = 'Not a JSON log line'
        result = analyzer.parse_log_line(invalid_line)
        assert result is None
        
        # Mixed output with JSON
        mixed_line = 'Some prefix {"timestamp": "2024-01-01T00:00:00Z", "message": "Test"} suffix'
        result = analyzer.parse_log_line(mixed_line)
        assert result is not None
        assert result['message'] == 'Test'
    
    def test_analyze_errors(self):
        """Test error analysis"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test log file
            log_file = Path(tmpdir) / "test.log"
            
            logs = [
                '{"timestamp": "2024-01-01T10:00:00Z", "level": "ERROR", "message": "Error 1", "module": "module1"}',
                '{"timestamp": "2024-01-01T10:01:00Z", "level": "ERROR", "message": "Error 1", "module": "module1"}',
                '{"timestamp": "2024-01-01T10:02:00Z", "level": "ERROR", "message": "Error 2", "module": "module2"}',
                '{"timestamp": "2024-01-01T10:03:00Z", "level": "INFO", "message": "Info message", "module": "module1"}',
            ]
            
            log_file.write_text('\n'.join(logs))
            
            analyzer = LogAnalyzer(tmpdir)
            analysis = analyzer.analyze_errors(hours=24, group_by='message')
            
            assert analysis['total_errors'] == 3
            assert analysis['error_distribution']['Error 1'] == 2
            assert analysis['error_distribution']['Error 2'] == 1
            assert len(analysis['most_common_errors']) == 2
            assert analysis['most_common_errors'][0][0] == 'Error 1'
            assert analysis['most_common_errors'][0][1] == 2
    
    def test_performance_report(self):
        """Test performance report generation"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test log file with timing data
            log_file = Path(tmpdir) / "test.log"
            
            logs = [
                '{"timestamp": "2024-01-01T10:00:00Z", "level": "INFO", "message": "Operation A", "operation": "op_a", "duration_ms": 100}',
                '{"timestamp": "2024-01-01T10:00:01Z", "level": "INFO", "message": "Operation A", "operation": "op_a", "duration_ms": 150}',
                '{"timestamp": "2024-01-01T10:00:02Z", "level": "INFO", "message": "Operation B", "operation": "op_b", "duration_ms": 50}',
            ]
            
            log_file.write_text('\n'.join(logs))
            
            analyzer = LogAnalyzer(tmpdir)
            report = analyzer.performance_report(hours=1)
            
            assert report['total_timed_operations'] == 3
            assert 'op_a' in report['operations']
            assert report['operations']['op_a']['count'] == 2
            assert report['operations']['op_a']['avg_ms'] == 125
            assert report['operations']['op_b']['count'] == 1
            assert report['operations']['op_b']['avg_ms'] == 50
    
    def test_search_logs(self):
        """Test log search"""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = Path(tmpdir) / "test.log"
            
            logs = [
                '{"timestamp": "2024-01-01T10:00:00Z", "level": "INFO", "message": "Database query executed"}',
                '{"timestamp": "2024-01-01T10:01:00Z", "level": "ERROR", "message": "Database connection failed"}',
                '{"timestamp": "2024-01-01T10:02:00Z", "level": "INFO", "message": "File processed successfully"}',
            ]
            
            log_file.write_text('\n'.join(logs))
            
            analyzer = LogAnalyzer(tmpdir)
            
            # Search for "database"
            results = analyzer.search_logs("database", hours=24)
            assert len(results) == 2
            
            # Search for "failed"
            results = analyzer.search_logs("failed", hours=24)
            assert len(results) == 1
            assert results[0]['level'] == 'ERROR'
    
    def test_log_summary(self):
        """Test log summary generation"""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = Path(tmpdir) / "test.log"
            
            logs = [
                '{"timestamp": "2024-01-01T10:00:00Z", "level": "INFO", "message": "Message 1", "logger": "logger1"}',
                '{"timestamp": "2024-01-01T10:01:00Z", "level": "ERROR", "message": "Message 2", "logger": "logger2"}',
                '{"timestamp": "2024-01-01T10:02:00Z", "level": "INFO", "message": "Message 3", "logger": "logger1"}',
            ]
            
            log_file.write_text('\n'.join(logs))
            
            analyzer = LogAnalyzer(tmpdir)
            summary = analyzer.log_summary(hours=1)
            
            assert summary['total_logs'] == 3
            assert summary['level_distribution']['INFO'] == 2
            assert summary['level_distribution']['ERROR'] == 1
            assert len(summary['unique_loggers']) == 2
            assert 'logger1' in summary['unique_loggers']
            assert 'logger2' in summary['unique_loggers']


class TestGlobalInstances:
    def test_structured_logger_singleton(self):
        """Test structured logger singleton pattern"""
        logger1 = get_structured_logger()
        logger2 = get_structured_logger()
        
        assert logger1 is logger2
    
    def test_log_analyzer_singleton(self):
        """Test log analyzer singleton pattern"""
        analyzer1 = get_log_analyzer()
        analyzer2 = get_log_analyzer()
        
        assert analyzer1 is analyzer2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])