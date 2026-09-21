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
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from custom_logging.structured_logger import (
    StructuredLogger,
    JSONFormatter,
    LogContext,
    get_structured_logger,
    setup_logging,
    log_context_var,
)
from custom_logging.log_analyzer import LogAnalyzer, get_log_analyzer


def _now_iso(offset_minutes: int = 0) -> str:
    ts = datetime.now(timezone.utc) - timedelta(minutes=offset_minutes)
    return ts.isoformat().replace('+00:00', 'Z')


class TestJSONFormatter:
    def test_format_basic(self):
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        result = formatter.format(record)
        data = json.loads(result)
        assert data['level'] == 'INFO'
        assert data['message'] == 'Test message'
        assert data['logger'] == 'test_logger'
        assert 'timestamp' in data

    def test_format_with_exception(self):
        formatter = JSONFormatter()
        try:
            raise ValueError("Test error")
        except ValueError:
            record = logging.LogRecord(
                name="test_logger",
                level=logging.ERROR,
                pathname="test.py",
                lineno=10,
                msg="Test error message",
                args=(),
                exc_info=sys.exc_info(),
            )
            result = formatter.format(record)
            data = json.loads(result)
            assert data['level'] == 'ERROR'
            assert 'exception' in data
            assert data['exception']['type'] == 'ValueError'


class TestStructuredLogger:
    def setup_method(self):
        log_context_var.set({})

    def _close(self, logger):
        if logger is not None:
            logger.close()

    def test_basic_logging(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                'level': 'DEBUG',
                'json_format': True,
                'file_enabled': True,
                'log_dir': tmpdir,
                'max_file_size': 1024 * 1024,
                'backup_count': 1,
                'include_caller': False,
            }
            logger = StructuredLogger('test_logger', config)
            try:
                logger.debug("Debug message", test_field="debug")
                logger.info("Info message", test_field="info")
                logger.warning("Warning message", test_field="warning")
                logger.error("Error message", test_field="error")
                logger.flush()

                log_file = Path(tmpdir) / "test_logger.log"
                assert log_file.exists()

                with open(log_file, 'r') as f:
                    lines = f.readlines()
                assert len(lines) >= 4
                for line in lines:
                    data = json.loads(line.strip())
                    assert 'timestamp' in data
                    assert 'message' in data
                    assert 'level' in data
                    assert 'extra' in data
                    assert 'test_field' in data['extra']
            finally:
                self._close(logger)

    def test_context_logging(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                'level': 'INFO',
                'json_format': True,
                'file_enabled': True,
                'log_dir': tmpdir,
                'include_caller': False,
            }
            logger = StructuredLogger('test_logger', config)
            try:
                logger.set_context(request_id='123', user_id='456')
                logger.info("Test message", additional="data")
                logger.flush()

                log_file = Path(tmpdir) / "test_logger.log"
                with open(log_file, 'r') as f:
                    data = json.loads(f.readline().strip())

                assert 'context' in data
                assert data['context']['request_id'] == '123'
                assert data['context']['user_id'] == '456'
                assert data['extra']['additional'] == 'data'
            finally:
                self._close(logger)

    def test_context_manager(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                'level': 'INFO',
                'json_format': True,
                'file_enabled': True,
                'log_dir': tmpdir,
                'include_caller': False,
            }
            logger = StructuredLogger('test_logger', config)
            try:
                with logger.with_context(operation='test_op', stage='start'):
                    logger.info("Inside context")
                logger.info("Outside context")
                logger.flush()

                log_file = Path(tmpdir) / "test_logger.log"
                with open(log_file, 'r') as f:
                    lines = f.readlines()

                data1 = json.loads(lines[0].strip())
                assert data1.get('context', {}).get('operation') == 'test_op'

                data2 = json.loads(lines[1].strip())
                ctx2 = data2.get('context', {})
                assert ctx2.get('operation') != 'test_op'
            finally:
                self._close(logger)

    def test_timer_context(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                'level': 'DEBUG',
                'json_format': True,
                'file_enabled': True,
                'log_dir': tmpdir,
                'include_caller': False,
            }
            logger = StructuredLogger('test_logger', config)
            try:
                with logger.timer("test_operation"):
                    pass
                logger.flush()

                log_file = Path(tmpdir) / "test_logger.log"
                with open(log_file, 'r') as f:
                    lines = f.readlines()

                assert len(lines) >= 2

                first = json.loads(lines[0].strip())
                assert 'Starting operation' in first['message']

                second = json.loads(lines[1].strip())
                assert 'duration_ms' in second['extra']
                assert second['extra']['operation'] == 'test_operation'
                assert second['extra']['status'] == 'success'
            finally:
                self._close(logger)

    def test_error_logging_with_exception(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                'level': 'ERROR',
                'json_format': True,
                'file_enabled': True,
                'log_dir': tmpdir,
                'separate_error_log': True,
                'include_caller': False,
            }
            logger = StructuredLogger('test_logger', config)
            try:
                try:
                    raise RuntimeError("Test runtime error")
                except RuntimeError as e:
                    logger.error("Failed operation", exception=e)
                logger.flush()

                error_file = Path(tmpdir) / "test_logger.error.log"
                assert error_file.exists()

                with open(error_file, 'r') as f:
                    data = json.loads(f.readline().strip())

                assert data['level'] == 'ERROR'
                assert 'exception' in data
                assert data['exception']['type'] == 'RuntimeError'
            finally:
                self._close(logger)

    def test_log_rotation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                'level': 'INFO',
                'json_format': False,
                'file_enabled': True,
                'log_dir': tmpdir,
                'max_file_size': 100,
                'backup_count': 2,
                'include_caller': False,
            }
            logger = StructuredLogger('test_logger', config)
            try:
                for i in range(50):
                    logger.info(f"Test message {i}")
                logger.flush()

                log_files = list(Path(tmpdir).glob("test_logger.log*"))
                assert len(log_files) >= 2

                logger.rotate_logs()
                log_files_after = list(Path(tmpdir).glob("test_logger.log*"))
                assert len(log_files_after) >= len(log_files)
            finally:
                self._close(logger)

    def test_get_stats(self):
        logger = StructuredLogger('stats_logger', {'include_caller': False})
        try:
            logger.info("Message 1")
            logger.error("Message 2")
            logger.info("Message 3")
            stats = logger.get_stats()
            assert stats['total_logs'] == 3
            assert stats['errors'] == 1
            assert stats['handlers'] > 0
        finally:
            logger.close()


class TestLogAnalyzer:
    def test_parse_log_line(self):
        analyzer = LogAnalyzer()
        valid_line = '{"timestamp": "2024-01-01T00:00:00Z", "level": "INFO", "message": "Test"}'
        result = analyzer.parse_log_line(valid_line)
        assert result['message'] == 'Test'

        assert analyzer.parse_log_line('Not a JSON log line') is None

    def test_analyze_errors(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = Path(tmpdir) / "test.log"
            logs = [
                f'{{"timestamp": "{_now_iso(10)}", "level": "ERROR", "message": "Error 1", "module": "module1"}}',
                f'{{"timestamp": "{_now_iso(9)}", "level": "ERROR", "message": "Error 1", "module": "module1"}}',
                f'{{"timestamp": "{_now_iso(8)}", "level": "ERROR", "message": "Error 2", "module": "module2"}}',
                f'{{"timestamp": "{_now_iso(7)}", "level": "INFO", "message": "Info", "module": "module1"}}',
            ]
            log_file.write_text('\n'.join(logs))

            analyzer = LogAnalyzer(tmpdir)
            analysis = analyzer.analyze_errors(hours=24, group_by='message')

            assert analysis['total_errors'] == 3
            assert analysis['error_distribution']['Error 1'] == 2
            assert analysis['error_distribution']['Error 2'] == 1

    def test_performance_report(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = Path(tmpdir) / "test.log"
            logs = [
                f'{{"timestamp": "{_now_iso(5)}", "level": "INFO", "message": "A", "operation": "op_a", "duration_ms": 100}}',
                f'{{"timestamp": "{_now_iso(4)}", "level": "INFO", "message": "A", "operation": "op_a", "duration_ms": 150}}',
                f'{{"timestamp": "{_now_iso(3)}", "level": "INFO", "message": "B", "operation": "op_b", "duration_ms": 50}}',
            ]
            log_file.write_text('\n'.join(logs))

            analyzer = LogAnalyzer(tmpdir)
            report = analyzer.performance_report(hours=1)

            assert report['total_timed_operations'] == 3
            assert report['operations']['op_a']['count'] == 2
            assert report['operations']['op_a']['avg_ms'] == 125
            assert report['operations']['op_b']['avg_ms'] == 50

    def test_search_logs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = Path(tmpdir) / "test.log"
            logs = [
                f'{{"timestamp": "{_now_iso(3)}", "level": "INFO", "message": "Database query executed"}}',
                f'{{"timestamp": "{_now_iso(2)}", "level": "ERROR", "message": "Database connection failed"}}',
                f'{{"timestamp": "{_now_iso(1)}", "level": "INFO", "message": "File processed"}}',
            ]
            log_file.write_text('\n'.join(logs))

            analyzer = LogAnalyzer(tmpdir)
            results = analyzer.search_logs("database", hours=24)
            assert len(results) == 2

            results = analyzer.search_logs("failed", hours=24)
            assert len(results) == 1
            assert results[0]['level'] == 'ERROR'

    def test_log_summary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = Path(tmpdir) / "test.log"
            logs = [
                f'{{"timestamp": "{_now_iso(3)}", "level": "INFO", "message": "M1", "logger": "logger1"}}',
                f'{{"timestamp": "{_now_iso(2)}", "level": "ERROR", "message": "M2", "logger": "logger2"}}',
                f'{{"timestamp": "{_now_iso(1)}", "level": "INFO", "message": "M3", "logger": "logger1"}}',
            ]
            log_file.write_text('\n'.join(logs))

            analyzer = LogAnalyzer(tmpdir)
            summary = analyzer.log_summary(hours=1)

            assert summary['total_logs'] == 3
            assert summary['level_distribution']['INFO'] == 2
            assert summary['level_distribution']['ERROR'] == 1
            assert set(summary['unique_loggers']) == {'logger1', 'logger2'}


class TestGlobalInstances:
    def test_structured_logger_singleton(self):
        logger1 = get_structured_logger()
        logger2 = get_structured_logger()
        assert logger1 is logger2

    def test_log_analyzer_singleton(self):
        analyzer1 = get_log_analyzer()
        analyzer2 = get_log_analyzer()
        assert analyzer1 is analyzer2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])