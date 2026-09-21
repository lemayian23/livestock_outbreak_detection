"""
Structured JSON logger with rotation and context management
"""
import json
import logging
import logging.handlers
import sys
import os
from typing import Dict, Any, Optional, Union
from pathlib import Path
from datetime import datetime, timezone
from enum import Enum
from dataclasses import dataclass, field
import threading
from contextvars import ContextVar
import traceback
import inspect

# Context variable for log context
log_context_var = ContextVar('log_context', default={})


class LogLevel(Enum):
    """Log level enumeration"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class LogContext:
    """Context for structured logging"""
    request_id: Optional[str] = None
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    component: Optional[str] = None
    operation: Optional[str] = None
    additional_context: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        result = {}
        if self.request_id:
            result['request_id'] = self.request_id
        if self.user_id:
            result['user_id'] = self.user_id
        if self.session_id:
            result['session_id'] = self.session_id
        if self.component:
            result['component'] = self.component
        if self.operation:
            result['operation'] = self.operation
        result.update(self.additional_context)
        return result

    def update(self, **kwargs) -> None:
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                self.additional_context[key] = value


class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging"""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            'timestamp': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
            'process_id': record.process,
            'thread_id': record.thread,
            'thread_name': record.threadName,
        }

        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = {
                'type': record.exc_info[0].__name__ if record.exc_info[0] else None,
                'message': str(record.exc_info[1]) if record.exc_info[1] else None,
                'traceback': self.formatException(record.exc_info),
            }
        elif hasattr(record, 'exception_object') and record.exception_object:
            exc = record.exception_object
            log_data['exception'] = {
                'type': type(exc).__name__,
                'message': str(exc),
                'traceback': ''.join(
                    traceback.format_exception(type(exc), exc, exc.__traceback__)
                ),
            }

        # Add our smuggled metadata (context, caller, user kwargs)
        if hasattr(record, 'log_extra') and isinstance(record.log_extra, dict):
            log_data['extra'] = record.log_extra
            # Surface context at top level for convenience
            if 'context' in record.log_extra:
                log_data['context'] = record.log_extra['context']

        return json.dumps(log_data, default=str)


class StructuredLogger:
    """Enhanced logger with structured JSON output, rotation, and context support"""

    def __init__(self, name: str = "livestock", config: Optional[Dict] = None):
        self.name = name
        self.config = config or {}
        self.logger = logging.getLogger(name)
        self._setup_logger()
        self._local = threading.local()
        self._setup_handlers()
        self._log_count = 0
        self._error_count = 0

    def _setup_logger(self) -> None:
        self.logger.setLevel(self.config.get('level', 'INFO'))
        self.logger.propagate = False

    def _setup_handlers(self) -> None:
        self.logger.handlers.clear()

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        if self.config.get('json_format', False):
            console_handler.setFormatter(JSONFormatter())
        else:
            console_handler.setFormatter(
                logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            )
        self.logger.addHandler(console_handler)

        # File handler
        if self.config.get('file_enabled', False):
            log_dir = self.config.get('log_dir', 'logs')
            Path(log_dir).mkdir(exist_ok=True)
            log_file = Path(log_dir) / f"{self.name}.log"

            file_handler = logging.handlers.RotatingFileHandler(
                filename=log_file,
                maxBytes=self.config.get('max_file_size', 10 * 1024 * 1024),
                backupCount=self.config.get('backup_count', 5),
            )

            if self.config.get('json_format', False):
                file_handler.setFormatter(JSONFormatter())
            else:
                file_handler.setFormatter(
                    logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                )
            self.logger.addHandler(file_handler)

        # Separate error log
        if self.config.get('separate_error_log', False):
            log_dir = self.config.get('log_dir', 'logs')
            error_file = Path(log_dir) / f"{self.name}.error.log"

            error_handler = logging.handlers.RotatingFileHandler(
                filename=error_file,
                maxBytes=self.config.get('max_file_size', 10 * 1024 * 1024),
                backupCount=self.config.get('backup_count', 5),
            )
            error_handler.setLevel(logging.ERROR)

            if self.config.get('json_format', False):
                error_handler.setFormatter(JSONFormatter())
            else:
                error_handler.setFormatter(
                    logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                )
            self.logger.addHandler(error_handler)

    def debug(self, message: str, **kwargs) -> None:
        self._log(logging.DEBUG, message, **kwargs)

    def info(self, message: str, **kwargs) -> None:
        self._log(logging.INFO, message, **kwargs)

    def warning(self, message: str, **kwargs) -> None:
        self._log(logging.WARNING, message, **kwargs)

    def error(self, message: str, exception: Optional[Exception] = None, **kwargs) -> None:
        """Log error message with optional exception"""
        self._error_count += 1

        if exception:
            # Pass exc_info as a first-class logging kwarg; keep exception_object
            # inside log_extra so JSONFormatter can render it if exc_info is absent.
            kwargs['exception_object'] = exception
            kwargs_without_exc = {
                k: v for k, v in kwargs.items() if k != 'exception_object'
            }
            self.logger.error(
                message,
                exc_info=exception,
                extra={'log_extra': kwargs_without_exc},
            )
        else:
            self._log(logging.ERROR, message, **kwargs)

    def critical(self, message: str, **kwargs) -> None:
        self._log(logging.CRITICAL, message, **kwargs)

    def _log(self, level: int, message: str, **kwargs) -> None:
        """Internal logging method with context handling"""
        self._log_count += 1

        context = log_context_var.get()

        record_extra: Dict[str, Any] = {}

        if context and 'context' not in kwargs:
            record_extra['context'] = context

        if self.config.get('include_caller', True):
            frame = inspect.currentframe().f_back.f_back
            record_extra['caller'] = {
                'file': frame.f_code.co_filename,
                'line': frame.f_lineno,
                'function': frame.f_code.co_name,
            }

        # Merge caller/context with user-provided kwargs
        record_extra.update(kwargs)

        # Smuggle everything under a single "log_extra" attribute
        self.logger.log(level, message, extra={'log_extra': record_extra})

    def with_context(self, **context_kwargs):
        return LogContextManager(self, context_kwargs)

    def set_context(self, **context_kwargs) -> None:
        current_context = log_context_var.get().copy()
        current_context.update(context_kwargs)
        log_context_var.set(current_context)

    def clear_context(self) -> None:
        log_context_var.set({})

    def get_context(self) -> Dict[str, Any]:
        return log_context_var.get().copy()

    def timer(self, operation: str):
        return TimerContext(self, operation)

    def get_stats(self) -> Dict[str, Any]:
        return {
            'total_logs': self._log_count,
            'errors': self._error_count,
            'loggers': len(logging.Logger.manager.loggerDict),
            'handlers': len(self.logger.handlers),
        }

    def flush(self) -> None:
        for handler in self.logger.handlers:
            handler.flush()

    def rotate_logs(self) -> None:
        for handler in self.logger.handlers:
            if isinstance(handler, logging.handlers.RotatingFileHandler):
                handler.doRollover()

    def close(self) -> None:
        """Close all handlers (important on Windows to release file locks)"""
        for handler in list(self.logger.handlers):
            try:
                handler.flush()
                handler.close()
            except Exception:
                pass
        self.logger.handlers.clear()


class LogContextManager:
    def __init__(self, logger: StructuredLogger, context: Dict[str, Any]):
        self.logger = logger
        self.context = context
        self.old_context = None

    def __enter__(self):
        self.old_context = self.logger.get_context()
        new_context = self.old_context.copy()
        new_context.update(self.context)
        log_context_var.set(new_context)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        log_context_var.set(self.old_context)


class TimerContext:
    def __init__(self, logger: StructuredLogger, operation: str):
        self.logger = logger
        self.operation = operation
        self.start_time = None

    def __enter__(self):
        self.start_time = datetime.now(timezone.utc)
        self.logger.debug(f"Starting operation: {self.operation}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        end_time = datetime.now(timezone.utc)
        duration = (end_time - self.start_time).total_seconds() * 1000

        if exc_type is None:
            self.logger.info(
                f"Completed operation: {self.operation}",
                operation=self.operation,
                duration_ms=duration,
                status="success",
            )
        else:
            self.logger.error(
                f"Failed operation: {self.operation}",
                operation=self.operation,
                duration_ms=duration,
                status="failed",
                error_type=exc_type.__name__,
                error_message=str(exc_val),
            )


def setup_logging(
    level: Union[str, LogLevel] = "INFO",
    json_format: bool = False,
    file_enabled: bool = False,
    log_dir: str = "logs",
    max_file_size: int = 10 * 1024 * 1024,
    backup_count: int = 5,
    separate_error_log: bool = False,
    include_caller: bool = True,
) -> StructuredLogger:
    config = {
        'level': level.value if isinstance(level, LogLevel) else level,
        'json_format': json_format,
        'file_enabled': file_enabled,
        'log_dir': log_dir,
        'max_file_size': max_file_size,
        'backup_count': backup_count,
        'separate_error_log': separate_error_log,
        'include_caller': include_caller,
    }
    return StructuredLogger(config=config)


_structured_logger: Optional[StructuredLogger] = None


def get_structured_logger(config: Optional[Dict] = None) -> StructuredLogger:
    global _structured_logger

    if _structured_logger is None:
        if config is None:
            config = {
                'level': os.getenv('LOG_LEVEL', 'INFO'),
                'json_format': os.getenv('LOG_JSON_FORMAT', 'false').lower() == 'true',
                'file_enabled': os.getenv('LOG_FILE_ENABLED', 'false').lower() == 'true',
                'log_dir': os.getenv('LOG_DIR', 'logs'),
                'max_file_size': int(os.getenv('LOG_MAX_SIZE', 10 * 1024 * 1024)),
                'backup_count': int(os.getenv('LOG_BACKUP_COUNT', 5)),
                'separate_error_log': os.getenv('LOG_SEPARATE_ERROR', 'false').lower() == 'true',
                'include_caller': True,
            }
        _structured_logger = StructuredLogger(config=config)

    return _structured_logger