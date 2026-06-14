"""
Centralized logging configuration with structured logging.

Usage:
  import logging_config
  logging_config.setup()
  logger = logging.getLogger(__name__)
  logger.info("event_type", extra={"request_id": "xyz", "asset": "orders"})

Outputs structured JSON for easier log aggregation and filtering.
"""
import json
import logging
import logging.handlers
import os
import sys
from datetime import datetime
from typing import Any


class JSONFormatter(logging.Formatter):
    """Formats log records as JSON with request context."""

    def format(self, record: logging.LogRecord) -> str:
        """Convert log record to JSON with structure."""
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "pathname": record.pathname,
            "line_number": record.lineno,
            "function": record.funcName,
        }

        # Include request ID if present
        if hasattr(record, "request_id"):
            log_data["request_id"] = record.request_id

        # Include asset context if present
        if hasattr(record, "asset"):
            log_data["asset"] = record.asset

        # Include agent context if present
        if hasattr(record, "agent"):
            log_data["agent"] = record.agent

        # Include extra fields from extra dict
        if hasattr(record, "extra_fields"):
            log_data.update(record.extra_fields)

        # Include exception info if present
        if record.exc_info:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]),
                "traceback": self.formatException(record.exc_info),
            }

        return json.dumps(log_data)


def setup(log_level: str = "INFO", log_file: str = None):
    """
    Setup structured logging for the application.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file path to also log to file
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Console handler (stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_formatter = JSONFormatter()
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # File handler (optional)
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=100 * 1024 * 1024,  # 100 MB
            backupCount=5,
        )
        file_handler.setLevel(log_level)
        file_formatter = JSONFormatter()
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

    # Suppress verbose third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.LoggerAdapter:
    """
    Get a logger with request context support.

    Returns LoggerAdapter that supports passing request_id, asset, agent
    as keyword arguments to logging methods.

    Usage:
        logger = get_logger(__name__)
        logger.info("Asset discovered", extra={"asset": "orders", "request_id": "req_123"})
    """
    logger = logging.getLogger(name)
    return LoggerWithContext(logger)


class LoggerWithContext(logging.LoggerAdapter):
    """LoggerAdapter that supports structured context fields."""

    def process(self, msg: str, kwargs: dict[str, Any]) -> tuple[str, dict]:
        """Process message and inject context."""
        extra = kwargs.pop("extra", {})

        # Flatten extra fields into record
        if extra:
            # Store in record via context dict
            self.extra = extra

        return msg, kwargs

    def _log(self, level, msg, args, **kwargs):
        """Override _log to inject extra fields."""
        extra = kwargs.pop("extra", {})
        if extra:
            # Temporarily set extra fields on each log call
            self.extra = extra

        super()._log(level, msg, args, **kwargs)

        # Reset extra
        self.extra = {}
