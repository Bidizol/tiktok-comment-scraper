"""Centralized structured logging for the TikTok Comment Scraper.

Provides console logging, persistent application logs with rotation,
different severity levels, UTC timestamps, run identifiers, worker
identifiers, video identifiers, batch numbers, and error categories.

Use:
  DEBUG   — detailed technical diagnostics without exposing secrets
  INFO    — normal scraping progress
  WARNING — suspicious conditions and recoverable problems
  ERROR   — failed operations
  CRITICAL — conditions threatening data integrity or system-wide operation

All log records include UTC timestamps and sanitized content.
"""

import logging
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Module logger setup
# ---------------------------------------------------------------------------

logger = logging.getLogger("tiktokscraper")
logger.setLevel(logging.DEBUG)

# Prevent double-logging if the parent logger has handlers
if not logger.handlers:
    # --- Console handler (stdout) ---
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)

    # Console formatter: concise, includes timestamp, level, message
    console_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)8s] %(name)s:%(filename)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # --- Persistent file handler with rotation ---
    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    file_handler = logging.handlers.RotatingFileHandler(
        log_dir / "tiktokscraper.log",
        maxBytes=10 * 1024 * 1024,  # 10 MB per file
        backupCount=5,  # Keep 5 rotated files
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)

    # File formatter: detailed includes timestamp, level, module, line, message
    file_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)8s] %(name)s:%(filename)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)


# ---------------------------------------------------------------------------
# Helper functions for structured logging
# ---------------------------------------------------------------------------


def _sanitize_log_value(value):
    """Safely serialize a value for inclusion in a log record."""
    if value is None:
        return ""
    if isinstance(value, str):
        if len(value) > 200:
            return value[:200] + "..."
        if "msToken" in value or "token" in value.lower():
            return "***REDACTED***"
        return value
    if isinstance(value, (bool, int, float)):
        return str(value)
    if isinstance(value, (list, tuple)):
        if len(value) > 5:
            return f"[{len(value)} items]"
        return " | ".join(_sanitize_log_value(item) for item in value)
    if isinstance(value, dict):
        keys = list(value.keys())[:5]
        if len(value) > 5:
            return f"dict({len(value)} keys, keys={keys})"
        return " | ".join(f"{k}={_sanitize_log_value(v)}" for k, v in value.items())
    return str(value)


def _build_log_record(level, component, operation, message, **kwargs):
    """Build a structured log message with identifiers."""
    parts = [
        f"level={level}",
        f"component={component}",
        f"operation={operation}",
        f"message={message}",
    ]
    for key, value in kwargs.items():
        safe_value = _sanitize_log_value(value)
        parts.append(f"{key}={safe_value}")
    structured_message = " | ".join(parts)
    level_num = getattr(logging, level.upper(), logging.INFO)
    logger.log(level_num, structured_message)


def debug(component, operation, message, **kwargs):
    """Log a DEBUG message with structured identifiers."""
    _build_log_record("DEBUG", component, operation, message, **kwargs)


def info(component, operation, message, **kwargs):
    """Log an INFO message with structured identifiers."""
    _build_log_record("INFO", component, operation, message, **kwargs)


def warning(component, operation, message, **kwargs):
    """Log a WARNING message with structured identifiers."""
    _build_log_record("WARNING", component, operation, message, **kwargs)


def error(component, operation, message, **kwargs):
    """Log an ERROR message with structured identifiers."""
    _build_log_record("ERROR", component, operation, message, **kwargs)


def critical(component, operation, message, **kwargs):
    """Log a CRITICAL message with structured identifiers."""
    _build_log_record("CRITICAL", component, operation, message, **kwargs)