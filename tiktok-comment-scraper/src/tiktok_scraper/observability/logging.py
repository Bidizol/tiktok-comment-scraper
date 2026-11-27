"""UTC structured logging with rotation and centralized redaction."""

from __future__ import annotations

import json
import logging
import logging.handlers
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..reliability.errors import redact_secrets


class UTCJsonFormatter(logging.Formatter):
    """Serialize log records as one sanitized JSON object per line."""

    _CONTEXT_FIELDS = (
        "run_id",
        "worker_id",
        "video_id",
        "batch_number",
        "error_code",
        "component",
        "operation",
    )

    def format(self, record: logging.LogRecord) -> str:
        message = redact_secrets(record.getMessage())
        payload: dict[str, Any] = {
            "timestamp_utc": datetime.fromtimestamp(
                record.created, timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": message,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        for field in self._CONTEXT_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = redact_secrets(value)
        if record.exc_info:
            payload["exception"] = redact_secrets(
                self.formatException(record.exc_info)
            )
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def configure_logging(
    log_dir: Path,
    level: str = "INFO",
    logger_name: str = "tiktok_scraper",
) -> logging.Logger:
    """Configure console and rotating persistent handlers exactly once."""
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(logger_name)
    logger.setLevel(getattr(logging, level))
    logger.propagate = False

    for handler in tuple(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    formatter = UTCJsonFormatter()
    console = logging.StreamHandler(sys.stderr)
    console.setLevel(getattr(logging, level))
    console.setFormatter(formatter)

    file_handler = logging.handlers.RotatingFileHandler(
        log_dir / "application.jsonl",
        maxBytes=10 * 1024 * 1024,
        backupCount=10,
        encoding="utf-8",
        delay=True,
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(console)
    logger.addHandler(file_handler)
    return logger
