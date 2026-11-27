"""Structured error record model for the TikTok Comment Scraper.

Defines the ScraperError data class that every error must instantiate
when reported. This ensures consistent error reporting across all
components of the system.

The ScraperError class includes all required fields as specified in
the production requirements (Section 5.1):
- Error code
- Severity
- Timestamp in UTC
- Component
- Function or operation
- Video ID, where applicable
- Run ID
- Worker ID, where applicable
- Batch/page number, where applicable
- Cursor information, safely represented
- HTTP status, where applicable
- Retry attempt
- Recovery action
- Final outcome
- A sanitized explanation
"""

from dataclasses import dataclass, field
from enum import Enum, auto
from datetime import datetime, timezone
from typing import Optional, Any


class ErrorSeverity(Enum):
    """Severity levels for errors."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class ErrorOutcome(Enum):
    """Outcome classifications for error recovery."""
    SUCCESS = "success"
    FAILED_RESUMABLE = "failed_resumable"
    FAILED_PERMANENT = "failed_permanent"
    RETRY_EXHAUSTED = "retry_exhausted"
    INTERRUPTED = "interrupted"
    CACHE_CORRUPTED = "cache_corrupted"
    SCHEMA_CHANGED = "schema_changed"


@dataclass
class ScraperError(Exception):
    """Structured error record for the TikTok Comment Scraper."""

    error_code: str
    severity: ErrorSeverity
    timestamp_utc: str
    component: str
    function: str
    video_id: Optional[str] = None
    run_id: Optional[str] = None
    worker_id: Optional[int] = None
    batch_number: Optional[int] = None
    cursor: Optional[str] = None
    http_status: Optional[int] = None
    retry_attempt: Optional[int] = None
    recovery_action: Optional[str] = None
    outcome: ErrorOutcome = ErrorOutcome.FAILED_PERMANENT
    explanation: str = ""

    def to_dict(self) -> dict:
        """Convert the error record to a dictionary for JSON serialization."""
        return {
            "error_code": self.error_code,
            "severity": self.severity.name,
            "timestamp_utc": self.timestamp_utc,
            "component": self.component,
            "function": self.function,
            "video_id": self.video_id,
            "run_id": self.run_id,
            "worker_id": self.worker_id,
            "batch_number": self.batch_number,
            "cursor": self.cursor,
            "http_status": self.http_status,
            "retry_attempt": self.retry_attempt,
            "recovery_action": self.recovery_action,
            "outcome": self.outcome.name,
            "explanation": self.explanation,
        }

    def to_jsonl(self) -> str:
        """Convert the error record to a JSONL line."""
        import json
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: dict) -> "ScraperError":
        """Create a ScraperError from a dictionary (for deserialization)."""
        return cls(
            error_code=data.get("error_code", ""),
            severity=ErrorSeverity(data.get("severity", "ERROR")),
            timestamp_utc=data.get(
                "timestamp_utc", datetime.now(timezone.utc).isoformat()
            ),
            component=data.get("component", ""),
            function=data.get("function", ""),
            video_id=data.get("video_id"),
            run_id=data.get("run_id"),
            worker_id=data.get("worker_id"),
            batch_number=data.get("batch_number"),
            cursor=data.get("cursor"),
            http_status=data.get("http_status"),
            retry_attempt=data.get("retry_attempt"),
            recovery_action=data.get("recovery_action"),
            outcome=ErrorOutcome[data.get("outcome", "FAILED_PERMANENT")],
            explanation=data.get("explanation", ""),
        )

    @classmethod
    def from_jsonl(cls, jsonl_line: str) -> "ScraperError":
        """Create a ScraperError from a JSONL line."""
        import json
        data = json.loads(jsonl_line)
        return cls.from_dict(data)