"""Canonical structured error taxonomy and immutable error records."""

from __future__ import annotations

import hashlib
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum


class Severity(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class ErrorOutcome(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED_RESUMABLE = "FAILED_RESUMABLE"
    FAILED_PERMANENT = "FAILED_PERMANENT"
    RETRY_EXHAUSTED = "RETRY_EXHAUSTED"
    INTERRUPTED = "INTERRUPTED"
    SCHEMA_CHANGED = "SCHEMA_CHANGED"


class ErrorCode(str, Enum):
    CONFIGURATION_ERROR = "CONFIGURATION_ERROR"
    AUTHENTICATION_ERROR = "AUTHENTICATION_ERROR"
    TOKEN_REFRESH_ERROR = "TOKEN_REFRESH_ERROR"
    PROXY_ERROR = "PROXY_ERROR"
    NETWORK_ERROR = "NETWORK_ERROR"
    DNS_ERROR = "DNS_ERROR"
    TLS_ERROR = "TLS_ERROR"
    HTTP_CLIENT_ERROR = "HTTP_CLIENT_ERROR"
    HTTP_SERVER_ERROR = "HTTP_SERVER_ERROR"
    RATE_LIMITED = "RATE_LIMITED"
    ACCESS_RESTRICTED = "ACCESS_RESTRICTED"
    VIDEO_NOT_FOUND = "VIDEO_NOT_FOUND"
    VIDEO_UNAVAILABLE = "VIDEO_UNAVAILABLE"
    API_RESPONSE_ERROR = "API_RESPONSE_ERROR"
    JSON_DECODE_ERROR = "JSON_DECODE_ERROR"
    API_SCHEMA_CHANGED = "API_SCHEMA_CHANGED"
    INVALID_COMMENT_DATA = "INVALID_COMMENT_DATA"
    MISSING_COMMENT_ID = "MISSING_COMMENT_ID"
    DUPLICATE_COMMENT = "DUPLICATE_COMMENT"
    PAGINATION_ERROR = "PAGINATION_ERROR"
    CURSOR_STALLED = "CURSOR_STALLED"
    CURSOR_CYCLE = "CURSOR_CYCLE"
    EMPTY_PAGE_WITH_MORE = "EMPTY_PAGE_WITH_MORE"
    CACHE_CORRUPTION = "CACHE_CORRUPTION"
    CACHE_VALIDATION_ERROR = "CACHE_VALIDATION_ERROR"
    STORAGE_ERROR = "STORAGE_ERROR"
    DISK_SPACE_ERROR = "DISK_SPACE_ERROR"
    DATABASE_ERROR = "DATABASE_ERROR"
    CONCURRENCY_ERROR = "CONCURRENCY_ERROR"
    TIMEOUT_ERROR = "TIMEOUT_ERROR"
    RETRY_EXHAUSTED = "RETRY_EXHAUSTED"
    INTERRUPTED = "INTERRUPTED"
    UNEXPECTED_INTERNAL_ERROR = "UNEXPECTED_INTERNAL_ERROR"


_SECRET_PATTERNS = (
    re.compile(r"(?i)(msToken|token|access_token|refresh_token)=([^&\s]+)"),
    re.compile(r"(?i)(authorization\s*[:=]\s*)([^,;\s]+)"),
    re.compile(r"(?i)((?:cookie|set-cookie)\s*[:=]\s*)([^\r\n]+)"),
    re.compile(r"(?i)(https?://)([^/@\s]+:[^/@\s]+)@"),
    re.compile(r"(?i)(password|passwd|proxy_pass|api_key)\s*[:=]\s*([^\s,;]+)"),
)


def redact_secrets(value: object) -> str:
    """Redact common credential forms before text enters logs or reports."""
    text = str(value)
    for pattern in _SECRET_PATTERNS:
        if pattern.groups == 2 and pattern.pattern.startswith("(?i)(https?"):
            text = pattern.sub(r"\1[REDACTED]@", text)
        elif pattern.groups == 2:
            text = pattern.sub(r"\1[REDACTED]", text)
    return text[:2000]


def safe_cursor_reference(cursor: object | None) -> str | None:
    """Return a stable one-way cursor fingerprint, never the raw cursor."""
    if cursor is None:
        return None
    raw = str(cursor).encode("utf-8", errors="replace")
    return hashlib.sha256(raw).hexdigest()[:16]


@dataclass(frozen=True, slots=True)
class ErrorRecord:
    error_code: ErrorCode
    severity: Severity
    component: str
    operation: str
    explanation: str
    run_id: str
    timestamp_utc: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    video_id: str | None = None
    worker_id: int | None = None
    batch_number: int | None = None
    cursor_reference: str | None = None
    http_status: int | None = None
    retry_attempt: int | None = None
    recovery_action: str | None = None
    final_outcome: ErrorOutcome = ErrorOutcome.FAILED_PERMANENT

    def __post_init__(self) -> None:
        if not self.component.strip() or not self.operation.strip():
            raise ValueError("component and operation are required")
        if not self.run_id.strip():
            raise ValueError("run_id is required")
        if self.worker_id is not None and self.worker_id < 0:
            raise ValueError("worker_id cannot be negative")
        if self.batch_number is not None and self.batch_number < 0:
            raise ValueError("batch_number cannot be negative")
        if self.retry_attempt is not None and self.retry_attempt < 0:
            raise ValueError("retry_attempt cannot be negative")
        if self.http_status is not None and not 100 <= self.http_status <= 599:
            raise ValueError("http_status must be between 100 and 599")
        parsed = datetime.fromisoformat(self.timestamp_utc.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("timestamp_utc must include a timezone")
        if parsed.utcoffset() != timezone.utc.utcoffset(parsed):
            raise ValueError("timestamp_utc must be UTC")
        object.__setattr__(self, "explanation", redact_secrets(self.explanation))
        if self.recovery_action is not None:
            object.__setattr__(
                self, "recovery_action", redact_secrets(self.recovery_action)
            )

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["error_code"] = self.error_code.value
        payload["severity"] = self.severity.value
        payload["final_outcome"] = self.final_outcome.value
        return payload
