"""Central error classifications for transport, token and API failures."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .model import ErrorOutcome, ErrorSeverity


@dataclass(frozen=True)
class ErrorClassification:
    """Stable, serializable classification for an operational failure."""

    error_code: str
    severity: ErrorSeverity
    explanation: str
    default_outcome: ErrorOutcome
    http_status: Optional[int] = None


def classify_dns_error(error: Exception) -> ErrorClassification:
    return ErrorClassification(
        "DNS_ERROR", ErrorSeverity.WARNING,
        "DNS resolution failed", ErrorOutcome.FAILED_RESUMABLE, 0
    )


def classify_tls_error(error: Exception) -> ErrorClassification:
    return ErrorClassification(
        "TLS_ERROR", ErrorSeverity.ERROR,
        "TLS negotiation or certificate validation failed",
        ErrorOutcome.FAILED_PERMANENT, 0
    )


def classify_http_error(status_code: int) -> ErrorClassification:
    if status_code == 401:
        return ErrorClassification(
            "AUTHENTICATION_ERROR", ErrorSeverity.ERROR,
            "HTTP authentication failed", ErrorOutcome.FAILED_PERMANENT, status_code
        )
    if status_code == 403:
        return ErrorClassification(
            "ACCESS_DENIED", ErrorSeverity.ERROR,
            "HTTP access was denied", ErrorOutcome.FAILED_PERMANENT, status_code
        )
    if status_code == 429:
        return ErrorClassification(
            "RATE_LIMITED", ErrorSeverity.WARNING,
            "HTTP request was rate limited", ErrorOutcome.FAILED_RESUMABLE, status_code
        )
    if 500 <= status_code < 600:
        return ErrorClassification(
            "HTTP_SERVER_ERROR", ErrorSeverity.WARNING,
            "Remote server returned a transient error",
            ErrorOutcome.FAILED_RESUMABLE, status_code
        )
    return ErrorClassification(
        "HTTP_ERROR", ErrorSeverity.ERROR,
        "HTTP request failed", ErrorOutcome.FAILED_PERMANENT, status_code
    )


def classify_connection_error(error: Exception) -> ErrorClassification:
    return ErrorClassification(
        "NETWORK_ERROR", ErrorSeverity.WARNING,
        "Network connection failed", ErrorOutcome.FAILED_RESUMABLE, 0
    )


def classify_json_error(error: Exception) -> ErrorClassification:
    return ErrorClassification(
        "INVALID_JSON", ErrorSeverity.WARNING,
        "Response body was not valid JSON", ErrorOutcome.FAILED_RESUMABLE, None
    )
