"""Error classification module for the TikTok Comment Scraper.

Defines the structured error record model and exports all error categories.
This module provides a consistent error taxonomy that every component
in the system must use when reporting failures.

Error categories cover the full lifecycle of a scraping operation:
from configuration through authentication, pagination, storage, and
recovery.

Each error record must include:
- Error code (from the defined taxonomy)
- Severity (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- Timestamp in UTC
- Component responsible
- Function or operation where the error occurred
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

from .model import ScraperError, ErrorSeverity, ErrorOutcome

# Export ErrorClassifier - it's defined in classifiers.py but we need to handle the import
# Check if we can import from classifiers
try:
    from .classifiers import ErrorClassifier
except ImportError:
    # Fallback: create a simple ErrorClassifier if classifiers can't be imported
    class ErrorClassifier:
        """Fallback ErrorClassifier for when classifiers module isn't available."""
        pass

# Export all error codes for easy reference
__all__ = [
    "ScraperError",
    "ErrorSeverity",
    "ErrorOutcome",
    "ErrorClassifier",
    # Error codes - CONFIGURATION
    "CONFIGURATION_ERROR",
    "CONFIG_VALIDATION_ERROR",
    "CONFIG_FILE_NOT_FOUND",
    "CONFIG_PARSE_ERROR",
    # Error codes - AUTHENTICATION
    "AUTHENTICATION_ERROR",
    "TOKEN_REFRESH_ERROR",
    "INVALID_TOKEN_ERROR",
    "TOKEN_EXPIRED_ERROR",
    # Error codes - PROXY
    "PROXY_ERROR",
    "PROXY_CONNECTION_ERROR",
    "PROXY_AUTHENTICATION_ERROR",
    "PROXY_ROTATION_ERROR",
    # Error codes - NETWORK
    "NETWORK_ERROR",
    "DNS_ERROR",
    "TLS_ERROR",
    "HTTP_CLIENT_ERROR",
    "HTTP_SERVER_ERROR",
    # Error codes - RATE LIMITS
    "RATE_LIMITED",
    "RATE_LIMIT_EXCEEDED",
    # Error codes - ACCESS
    "ACCESS_RESTRICTED",
    "ACCESS_DENIED",
    "ACCOUNT_BLOCKED",
    # Error codes - VIDEO
    "VIDEO_NOT_FOUND",
    "VIDEO_UNAVAILABLE",
    "VIDEO_PRIVATE",
    "VIDEO_REGIONLOCKED",
    # Error codes - API RESPONSE
    "API_RESPONSE_ERROR",
    "JSON_DECODE_ERROR",
    "API_SCHEMA_CHANGED",
    # Error codes - COMMENT DATA
    "INVALID_COMMENT_DATA",
    "MISSING_COMMENT_ID",
    "DUPLICATE_COMMENT",
    # Error codes - PAGINATION
    "PAGINATION_ERROR",
    "CURSOR_STALLED",
    "CURSOR_CYCLE",
    "EMPTY_PAGE_WITH_MORE",
    # Error codes - CACHE/STORAGE
    "CACHE_CORRUPTION",
    "CACHE_VALIDATION_ERROR",
    "STORAGE_ERROR",
    "DISK_SPACE_ERROR",
    "DATABASE_ERROR",
    # Error codes - CONCURRENCY
    "CONCURRENCY_ERROR",
    "WORKER_CONFLICT",
    # Error codes - RETRY
    "RETRY_EXHAUSTED",
    "RETRY_BUDGET_EXCEEDED",
    # Error codes - TIMEOUT
    "TIMEOUT_ERROR",
    # Error codes - INTERNAL
    "INTERRUPTED",
    "UNEXPECTED_INTERNAL_ERROR",
]
