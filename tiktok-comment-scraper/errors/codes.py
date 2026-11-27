"""Error code constants for the TikTok Comment Scraper.

Defines all error codes from the centralized taxonomy (Section 5.1).
These constants should be imported and used throughout the codebase
instead of hardcoding error strings.

Each error code corresponds to a specific category of failure and
has associated severity and recovery logic.
"""

# ---------------------------------------------------------------------------
# CONFIGURATION ERRORS
# ---------------------------------------------------------------------------
CONFIGURATION_ERROR = "CONFIGURATION_ERROR"
CONFIG_VALIDATION_ERROR = "CONFIG_VALIDATION_ERROR"
CONFIG_FILE_NOT_FOUND = "CONFIG_FILE_NOT_FOUND"
CONFIG_PARSE_ERROR = "CONFIG_PARSE_ERROR"

# ---------------------------------------------------------------------------
# AUTHENTICATION ERRORS
# ---------------------------------------------------------------------------
AUTHENTICATION_ERROR = "AUTHENTICATION_ERROR"
TOKEN_REFRESH_ERROR = "TOKEN_REFRESH_ERROR"
INVALID_TOKEN_ERROR = "INVALID_TOKEN_ERROR"
TOKEN_EXPIRED_ERROR = "TOKEN_EXPIRED_ERROR"

# ---------------------------------------------------------------------------
# PROXY ERRORS
# ---------------------------------------------------------------------------
PROXY_ERROR = "PROXY_ERROR"
PROXY_CONNECTION_ERROR = "PROXY_CONNECTION_ERROR"
PROXY_AUTHENTICATION_ERROR = "PROXY_AUTHENTICATION_ERROR"
PROXY_ROTATION_ERROR = "PROXY_ROTATION_ERROR"

# ---------------------------------------------------------------------------
# NETWORK ERRORS
# ---------------------------------------------------------------------------
NETWORK_ERROR = "NETWORK_ERROR"
DNS_ERROR = "DNS_ERROR"
TLS_ERROR = "TLS_ERROR"
HTTP_CLIENT_ERROR = "HTTP_CLIENT_ERROR"
HTTP_SERVER_ERROR = "HTTP_SERVER_ERROR"

# ---------------------------------------------------------------------------
# RATE LIMIT ERRORS
# ---------------------------------------------------------------------------
RATE_LIMITED = "RATE_LIMITED"
RATE_LIMIT_EXCEEDED = "RATE_LIMIT_EXCEEDED"

# ---------------------------------------------------------------------------
# ACCESS ERRORS
# ---------------------------------------------------------------------------
ACCESS_RESTRICTED = "ACCESS_RESTRICTED"
ACCESS_DENIED = "ACCESS_DENIED"
ACCOUNT_BLOCKED = "ACCOUNT_BLOCKED"

# ---------------------------------------------------------------------------
# VIDEO ERRORS
# ---------------------------------------------------------------------------
VIDEO_NOT_FOUND = "VIDEO_NOT_FOUND"
VIDEO_UNAVAILABLE = "VIDEO_UNAVAILABLE"
VIDEO_PRIVATE = "VIDEO_PRIVATE"
VIDEO_REGIONLOCKED = "VIDEO_REGIONLOCKED"

# ---------------------------------------------------------------------------
# API RESPONSE ERRORS
# ---------------------------------------------------------------------------
API_RESPONSE_ERROR = "API_RESPONSE_ERROR"
JSON_DECODE_ERROR = "JSON_DECODE_ERROR"
API_SCHEMA_CHANGED = "API_SCHEMA_CHANGED"

# ---------------------------------------------------------------------------
# COMMENT DATA ERRORS
# ---------------------------------------------------------------------------
INVALID_COMMENT_DATA = "INVALID_COMMENT_DATA"
MISSING_COMMENT_ID = "MISSING_COMMENT_ID"
DUPLICATE_COMMENT = "DUPLICATE_COMMENT"

# ---------------------------------------------------------------------------
# PAGINATION ERRORS
# ---------------------------------------------------------------------------
PAGINATION_ERROR = "PAGINATION_ERROR"
CURSOR_STALLED = "CURSOR_STALLED"
CURSOR_CYCLE = "CURSOR_CYCLE"
EMPTY_PAGE_WITH_MORE = "EMPTY_PAGE_WITH_MORE"

# ---------------------------------------------------------------------------
# CACHE/STORAGE ERRORS
# ---------------------------------------------------------------------------
CACHE_CORRUPTION = "CACHE_CORRUPTION"
CACHE_VALIDATION_ERROR = "CACHE_VALIDATION_ERROR"
STORAGE_ERROR = "STORAGE_ERROR"
DISK_SPACE_ERROR = "DISK_SPACE_ERROR"
DATABASE_ERROR = "DATABASE_ERROR"

# ---------------------------------------------------------------------------
# CONCURRENCY ERRORS
# ---------------------------------------------------------------------------
CONCURRENCY_ERROR = "CONCURRENCY_ERROR"
WORKER_CONFLICT = "WORKER_CONFLICT"

# ---------------------------------------------------------------------------
# RETRY ERRORS
# ---------------------------------------------------------------------------
RETRY_EXHAUSTED = "RETRY_EXHAUSTED"
RETRY_BUDGET_EXCEEDED = "RETRY_BUDGET_EXCEEDED"

# ---------------------------------------------------------------------------
# TIMEOUT ERRORS
# ---------------------------------------------------------------------------
TIMEOUT_ERROR = "TIMEOUT_ERROR"

# ---------------------------------------------------------------------------
# INTERNAL ERRORS
# ---------------------------------------------------------------------------
INTERRUPTED = "INTERRUPTED"
UNEXPECTED_INTERNAL_ERROR = "UNEXPECTED_INTERNAL_ERROR"