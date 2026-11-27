"""Centralized token acquisition and refresh management.

Provides token expiry handling, bounded refresh attempts,
clear distinction between authentication failures, network failures
and API errors. No token values or token prefixes in logs.

Uses centralized HTTP transport for token acquisition.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

# Import error taxonomy - use try/except for compatibility with
# Phase 1's relative-import issues in errors/classifiers.py
try:
    from errors.classifiers import ErrorClassification
    from errors.model import ScraperError, ErrorSeverity, ErrorOutcome
except ImportError:
    # Fallback definitions for when errors module's relative imports fail
    class ErrorClassification:
        def __init__(self, error_code, severity, explanation, default_outcome, http_status):
            self.error_code = error_code
            self.severity = severity
            self.explanation = explanation
            self.default_outcome = default_outcome
            self.http_status = http_status

    class ScraperError(Exception):
        def __init__(self, error_code, severity, component, function,
                     explanation="", http_status=None, outcome="failed_permanent",
                     video_id=None, run_id=None, worker_id=None,
                     batch_number=None, cursor=None, retry_attempt=None,
                     recovery_action=None):
            self.error_code = error_code
            self.severity = severity
            self.component = component
            self.function = function
            self.explanation = explanation
            self.http_status = http_status
            self.outcome = outcome
            self.video_id = video_id
            self.run_id = run_id
            self.worker_id = worker_id
            self.batch_number = batch_number
            self.cursor = cursor
            self.retry_attempt = retry_attempt
            self.recovery_action = recovery_action

    class ErrorSeverity:
        DEBUG = "DEBUG"
        INFO = "INFO"
        WARNING = "WARNING"
        ERROR = "ERROR"
        CRITICAL = "CRITICAL"

    class ErrorOutcome:
        SUCCESS = "success"
        FAILED_RESUMABLE = "failed_resumable"
        FAILED_PERMANENT = "failed_permanent"
        RETRY_EXHAUSTED = "retry_exhausted"
        INTERRUPTED = "interrupted"

#: Maximum number of bounded refresh attempts before giving up
MAX_REFRESH_ATTEMPTS = 3
#: Default delay between refresh attempts in seconds
DEFAULT_REFRESH_DELAY = 2
#: Token cache TTL - how long a token is considered valid
DEFAULT_TOKEN_TTL_SECONDS = 3600  # 1 hour

# HTTP transport for token acquisition - will be set when package loads
TOKEN_TRANSPORT = None

logger = logging.getLogger(__name__)


class TokenError(Exception):
    """Base class for token-related errors."""

    pass


class TokenExhaustedError(TokenError):
    """Raised when token refresh attempts are exhausted."""

    pass


class TokenManager:
    """Centralized token acquisition and refresh management.

    Handles:
    - Token storage with expiry tracking
    - Bounded refresh attempts
    - Classification of refresh failures (auth vs network vs API)
    - No token values logged or reported
    - Thread-safe shared token refresh where workers share a token
    - Real token acquisition via centralized HTTP transport
    """

    def __init__(
        self,
        worker_id: int,
        initial_token: Optional[str] = None,
        token_ttl_seconds: int = DEFAULT_TOKEN_TTL_SECONDS,
        transport: Optional[object] = None,
    ):
        self.worker_id = worker_id
        self._token: Optional[str] = initial_token
        self._token_issue_time: Optional[datetime] = (
            datetime.now(timezone.utc) if initial_token else None
        )
        self._token_ttl_seconds = token_ttl_seconds
        self._refresh_attempts = 0
        self._max_refresh_attempts = MAX_REFRESH_ATTEMPTS
        self._lock = __import__("threading").Lock()
        self._transport = transport

    @property
    def token(self) -> Optional[str]:
        """Return the current token if valid, otherwise None."""
        if self._token is None:
            return None
        # Check if token is still within TTL
        elapsed = (datetime.now(timezone.utc) - self._token_issue_time).total_seconds()
        if elapsed > self._token_ttl_seconds:
            logger.debug("Token expired for worker %s (elapsed: %.1fs)", self.worker_id, elapsed)
            return None
        return self._token

    def get_token(self, force_refresh: bool = False) -> Optional[str]:
        """Get a valid token, refreshing if needed.

        Returns the token string if available, or None if exhausted.
        No token values or prefixes are ever logged.
        Thread-safe: uses self._lock.
        """
        with self._lock:
            needs_refresh = (
                self.token is None
                or force_refresh
                or self._should_refresh()
            )

            if needs_refresh:
                return self._refresh_token()
            return self._token

    def _should_refresh(self) -> bool:
        """Determine if a refresh is needed based on attempt counters."""
        return self._refresh_attempts >= self._max_refresh_attempts

    def _refresh_token(self) -> Optional[str]:
        """Acquire a token with a finite retry budget.

        Permanent authentication failures stop immediately. Transient
        acquisition failures use bounded exponential backoff. Token values
        are never included in logs or exception messages.
        """
        last_error: Optional[Exception] = None

        while self._refresh_attempts < self._max_refresh_attempts:
            self._refresh_attempts += 1
            attempt = self._refresh_attempts
            logger.debug(
                "Token refresh attempt %d/%d for worker %s",
                attempt,
                self._max_refresh_attempts,
                self.worker_id,
            )

            try:
                new_token = self._acquire_token()
                if new_token:
                    self._token = new_token
                    self._token_issue_time = datetime.now(timezone.utc)
                    self._refresh_attempts = 0
                    logger.info("Token refreshed successfully for worker %s", self.worker_id)
                    return new_token
                last_error = TokenError("Token acquisition returned no token")
            except TokenExhaustedError:
                raise
            except Exception as error:
                last_error = error
                outcome = getattr(error, "outcome", None)
                permanent_values = (
                    "failed_permanent",
                    getattr(ErrorOutcome, "FAILED_PERMANENT", "failed_permanent"),
                )
                if outcome in permanent_values:
                    raise

            if self._refresh_attempts < self._max_refresh_attempts:
                delay = min(2 ** (attempt - 1), 8)
                logger.warning(
                    "Token refresh failed for worker %s; retrying within bounded budget",
                    self.worker_id,
                )
                time.sleep(delay)

        logger.warning(
            "Token refresh exhausted %d attempts for worker %s",
            self._max_refresh_attempts,
            self.worker_id,
        )
        raise TokenExhaustedError(
            f"Token refresh exhausted after {self._max_refresh_attempts} attempts"
        ) from last_error

    def _acquire_token(self) -> Optional[str]:
        """Acquire a fresh token from the TikTok API using centralized HTTP transport.

        Uses fetch_json to call the TikTok token endpoint.
        Implements explicit timeouts and proper HTTP and API response validation.
        Implements token expiry handling.
        Implements bounded refresh attempts.
        Never returns token values in logs - only success/failure status.

        The TikTok msToken acquisition endpoint pattern:
        GET https://www.tiktok.com/api/recommend/item_list/?aid=1988&count=1
        Returns cookies containing msToken.

        Subclasses or integrations should provide the appropriate endpoint URL.
        """
        # Use the real HTTP transport if available, otherwise raise clear error
        if self._transport is None:
            raise RuntimeError(
                "HTTPTransport not configured for TokenManager. "
                "Configure a transport instance when creating TokenManager."
            )

        try:
            # Call the TikTok API endpoint for token acquisition
            url = "https://www.tiktok.com/api/recommend/item_list/?aid=1988&count=1"
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

            # Use the transport with explicit timeouts
            status, response = self._transport.fetch_json(
                url,
                headers=headers,
                connect_timeout=15,
                read_timeout=20,
                overall_timeout=30,
            )

            if status != 200:
                # Non-2xx response - classify the error
                classification = self._classify_refresh_error(
                    Exception(f"HTTP {status} response from TikTok token endpoint"),
                    self._refresh_attempts,
                )
                raise ScraperError(
                    timestamp_utc=datetime.now(timezone.utc).isoformat(),
                    error_code=classification.error_code,
                    severity=classification.severity,
                    component="token_manager",
                    function="_acquire_token",
                    explanation=classification.explanation,
                    http_status=classification.http_status,
                    outcome=classification.default_outcome,
                )

            # A token response may be cookie-only, so a missing/non-dict
            # JSON body is acceptable until both cookie and JSON extraction
            # have been attempted.
            if not isinstance(response, dict):
                response = {}

            # TikTok commonly issues msToken as a response cookie rather
            # than a JSON property. Prefer that cookie, with JSON fields as
            # compatibility fallbacks for alternate endpoint responses.
            ms_token = None
            response_headers = getattr(self._transport, "last_response_headers", None)
            if response_headers is not None:
                from http.cookies import SimpleCookie

                cookie_headers = (
                    response_headers.get_all("Set-Cookie", [])
                    if hasattr(response_headers, "get_all")
                    else []
                )
                parsed_cookies = SimpleCookie()
                for cookie_header in cookie_headers:
                    parsed_cookies.load(cookie_header)
                if "msToken" in parsed_cookies:
                    ms_token = parsed_cookies["msToken"].value

            if not ms_token:
                ms_token = (
                    response.get("msToken")
                    or response.get("token")
                    or response.get("unique_id")
                )

            if ms_token:
                # Return the token string - caller is responsible for extraction
                # No token values logged - only success/failure status
                return ms_token
            else:
                # No token found in response - token acquisition failed
                logger.warning("No msToken found in TikTok token endpoint response (status %s)", status)
                # Classify as authentication/API error
                classification = self._classify_refresh_error(
                    Exception("No msToken in TikTok token endpoint response"),
                    self._refresh_attempts,
                )
                raise ScraperError(
                    timestamp_utc=datetime.now(timezone.utc).isoformat(),
                    error_code=classification.error_code,
                    severity=classification.severity,
                    component="token_manager",
                    function="_acquire_token",
                    explanation=classification.explanation,
                    http_status=classification.http_status,
                    outcome=classification.default_outcome,
                )

        except ScraperError:
            raise
        except Exception as error:
            # Classify without exposing token data
            classification = self._classify_refresh_error(error, 1)
            raise ScraperError(
                timestamp_utc=datetime.now(timezone.utc).isoformat(),
                error_code=classification.error_code,
                severity=classification.severity,
                component="token_manager",
                function="_acquire_token",
                explanation=classification.explanation,
                http_status=classification.http_status,
                outcome=classification.default_outcome,
            )

    def _classify_refresh_error(
        self, error: Exception, attempt: int
    ) -> ErrorClassification:
        """Classify a token refresh error without exposing sensitive data.

        Determines whether the error is authentication, network, or API-related.
        """
        error_str = str(error).lower()

        # Check for authentication-related indicators
        auth_indicators = ["unauthorized", "invalid", "authentication", "forbidden", "401", "403"]
        # Check for network-related indicators
        network_indicators = ["timeout", "connection", "dns", "tls", "network", "refused"]
        # Check for API/rate-limit indicators
        api_indicators = ["rate limit", "429", "quota", "too many requests"]

        has_auth = any(indicator in error_str for indicator in auth_indicators)
        has_network = any(indicator in error_str for indicator in network_indicators)
        has_api = any(indicator in error_str for indicator in api_indicators)

        if has_auth:
            return ErrorClassification(
                error_code="AUTHENTICATION_ERROR",
                severity=ErrorSeverity.ERROR,
                explanation="Token refresh authentication failed - invalid or revoked token",
                default_outcome="failed_permanent",
                http_status=401,
            )
        if has_api:
            return ErrorClassification(
                error_code="RATE_LIMITED",
                severity=ErrorSeverity.WARNING,
                explanation="Token refresh rate-limited by API",
                default_outcome="failed_resumable",
                http_status=429,
            )
        if has_network:
            return ErrorClassification(
                error_code="NETWORK_ERROR",
                severity=ErrorSeverity.WARNING,
                explanation="Token refresh network failure",
                default_outcome="failed_resumable",
                http_status=0,
            )

        # Default classification
        return ErrorClassification(
            error_code="TOKEN_REFRESH_ERROR",
            severity=ErrorSeverity.WARNING,
            explanation="Token refresh failed with unknown error",
            default_outcome="failed_resumable",
            http_status=0,
        )

    def reset_refresh_attempts(self) -> None:
        """Reset the refresh attempt counter (call after successful token acquisition)."""
        with self._lock:
            self._refresh_attempts = 0
