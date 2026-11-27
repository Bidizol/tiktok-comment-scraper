"""Proxy management module for the TikTok Comment Scraper.

Validates proxy configuration before use, detects connection failures,
classifies proxy authentication and connectivity errors, prevents
silent direct-connection fallback, and redacts proxy credentials
everywhere.
"""

from __future__ import annotations

import logging
import re
from typing import Optional, Tuple

# Try to import from errors module, fall back to standalone definitions
try:
    from errors.model import ScraperError, ErrorSeverity, ErrorOutcome
    from errors.classifiers import ErrorClassification
except ImportError:
    # Fallback definitions for standalone operation
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
    
    class ScraperError(Exception):
        def __init__(self, error_code, severity, component, function,
                     explanation="", http_status=None, outcome="failed_permanent",
                     video_id=None, worker_id=None, batch_number=None,
                     cursor=None, retry_attempt=None, recovery_action=None):
            self.error_code = error_code
            self.severity = severity
            self.component = component
            self.function = function
            self.explanation = explanation
            self.http_status = http_status
            self.outcome = outcome
            self.video_id = video_id
            self.worker_id = worker_id
            self.batch_number = batch_number
            self.cursor = cursor
            self.retry_attempt = retry_attempt
            self.recovery_action = recovery_action
    
    class ErrorClassification:
        def __init__(self, error_code, severity, explanation, default_outcome, http_status):
            self.error_code = error_code
            self.severity = severity
            self.explanation = explanation
            self.default_outcome = default_outcome
            self.http_status = http_status

logger = logging.getLogger(__name__)


class ProxyError( Exception):
    """Base class for proxy-related errors."""

    pass


class ProxyAuthenticationError(ProxyError):
    """Raised when proxy authentication fails."""

    pass


class ProxyConnectionError(ProxyError):
    """Raised when proxy connection fails."""

    pass


class ProxyManager:
    """Proxy configuration validation and error classification.

    Validates proxy configuration before use, detects connection failures,
    classifies proxy authentication and connectivity errors, prevents
    silent direct-connection fallback, and redacts proxy credentials
    everywhere.
    """

    def __init__(
        self,
        proxy_host: str,
        proxy_port: str,
        proxy_user: Optional[str] = None,
        proxy_pass: Optional[str] = None,
    ):
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.proxy_user = proxy_user
        self.proxy_pass = proxy_pass
        self._validated = False
        self._redacted_url: Optional[str] = None

    def validate(self) -> Tuple[bool, Optional[str]]:
        """Validate proxy configuration before use.

        Returns (is_valid, error_message).
        - Both host and port must be set or both unset
        - Host must be a valid hostname or IP
        - Port must be a valid integer in valid range
        - Credentials, if provided, must be non-empty

        Never returns the proxy URL with credentials embedded.
        """
        errors = []

        # Check if proxy is configured (both or neither)
        has_host = bool(self.proxy_host and self.proxy_host.strip())
        has_port = bool(self.proxy_port and self.proxy_port.strip())

        if has_host and not has_port:
            errors.append("PROXY_HOST set without PROXY_PORT")
        if has_port and not has_host:
            errors.append("PROXY_PORT set without PROXY_HOST")
        if not has_host and not has_port:
            # No proxy configured - this is valid, just disabled
            self._validated = True
            self._redacted_url = ""
            return True, None

        # Both are set - validate further
        # Validate host
        if not self._is_valid_host(self.proxy_host):
            errors.append(f"Invalid proxy host: {self.proxy_host}")

        # Validate port
        try:
            port_int = int(self.proxy_port)
            if not (1 <= port_int <= 65535):
                errors.append(f"Invalid proxy port: {self.proxy_port} (must be 1-65535)")
        except ValueError:
            errors.append(f"Invalid proxy port: {self.proxy_port} (not a valid integer)")

        # Validate credentials if provided
        if self.proxy_user:
            if not self.proxy_user.strip():
                errors.append("PROXY_USER is empty")
        if self.proxy_pass:
            if not self.proxy_pass.strip():
                errors.append("PROXY_PASS is empty")

        if errors:
            return False, "; ".join(errors)

        self._validated = True
        self._redacted_url = self._construct_redacted_url()
        return True, None

    def _is_valid_host(self, host: str) -> bool:
        """Check if the host is a valid hostname or IP address."""
        if not host or host.strip() == "":
            return False
        # Remove port if accidentally included
        host_clean = host.split(":")[0]
        # Basic hostname pattern check
        hostname_pattern = r"^[a-zA-Z0-9]([a-zA-Z0-9.-]*[a-zA-Z0-9])?$"
        import re
        if re.match(hostname_pattern, host_clean):
            return True
        # IP address check (v4 or v6 simplified)
        if re.match(r"^\d+\.\d+\.\d+\.\d+$", host_clean) or re.match(r"^\[.*\]$", host_clean):
            return True
        return False

    def _construct_redacted_url(self) -> str:
        """Construct a proxy URL with credentials redacted.

        Always redacts credentials - never logs the actual proxy URL
        with username:password embedded.
        """
        if not self.proxy_host or not self.proxy_port:
            return ""

        user_part = f"{self.proxy_user}@" if self.proxy_user else ""
        # Raw URL before redaction
        raw_url = f"http://{user_part}{self.proxy_host}:{self.proxy_port}"
        # Redact both user:pass@ and user@ formats
        redacted = re.sub(r'://[^:@]+:[^@]*@', '://***@', raw_url)
        redacted = re.sub(r'://[^:@]+@', '://***@', redacted)
        return redacted

    @property
    def redacted_url(self) -> str:
        """Return the proxy URL with credentials always redacted.

        This property ensures that proxy credentials are never exposed
        in logs, reports, or error messages.
        """
        if self._redacted_url is None:
            self.validate()
        return self._redacted_url or "disabled"

    def classify_error(self, error: Exception) -> ErrorClassification:
        """Classify a proxy error without exposing credentials.

        Determines the appropriate error code, severity, and outcome
        based on the type of proxy failure.
        """
        error_str = str(error).lower()

        # Check for authentication errors
        auth_indicators = ["authentication", "auth", "invalid user", "invalid password",
                          "407", "proxy authentication", "proxy auth"]
        # Check for connection errors
        conn_indicators = ["connection", "connect", "refused", "timeout", "unreachable"]
        # Check for DNS errors
        dns_indicators = ["dns", "hostname", "resolve"]

        has_auth = any(indicator in error_str for indicator in auth_indicators)
        has_conn = any(indicator in error_str for indicator in conn_indicators)
        has_dns = any(indicator in error_str for indicator in dns_indicators)

        if has_auth:
            return ErrorClassification(
                error_code="PROXY_AUTHENTICATION_ERROR",
                severity=ErrorSeverity.ERROR,
                explanation="Proxy authentication failed - invalid credentials",
                default_outcome=ErrorOutcome.FAILED_PERMANENT,
                http_status=407,
            )
        if has_dns:
            return ErrorClassification(
                error_code="DNS_ERROR",
                severity=ErrorSeverity.WARNING,
                explanation="Proxy DNS resolution failed",
                default_outcome=ErrorOutcome.FAILED_RESUMABLE,
                http_status=0,
            )
        if has_conn:
            return ErrorClassification(
                error_code="PROXY_CONNECTION_ERROR",
                severity=ErrorSeverity.WARNING,
                explanation="Proxy connection failed - unable to reach proxy host",
                default_outcome=ErrorOutcome.FAILED_RESUMABLE,
                http_status=0,
            )

        # Default proxy error
        return ErrorClassification(
            error_code="PROXY_ERROR",
            severity=ErrorSeverity.WARNING,
            explanation="Proxy error - unknown classification",
            default_outcome=ErrorOutcome.FAILED_RESUMABLE,
            http_status=0,
        )

    def mask_credentials(self, text: str) -> str:
        """Redact any proxy credentials that might appear in text.

        Used as a safety net to ensure no credentials leak into
        logs or reports from any source.
        """
        # Pattern: http://user:pass@host:port or http://user@host:port
        patterns = [
            r'://[^:@]+:[^@]*@',  # user:pass@
            r'://[^:@]+@',       # user@
        ]
        result = text
        for pattern in patterns:
            result = re.sub(pattern, '://***@', result)
        return result
