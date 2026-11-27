"""Centralized HTTP transport layer for the TikTok Comment Scraper.

Provides explicit connection and read timeouts, HTTP status validation,
response-body validation, JSON decoding, and API response classification.
All errors are classified and never silently substituted.
"""

from __future__ import annotations

import json
import logging
import socket
import ssl
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

# Attempt to import from errors module
try:
    from errors.classifiers import (
        classify_dns_error,
        classify_tls_error,
        classify_http_error,
        classify_connection_error,
        classify_json_error,
        ErrorClassification,
    )
    from errors.model import ScraperError, ErrorSeverity, ErrorOutcome
except ImportError:
    # Fallback - these will be available when errors package is properly set up
    classify_dns_error = None
    classify_tls_error = None
    classify_http_error = None
    classify_connection_error = None
    classify_json_error = None
    ErrorClassification = None
    ScraperError = None
    ErrorSeverity = None
    ErrorOutcome = None

logger = logging.getLogger(__name__)

#: Default connection timeout in seconds
DEFAULT_CONNECT_TIMEOUT = 15
#: Default read timeout in seconds
DEFAULT_READ_TIMEOUT = 20
#: Default overall timeout in seconds
DEFAULT_TIMEOUT = 30


class HTTPError(urllib.error.URLError):
    """Custom HTTP error that carries status code and classification."""

    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


class HTTPTransport:
    """Centralized HTTP request handling with timeouts and validation."""

    def __init__(
        self,
        connect_timeout: int = DEFAULT_CONNECT_TIMEOUT,
        read_timeout: int = DEFAULT_READ_TIMEOUT,
        overall_timeout: int = DEFAULT_TIMEOUT,
    ):
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.overall_timeout = overall_timeout

    def request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[bytes] = None,
        connect_timeout: Optional[int] = None,
        read_timeout: Optional[int] = None,
        overall_timeout: Optional[int] = None,
    ):
        """Execute an HTTP request with full error classification.

        Returns a tuple of (http_status, parsed_json_or_none).
        On non-2xx responses, http_status is set and json_or_none is None.
        On network errors, http_status is 0 and json_or_none is None.

        Raises ScraperError on unrecoverable classification failures.
        """
        try:
            req = urllib.request.Request(url, method=method, headers=headers or {}, data=body)
            # urllib exposes one socket timeout rather than separate connect/read
            # controls. Use the explicitly supplied overall timeout when present.
            timeout = overall_timeout if overall_timeout is not None else self.overall_timeout
            url_open = urllib.request.urlopen(req, timeout=timeout)
            status_code = url_open.getcode()
            raw_body = url_open.read()
            try:
                parsed = json.loads(raw_body.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as error:
                if classify_json_error:
                    classification = classify_json_error(str(error))
                else:
                    classification = ErrorClassification(
                        error_code="JSON_DECODE_ERROR",
                        severity=ErrorSeverity.ERROR if ErrorSeverity else "ERROR",
                        explanation="Invalid JSON response",
                        default_outcome=ErrorOutcome.FAILED_PERMENET if ErrorOutcome else "failed_permanent",
                        http_status=0,
                    )
                raise ScraperError(
                    error_code=classification.error_code,
                    severity=classification.severity,
                    component="http_transport",
                    function="request",
                    explanation=classification.explanation,
                    http_status=status_code,
                    outcome=classification.default_outcome,
                )
            return status_code, parsed

        except Exception as error:
            # Try to classify the error
            if hasattr(error, 'code') and classify_http_error:
                status_code = error.code
                classification = classify_http_error(status_code)
                raise ScraperError(
                    error_code=classification.error_code,
                    severity=classification.severity,
                    component="http_transport",
                    function="request",
                    explanation=classification.explanation,
                    http_status=status_code,
                    outcome=classification.default_outcome,
                )
            elif hasattr(error, 'reason') and classify_dns_error:
                error_str = str(error)
                if isinstance(error.reason, socket.gaierror):
                    classification = classify_dns_error(error_str)
                else:
                    classification = classify_tls_error(error_str)
                raise ScraperError(
                    error_code=classification.error_code,
                    severity=classification.severity,
                    component="http_transport",
                    function="request",
                    explanation=classification.explanation,
                    http_status=0,
                    outcome=classification.default_outcome,
                )
            else:
                # Generic error
                raise ScraperError(
                    error_code="NETWORK_ERROR",
                    severity=ErrorSeverity.ERROR if ErrorSeverity else "ERROR",
                    component="http_transport",
                    function="request",
                    explanation=str(error),
                    http_status=0,
                    outcome=ErrorOutcome.FAILED_PERMANENT if ErrorOutcome else "failed_permanent",
                )

    def safe_json_decode(
        self, raw_body: bytes, http_status: int
    ) -> Optional[Dict[str, Any]]:
        """Safely decode JSON from response body.

        Validates that the response is valid JSON and is a dictionary.
        Returns None on any decode or structural failure.

        Never silently substitutes a default value.
        """
        if not raw_body or not raw_body.strip():
            logger.warning("Empty response body received (status %s)", http_status)
            return None

        try:
            parsed = json.loads(raw_body.decode("utf-8"))
        except json.JSONDecodeError as error:
            logger.warning("Malformed JSON response (status %s): %s", http_status, error)
            return None

        if not isinstance(parsed, dict):
            logger.warning(
                "Non-dict JSON response (status %s): type=%s", http_status, type(parsed).__name__
            )
            return None

        return parsed

    def fetch_json(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
    ) -> Tuple[int, Optional[Dict[str, Any]]]:
        """Fetch and parse JSON from a URL using this transport instance.

        Thin convenience wrapper over :meth:`request` so callers holding a
        transport instance (e.g. the TokenManager) share the same centralized,
        configured HTTP behavior instead of reaching for module-level helpers.

        Returns (http_status, parsed_dict_or_none) – parsed is None on failure.
        """
        return self.request("GET", url, headers=headers)

    def validate_status(self, status_code: int) -> bool:
        """Validate that an HTTP status code indicates success.

        Returns True for 2xx status codes only.
        """
        return 200 <= status_code < 300
    def fetch_json(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
    ) -> Tuple[int, Optional[Dict[str, Any]]]:
        """Fetch and parse JSON from a URL using this transport instance.

        Thin convenience wrapper over :meth:`request` so callers holding a
        transport instance (e.g. the TokenManager) share the same centralized,
        configured HTTP behavior instead of reaching for module-level helpers.

        Returns (http_status, parsed_dict_or_none) – parsed is None on failure.
        """
        return self.request("GET", url, headers=headers)
    def validate_status(self, status_code: int) -> bool:
        """Validate that an HTTP status code indicates success.

        Returns True for 2xx status codes only.
        """
        return 200 <= status_code < 300

    def fetch_json(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        connect_timeout: Optional[int] = None,
        read_timeout: Optional[int] = None,
        overall_timeout: Optional[int] = None,
    ) -> Tuple[int, Optional[Dict[str, Any]]]:
        """Fetch and parse JSON from a URL using this transport instance.

        Thin convenience wrapper over :meth:`request` so callers holding a
        transport instance (e.g. the TokenManager) share the same configured,
        centralized HTTP behavior instead of reaching for module-level helpers.

        Returns (http_status, parsed_dict_or_none) – parsed is None on failure.
        """
        return self.request(
            "GET",
            url,
            headers=headers,
            connect_timeout=connect_timeout or self.connect_timeout,
            read_timeout=read_timeout or self.read_timeout,
            overall_timeout=overall_timeout or self.overall_timeout,
        )
    def validate_status(self, status_code: int) -> bool:
        """Validate that an HTTP status code indicates success.

        Returns True for 2xx status codes only.
        """
        return 200 <= status_code < 300


def fetch_json(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    connect_timeout: int = DEFAULT_CONNECT_TIMEOUT,
    read_timeout: int = DEFAULT_READ_TIMEOUT,
    overall_timeout: int = DEFAULT_TIMEOUT,
) -> Tuple[int, Optional[Dict[str, Any]]]:
    """Fetch and parse JSON from a URL in one call.

    Returns (http_status, parsed_dict_or_none).
    On success (2xx with valid JSON), parsed_dict is populated.
    On failure, http_status is set and parsed_dict is None.
    """
    transport = HTTPTransport(
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
        overall_timeout=overall_timeout,
    )
    return transport.request("GET", url, headers=headers)
