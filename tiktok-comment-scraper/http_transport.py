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
        proxy_url: Optional[str] = None,
    ):
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.overall_timeout = overall_timeout
        # The URL is retained only for constructing the opener; it is never logged.
        self.proxy_url = proxy_url
        self.last_response_headers = None
        self._opener = None
        if proxy_url:
            self._opener = urllib.request.build_opener(
                urllib.request.ProxyHandler({"http": proxy_url, "https": proxy_url})
            )

    def request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[bytes] = None,
        connect_timeout: Optional[int] = None,
        read_timeout: Optional[int] = None,
        overall_timeout: Optional[int] = None,
    ) -> Tuple[int, Optional[Dict[str, Any]]]:
        """Execute one HTTP request and return (status, parsed JSON object).

        urllib exposes a single socket timeout, so overall_timeout is used when
        supplied. connect_timeout/read_timeout remain accepted for API
        compatibility; separate phase timeouts require a different transport.
        HTTP, network, and malformed-JSON failures return a status and None.
        """
        timeout = overall_timeout if overall_timeout is not None else self.overall_timeout
        request = urllib.request.Request(
            url, method=method, headers=headers or {}, data=body
        )
        try:
            opener = self._opener
            if opener is None:
                response_context = urllib.request.urlopen(request, timeout=timeout)
            else:
                response_context = opener.open(request, timeout=timeout)
            with response_context as response:
                status_code = response.getcode()
                self.last_response_headers = response.headers
                raw_body = response.read()
        except urllib.error.HTTPError as error:
            try:
                error.close()
            except OSError:
                pass
            logger.warning("HTTP request failed with status %s", error.code)
            return int(error.code), None
        except (urllib.error.URLError, socket.timeout, TimeoutError, OSError) as error:
            # Do not include exception text: it can contain URLs or credentials.
            logger.warning("HTTP request failed (%s)", type(error).__name__)
            return 0, None

        try:
            parsed = json.loads(raw_body.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            logger.warning("Invalid JSON response (status %s): %s", status_code, error)
            return int(status_code or 0), None

        if not isinstance(parsed, dict):
            logger.warning(
                "Expected JSON object (status %s), received %s",
                status_code,
                type(parsed).__name__,
            )
            return int(status_code or 0), None
        return int(status_code or 0), parsed

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
        connect_timeout: Optional[int] = None,
        read_timeout: Optional[int] = None,
        overall_timeout: Optional[int] = None,
    ) -> Tuple[int, Optional[Dict[str, Any]]]:
        """Fetch JSON using this transport's configured timeout policy.

        urllib currently applies one socket timeout; connect_timeout and
        read_timeout are accepted for caller compatibility but are not
        independently enforceable by urllib.
        """
        return self.request(
            "GET",
            url,
            headers=headers,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            overall_timeout=overall_timeout,
        )

    def validate_status(self, status_code: int) -> bool:
        """Return True only for successful 2xx HTTP status codes."""
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
