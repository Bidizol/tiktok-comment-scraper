"""Bounded, classified HTTP transport used by the scraper runtime.

The legacy request()/fetch_json() tuple API is retained for existing callers.
New integrations should use request_detailed() to retain failure classification
and response headers without shared mutable response state.
"""
from __future__ import annotations

import email.message
import http.client
import http.cookiejar
import ipaddress
import json
import logging
import math
import socket
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Optional

logger = logging.getLogger(__name__)

DEFAULT_CONNECT_TIMEOUT = 15.0
DEFAULT_READ_TIMEOUT = 20.0
DEFAULT_TIMEOUT = 30.0
DEFAULT_MAX_RESPONSE_BYTES = 5 * 1024 * 1024
_READ_CHUNK_BYTES = 64 * 1024


class HTTPTransportError(Exception):
    """Sanitized transport failure; never includes URL, headers or body."""

    def __init__(self, code: str, message: str, *, retryable: bool,
                 http_status: int | None = None, retry_after: float | None = None):
        super().__init__(message)
        self.error_code = code
        self.retryable = retryable
        self.http_status = http_status
        self.retry_after_seconds = retry_after


@dataclass(frozen=True)
class HTTPResponse:
    status_code: int
    headers: email.message.Message
    body: bytes
    json_data: dict[str, Any] | None
    received_at_utc: str

    def header_values(self, name: str) -> list[str]:
        return list(self.headers.get_all(name, []))


def _positive_timeout(value: float, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a positive finite number")
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise ValueError(f"{name} must be a positive finite number")
    return result


def _retry_after(headers: email.message.Message) -> float | None:
    raw = headers.get("Retry-After")
    if not raw:
        return None
    try:
        return max(0.0, min(float(raw.strip()), 300.0))
    except (TypeError, ValueError):
        try:
            parsed = email.utils.parsedate_to_datetime(raw)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return max(0.0, min((parsed - datetime.now(timezone.utc)).total_seconds(), 300.0))
        except (TypeError, ValueError, OverflowError):
            return None


def _validate_url(url: str) -> urllib.parse.SplitResult:
    if not isinstance(url, str) or not url or any(ch in url for ch in "\r\n\t "):
        raise ValueError("Request URL must be a non-empty absolute HTTP(S) URL")
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
        raise ValueError("Request URL must use HTTP or HTTPS and include a host")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("Credentials must not be embedded in request URLs")
    try:
        _ = parsed.port
    except ValueError as exc:
        raise ValueError("Request URL contains an invalid port") from exc
    if parsed.fragment:
        raise ValueError("Request URL must not contain a fragment")
    return parsed


def _classify_exception(error: BaseException, proxy_enabled: bool) -> tuple[str, bool]:
    if isinstance(error, ssl.SSLCertVerificationError):
        return "TLS_ERROR", False
    if isinstance(error, ssl.SSLError):
        return "TLS_ERROR", False
    if isinstance(error, socket.gaierror):
        return ("PROXY_ERROR" if proxy_enabled else "DNS_ERROR"), True
    if isinstance(error, (socket.timeout, TimeoutError)):
        return ("PROXY_ERROR" if proxy_enabled else "TIMEOUT_ERROR"), True
    if isinstance(error, ConnectionError):
        return ("PROXY_ERROR" if proxy_enabled else "NETWORK_ERROR"), True
    if isinstance(error, urllib.error.URLError):
        reason = getattr(error, "reason", None)
        if isinstance(reason, ssl.SSLError):
            return "TLS_ERROR", False
        if isinstance(reason, socket.gaierror):
            return ("PROXY_ERROR" if proxy_enabled else "DNS_ERROR"), True
        if isinstance(reason, (socket.timeout, TimeoutError)):
            return ("PROXY_ERROR" if proxy_enabled else "TIMEOUT_ERROR"), True
        return ("PROXY_ERROR" if proxy_enabled else "NETWORK_ERROR"), True
    if isinstance(error, OSError):
        return ("PROXY_ERROR" if proxy_enabled else "NETWORK_ERROR"), True
    return "UNEXPECTED_INTERNAL_ERROR", False


class _TimeoutHTTPConnection(http.client.HTTPConnection):
    def __init__(self, host, connect_timeout, read_timeout, **kwargs):
        super().__init__(host, timeout=connect_timeout)
        self._read_timeout = read_timeout

    def connect(self):
        super().connect()
        if self.sock is not None:
            self.sock.settimeout(self._read_timeout)


class _TimeoutHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, host, connect_timeout, read_timeout, context=None,
                 check_hostname=None, **kwargs):
        super().__init__(host, timeout=connect_timeout, context=context,
                         check_hostname=check_hostname)
        self._read_timeout = read_timeout

    def connect(self):
        super().connect()
        if self.sock is not None:
            self.sock.settimeout(self._read_timeout)


class _TimeoutHTTPHandler(urllib.request.HTTPHandler):
    def __init__(self, connect_timeout, read_timeout):
        super().__init__()
        self.connect_timeout, self.read_timeout = connect_timeout, read_timeout

    def http_open(self, request):
        return self.do_open(
            lambda host, **kwargs: _TimeoutHTTPConnection(
                host, self.connect_timeout, self.read_timeout), request)


class _TimeoutHTTPSHandler(urllib.request.HTTPSHandler):
    def __init__(self, connect_timeout, read_timeout):
        super().__init__()
        self.connect_timeout, self.read_timeout = connect_timeout, read_timeout

    def https_open(self, request):
        return self.do_open(
            lambda host, **kwargs: _TimeoutHTTPSConnection(
                host, self.connect_timeout, self.read_timeout,
                context=self._context, check_hostname=self._check_hostname),
            request, context=self._context, check_hostname=self._check_hostname)


class HTTPTransport:
    """Thread-safe transport; each call owns its opener and response metadata."""

    def __init__(self, connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
                 read_timeout: float = DEFAULT_READ_TIMEOUT,
                 overall_timeout: float = DEFAULT_TIMEOUT,
                 proxy_url: str | None = None, *,
                 proxy_required: bool = False,
                 max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES):
        self.connect_timeout = _positive_timeout(connect_timeout, "connect_timeout")
        self.read_timeout = _positive_timeout(read_timeout, "read_timeout")
        self.overall_timeout = _positive_timeout(overall_timeout, "overall_timeout")
        if isinstance(max_response_bytes, bool) or not isinstance(max_response_bytes, int) or max_response_bytes < 1:
            raise ValueError("max_response_bytes must be a positive integer")
        if proxy_required and not proxy_url:
            raise ValueError("A proxy is required but no proxy URL was configured")
        if proxy_url:
            proxy = _validate_url(proxy_url)
            if proxy.scheme not in {"http", "https"}:
                raise ValueError("Proxy URL must use HTTP or HTTPS")
            if not proxy.port:
                raise ValueError("Proxy URL must include an explicit port")
        self.proxy_url = proxy_url
        self.proxy_required = bool(proxy_required)
        self.max_response_bytes = max_response_bytes

    def request_detailed(self, method: str, url: str, *,
                         headers: Mapping[str, str] | None = None,
                         body: bytes | None = None,
                         expect_json: bool = True,
                         connect_timeout: float | None = None,
                         read_timeout: float | None = None,
                         overall_timeout: float | None = None) -> HTTPResponse:
        parsed_url = _validate_url(url)
        method = str(method).upper()
        if not method.isalpha() or len(method) > 16:
            raise ValueError("Invalid HTTP method")
        if body is not None and not isinstance(body, bytes):
            raise TypeError("HTTP request body must be bytes")
        clean_headers: dict[str, str] = {}
        for key, value in (headers or {}).items():
            if not isinstance(key, str) or not isinstance(value, str):
                raise TypeError("HTTP header names and values must be strings")
            if not key or any(c in key + value for c in "\r\n"):
                raise ValueError("HTTP headers must not contain CR/LF or empty names")
            clean_headers[key] = value

        connect_limit = _positive_timeout(connect_timeout or self.connect_timeout, "connect_timeout")
        read_limit = _positive_timeout(read_timeout or self.read_timeout, "read_timeout")
        total_limit = _positive_timeout(overall_timeout or self.overall_timeout, "overall_timeout")
        connect_limit = min(connect_limit, total_limit)
        read_limit = min(read_limit, total_limit)
        request = urllib.request.Request(url, data=body, headers=clean_headers, method=method)
        proxy_map = ({"http": self.proxy_url, "https": self.proxy_url}
                     if self.proxy_url else {})
        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler(proxy_map),
            _TimeoutHTTPHandler(connect_limit, read_limit),
            _TimeoutHTTPSHandler(connect_limit, read_limit),
        )
        started = time.monotonic()
        response = None
        try:
            try:
                response = opener.open(request, timeout=connect_limit)
            except urllib.error.HTTPError as http_error:
                response = http_error
            status = int(response.getcode() or 0)
            response_headers = response.headers
            declared = response_headers.get("Content-Length")
            if declared:
                try:
                    if int(declared) > self.max_response_bytes:
                        raise HTTPTransportError("HTTP_RESPONSE_TOO_LARGE",
                            "HTTP response exceeded the configured size limit",
                            retryable=False, http_status=status)
                except ValueError:
                    raise HTTPTransportError("HTTP_RESPONSE_ERROR",
                        "HTTP response contained an invalid Content-Length",
                        retryable=False, http_status=status)
            chunks: list[bytes] = []
            total = 0
            while True:
                remaining = total_limit - (time.monotonic() - started)
                if remaining <= 0:
                    raise HTTPTransportError("TIMEOUT_ERROR",
                        "HTTP request exceeded its total time budget",
                        retryable=True, http_status=status or None)
                sock = getattr(getattr(getattr(response, "fp", None), "raw", None), "_sock", None)
                if sock is not None:
                    sock.settimeout(min(read_limit, remaining))
                chunk = response.read(min(_READ_CHUNK_BYTES, self.max_response_bytes + 1 - total))
                if not chunk:
                    break
                total += len(chunk)
                if total > self.max_response_bytes:
                    raise HTTPTransportError("HTTP_RESPONSE_TOO_LARGE",
                        "HTTP response exceeded the configured size limit",
                        retryable=False, http_status=status)
                chunks.append(chunk)
            raw_body = b"".join(chunks)
        except HTTPTransportError:
            raise
        except BaseException as error:
            if isinstance(error, (KeyboardInterrupt, SystemExit)):
                raise
            code, retryable = _classify_exception(error, bool(self.proxy_url))
            logger.warning("HTTP transport failure: code=%s exception_type=%s", code, type(error).__name__)
            raise HTTPTransportError(code, "HTTP transport could not complete the request",
                retryable=retryable) from None
        finally:
            if response is not None:
                try:
                    response.close()
                except OSError:
                    pass

        retry_after = _retry_after(response_headers)
        if status == 407:
            raise HTTPTransportError("PROXY_AUTHENTICATION_ERROR",
                "Proxy authentication was rejected", retryable=False, http_status=status)
        if status == 429:
            raise HTTPTransportError("RATE_LIMITED", "Remote service rate-limited the request",
                retryable=True, http_status=status, retry_after=retry_after)
        if status in (408, 425) or 500 <= status <= 599:
            raise HTTPTransportError("HTTP_SERVER_ERROR",
                "Remote server returned a transient HTTP response",
                retryable=True, http_status=status, retry_after=retry_after)
        if status in (401, 403):
            raise HTTPTransportError("ACCESS_RESTRICTED" if status == 403 else "AUTHENTICATION_ERROR",
                "Remote service rejected the request", retryable=False, http_status=status)
        if not 200 <= status < 300:
            code = "HTTP_CLIENT_ERROR" if 400 <= status < 500 else "HTTP_RESPONSE_ERROR"
            raise HTTPTransportError(code, "Remote service returned a non-success HTTP response",
                retryable=False, http_status=status)

        parsed_json = None
        if raw_body:
            try:
                parsed = json.loads(raw_body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                if expect_json:
                    raise HTTPTransportError("JSON_DECODE_ERROR",
                        "Successful HTTP response did not contain valid UTF-8 JSON",
                        retryable=False, http_status=status) from None
            else:
                if isinstance(parsed, dict):
                    parsed_json = parsed
                elif expect_json:
                    raise HTTPTransportError("API_RESPONSE_ERROR",
                        "Successful HTTP response JSON was not an object",
                        retryable=False, http_status=status)
        elif expect_json:
            raise HTTPTransportError("API_RESPONSE_ERROR",
                "Successful HTTP response body was empty", retryable=False, http_status=status)

        return HTTPResponse(status, response_headers, raw_body, parsed_json,
                            datetime.now(timezone.utc).isoformat())

    def request(self, method: str, url: str, headers: dict[str, str] | None = None,
                body: bytes | None = None, connect_timeout: float | None = None,
                read_timeout: float | None = None, overall_timeout: float | None = None):
        """Compatibility tuple API. New code should use request_detailed()."""
        try:
            response = self.request_detailed(method, url, headers=headers, body=body,
                connect_timeout=connect_timeout, read_timeout=read_timeout,
                overall_timeout=overall_timeout)
            return response.status_code, response.json_data
        except HTTPTransportError as error:
            logger.warning("HTTP request ended with %s (status=%s)",
                           error.error_code, error.http_status)
            return error.http_status or 0, None

    def fetch_json(self, url: str, headers: dict[str, str] | None = None,
                   connect_timeout: float | None = None,
                   read_timeout: float | None = None,
                   overall_timeout: float | None = None):
        return self.request("GET", url, headers=headers,
            connect_timeout=connect_timeout, read_timeout=read_timeout,
            overall_timeout=overall_timeout)

    def validate_status(self, status_code: int) -> bool:
        return 200 <= status_code < 300


def fetch_json(url: str, headers: dict[str, str] | None = None,
               connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
               read_timeout: float = DEFAULT_READ_TIMEOUT,
               overall_timeout: float = DEFAULT_TIMEOUT):
    return HTTPTransport(connect_timeout, read_timeout, overall_timeout).fetch_json(url, headers)
