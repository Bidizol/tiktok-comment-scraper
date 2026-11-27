"""Thread-safe TikTok msToken acquisition with bounded refresh attempts."""
from __future__ import annotations

import email.message
import logging
import random
import threading
import time
from datetime import datetime, timezone
from http.cookies import SimpleCookie
from typing import Optional

from http_transport import HTTPTransport, HTTPTransportError

logger = logging.getLogger(__name__)

MAX_REFRESH_ATTEMPTS = 3
DEFAULT_TOKEN_TTL_SECONDS = 3600
TOKEN_ENDPOINT = "https://www.tiktok.com/api/recommend/item_list/?aid=1988&count=1"
TOKEN_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


class TokenError(Exception):
    def __init__(self, code: str, message: str, *, retryable: bool = False):
        super().__init__(message)
        self.error_code = code
        self.retryable = retryable


class TokenExhaustedError(TokenError):
    pass


class TokenManager:
    """One refresh lock per manager; token lifetime uses monotonic time."""

    def __init__(self, worker_id: int, initial_token: Optional[str] = None,
                 token_ttl_seconds: int = DEFAULT_TOKEN_TTL_SECONDS,
                 transport: Optional[HTTPTransport] = None,
                 max_refresh_attempts: int = MAX_REFRESH_ATTEMPTS,
                 refresh_endpoint: str = TOKEN_ENDPOINT):
        if isinstance(token_ttl_seconds, bool) or token_ttl_seconds < 1:
            raise ValueError("token_ttl_seconds must be a positive integer")
        if isinstance(max_refresh_attempts, bool) or max_refresh_attempts < 1:
            raise ValueError("max_refresh_attempts must be a positive integer")
        self.worker_id = worker_id
        self._token = initial_token
        self._issued_monotonic = time.monotonic() if initial_token else None
        self._ttl = token_ttl_seconds
        self._max_attempts = max_refresh_attempts
        self._endpoint = refresh_endpoint
        self._transport = transport or HTTPTransport()
        self._lock = threading.RLock()

    @property
    def token(self) -> Optional[str]:
        with self._lock:
            if not self._token or self._issued_monotonic is None:
                return None
            if time.monotonic() - self._issued_monotonic >= self._ttl:
                return None
            return self._token

    def get_token(self, force_refresh: bool = False) -> str:
        with self._lock:
            if not force_refresh:
                valid = self.token
                if valid:
                    return valid
            return self._refresh_token()

    def _refresh_token(self) -> str:
        last_error: Exception | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                response = self._transport.request_detailed(
                    "GET", self._endpoint,
                    headers={"User-Agent": TOKEN_USER_AGENT,
                             "Accept": "application/json, text/plain, */*"},
                    expect_json=False,
                )
                token = self._extract_token(response.header_values("Set-Cookie"),
                                            response.json_data)
                if not token:
                    raise TokenError("TOKEN_REFRESH_ERROR",
                        "Token endpoint did not provide an msToken", retryable=True)
                self._token = token
                self._issued_monotonic = time.monotonic()
                logger.info("Token acquired for worker %s", self.worker_id)
                return token
            except HTTPTransportError as error:
                last_error = TokenError(error.error_code, str(error), retryable=error.retryable)
                if not error.retryable:
                    raise last_error from None
                if error.retry_after_seconds is not None:
                    delay = error.retry_after_seconds
                else:
                    ceiling = min(8.0, 1.0 * (2 ** (attempt - 1)))
                    delay = random.uniform(0.0, ceiling)
            except TokenError as error:
                last_error = error
                if not error.retryable:
                    raise
                ceiling = min(8.0, 1.0 * (2 ** (attempt - 1)))
                delay = random.uniform(0.0, ceiling)
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as error:
                # Do not preserve arbitrary exception text; it may contain secrets.
                last_error = TokenError("TOKEN_REFRESH_ERROR",
                    f"Unexpected token acquisition failure ({type(error).__name__})",
                    retryable=False)
                raise last_error from None

            logger.warning("Token acquisition attempt %d/%d failed for worker %s",
                           attempt, self._max_attempts, self.worker_id)
            if attempt < self._max_attempts:
                time.sleep(delay)

        code = getattr(last_error, "error_code", "TOKEN_REFRESH_ERROR")
        raise TokenExhaustedError(code,
            f"Token acquisition exhausted after {self._max_attempts} bounded attempts",
            retryable=False) from None

    @staticmethod
    def _extract_token(set_cookie_headers: list[str],
                       payload: dict | None) -> str | None:
        cookie = SimpleCookie()
        for header in set_cookie_headers:
            try:
                cookie.load(header)
            except Exception:
                continue
        morsel = cookie.get("msToken")
        if morsel and morsel.value:
            return morsel.value
        if isinstance(payload, dict):
            for key in ("msToken", "token", "unique_id"):
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
        return None

    def reset_refresh_attempts(self) -> None:
        """Retained for compatibility; each refresh invocation has its own budget."""
        return None
