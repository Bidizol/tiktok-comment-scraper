"""Strict proxy configuration and credential-safe URL construction."""
from __future__ import annotations

import ipaddress
import logging
import re
from typing import Optional, Tuple
from urllib.parse import quote, urlsplit

logger = logging.getLogger(__name__)


class ProxyError(Exception):
    pass


class ProxyAuthenticationError(ProxyError):
    pass


class ProxyConnectionError(ProxyError):
    pass


class ProxyManager:
    """Validate proxy settings and never silently substitute a direct route."""

    def __init__(self, proxy_host: str, proxy_port: str,
                 proxy_user: Optional[str] = None,
                 proxy_pass: Optional[str] = None):
        self.proxy_host = (proxy_host or "").strip()
        self.proxy_port = str(proxy_port or "").strip()
        self.proxy_user = proxy_user
        self.proxy_pass = proxy_pass
        self._validated = False
        self._url: str | None = None
        self._redacted_url: str | None = None

    @staticmethod
    def _valid_host(host: str) -> bool:
        candidate = host.strip()
        if not candidate or any(ch in candidate for ch in "/@?#"):
            return False
        try:
            ipaddress.ip_address(candidate.strip("[]"))
            return True
        except ValueError:
            if ":" in candidate:
                return False
            try:
                ascii_host = candidate.encode("idna").decode("ascii")
            except UnicodeError:
                return False
            if len(ascii_host) > 253:
                return False
            labels = ascii_host.rstrip(".").split(".")
            return all(
                1 <= len(label) <= 63
                and re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?", label)
                for label in labels
            )

    def validate(self) -> Tuple[bool, Optional[str]]:
        has_host, has_port = bool(self.proxy_host), bool(self.proxy_port)
        if has_host != has_port:
            return False, "PROXY_HOST and PROXY_PORT must both be set or both be unset"
        if not has_host:
            if self.proxy_user or self.proxy_pass:
                return False, "Proxy credentials were supplied without a proxy endpoint"
            self._url, self._redacted_url = None, "disabled"
            self._validated = True
            return True, None
        if not self._valid_host(self.proxy_host):
            return False, "Proxy host is not a valid hostname or IP address"
        try:
            port = int(self.proxy_port)
        except ValueError:
            return False, "Proxy port must be an integer"
        if not 1 <= port <= 65535:
            return False, "Proxy port must be between 1 and 65535"
        if bool(self.proxy_user) != bool(self.proxy_pass):
            return False, "Both proxy username and password must be supplied together"
        if self.proxy_user is not None and not self.proxy_user.strip():
            return False, "Proxy username must not be blank"
        if self.proxy_pass is not None and not self.proxy_pass.strip():
            return False, "Proxy password must not be blank"
        host = f"[{self.proxy_host.strip('[]')}]" if ":" in self.proxy_host else self.proxy_host
        credentials = ""
        if self.proxy_user is not None:
            credentials = quote(self.proxy_user, safe="") + ":" + quote(self.proxy_pass or "", safe="") + "@"
        self._url = f"http://{credentials}{host}:{port}"
        self._redacted_url = f"http://[REDACTED]@{host}:{port}" if credentials else f"http://{host}:{port}"
        self._validated = True
        return True, None

    @property
    def proxy_url(self) -> Optional[str]:
        if not self._validated:
            valid, error = self.validate()
            if not valid:
                raise ValueError(error or "Invalid proxy configuration")
        return self._url

    @property
    def redacted_url(self) -> str:
        if not self._validated:
            valid, error = self.validate()
            if not valid:
                raise ValueError(error or "Invalid proxy configuration")
        return self._redacted_url or "disabled"

    def classify_error(self, error: Exception):
        from errors.classifiers import ErrorClassification
        from errors.model import ErrorOutcome, ErrorSeverity
        status = getattr(error, "http_status", None)
        code = getattr(error, "error_code", "")
        if status == 407 or code == "PROXY_AUTHENTICATION_ERROR":
            return ErrorClassification("PROXY_AUTHENTICATION_ERROR", ErrorSeverity.ERROR,
                "Proxy authentication was rejected", ErrorOutcome.FAILED_PERMANENT, 407)
        if code == "DNS_ERROR":
            return ErrorClassification("DNS_ERROR", ErrorSeverity.WARNING,
                "Proxy host name could not be resolved", ErrorOutcome.FAILED_RESUMABLE, 0)
        return ErrorClassification("PROXY_ERROR", ErrorSeverity.WARNING,
            "Proxy connection failed", ErrorOutcome.FAILED_RESUMABLE, status or 0)

    def mask_credentials(self, text: str) -> str:
        return re.sub(r"(?i)(https?://)[^/@\s]+@", r"\1[REDACTED]@", str(text))
