"""Bounded, classified HTTP transport used by the scraper runtime.

The legacy request()/fetch_json() tuple API is retained for existing callers.
New integrations should use request_detailed() to retain failure classification
and response headers without shared mutable response state.
"""
from __future__ import annotations
import email.message, http.client, json, logging, math, random, socket, ssl, time
import urllib.error, urllib.parse, urllib.request, re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

logger = logging.getLogger(__name__)
DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT, DEFAULT_TIMEOUT = 15.0, 20.0, 30.0
DEFAULT_MAX_RESPONSE_BYTES = 5 * 1024 * 1024
_READ_CHUNK_BYTES = 64 * 1024

class HTTPTransportError(Exception):
    """Sanitized transport failure; never includes URL, headers or body."""
    def __init__(self, code: str, message: str, *, retryable: bool,
                 http_status: int | None = None, retry_after: float | None = None):
        super().__init__(message)
        self.error_code, self.retryable = code, retryable
        self.http_status, self.retry_after_seconds = http_status, retry_after

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
    value = float(value)
    if not math.isfinite(value) or value <= 0 or value > 600:
        raise ValueError(f"{name} must be finite and between 0 and 600 seconds")
    return value

def _retry_after(headers) -> float | None:
    raw = headers.get("Retry-After")
    if not raw: return None
    try: return max(0.0, min(float(raw.strip()), 300.0))
    except (TypeError, ValueError):
        try:
            import email.utils
            parsed = email.utils.parsedate_to_datetime(raw)
            if parsed.tzinfo is None: parsed = parsed.replace(tzinfo=timezone.utc)
            return max(0.0, min((parsed-datetime.now(timezone.utc)).total_seconds(), 300.0))
        except (TypeError, ValueError, OverflowError): return None

def _validate_url(url: str):
    if not isinstance(url, str) or not url or any(c in url for c in "\r\n\t "):
        raise ValueError("Request URL must be a non-empty absolute HTTP(S) URL")
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
        raise ValueError("Request URL must use HTTP or HTTPS and include a host")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("Credentials must not be embedded in request URLs")
    try: _ = parsed.port
    except ValueError as exc: raise ValueError("Request URL contains an invalid port") from exc
    if parsed.fragment: raise ValueError("Request URL must not contain a fragment")
    return parsed

def _classify_exception(error: BaseException, proxy_enabled: bool):
    if isinstance(error, ssl.SSLError): return "TLS_ERROR", False
    if isinstance(error, socket.gaierror): return "DNS_ERROR", True
    nested_reason = getattr(error, "reason", None)
    if isinstance(nested_reason, ssl.SSLError): return "TLS_ERROR", False
    if isinstance(nested_reason, socket.gaierror): return "DNS_ERROR", True
    if proxy_enabled:
        candidates = (getattr(error, "args", ()), getattr(nested_reason, "args", ()))
        if any(re.search(r"\b407\b", str(part)) for group in candidates for part in group):
            return "PROXY_AUTHENTICATION_ERROR", False
    if isinstance(error, http.client.HTTPException): return "NETWORK_ERROR", True
    if isinstance(error, (socket.timeout, TimeoutError)): return ("PROXY_ERROR" if proxy_enabled else "TIMEOUT_ERROR"), True
    if isinstance(error, ConnectionError): return ("PROXY_ERROR" if proxy_enabled else "NETWORK_ERROR"), True
    if isinstance(error, urllib.error.URLError):
        reason = getattr(error, "reason", None)
        if isinstance(reason, ssl.SSLError): return "TLS_ERROR", False
        if isinstance(reason, socket.gaierror): return ("PROXY_ERROR" if proxy_enabled else "DNS_ERROR"), True
        if isinstance(reason, (socket.timeout, TimeoutError)): return ("PROXY_ERROR" if proxy_enabled else "TIMEOUT_ERROR"), True
        return ("PROXY_ERROR" if proxy_enabled else "NETWORK_ERROR"), True
    if isinstance(error, OSError): return ("PROXY_ERROR" if proxy_enabled else "NETWORK_ERROR"), True
    return "UNEXPECTED_INTERNAL_ERROR", False

class _SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Permit only same-origin redirects; never forward query credentials cross-host."""
    def redirect_request(self, request, fp, code, message, headers, new_url):
        old = urllib.parse.urlsplit(request.full_url)
        new = urllib.parse.urlsplit(new_url)
        old_port = old.port or (443 if old.scheme == "https" else 80)
        new_port = new.port or (443 if new.scheme == "https" else 80)
        same_origin = (
            old.hostname == new.hostname
            and old_port == new_port
            and new.scheme == old.scheme
            and new.username is None
            and new.password is None
        )
        if not same_origin:
            return None
        return super().redirect_request(request, fp, code, message, headers, new_url)


class _TimeoutHTTPConnection(http.client.HTTPConnection):
    def __init__(self, host, connect_timeout, read_timeout, **kwargs):
        super().__init__(host, timeout=connect_timeout); self._read_timeout=read_timeout
    def connect(self):
        super().connect()
        if self.sock is not None: self.sock.settimeout(self._read_timeout)

class _TimeoutHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, host, connect_timeout, read_timeout, context=None, check_hostname=None, **kwargs):
        super().__init__(host, timeout=connect_timeout, context=context, check_hostname=check_hostname)
        self._read_timeout=read_timeout
    def connect(self):
        super().connect()
        if self.sock is not None: self.sock.settimeout(self._read_timeout)

class _TimeoutHTTPHandler(urllib.request.HTTPHandler):
    def __init__(self, connect_timeout, read_timeout):
        super().__init__(); self.connect_timeout=connect_timeout; self.read_timeout=read_timeout
    def http_open(self, request):
        return self.do_open(lambda host, **kw: _TimeoutHTTPConnection(host,self.connect_timeout,self.read_timeout),request)

class _TimeoutHTTPSHandler(urllib.request.HTTPSHandler):
    def __init__(self, connect_timeout, read_timeout):
        super().__init__(); self.connect_timeout=connect_timeout; self.read_timeout=read_timeout
    def https_open(self, request):
        return self.do_open(lambda host, **kw: _TimeoutHTTPSConnection(host,self.connect_timeout,self.read_timeout,
            context=self._context,check_hostname=self._check_hostname),request,
            context=self._context,check_hostname=self._check_hostname)

class HTTPTransport:
    """Thread-safe transport; each request owns its opener and response metadata.

    Connect timeout bounds connection setup; read timeout bounds individual
    socket reads; overall timeout is enforced while consuming the response body.
    urllib's synchronous response-header parsing is still bounded by its socket
    timeout rather than a strict end-to-end wall-clock deadline.
    """
    def __init__(self, connect_timeout=DEFAULT_CONNECT_TIMEOUT, read_timeout=DEFAULT_READ_TIMEOUT,
                 overall_timeout=DEFAULT_TIMEOUT, proxy_url=None, *, proxy_required=False,
                 max_response_bytes=DEFAULT_MAX_RESPONSE_BYTES, max_retries=3):
        self.connect_timeout=_positive_timeout(connect_timeout,"connect_timeout")
        self.read_timeout=_positive_timeout(read_timeout,"read_timeout")
        self.overall_timeout=_positive_timeout(overall_timeout,"overall_timeout")
        if isinstance(max_response_bytes,bool) or not isinstance(max_response_bytes,int) or not 1 <= max_response_bytes <= 50 * 1024 * 1024:
            raise ValueError("max_response_bytes must be between 1 and 52428800")
        if proxy_required and not proxy_url: raise ValueError("A proxy is required but no proxy URL was configured")
        if proxy_url:
            proxy=urllib.parse.urlsplit(proxy_url)
            if proxy.scheme.lower() != "http" or not proxy.hostname:
                raise ValueError("Proxy URL must be an absolute HTTP URL")
            if proxy.path not in {"", "/"} or proxy.query or proxy.fragment:
                raise ValueError("Proxy URL must not contain a path, query or fragment")
            try:
                if not proxy.port or not 1 <= proxy.port <= 65535:
                    raise ValueError
            except ValueError as exc:
                raise ValueError("Proxy URL must include a valid explicit port") from exc
        if isinstance(max_retries, bool) or not isinstance(max_retries, int) or not 1 <= max_retries <= 10:
            raise ValueError("max_retries must be an integer between 1 and 10")
        self.proxy_url=proxy_url; self.proxy_required=bool(proxy_required)
        self.max_response_bytes=max_response_bytes; self.max_retries=max_retries

    def request_detailed(self, method, url, *, headers=None, body=None, expect_json=True,
                         connect_timeout=None, read_timeout=None, overall_timeout=None):
        _validate_url(url)
        method=str(method).upper()
        if not method.isalpha() or len(method)>16: raise ValueError("Invalid HTTP method")
        if body is not None and not isinstance(body,bytes): raise TypeError("HTTP request body must be bytes")
        clean={}
        for key,value in (headers or {}).items():
            if not isinstance(key,str) or not isinstance(value,str): raise TypeError("HTTP header names and values must be strings")
            if not key or any(c in key+value for c in "\r\n"): raise ValueError("HTTP headers must not contain CR/LF or empty names")
            clean[key]=value
        connect_value = self.connect_timeout if connect_timeout is None else connect_timeout
        read_value = self.read_timeout if read_timeout is None else read_timeout
        total_value = self.overall_timeout if overall_timeout is None else overall_timeout
        total = _positive_timeout(total_value, "overall_timeout")
        connect = min(_positive_timeout(connect_value, "connect_timeout"), total)
        read = min(_positive_timeout(read_value, "read_timeout"), total)
        request=urllib.request.Request(url,data=body,headers=clean,method=method)
        proxy_map={"http":self.proxy_url,"https":self.proxy_url} if self.proxy_url else {}
        opener=urllib.request.build_opener(urllib.request.ProxyHandler(proxy_map),
            _SafeRedirectHandler(),_TimeoutHTTPHandler(connect,read),_TimeoutHTTPSHandler(connect,read))
        started=time.monotonic(); response=None
        try:
            try: response=opener.open(request,timeout=connect)
            except urllib.error.HTTPError as http_error: response=http_error
            status=int(response.getcode() or 0); response_headers=response.headers
            declared=response_headers.get("Content-Length")
            if declared:
                try:
                    declared_size = int(declared)
                    if declared_size < 0:
                        raise HTTPTransportError("HTTP_RESPONSE_ERROR","HTTP response contained a negative Content-Length",retryable=False,http_status=status)
                    if declared_size > self.max_response_bytes:
                        raise HTTPTransportError("HTTP_RESPONSE_TOO_LARGE","HTTP response exceeded configured size limit",retryable=False,http_status=status)
                except ValueError:
                    raise HTTPTransportError("HTTP_RESPONSE_ERROR","HTTP response contained invalid Content-Length",retryable=False,http_status=status)
            chunks=[]; size=0
            while True:
                remaining=total-(time.monotonic()-started)
                if remaining<=0: raise HTTPTransportError("TIMEOUT_ERROR","HTTP request exceeded its total time budget",retryable=True,http_status=status or None)
                sock=getattr(getattr(getattr(response,"fp",None),"raw",None),"_sock",None)
                if sock is not None: sock.settimeout(min(read,remaining))
                chunk=response.read(min(_READ_CHUNK_BYTES,self.max_response_bytes+1-size))
                if not chunk: break
                size+=len(chunk)
                if size>self.max_response_bytes: raise HTTPTransportError("HTTP_RESPONSE_TOO_LARGE","HTTP response exceeded configured size limit",retryable=False,http_status=status)
                chunks.append(chunk)
            raw=b"".join(chunks)
        except HTTPTransportError: raise
        except BaseException as error:
            if isinstance(error,(KeyboardInterrupt,SystemExit)): raise
            code,retryable=_classify_exception(error,bool(self.proxy_url))
            logger.warning("HTTP transport failure: code=%s exception_type=%s",code,type(error).__name__)
            raise HTTPTransportError(code,"HTTP transport could not complete the request",retryable=retryable) from None
        finally:
            if response is not None:
                try: response.close()
                except OSError: pass
        retry_after=_retry_after(response_headers)
        if status==407: raise HTTPTransportError("PROXY_AUTHENTICATION_ERROR","Proxy authentication was rejected",retryable=False,http_status=status)
        if status==429: raise HTTPTransportError("RATE_LIMITED","Remote service rate-limited the request",retryable=True,http_status=status,retry_after=retry_after)
        if status in (408,425) or 500<=status<=599:
            raise HTTPTransportError("HTTP_SERVER_ERROR","Remote server returned a transient HTTP response",retryable=True,http_status=status,retry_after=retry_after)
        if status in (401,403):
            raise HTTPTransportError("ACCESS_RESTRICTED" if status==403 else "AUTHENTICATION_ERROR","Remote service rejected the request",retryable=False,http_status=status)
        if not 200<=status<300:
            code="HTTP_CLIENT_ERROR" if 400<=status<500 else "HTTP_RESPONSE_ERROR"
            raise HTTPTransportError(code,"Remote service returned a non-success HTTP response",retryable=False,http_status=status)
        parsed_json=None
        if raw:
            try: parsed=json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError,json.JSONDecodeError):
                if expect_json: raise HTTPTransportError("JSON_DECODE_ERROR","Successful HTTP response did not contain valid UTF-8 JSON",retryable=False,http_status=status) from None
            else:
                if isinstance(parsed,dict): parsed_json=parsed
                elif expect_json: raise HTTPTransportError("API_RESPONSE_ERROR","Successful HTTP response JSON was not an object",retryable=False,http_status=status)
        elif expect_json: raise HTTPTransportError("API_RESPONSE_ERROR","Successful HTTP response body was empty",retryable=False,http_status=status)
        return HTTPResponse(status,response_headers,raw,parsed_json,datetime.now(timezone.utc).isoformat())

    def request_with_retry(self, method, url, *, headers=None, body=None, expect_json=True,
                           maximum_attempts=None, max_backoff_seconds=300.0):
        if method.upper() not in {"GET","HEAD"} or body is not None:
            raise ValueError("Automatic retries are restricted to idempotent GET/HEAD requests")
        maximum_attempts = self.max_retries if maximum_attempts is None else maximum_attempts
        if isinstance(maximum_attempts,bool) or not isinstance(maximum_attempts,int) or not 1 <= maximum_attempts <= 10:
            raise ValueError("maximum_attempts must be an integer between 1 and 10")
        if not math.isfinite(max_backoff_seconds) or not 0 <= max_backoff_seconds <= 300:
            raise ValueError("max_backoff_seconds must be finite and between 0 and 300")
        last=None
        for attempt in range(1,maximum_attempts+1):
            try:
                return self.request_detailed(method,url,headers=headers,body=body,expect_json=expect_json)
            except HTTPTransportError as error:
                last=error
                if not error.retryable or attempt==maximum_attempts: raise
                ceiling=min(max_backoff_seconds,2.0**(attempt-1))
                delay=min(error.retry_after_seconds,max_backoff_seconds) if error.retry_after_seconds is not None else random.uniform(0.0,ceiling)
                logger.warning("HTTP retry scheduled: attempt=%d/%d code=%s",attempt+1,maximum_attempts,error.error_code)
                time.sleep(delay)
        raise last

    def request(self,method,url,headers=None,body=None,connect_timeout=None,read_timeout=None,overall_timeout=None):
        try:
            result=self.request_detailed(method,url,headers=headers,body=body,
                connect_timeout=connect_timeout,read_timeout=read_timeout,overall_timeout=overall_timeout)
            return result.status_code,result.json_data
        except HTTPTransportError as error:
            logger.warning("HTTP request ended with %s (status=%s)",error.error_code,error.http_status)
            return error.http_status or 0,None

    def fetch_json(self,url,headers=None,connect_timeout=None,read_timeout=None,overall_timeout=None):
        return self.request("GET",url,headers=headers,connect_timeout=connect_timeout,read_timeout=read_timeout,overall_timeout=overall_timeout)
    def validate_status(self,status_code): return 200<=status_code<300

def fetch_json(url,headers=None,connect_timeout=DEFAULT_CONNECT_TIMEOUT,read_timeout=DEFAULT_READ_TIMEOUT,overall_timeout=DEFAULT_TIMEOUT):
    return HTTPTransport(connect_timeout,read_timeout,overall_timeout).fetch_json(url,headers)
