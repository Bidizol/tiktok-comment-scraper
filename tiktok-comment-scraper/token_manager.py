"""Thread-safe TikTok msToken acquisition with bounded refresh attempts."""
from __future__ import annotations
import logging, random, threading, time
from datetime import datetime, timezone
from http.cookies import SimpleCookie
from typing import Optional
from http_transport import HTTPTransport, HTTPTransportError
logger=logging.getLogger(__name__)
MAX_REFRESH_ATTEMPTS=3
DEFAULT_TOKEN_TTL_SECONDS=3600
TOKEN_ENDPOINT="https://www.tiktok.com/api/recommend/item_list/?aid=1988&count=1"
TOKEN_USER_AGENT="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
class TokenError(Exception):
    def __init__(self,code,message,*,retryable=False):
        super().__init__(message); self.error_code=code; self.retryable=retryable
class TokenExhaustedError(TokenError): pass
class TokenManager:
    """One refresh lock per manager; token lifetime uses monotonic time."""
    def __init__(self,worker_id,initial_token=None,token_ttl_seconds=DEFAULT_TOKEN_TTL_SECONDS,
                 transport=None,max_refresh_attempts=MAX_REFRESH_ATTEMPTS,refresh_endpoint=TOKEN_ENDPOINT,
                 retry_budget=None):
        if isinstance(token_ttl_seconds,bool) or token_ttl_seconds<1: raise ValueError("token_ttl_seconds must be positive")
        if isinstance(max_refresh_attempts,bool) or not isinstance(max_refresh_attempts,int) or not 1<=max_refresh_attempts<=10: raise ValueError("max_refresh_attempts must be an integer between 1 and 10")
        self.worker_id=worker_id; self._token=initial_token
        self._issued=time.monotonic() if initial_token else None
        self._ttl=token_ttl_seconds; self._max_attempts=max_refresh_attempts
        self._endpoint=refresh_endpoint; self._transport=transport or HTTPTransport()
        self._retry_budget=retry_budget
        self._lock=threading.RLock()
    @property
    def token(self):
        with self._lock:
            if not self._token or self._issued is None or time.monotonic()-self._issued>=self._ttl: return None
            return self._token
    def get_token(self,force_refresh=False):
        with self._lock:
            if not force_refresh:
                current=self.token
                if current: return current
            return self._refresh_token()
    def _refresh_token(self):
        last=None
        for attempt in range(1,self._max_attempts+1):
            try:
                response=self._transport.request_with_retry("GET",self._endpoint,
                    headers={"User-Agent":TOKEN_USER_AGENT,"Accept":"application/json, text/plain, */*"},
                    expect_json=False,maximum_attempts=1,
                    retry_budget=self._retry_budget,retry_component="token")
                token=self._extract_token(response.header_values("Set-Cookie"),response.json_data)
                if not token: raise TokenError("TOKEN_REFRESH_ERROR","Token endpoint did not provide an msToken",retryable=True)
                self._token=token; self._issued=time.monotonic()
                if self._retry_budget is not None: self._retry_budget.record_success("token")
                logger.info("Token acquired for worker %s",self.worker_id)
                return token
            except HTTPTransportError as error:
                last=TokenError(error.error_code,str(error),retryable=error.retryable)
                if not error.retryable: raise last from None
                if self._retry_budget is not None and not self._retry_budget.consume_retry("token", error.error_code):
                    raise TokenExhaustedError("RETRY_EXHAUSTED","Shared per-video recovery budget exhausted during token refresh") from None
                ceiling=min(8.0,2.0**(attempt-1))
                delay=min(error.retry_after_seconds,300.0) if error.retry_after_seconds is not None else random.uniform(0.0,ceiling)
            except TokenError as error:
                last=error
                if not error.retryable: raise
                if self._retry_budget is not None and not self._retry_budget.consume_retry("token", error.error_code):
                    raise TokenExhaustedError("RETRY_EXHAUSTED","Shared per-video recovery budget exhausted during token refresh") from None
                delay=random.uniform(0.0,min(8.0,2.0**(attempt-1)))
            except (KeyboardInterrupt,SystemExit): raise
            except Exception as error:
                raise TokenError("TOKEN_REFRESH_ERROR",f"Unexpected token acquisition failure ({type(error).__name__})") from None
            logger.warning("Token acquisition attempt %d/%d failed for worker %s",attempt,self._max_attempts,self.worker_id)
            if attempt<self._max_attempts:
                if self._retry_budget is not None: delay = min(delay, self._retry_budget.remaining_seconds())
                if delay > 0: time.sleep(delay)
        code=getattr(last,"error_code","TOKEN_REFRESH_ERROR")
        raise TokenExhaustedError(code,f"Token acquisition exhausted after {self._max_attempts} bounded attempts")
    @staticmethod
    def _extract_token(set_cookie_headers,payload):
        cookie=SimpleCookie()
        for header in set_cookie_headers:
            try: cookie.load(header)
            except Exception: continue
        morsel=cookie.get("msToken")
        if morsel and morsel.value: return morsel.value
        if isinstance(payload,dict):
            for key in ("msToken","token","unique_id"):
                value=payload.get(key)
                if isinstance(value,str) and value.strip(): return value.strip()
        return None
    def reset_refresh_attempts(self): return None
