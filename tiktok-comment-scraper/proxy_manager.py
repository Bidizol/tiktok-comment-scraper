"""Strict proxy configuration and credential-safe URL construction."""
from __future__ import annotations
import ipaddress, re
from typing import Optional, Tuple
from urllib.parse import quote
class ProxyError(Exception): pass
class ProxyAuthenticationError(ProxyError): pass
class ProxyConnectionError(ProxyError): pass
class ProxyManager:
    """Validate proxy settings and never silently substitute a direct route."""
    def __init__(self,proxy_host,proxy_port,proxy_user=None,proxy_pass=None):
        self.proxy_host=(proxy_host or "").strip(); self.proxy_port=str(proxy_port or "").strip()
        self.proxy_user=proxy_user; self.proxy_pass=proxy_pass
        self._validated=False; self._url=None; self._redacted_url=None
    @staticmethod
    def _valid_host(host):
        candidate=host.strip()
        if not candidate or any(ch in candidate for ch in "/@?#"): return False
        try: ipaddress.ip_address(candidate.strip("[]")); return True
        except ValueError:
            if ":" in candidate: return False
            try: ascii_host=candidate.encode("idna").decode("ascii")
            except UnicodeError: return False
            if len(ascii_host)>253: return False
            return all(1<=len(label)<=63 and re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?",label)
                for label in ascii_host.rstrip(".").split("."))
    def validate(self)->Tuple[bool,Optional[str]]:
        has_host,has_port=bool(self.proxy_host),bool(self.proxy_port)
        if has_host!=has_port: return False,"PROXY_HOST and PROXY_PORT must both be set or both be unset"
        if not has_host:
            if self.proxy_user or self.proxy_pass: return False,"Proxy credentials supplied without a proxy endpoint"
            self._url=None; self._redacted_url="disabled"; self._validated=True; return True,None
        if not self._valid_host(self.proxy_host): return False,"Proxy host is not a valid hostname or IP address"
        try: port=int(self.proxy_port)
        except ValueError: return False,"Proxy port must be an integer"
        if not 1<=port<=65535: return False,"Proxy port must be between 1 and 65535"
        if bool(self.proxy_user)!=bool(self.proxy_pass): return False,"Both proxy username and password must be supplied together"
        if self.proxy_user is not None and (not self.proxy_user.strip() or not self.proxy_pass.strip()):
            return False,"Proxy credentials must not be blank"
        host=f"[{self.proxy_host.strip('[]')}]" if ":" in self.proxy_host else self.proxy_host
        credentials=(quote(self.proxy_user,safe="")+":"+quote(self.proxy_pass or "",safe="")+"@") if self.proxy_user is not None else ""
        self._url=f"http://{credentials}{host}:{port}"
        self._redacted_url=f"http://[REDACTED]@{host}:{port}" if credentials else f"http://{host}:{port}"
        self._validated=True; return True,None
    @property
    def proxy_url(self):
        if not self._validated:
            valid,error=self.validate()
            if not valid: raise ValueError(error or "Invalid proxy configuration")
        return self._url
    @property
    def redacted_url(self):
        if not self._validated:
            valid,error=self.validate()
            if not valid: raise ValueError(error or "Invalid proxy configuration")
        return self._redacted_url or "disabled"
    def classify_error(self,error):
        from errors.classifiers import ErrorClassification
        from errors.model import ErrorOutcome,ErrorSeverity
        status=getattr(error,"http_status",None); code=getattr(error,"error_code","")
        if status==407 or code=="PROXY_AUTHENTICATION_ERROR":
            return ErrorClassification("PROXY_AUTHENTICATION_ERROR",ErrorSeverity.ERROR,"Proxy authentication was rejected",ErrorOutcome.FAILED_PERMANENT,407)
        if code=="DNS_ERROR":
            return ErrorClassification("DNS_ERROR",ErrorSeverity.WARNING,"Proxy host name could not be resolved",ErrorOutcome.FAILED_RESUMABLE,0)
        return ErrorClassification("PROXY_ERROR",ErrorSeverity.WARNING,"Proxy connection failed",ErrorOutcome.FAILED_RESUMABLE,status or 0)
    def mask_credentials(self,text):
        return re.sub(r"(?i)(https?://)[^/@\s]+@",r"\1[REDACTED]@",str(text))
