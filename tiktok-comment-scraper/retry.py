"""Bounded retry policy with structured classification and jitter."""
from __future__ import annotations
import logging, random, time
from dataclasses import dataclass
from typing import Any, Callable, Generic, TypeVar
logger=logging.getLogger(__name__)
MAX_DEFAULT_RETRIES=3; DEFAULT_BACKOFF_FACTOR=1.0; MAX_BACKOFF_DELAY=300.0; JITTER_FRACTION=0.25
T=TypeVar("T")
@dataclass
class RetryRecord:
    error_code:str; attempt_number:int; maximum_attempts:int
    delay_seconds:float|None=None; backoff_decision:str|None=None
    recovery_action:str|None=None; recovery_outcome:str|None=None
@dataclass(frozen=True)
class RetryPolicy:
    maximum_attempts:int=MAX_DEFAULT_RETRIES
    backoff_factor:float=DEFAULT_BACKOFF_FACTOR
    max_backoff_delay:float=MAX_BACKOFF_DELAY
    jitter_fraction:float=JITTER_FRACTION
    retryable_error_codes:tuple[str,...]=("RATE_LIMITED","NETWORK_ERROR","DNS_ERROR","TIMEOUT_ERROR","HTTP_SERVER_ERROR","PROXY_ERROR")
    permanent_error_codes:tuple[str,...]=("AUTHENTICATION_ERROR","ACCESS_RESTRICTED","PROXY_AUTHENTICATION_ERROR","TLS_ERROR","HTTP_CLIENT_ERROR","JSON_DECODE_ERROR","API_RESPONSE_ERROR","UNEXPECTED_INTERNAL_ERROR")
    def __post_init__(self):
        if isinstance(self.maximum_attempts,bool) or self.maximum_attempts<1: raise ValueError("maximum_attempts must be at least 1")
        if self.backoff_factor<0 or self.max_backoff_delay<0: raise ValueError("Backoff values cannot be negative")
        if not 0<=self.jitter_fraction<=1: raise ValueError("jitter_fraction must be between 0 and 1")
class BoundedRetry(Generic[T]):
    def __init__(self,policy=None,sleep=time.sleep,random_source=random.random):
        self.policy=policy or RetryPolicy(); self.records=[]; self._sleep=sleep; self._random=random_source
    def should_retry(self,error_code,attempt):
        return attempt<self.policy.maximum_attempts and error_code in self.policy.retryable_error_codes and error_code not in self.policy.permanent_error_codes
    def calculate_delay(self,attempt):
        base=min(self.policy.max_backoff_delay,self.policy.backoff_factor*(2**max(0,attempt-1)))
        spread=base*self.policy.jitter_fraction
        return max(0.0,base-spread+2*spread*self._random())
    @staticmethod
    def _classify_error(error):
        from errors.classifiers import ErrorClassification
        from errors.model import ErrorOutcome,ErrorSeverity
        code=getattr(error,"error_code",None); status=getattr(error,"http_status",None)
        retryable=getattr(error,"retryable",None)
        if status==429: code="RATE_LIMITED"
        elif status is not None and 500<=status<=599: code="HTTP_SERVER_ERROR"
        elif status is not None and 400<=status<=499: code="HTTP_CLIENT_ERROR"
        if not code: code="TIMEOUT_ERROR" if isinstance(error,TimeoutError) else "UNEXPECTED_INTERNAL_ERROR"
        can_retry=bool(retryable) if retryable is not None else code in RetryPolicy().retryable_error_codes
        return ErrorClassification(code,ErrorSeverity.WARNING if can_retry else ErrorSeverity.ERROR,
            "Transient operation failure" if can_retry else "Non-retryable operation failure",
            ErrorOutcome.FAILED_RESUMABLE if can_retry else ErrorOutcome.FAILED_PERMANENT,status)
    def execute(self,operation:Callable[[],T],error_classifier:Any=None):
        self.records=[]
        for attempt in range(1,self.policy.maximum_attempts+1):
            try: return operation(),self.records
            except (KeyboardInterrupt,SystemExit): raise
            except Exception as error:
                classification=error_classifier.classify(error) if error_classifier else self._classify_error(error)
                code=classification.error_code; retryable=getattr(error,"retryable",None)
                should_retry=(attempt<self.policy.maximum_attempts if retryable is True else
                    False if retryable is False else self.should_retry(code,attempt))
                record=RetryRecord(code,attempt,self.policy.maximum_attempts,
                    backoff_decision="pending",recovery_action="retry_transient_operation" if should_retry else "stop")
                self.records.append(record)
                if not should_retry:
                    record.recovery_outcome="retry_exhausted" if retryable else "permanent_failure"
                    logger.warning("Operation stopped after attempt %d (%s)",attempt,code)
                    return None,self.records
                retry_after=getattr(error,"retry_after_seconds",None)
                delay=min(max(0.0,float(retry_after)),self.policy.max_backoff_delay) if retry_after is not None else self.calculate_delay(attempt)
                record.delay_seconds=delay; record.backoff_decision="server_retry_after" if retry_after is not None else "exponential_backoff_with_jitter"
                record.recovery_outcome="retry_scheduled"
                logger.warning("Retry scheduled: attempt=%d/%d code=%s",attempt+1,self.policy.maximum_attempts,code)
                self._sleep(delay)
        return None,self.records
