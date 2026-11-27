"""Coordinated retry budget and failure recovery for TikTok Comment Scraper."""

from __future__ import annotations

import logging
import time
import random
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Callable, TypeVar

# Import error taxonomy - use try/except for compatibility with Phase 1 imports
try:
    from errors.model import ScraperError, ErrorSeverity, ErrorOutcome
    from errors.classifiers import ErrorClassification
    _HAS_ERRORS_MODULE = True
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
        RETRY_EXHAUSTED = "retry_exhausted"
        INTERRUPTED = "interrupted"
    
    class ScraperError(Exception):
        def __init__(self, error_code, severity, component, function,
                     explanation="", http_status=None, outcome="failed_permanent",
                     video_id=None, run_id=None, worker_id=None,
                     batch_number=None, cursor=None, retry_attempt=None,
                     recovery_action=None):
            self.error_code = error_code
            self.severity = severity
            self.component = component
            self.function = function
            self.explanation = explanation
            self.http_status = http_status
            self.outcome = outcome
            self.video_id = video_id
            self.run_id = run_id
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
    
    _HAS_ERRORS_MODULE = False

logger = logging.getLogger(__name__)

T = TypeVar("T")

#: Maximum default retry attempts for the coordinated budget
MAX_DEFAULT_RETRIES = 3
#: Default backoff factor (exponential backoff: delay = factor * 2^(attempt-1))
DEFAULT_BACKOFF_FACTOR = 1.0
#: Maximum backoff delay in seconds
MAX_BACKOFF_DELAY = 60
#: Jitter range as fraction of calculated delay
JITTER_FRACTION = 0.1


@dataclass
class AttemptRecord:
    """A single attempt record for the coordinated retry budget.
    
    Every attempt is recorded for traceability and diagnostics.
    """
    attempt_number: int
    maximum_attempts: int
    error_code: Optional[str] = None
    error_classification: Optional[ErrorClassification] = None
    http_status: Optional[int] = None
    delay_seconds: Optional[float] = None
    backoff_decision: Optional[str] = None
    recovery_action: Optional[str] = None
    recovery_outcome: Optional[str] = None
    success: bool = False
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class ErrorClassifier:
    """Classifies errors per Phase 4 requirements."""
    
    maximum_attempt_budget: int = MAX_DEFAULT_RETRIES
    
    def classify(self, error: Exception, http_status: Optional[int] = None,
                 attempt_number: int = 0) -> ErrorClassification:
        error_str = str(error).lower() if error else ""
        
        if http_status is not None:
            if http_status == 403:
                return ErrorClassification(
                    error_code="HTTP_403_ACCESS_DENIED",
                    severity=ErrorSeverity.WARNING,
                    explanation="HTTP 403 Access Denied - access to resource forbidden",
                    default_outcome=ErrorOutcome.FAILED_PERMANENT,
                    http_status=403,
                )
            if http_status == 404:
                return ErrorClassification(
                    error_code="HTTP_404_NOT_FOUND",
                    severity=ErrorSeverity.WARNING,
                    explanation="HTTP 404 Not Found - requested resource does not exist",
                    default_outcome=ErrorOutcome.FAILED_PERMANENT,
                    http_status=404,
                )
            if 500 <= http_status < 600:
                return ErrorClassification(
                    error_code="HTTP_5XX",
                    severity=ErrorSeverity.WARNING,
                    explanation=f"HTTP {http_status} Server Error - retryable within budget",
                    default_outcome=ErrorOutcome.FAILED_RESUMABLE,
                    http_status=http_status,
                )
            if http_status == 429:
                return ErrorClassification(
                    error_code="HTTP_429_RATE_LIMITED",
                    severity=ErrorSeverity.WARNING,
                    explanation="HTTP 429 Rate Limited - respect Retry-After within budget",
                    default_outcome=ErrorOutcome.FAILED_RESUMABLE,
                    http_status=429,
                )
            if http_status == 401:
                return ErrorClassification(
                    error_code="HTTP_401_UNAUTHORIZED",
                    severity=ErrorSeverity.WARNING,
                    explanation="HTTP 401 Unauthorized - controlled token refresh then retry within budget",
                    default_outcome=ErrorOutcome.FAILED_RESUMABLE,
                    http_status=401,
                )
        
        timeout_indicators = ["timeout", "connect timeout", "timed out"]
        dns_indicators = ["dns", "dns lookup", "unknown host", "resolution failed"]
        conn_indicators = ["connection refused", "connection reset", "unable to connect"]
        network_indicators = ["connection", "network", "unreachable", "refused"]
        auth_indicators = ["unauthorized", "invalid", "authentication", "forbidden", "401", "403"]
        api_indicators = ["rate limit", "429", "quota", "too many requests", "exceeded"]
        json_indicators = ["json decode", "invalid json", "malformed json"]
        
        has_timeout = any(ind in error_str for ind in timeout_indicators)
        has_dns = any(ind in error_str for ind in dns_indicators)
        has_conn = any(ind in error_str for ind in conn_indicators)
        has_network = any(ind in error_str for ind in network_indicators)
        has_auth = any(ind in error_str for ind in auth_indicators)
        has_api = any(ind in error_str for ind in api_indicators)
        has_json = any(ind in error_str for ind in json_indicators)
        
        if has_json:
            if attempt_number < 3:
                return ErrorClassification(
                    error_code="INVALID_JSON_TRANSIENT",
                    severity=ErrorSeverity.WARNING,
                    explanation="Invalid JSON response - may be transient, retry within budget",
                    default_outcome=ErrorOutcome.FAILED_RESUMABLE,
                    http_status=http_status or 0,
                )
            else:
                return ErrorClassification(
                    error_code="INVALID_JSON_PERMANENT",
                    severity=ErrorSeverity.ERROR,
                    explanation="Invalid JSON response - permanent structural failure",
                    default_outcome=ErrorOutcome.FAILED_PERMANENT,
                    http_status=http_status or 0,
                )
        
        if has_dns or has_timeout:
            return ErrorClassification(
                error_code="NETWORK_ERROR",
                severity=ErrorSeverity.WARNING,
                explanation="Operation network failure - bounded retry within budget",
                default_outcome=ErrorOutcome.FAILED_RESUMABLE,
                http_status=0,
            )
        
        if has_conn:
            return ErrorClassification(
                error_code="NETWORK_ERROR",
                severity=ErrorSeverity.WARNING,
                explanation="Operation connection failure - bounded retry within budget",
                default_outcome=ErrorOutcome.FAILED_RESUMABLE,
                http_status=0,
            )
        
        if has_api:
            return ErrorClassification(
                error_code="RATE_LIMITED",
                severity=ErrorSeverity.WARNING,
                explanation="Operation rate-limited by API - retry within budget with Retry-After",
                default_outcome=ErrorOutcome.FAILED_RESUMABLE,
                http_status=http_status or 429,
            )
        
        if has_auth:
            return ErrorClassification(
                error_code="AUTHENTICATION_ERROR",
                severity=ErrorSeverity.ERROR,
                explanation="Operation authentication failed",
                default_outcome=ErrorOutcome.FAILED_PERMANENT,
                http_status=http_status or 401,
            )
        
        return ErrorClassification(
            error_code="UNEXPECTED_INTERNAL_ERROR",
            severity=ErrorSeverity.ERROR,
            explanation="Unknown error occurred",
            default_outcome=ErrorOutcome.FAILED_PERMANENT,
            http_status=http_status or 0,
        )

    def outcome_for_classification(self, classification: ErrorClassification) -> str:
        return classification.default_outcome


@dataclass
class CoordinatedRetryBudget:
    """Authoritative maximum attempt budget shared across all retry layers."""
    
    def __init__(self, maximum_attempts: int = MAX_DEFAULT_RETRIES, 
                 run_id: Optional[str] = None,
                 video_id: Optional[str] = None,
                 worker_id: Optional[int] = None):
        import uuid
        self.maximum_attempts = maximum_attempts
        self.run_id = run_id or str(uuid.uuid4())
        self.video_id = video_id
        self.worker_id = worker_id
        self._remaining: int = maximum_attempts
        self._exhausted: bool = False
        self._records: List[AttemptRecord] = []
        self._lock = threading.Lock()
        
        logger.info(
            "CoordinatedRetryBudget initialized: run_id=%s, video_id=%s, "
            "worker_id=%s, maximum_attempts=%d",
            self.run_id, self.video_id, self.worker_id, self.maximum_attempts
        )
    
    def remaining(self) -> int:
        with self._lock:
            return self._remaining
    
    def is_exhausted(self) -> bool:
        with self._lock:
            return self._exhausted
    
    def consume(self, count: int = 1) -> bool:
        with self._lock:
            if self._exhausted:
                return False
            if self._remaining - count < 0:
                self._exhausted = True
                self._remaining = 0
                logger.warning(
                    "CoordinatedRetryBudget exhausted: run_id=%s, "
                    "remaining=%d, attempting to consume %d",
                    self.run_id, self._remaining + count, count
                )
                return False
            self._remaining -= count
            logger.debug(
                "CoordinatedRetryBudget consumed: run_id=%s, remaining=%d",
                self.run_id, self._remaining
            )
            return True
    
    def record(self, record: AttemptRecord) -> None:
        with self._lock:
            self._records.append(record)
    
    def get_records(self) -> List[AttemptRecord]:
        with self._lock:
            return self._records.copy()
    
    def reset(self) -> None:
        with self._lock:
            self._remaining = self.maximum_attempts
            self._exhausted = False
            self._records = []


class CoordinatedRetry:
    """Bounded retry policy that respects a shared CoordinatedRetryBudget."""
    
    def __init__(self, budget: CoordinatedRetryBudget, 
                 error_classifier: ErrorClassifier,
                 backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
                 max_backoff_delay: float = MAX_BACKOFF_DELAY,
                 jitter_fraction: float = JITTER_FRACTION):
        self.budget = budget
        self.error_classifier = error_classifier
        self.backoff_factor = backoff_factor
        self.max_backoff_delay = max_backoff_delay
        self.jitter_fraction = jitter_fraction
    
    def calculate_delay(self, attempt: int) -> float:
        delay = self.backoff_factor * (2 ** (attempt - 1))
        if delay > self.max_backoff_delay:
            delay = self.max_backoff_delay
        if self.jitter_fraction > 0:
            jitter = delay * self.jitter_fraction * (2 * random.random() - 1)
            delay += jitter
        delay = max(0, delay)
        logger.debug(
            "CoordinatedRetry delay calculation: attempt=%d, delay=%.2fs",
            attempt, delay
        )
        return delay
    
    def should_retry(self, classification: ErrorClassification) -> bool:
        if classification.default_outcome in (
            ErrorOutcome.FAILED_PERMANENT,
            ErrorOutcome.RETRY_EXHAUSTED,
            ErrorOutcome.INTERRUPTED
        ):
            return False
        if self.budget.is_exhausted():
            return False
        if classification.error_code == "HTTP_429_RATE_LIMITED":
            return True
        if classification.error_code == "HTTP_401_UNAUTHORIZED":
            return False
        if classification.error_code == "HTTP_403_ACCESS_DENIED":
            return False
        return True
    
    def execute_with_budget(self, operation, respect_retry_after: bool = True):
        attempt = 0
        last_classification: Optional[ErrorClassification] = None
        
        logger.info(
            "CoordinatedRetry.execute_with_budget: starting, run_id=%s, max_attempts=%d",
            self.budget.run_id, self.budget.maximum_attempts
        )
        
        while True:
            attempt += 1
            
            if not self.budget.consume(1):
                logger.warning(
                    "CoordinatedRetry budget exhausted before attempt %d", attempt
                )
                self.budget.record(AttemptRecord(
                    attempt_number=attempt,
                    maximum_attempts=self.budget.maximum_attempts,
                    error_code="RETRY_BUDGET_EXHAUSTED",
                    recovery_outcome=ErrorOutcome.RETRY_EXHAUSTED,
                    success=False,
                ))
                return None, ErrorOutcome.RETRY_EXHAUSTED
            
            try:
                result, classification = operation()
                
                if classification is not None:
                    last_classification = classification
                
                if result is not None:
                    self.budget.record(AttemptRecord(
                        attempt_number=attempt,
                        maximum_attempts=self.budget.maximum_attempts,
                        error_code=last_classification.error_code if last_classification else None,
                        http_status=last_classification.http_status if last_classification else None,
                        success=True,
                    ))
                    logger.info(
                        "CoordinatedRetry.execute_with_budget: successful on attempt %d / %d",
                        attempt, self.budget.maximum_attempts
                    )
                    return result, ErrorOutcome.SUCCESS
                
                last_classification = last_classification or ErrorClassification(
                    error_code="OPERATION_RETURNED_NONE",
                    severity=ErrorSeverity.WARNING,
                    explanation="Operation returned None without exception",
                    default_outcome=ErrorOutcome.FAILED_RESUMABLE,
                    http_status=None,
                )
            except Exception as error:
                last_classification = last_classification or self.error_classifier.classify(
                    error, http_status=getattr(error, 'http_status', None)
                )
                
                self.budget.record(AttemptRecord(
                    attempt_number=attempt,
                    maximum_attempts=self.budget.maximum_attempts,
                    error_code=last_classification.error_code if last_classification else None,
                    error_classification=last_classification,
                    http_status=last_classification.http_status if last_classification else None,
                    success=False,
                ))
                
                if not self.should_retry(last_classification):
                    logger.warning(
                        "CoordinatedRetry: no more retries for %s (attempt %d / %d): %s",
                        last_classification.error_code, attempt,
                        self.budget.maximum_attempts,
                        last_classification.explanation
                    )
                    return None, last_classification.default_outcome
                
                delay = self.calculate_delay(attempt)
                
                logger.info(
                    "CoordinatedRetry attempt %d/%d: %s - retrying in %.1fs (budget remaining: %d)",
                    attempt, self.budget.maximum_attempts,
                    last_classification.error_code,
                    delay,
                    self.budget.remaining()
                )
                
                time.sleep(delay)
        
        return None, ErrorOutcome.RETRY_EXHAUSTED


class CoordinatedRetryContext:
    """Context manager for creating a coordinated retry budget for an operation."""
    
    def __init__(self, maximum_attempts: int = MAX_DEFAULT_RETRIES,
                 run_id: Optional[str] = None,
                 video_id: Optional[str] = None,
                 worker_id: Optional[int] = None):
        self.maximum_attempts = maximum_attempts
        self.run_id = run_id or str(uuid.uuid4())
        self.video_id = video_id
        self.worker_id = worker_id
        self.budget: Optional[CoordinatedRetryBudget] = None
    
    def __enter__(self) -> CoordinatedRetryBudget:
        self.budget = CoordinatedRetryBudget(
            maximum_attempts=self.maximum_attempts,
            run_id=self.run_id,
            video_id=self.video_id,
            worker_id=self.worker_id,
        )
        return self.budget
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        pass


def make_classifier_for_operation(operation: str) -> ErrorClassifier:
    return ErrorClassifier()
