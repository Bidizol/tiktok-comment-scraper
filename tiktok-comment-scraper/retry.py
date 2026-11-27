"""Bounded retry policies with detailed recording for the TikTok Comment Scraper.

Each retry records:
- Error classification.
- Attempt number.
- Maximum attempts.
- Delay or backoff decision.
- Recovery action.
- Recovery outcome.

Permanent failures are not retried indefinitely.
Rate-limit responses are handled conservatively.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Callable, TypeVar

# Try to import from errors module, fall back to standalone definitions
try:
    from errors.model import ScraperError, ErrorSeverity, ErrorOutcome
    from errors.classifiers import ErrorClassification
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

logger = logging.getLogger(__name__)

#: Maximum default retry attempts
MAX_DEFAULT_RETRIES = 3
#: Default backoff factor (exponential backoff: delay = factor * 2^(attempt-1))
DEFAULT_BACKOFF_FACTOR = 1.0
#: Maximum backoff delay in seconds
MAX_BACKOFF_DELAY = 60
#: Jitter range as fraction of calculated delay
JITTER_FRACTION = 0.1

T = TypeVar("T")


@dataclass
class RetryRecord:
    """A single retry attempt record for diagnostics.

    Every retry must record these fields for traceability.
    """
    error_code: str
    attempt_number: int
    maximum_attempts: int
    delay_seconds: Optional[float] = None
    backoff_decision: Optional[str] = None
    recovery_action: Optional[str] = None
    recovery_outcome: Optional[str] = None


@dataclass
class RetryPolicy:
    """Configuration for a bounded retry policy.

    Defines the maximum attempts, backoff strategy, and which
    error types are retryable vs permanent.
    """
    maximum_attempts: int = MAX_DEFAULT_RETRIES
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR
    max_backoff_delay: float = MAX_BACKOFF_DELAY
    jitter_fraction: float = JITTER_FRACTION
    retryable_error_codes: tuple = field(default_factory=lambda: (
        "RATE_LIMITED",
        "NETWORK_ERROR",
        "TIMEOUT_ERROR",
    ))
    permanent_error_codes: tuple = field(default_factory=lambda: (
        "AUTHENTICATION_ERROR",
        "PROXY_AUTHENTICATION_ERROR",
        "UNEXPECTED_INTERNAL_ERROR",
    ))


class BoundedRetry:
    """Bounded retry policy with detailed recording.

    Implements exponential backoff with jitter and classifies
    each retry attempt for diagnostics.
    """

    def __init__(self, policy: Optional[RetryPolicy] = None):
        self.policy = policy or RetryPolicy()
        self.records: List[RetryRecord] = []

    def should_retry(
        self,
        error_code: str,
        attempt: int,
    ) -> bool:
        """Determine if a retry should be attempted.

        Permanent errors are never retried.
        Rate-limited errors are retried conservatively.
        Network/timeout errors are retried with backoff.
        """
        # Check if we've exhausted attempts
        if attempt >= self.policy.maximum_attempts:
            logger.debug("Not retrying: maximum attempts (%d) reached", self.policy.maximum_attempts)
            return False

        # Permanent errors - never retry
        if error_code in self.policy.permanent_error_codes:
            logger.debug("Not retrying: permanent error %s", error_code)
            return False

        # Retryable errors
        if error_code in self.policy.retryable_error_codes:
            logger.debug("Will retry: retryable error %s (attempt %d/%d)", error_code, attempt + 1, self.policy.maximum_attempts)
            return True

        # Default: don't retry unknown errors
        logger.debug("Not retrying: unknown error %s", error_code)
        return False

    def calculate_delay(self, attempt: int) -> float:
        """Calculate the delay before the next retry attempt.

        Uses exponential backoff: delay = factor * 2^(attempt-1)
        With jitter and maximum cap.
        """
        # Exponential backoff
        delay = self.policy.backoff_factor * (2 ** (attempt - 1))

        # Cap at maximum
        if delay > self.policy.max_backoff_delay:
            delay = self.policy.max_backoff_delay

        # Add jitter (± fraction of delay)
        import random
        jitter = delay * self.policy.jitter_fraction * (
            2 * random.random() - 1
        )
        delay += jitter

        # Ensure non-negative
        delay = max(0, delay)

        logger.debug("Retry delay calculation: attempt=%d, delay=%.2fs", attempt, delay)
        return delay

    def execute(
        self,
        operation: Callable[[], T],
        error_classifier: Optional[Any] = None,
    ) -> Tuple[Optional[T], Optional[List[RetryRecord]]]:
        """Execute an operation with bounded retry.

        Args:
            operation: Callable that returns a result or raises an exception.
            error_classifier: Optional classifier to determine error codes from exceptions.

        Returns:
            (result, records) - result if successful, and list of retry records.
            If all attempts fail, result is None and records contain the retry history.
        """
        self.records = []
        attempt = 0

        while True:
            attempt += 1

            try:
                result = operation()
                return result, self.records

            except Exception as error:
                # Classify the error
                if error_classifier:
                    classification = error_classifier.classify(error)
                else:
                    classification = self._classify_error(error)

                # Record this retry attempt
                record = RetryRecord(
                    error_code=classification.error_code,
                    attempt_number=attempt,
                    maximum_attempts=self.policy.maximum_attempts,
                    delay_seconds=None,  # Will be set if we retry
                    backoff_decision=self._determine_backoff(classification),
                    recovery_action=self._determine_recovery_action(classification),
                    recovery_outcome=None,  # Set after retry decision
                )
                self.records.append(record)

                # Determine if we should retry
                if not self.should_retry(classification.error_code, attempt):
                    # No more retries - record final outcome
                    record.recovery_outcome = "exhausted"
                    record.recovery_action = "none"
                    logger.warning(
                        "Retry exhausted: %s after %d attempts (error: %s)",
                        classification.explanation, attempt, classification.error_code
                    )
                    return None, self.records

                # Calculate delay and wait
                delay = self.calculate_delay(attempt)
                record.delay_seconds = delay
                record.backoff_decision = f"exponential_backoff_{delay:.1f}s"

                logger.info(
                    "Retry %d/%d: %s - waiting %.1fs before next attempt",
                    attempt,
                    self.policy.maximum_attempts,
                    classification.explanation,
                    delay,
                )

                # Sleep before next attempt
                time.sleep(delay)

    def _classify_error(self, error: Exception) -> ErrorClassification:
        """Classify a generic error into the error taxonomy."""
        error_str = str(error).lower()

        # Authentication indicators
        auth_indicators = ["unauthorized", "invalid", "authentication", "forbidden", "401", "403"]
        # Network indicators
        network_indicators = ["timeout", "connection", "dns", "tls", "refused"]
        # API/rate limit indicators
        api_indicators = ["rate limit", "429", "quota", "too many requests"]

        has_auth = any(indicator in error_str for indicator in auth_indicators)
        has_network = any(indicator in error_str for indicator in network_indicators)
        has_api = any(indicator in error_str for indicator in api_indicators)

        if has_auth:
            return ErrorClassification(
                error_code="AUTHENTICATION_ERROR",
                severity=ErrorSeverity.ERROR,
                explanation="Operation authentication failed",
                default_outcome=ErrorOutcome.FAILED_PERMANENT,
                http_status=401,
            )
        if has_api:
            return ErrorClassification(
                error_code="RATE_LIMITED",
                severity=ErrorSeverity.WARNING,
                explanation="Operation rate-limited by API",
                default_outcome=ErrorOutcome.FAILED_RESUMABLE,
                http_status=429,
            )
        if has_network:
            return ErrorClassification(
                error_code="NETWORK_ERROR",
                severity=ErrorSeverity.WARNING,
                explanation="Operation network failure",
                default_outcome=ErrorOutcome.FAILED_RESUMABLE,
                http_status=0,
            )

        # Default
        return ErrorClassification(
            error_code="UNEXPECTED_INTERNAL_ERROR",
            severity=ErrorSeverity.ERROR,
            explanation="Unknown error occurred",
            default_outcome=ErrorOutcome.FAILED_PERMANENT,
            http_status=0,
        )

    def _determine_backoff(self, classification: ErrorClassification) -> Optional[str]:
        """Determine the backoff decision based on error classification."""
        if classification.http_status == 429:
            return "rate_limit_backoff"
        if classification.error_code in ("NETWORK_ERROR", "TIMEOUT_ERROR"):
            return "exponential_backoff"
        return "none"

    def _determine_recovery_action(self, classification: ErrorClassification) -> Optional[str]:
        """Determine the recovery action based on error classification."""
        if classification.http_status == 429:
            return "respect_rate_limit"
        if classification.severity == ErrorSeverity.ERROR:
            return "auth_retry"
        if classification.error_code in ("NETWORK_ERROR", "TIMEOUT_ERROR"):
            return "network_retry"
        return "standard_retry"
