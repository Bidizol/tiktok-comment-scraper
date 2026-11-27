"""Diagnostics recording for failed HTTP or token operations.

For every failed operation, records:
- UTC timestamp.
- Run ID.
- Component and function.
- Error code.
- HTTP status, where available.
- Retry attempt.
- Recovery action.
- Final outcome.

Never records secrets.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# Import error taxonomy - use try/except for compatibility with
# Phase 1's relative-import issues in errors/classifiers.py
try:
    from errors.model import ScraperError, ErrorSeverity, ErrorOutcome
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

    class ScraperError:
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

logger = logging.getLogger(__name__)


class DiagnosticsRecorder:
    """Records diagnostics for failed HTTP or token operations.

    Every failed operation gets a structured diagnostic record that includes:
    - UTC timestamp
    - Run ID (unique per execution)
    - Component and function where the error occurred
    - Error code from the taxonomy
    - HTTP status, where available
    - Retry attempt number
    - Recovery action taken
    - Final outcome

    No secrets (tokens, passwords, etc.) are ever recorded.

    Persistence:
    - Records can be serialized to JSONL format for storage across restarts.
    - A single shared recorder instance should be used for consistent persistence.
    - Writes are atomic per-record to prevent corruption.
    - Failed write of a single record does not destroy previously recorded records.
    """

    def __init__(
        self,
        run_id: Optional[str] = None,
        jsonl_path: Optional[str] = None,
    ):
        self.run_id = run_id or str(uuid.uuid4())
        self.jsonl_path = jsonl_path
        self.records: List[Dict[str, Any]] = []
        # Track if we've written to JSONL to avoid duplicate writes
        # (each record triggers auto-write, but we only write once per line)
        self._written_indices: set = set()

    def record_error(
        self,
        component: str,
        function: str,
        error: ScraperError,
        retry_attempt: int = 0,
        recovery_action: Optional[str] = None,
        final_outcome: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Record a diagnostics entry for a failed operation.

        Args:
            component: The component where the error occurred (e.g., "http_transport", "token_manager")
            function: The function where the error occurred
            error: The ScraperError instance containing all classification data
            retry_attempt: The retry attempt number (0 if no retries)
            recovery_action: The recovery action taken, or None
            final_outcome: The final outcome (success, failed_permanent, retry_exhausted, etc.)

        Returns:
            The diagnostics record dictionary.
        """
        record = {
            "run_id": self.run_id,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "component": component,
            "function": function,
            "error_code": error.error_code,
            "severity": getattr(error.severity, "name", error.severity),
            "http_status": error.http_status,
            "retry_attempt": retry_attempt,
            "recovery_action": recovery_action,
            "final_outcome": getattr(
                final_outcome or error.outcome,
                "name",
                final_outcome or error.outcome,
            ),
            "explanation": error.explanation,
            "video_id": error.video_id,
            "worker_id": error.worker_id,
            "batch_number": error.batch_number,
            "cursor": error.cursor,
        }

        # Ensure no secrets are in the explanation or any field
        self._sanitize_record(record)

        self.records.append(record)

        # Automatically append to JSONL file if path is configured
        self._auto_write_jsonl(record, len(self.records) - 1)

        logger.debug("Diagnostics recorded: %s", record["error_code"])
        return record

    def _auto_write_jsonl(self, record: Dict[str, Any], index: int) -> None:
        """Append a single record to JSONL file atomically.

        Writes are per-record (single line) to prevent corruption.
        A failed write for one record does not destroy previously written records.
        """
        if not self.jsonl_path:
            return

        # Only write each record once (track by index)
        if index in self._written_indices:
            return

        try:
            # Format the record as a single JSONL line
            serializable = {
                k: v for k, v in record.items() if not k.startswith("_")
            }
            line = json.dumps(serializable, ensure_ascii=False) + "\n"

            # Read existing content if file exists, then append
            existing_lines: List[str] = []
            try:
                with open(self.jsonl_path, "r", encoding="utf-8") as f:
                    existing_lines = f.readlines()
            except FileNotFoundError:
                pass  # File doesn't exist yet - that's OK
            except PermissionError:
                logger.warning("Permission denied writing diagnostics JSONL: %s", self.jsonl_path)
                return
            except OSError as e:
                logger.warning("OS error reading diagnostics JSONL: %s", e)
                return

            # Append new line
            existing_lines.append(line)

            # Write all lines to temp file, then replace (atomic on POSIX)
            temp_path = self.jsonl_path + ".tmp"
            try:
                with open(temp_path, "w", encoding="utf-8") as f:
                    f.writelines(existing_lines)
                os.replace(temp_path, self.jsonl_path)
            except PermissionError:
                logger.warning("Permission denied renaming diagnostics JSONL: %s", self.jsonl_path)
            except OSError as e:
                logger.warning("OS error renaming diagnostics JSONL: %s", e)
            except Exception as e:
                logger.warning("Unexpected error writing diagnostics JSONL: %s", e)

            # Mark this index as written
            self._written_indices.add(index)

        except Exception as e:
            # Never let a write failure destroy existing records or crash the caller
            logger.warning("Unexpected error writing diagnostics JSONL: %s", e)

    def get_records(self) -> List[Dict[str, Any]]:
        """Return all recorded diagnostics records."""
        return self.records

    def to_jsonl(self) -> str:
        """Convert all records to JSONL format for storage or transmission."""
        lines = []
        for record in self.records:
            # Remove internal metadata before serialization
            serializable = {k: v for k, v in record.items() if not k.startswith("_")}
            lines.append(json.dumps(serializable, ensure_ascii=False))
        return "\n".join(lines)

    def _sanitize_record(self, record: Dict[str, Any]) -> None:
        """Ensure no sensitive data appears in the diagnostics record.

        This is a safety net to ensure tokens, passwords, or other
        credentials never leak into diagnostics.
        """
        # These fields should never contain sensitive data, but sanitize anyway
        sensitive_fields = ["explanation"]
        for field in sensitive_fields:
            if field in record and record[field]:
                # Remove any potential token-like patterns
                record[field] = self._redact_sensitive(record[field])

        # Worker ID should be an integer or None, not a token
        if "worker_id" in record and record["worker_id"] is not None:
            if not isinstance(record["worker_id"], int):
                record["worker_id"] = None

        # Video ID should be a valid format or None
        if "video_id" in record and record["video_id"]:
            # Basic validation - video IDs are typically numeric or alphanumeric
            # but not token-like
            if len(record["video_id"]) > 50:
                record["video_id"] = None

    @staticmethod
    def _redact_sensitive(text: str) -> str:
        """Redact sensitive patterns from text.

        This is a safety measure for the explanation field.
        """
        import re
        # Remove patterns that look like tokens/keys
        patterns = [
            r'[a-zA-Z0-9]{40,}',  # Very long alphanumeric strings (potential tokens)
            r'token=[^&\s]+',     # token=... in URLs
            r'key=[^&\s]+',       # key=... in URLs
            r'sb_publishable_[a-zA-Z0-9_-]+',  # Supabase publishable keys
        ]
        result = text
        for pattern in patterns:
            result = re.sub(pattern, '[REDACTED]', result)
        return result

    def record_from_exception(
        self,
        component: str,
        function: str,
        error: Exception,
        retry_attempt: int = 0,
        recovery_action: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Record diagnostics from a generic Exception.

        Converts the exception to a ScraperError classification
        and records the diagnostics.

        Args:
            component: The component where the error occurred
            function: The function where the error occurred
            error: The exception instance
            retry_attempt: The retry attempt number

        Returns:
            The diagnostics record dictionary.
        """
        # Classify the error into our taxonomy
        try:
            from errors.classifiers import ErrorClassification
            classification = self._classify_exception(error)
        except ImportError:
            # Fallback classification
            classification = ErrorClassification(
                error_code="UNEXPECTED_INTERNAL_ERROR",
                severity="ERROR" if "ErrorSeverity" not in dir() else ErrorSeverity.ERROR,
                explanation=str(error),
                default_outcome="failed_permanent",
                http_status=0,
            )

        # Use the fallback ScraperError if the real one isn't available
        try:
            from errors.model import ScraperError as RealScraperError
            scraper_error = RealScraperError(
                error_code=classification.error_code,
                severity=classification.severity,
                component=component,
                function=function,
                explanation=classification.explanation,
                http_status=classification.http_status,
                outcome=classification.default_outcome,
                video_id=getattr(error, 'video_id', None),
                worker_id=getattr(error, 'worker_id', None),
                batch_number=getattr(error, 'batch_number', None),
                cursor=getattr(error, 'cursor', None),
                retry_attempt=retry_attempt,
                recovery_action=recovery_action,
            )
        except ImportError:
            # Create a minimal scraper error using the fallback class
            scraper_error = type('ScraperError', (), {
                'error_code': classification.error_code,
                'severity': classification.severity,
                'component': component,
                'function': function,
                'explanation': classification.explanation,
                'http_status': classification.http_status,
                'outcome': classification.default_outcome,
                'video_id': getattr(error, 'video_id', None),
                'worker_id': getattr(error, 'worker_id', None),
                'batch_number': getattr(error, 'batch_number', None),
                'cursor': getattr(error, 'cursor', None),
                'retry_attempt': retry_attempt,
                'recovery_action': recovery_action,
            })()

        return self.record_error(
            component=component,
            function=function,
            error=scraper_error,
            retry_attempt=retry_attempt,
            recovery_action=recovery_action,
            final_outcome=scraper_error.outcome,
        )

    def _classify_exception(self, error: Exception) -> ErrorClassification:
        """Classify a generic exception into the error taxonomy."""
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
                severity="ERROR",
                explanation="Operation authentication failed",
                default_outcome="failed_permanent",
                http_status=401,
            )
        if has_api:
            return ErrorClassification(
                error_code="RATE_LIMITED",
                severity="WARNING",
                explanation="Operation rate-limited by API",
                default_outcome="failed_resumable",
                http_status=429,
            )
        if has_network:
            return ErrorClassification(
                error_code="NETWORK_ERROR",
                severity="WARNING",
                explanation="Operation network failure",
                default_outcome="failed_resumable",
                http_status=0,
            )

        # Default
        return ErrorClassification(
            error_code="UNEXPECTED_INTERNAL_ERROR",
            severity="ERROR",
            explanation="Unknown error occurred",
            default_outcome="failed_permanent",
            http_status=0,
        )
