"""Strict, bounded pagination and comment-record validation."""

from __future__ import annotations

import dataclasses
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
from http_transport import HTTPTransport
from platform_adapters import PlatformAdapter, TikTokAdapter
from pagination_state import CursorIntegrityChecker, PaginationMetadata, PaginationState, normalise_has_more
from errors.model import ScraperError, ErrorOutcome, ErrorSeverity

logger = logging.getLogger(__name__)

class PaginatorOutcome(Enum):
    COMPLETE = "COMPLETE"
    COMPLETE_SUSPECT = "COMPLETE_SUSPECT"
    EMPTY_UNCONFIRMED = "EMPTY_UNCONFIRMED"
    TRUNCATED_LIMIT = "TRUNCATED_LIMIT"
    FAILED_RESUMABLE = "FAILED_RESUMABLE"
    FAILED_PERMANENT = "FAILED_PERMANENT"
    INTERRUPTED = "INTERRUPTED"
    SCHEMA_CHANGED = "SCHEMA_CHANGED"
    PAGINATION_STALLED = "PAGINATION_STALLED"

    # Compatibility aliases retained for existing integrations.
    COMPLETED = "COMPLETE"
    PARTIAL = "TRUNCATED_LIMIT"
    FAILED = "FAILED_PERMANENT"
    STALLED = "PAGINATION_STALLED"

DEFAULT_MAX_BATCHES = 500
DEFAULT_MAX_COMMENTS = 10_000
DEFAULT_MAX_CONSECUTIVE_EMPTY = 12
DEFAULT_STALLED_TIMEOUT_SECONDS = 600
DEFAULT_ELAPSED_TIMEOUT_SECONDS = 900
DEFAULT_TERMINAL_CONFIRMATION_ATTEMPTS = 6
DEFAULT_TERMINAL_CONFIRMATION_INTERVAL_SECONDS = 5.0
MAX_CURSOR_VALUE = (2 ** 63) - 1
MAX_MALFORMED_RECORDS_IN_MEMORY = 1_000


class PaginationStop(Exception):
    """A caller-requested resource boundary reached while processing a page."""


@dataclass
class PaginatorResult:
    outcome: PaginatorOutcome
    metadata: PaginationMetadata
    error: Optional[ScraperError] = None
    comments_returned: int = 0
    malformed_comments: List[Any] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "outcome": self.outcome.name,
            "metadata": self.metadata.to_dict(),
            "error": self.error.to_dict() if self.error else None,
            "comments_returned": self.comments_returned,
            "malformed_comments_count": len(self.malformed_comments),
        }


class CommentPaginator:
    """Paginate with bounded recovery and repeated confirmation of terminal pages."""

    def __init__(
        self, *, video_id: str, run_id: str, worker_id: Optional[int] = None,
        max_batches: int = DEFAULT_MAX_BATCHES, max_comments: int = DEFAULT_MAX_COMMENTS,
        max_consecutive_empty: int = DEFAULT_MAX_CONSECUTIVE_EMPTY,
        stalled_timeout_seconds: int = DEFAULT_STALLED_TIMEOUT_SECONDS,
        elapsed_timeout_seconds: int = DEFAULT_ELAPSED_TIMEOUT_SECONDS,
        terminal_confirmation_attempts: int = DEFAULT_TERMINAL_CONFIRMATION_ATTEMPTS,
        terminal_confirmation_interval_seconds: float = DEFAULT_TERMINAL_CONFIRMATION_INTERVAL_SECONDS,
        transport: Optional[HTTPTransport] = None, retry_budget: Any = None,
        stop_event: Any = None, adapter: Optional[PlatformAdapter] = None,
    ):
        if isinstance(terminal_confirmation_attempts, bool) or not isinstance(terminal_confirmation_attempts, int):
            raise ValueError("terminal_confirmation_attempts must be an integer")
        limits = {
            "max_batches": max_batches, "max_comments": max_comments,
            "max_consecutive_empty": max_consecutive_empty,
            "stalled_timeout_seconds": stalled_timeout_seconds,
            "elapsed_timeout_seconds": elapsed_timeout_seconds,
            "terminal_confirmation_attempts": terminal_confirmation_attempts,
            "terminal_confirmation_interval_seconds": terminal_confirmation_interval_seconds,
        }
        invalid = [name for name, value in limits.items()
                   if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 1]
        if invalid:
            raise ValueError("Pagination limits must be positive: " + ", ".join(invalid))
        if not isinstance(video_id, (str, int)) or isinstance(video_id, bool):
            raise ValueError("resource identifier must be a non-empty string or integer")
        normalized_video_id = str(video_id).strip()
        if not normalized_video_id or len(normalized_video_id) > 128:
            raise ValueError("resource identifier must contain 1-128 characters")
        if not isinstance(run_id, str) or not run_id.strip():
            raise ValueError("run_id must be a non-empty string")

        self.video_id = normalized_video_id
        self.run_id = run_id.strip()
        self.worker_id = worker_id
        self.max_batches = int(max_batches)
        self.max_comments = int(max_comments)
        self.max_consecutive_empty = int(max_consecutive_empty)
        self.stalled_timeout_seconds = float(stalled_timeout_seconds)
        self.elapsed_timeout_seconds = float(elapsed_timeout_seconds)
        self.terminal_confirmation_attempts = int(terminal_confirmation_attempts)
        self.terminal_confirmation_interval_seconds = float(terminal_confirmation_interval_seconds)
        self.transport = transport or HTTPTransport()
        self.retry_budget = retry_budget
        self.stop_event = stop_event
        self.adapter = adapter or TikTokAdapter(
            transport=self.transport, video_id=self.video_id,
            run_id=self.run_id, retry_budget=self.retry_budget,
        )
        if not isinstance(self.adapter, PlatformAdapter):
            raise TypeError("adapter must implement PlatformAdapter")
        self.adapter.validate_resource_id(self.video_id)
        self.cursor_checker = CursorIntegrityChecker(
            max_cycle_detections=max(5000, self.max_batches + 1),
            max_consecutive_empty=self.max_consecutive_empty,
        )
        self.metadata = self._new_metadata(0)

    def _new_metadata(self, cursor: int) -> PaginationMetadata:
        return PaginationMetadata(
            video_id=self.video_id, run_id=self.run_id, worker_id=self.worker_id,
            requested_cursor=cursor, returned_cursor=cursor, batch_number=0,
            records_returned=0, valid_comments_extracted=0, duplicate_comments=0,
            malformed_rejected=0, has_more=True,
        )

    def run(
        self, *, initial_cursor: int = 0, mstoken: Any,
        on_comment: Optional[Callable[[dict], Any]] = None,
        on_batch: Optional[Callable[[dict], Any]] = None,
        on_malformed_comment: Optional[Callable[[Any, str], Any]] = None,
    ) -> PaginatorResult:
        if isinstance(initial_cursor, bool) or not isinstance(initial_cursor, int) or not 0 <= initial_cursor <= MAX_CURSOR_VALUE:
            raise ValueError("initial_cursor must be a non-negative 64-bit integer")

        self.metadata = self._new_metadata(initial_cursor)
        self.cursor_checker.reset(initial_cursor)
        started = time.monotonic()
        last_progress = started
        cursor = initial_cursor
        consecutive_empty = 0
        consecutive_no_new = 0
        terminal_page_fingerprint = None
        terminal_confirmation_count = 0
        malformed_records: List[Any] = []

        def remember_malformed(record: Any) -> None:
            # Every malformed record is persisted by the quarantine callback;
            # retain only a bounded sample in the in-memory run result.
            if len(malformed_records) < MAX_MALFORMED_RECORDS_IN_MEMORY:
                malformed_records.append(record)

        try:
            while True:
                if self.stop_event is not None and self.stop_event.is_set():
                    return self._finish(
                        PaginatorOutcome.INTERRUPTED, PaginationState.INTERRUPTED,
                        "run_stop_requested", malformed=malformed_records,
                    )
                if self.retry_budget is not None and not self.retry_budget.check_deadline():
                    reason = self.retry_budget.snapshot().get("exhausted_reason") or "recovery_budget_exhausted"
                    return self._finish(
                        PaginatorOutcome.FAILED_RESUMABLE, PaginationState.FAILED_RESUMABLE, reason,
                        self._error("RETRY_EXHAUSTED", "Per-video recovery budget exhausted", ErrorOutcome.RETRY_EXHAUSTED),
                    )
                if time.monotonic() - started >= self.elapsed_timeout_seconds:
                    return self._finish(PaginatorOutcome.TRUNCATED_LIMIT, PaginationState.TRUNCATED_LIMIT, "elapsed_timeout")
                if self.metadata.batch_number >= self.max_batches:
                    return self._finish(PaginatorOutcome.TRUNCATED_LIMIT, PaginationState.TRUNCATED_LIMIT, "max_batches_reached")
                remaining = self.max_comments - self.metadata.records_received
                if remaining <= 0:
                    return self._finish(PaginatorOutcome.TRUNCATED_LIMIT, PaginationState.TRUNCATED_LIMIT, "max_comments_reached")

                try:
                    current_token = mstoken() if callable(mstoken) else mstoken
                except Exception as token_error:
                    token_code = getattr(token_error, "error_code", "TOKEN_REFRESH_ERROR")
                    token_outcome = ErrorOutcome.RETRY_EXHAUSTED if token_code == "RETRY_EXHAUSTED" else ErrorOutcome.FAILED_RESUMABLE
                    return self._finish(
                        PaginatorOutcome.FAILED_RESUMABLE, PaginationState.FAILED_RESUMABLE,
                        "token_refresh_failed:" + token_code,
                        self._error(token_code, type(token_error).__name__, outcome=token_outcome),
                    )
                if (not isinstance(current_token, str) or not current_token or len(current_token) > 4096
                        or any(character in current_token for character in "\r\n")):
                    return self._finish(
                        PaginatorOutcome.FAILED_PERMANENT, PaginationState.FAILED_PERMANENT,
                        "token_unavailable",
                        self._error("TOKEN_REFRESH_ERROR", "Pagination token unavailable or malformed"),
                    )

                try:
                    batch = self._fetch_batch(cursor=cursor, mstoken=current_token)
                except ScraperError as error:
                    outcome = (PaginatorOutcome.FAILED_RESUMABLE
                               if error.outcome in (ErrorOutcome.FAILED_RESUMABLE, ErrorOutcome.RETRY_EXHAUSTED)
                               else PaginatorOutcome.FAILED_PERMANENT)
                    state = PaginationState.FAILED_RESUMABLE if outcome == PaginatorOutcome.FAILED_RESUMABLE else PaginationState.FAILED_PERMANENT
                    return self._finish(outcome, state, "transport_or_api_error:" + error.error_code, error)

                if not isinstance(batch, dict):
                    return self._schema_failure("response_not_object", "Comment API response was not an object")
                if batch.get("error"):
                    return self._finish(
                        PaginatorOutcome.FAILED_PERMANENT, PaginationState.FAILED_PERMANENT,
                        "api_error",
                        self._error("API_RESPONSE_ERROR", "Comment API returned an error status"),
                    )
                if batch.get("schema_error"):
                    return self._schema_failure(str(batch["schema_error"]), "Comment API response schema is missing or invalid")
                try:
                    has_more = normalise_has_more(batch.get("has_more"))
                except ValueError:
                    return self._schema_failure("malformed_has_more", "Comment API has_more metadata is missing or invalid")

                comments = batch.get("comments")
                if not isinstance(comments, list):
                    return self._schema_failure("malformed_comments", "Comment API comments field must be a list")
                next_cursor = batch.get("next_cursor")
                api_returned_cursor = next_cursor
                reported_total = batch.get("reported_total")
                page_size = batch.get("page_size", 50)
                if (reported_total is not None and
                        (isinstance(reported_total, bool) or not isinstance(reported_total, int)
                         or reported_total < 0)):
                    reported_total = None
                if (isinstance(page_size, bool) or not isinstance(page_size, int)
                        or page_size < 1 or page_size > 100):
                    page_size = 50
                # TikTok can report has_more=False before the end of the
                # offset-based list. Continue past that provisional signal
                # when this page contains records or the API's total says
                # there are still positions beyond the requested cursor.
                continue_pagination = has_more or bool(comments) or (
                    reported_total is not None and reported_total > cursor
                )
                page_fingerprint = tuple(
                    (
                        str(record.get("cid", "")),
                        str(record.get("text", "")),
                        str(record.get("create_time", "")),
                    )
                    for record in comments if isinstance(record, dict)
                )

                if continue_pagination:
                    terminal_page_fingerprint = None
                    terminal_confirmation_count = 0
                    candidate_cursor = next_cursor if has_more else cursor + page_size
                    if (isinstance(candidate_cursor, bool) or
                            not isinstance(candidate_cursor, int) or
                            not 0 <= candidate_cursor <= MAX_CURSOR_VALUE):
                        return self._finish(
                            PaginatorOutcome.PAGINATION_STALLED, PaginationState.PAGINATION_STALLED,
                            "missing_or_invalid_next_cursor",
                            self._error("PAGINATION_ERROR", "Pagination could not determine a valid advancing cursor", ErrorOutcome.FAILED_RESUMABLE),
                        )
                    next_cursor = candidate_cursor
                    can_continue, reason = self.cursor_checker.check_cursor_advancement(next_cursor)
                    if not can_continue:
                        code = "CURSOR_CYCLE" if "cycle" in reason else "CURSOR_STALLED"
                        return self._finish(
                            PaginatorOutcome.PAGINATION_STALLED, PaginationState.PAGINATION_STALLED,
                            reason, self._error(code, "Pagination cursor did not advance safely", ErrorOutcome.FAILED_RESUMABLE),
                        )
                else:
                    next_cursor = cursor
                    if terminal_page_fingerprint is None or page_fingerprint != terminal_page_fingerprint:
                        terminal_page_fingerprint = page_fingerprint
                        terminal_confirmation_count = 0
                    else:
                        terminal_confirmation_count += 1
                        logger.warning(
                            "TikTok terminal page repeated: video_id=%s cursor=%s "
                            "confirmation=%d/%d records=%d",
                            self.video_id, cursor, terminal_confirmation_count,
                            self.terminal_confirmation_attempts, len(comments),
                        )
                        if terminal_confirmation_count >= self.terminal_confirmation_attempts:
                            if self.metadata.records_received == 0:
                                return self._finish(
                                    PaginatorOutcome.EMPTY_UNCONFIRMED,
                                    PaginationState.EMPTY_UNCONFIRMED,
                                    "empty_result_after_repeated_terminal_confirmation",
                                    malformed=malformed_records,
                                )
                            return self._finish(
                                PaginatorOutcome.COMPLETE,
                                PaginationState.COMPLETE,
                                "terminal_page_stable_after_repeated_confirmation",
                                malformed=malformed_records,
                            )
                        if self.terminal_confirmation_interval_seconds > 0:
                            time.sleep(self.terminal_confirmation_interval_seconds)
                        continue

                retained = comments[:remaining]
                page_truncated = len(retained) < len(comments)
                accepted = 0
                duplicate_count = 0
                missing_id_count = 0
                malformed_count = 0
                usable_comment_records = 0
                for record in retained:
                    if not isinstance(record, dict):
                        reason = "comment_record_not_object"
                        remember_malformed(record)
                        malformed_count += 1
                        if on_malformed_comment is not None:
                            try:
                                on_malformed_comment(record, reason)
                            except Exception as quarantine_error:
                                return self._finish(
                                    PaginatorOutcome.FAILED_RESUMABLE, PaginationState.FAILED_RESUMABLE,
                                    "malformed_comment_quarantine_failed",
                                    self._error("STORAGE_ERROR", type(quarantine_error).__name__, outcome=ErrorOutcome.FAILED_RESUMABLE),
                                    malformed_records,
                                )
                        continue
                    if not self.adapter.is_comment_record(record):
                        reason = "unrecognized_comment_schema"
                        remember_malformed(record)
                        malformed_count += 1
                        if on_malformed_comment is not None:
                            try:
                                on_malformed_comment(record, reason)
                            except Exception as quarantine_error:
                                return self._finish(
                                    PaginatorOutcome.FAILED_RESUMABLE, PaginationState.FAILED_RESUMABLE,
                                    "malformed_comment_quarantine_failed",
                                    self._error("STORAGE_ERROR", type(quarantine_error).__name__, outcome=ErrorOutcome.FAILED_RESUMABLE),
                                    malformed_records,
                                )
                        continue
                    if not self.adapter.has_minimum_content(record):
                        reason = "comment_text_missing_or_invalid"
                        remember_malformed(record)
                        malformed_count += 1
                        if on_malformed_comment is not None:
                            try:
                                on_malformed_comment(record, reason)
                            except Exception as quarantine_error:
                                return self._finish(
                                    PaginatorOutcome.FAILED_RESUMABLE, PaginationState.FAILED_RESUMABLE,
                                    "malformed_comment_quarantine_failed",
                                    self._error("STORAGE_ERROR", type(quarantine_error).__name__, outcome=ErrorOutcome.FAILED_RESUMABLE),
                                    malformed_records,
                                )
                        continue
                    usable_comment_records += 1
                    if not self.adapter.has_comment_id(record):
                        missing_id_count += 1
                    try:
                        callback_result = on_comment(record) if on_comment is not None else True
                    except PaginationStop as stop:
                        stop_reason = str(stop) or "caller_resource_limit_reached"
                        if stop_reason == "keyboard_interrupt":
                            return self._finish(
                                PaginatorOutcome.INTERRUPTED, PaginationState.INTERRUPTED,
                                stop_reason, malformed=malformed_records,
                            )
                        return self._finish(
                            PaginatorOutcome.TRUNCATED_LIMIT, PaginationState.TRUNCATED_LIMIT,
                            stop_reason, malformed=malformed_records,
                        )
                    except Exception as callback_error:
                        return self._finish(
                            PaginatorOutcome.FAILED_PERMANENT, PaginationState.FAILED_PERMANENT,
                            "comment_callback_failed",
                            self._error("INVALID_COMMENT_DATA", type(callback_error).__name__),
                            malformed_records,
                        )
                    if callback_result is False:
                        duplicate_count += 1
                    else:
                        accepted += 1

                # Count the full API page separately from records retained
                # under the configured cap. A truncated page is revisited from
                # the last durable cursor on resume.
                records_received = len(comments)
                records_retained = len(retained)
                self.metadata = dataclasses.replace(
                    self.metadata,
                    requested_cursor=cursor,
                    returned_cursor=next_cursor,
                    batch_number=self.metadata.batch_number + 1,
                    records_received=self.metadata.records_received + records_received,
                    records_returned=self.metadata.records_returned + accepted,
                    valid_comments_extracted=self.metadata.valid_comments_extracted + accepted,
                    duplicate_comments=self.metadata.duplicate_comments + duplicate_count,
                    missing_comment_ids=self.metadata.missing_comment_ids + missing_id_count,
                    malformed_rejected=self.metadata.malformed_rejected + malformed_count,
                    has_more=has_more,
                    timestamp=datetime.now(timezone.utc),
                )

                if records_received > 0 and usable_comment_records == 0:
                    return self._schema_failure(
                        "comment_schema_unrecognized",
                        "No returned comment record matched the required comment data structure",
                        malformed_records,
                    )

                if records_received == 0:
                    consecutive_empty += 1
                else:
                    consecutive_empty = 0
                if records_received > 0 and accepted == 0:
                    consecutive_no_new += 1
                elif accepted > 0:
                    consecutive_no_new = 0

                self.metadata = dataclasses.replace(
                    self.metadata,
                    consecutive_empty_batches=consecutive_empty,
                    pages_without_new_comments=consecutive_no_new,
                )

                logger.info(
                    "Pagination page received: video_id=%s batch=%d requested_cursor=%s "
                    "api_returned_cursor=%s next_cursor=%s has_more=%s records=%d "
                    "accepted=%d duplicates=%d consecutive_no_new=%d reported_total=%s "
                    "continue_after_page=%s",
                    self.video_id, self.metadata.batch_number, cursor, api_returned_cursor,
                    next_cursor, has_more, records_received, accepted, duplicate_count,
                    consecutive_no_new, reported_total, continue_pagination,
                )

                checkpoint_cursor = cursor if page_truncated else next_cursor
                batch_info = {
                    "comments": retained,
                    "requested_cursor": cursor,
                    "next_cursor": next_cursor,
                    "checkpoint_cursor": checkpoint_cursor,
                    "has_more": has_more,
                    "batch_number": self.metadata.batch_number,
                    "page_truncated": page_truncated,
                    "records_received": records_received,
                    "records_retained": records_retained,
                    "valid_comments_extracted": accepted,
                    "duplicate_comments": duplicate_count,
                    "missing_comment_ids": missing_id_count,
                    "malformed_rejected": malformed_count,
                    "malformed_comments": list(malformed_records),
                    "metadata": self.metadata,
                }
                if on_batch is not None:
                    try:
                        on_batch(batch_info)
                    except Exception as callback_error:
                        return self._finish(
                            PaginatorOutcome.FAILED_RESUMABLE, PaginationState.FAILED_RESUMABLE,
                            "batch_callback_failed",
                            self._error("STORAGE_ERROR", type(callback_error).__name__, ErrorOutcome.FAILED_RESUMABLE),
                            malformed_records,
                        )

                if page_truncated:
                    return self._finish(PaginatorOutcome.TRUNCATED_LIMIT, PaginationState.TRUNCATED_LIMIT,
                                        "max_comments_reached", malformed=malformed_records)

                if records_received == 0 and continue_pagination:
                    if consecutive_empty >= self.max_consecutive_empty:
                        return self._finish(
                            PaginatorOutcome.PAGINATION_STALLED, PaginationState.PAGINATION_STALLED,
                            "empty_page_with_more",
                            self._error("EMPTY_PAGE_WITH_MORE", "Repeated empty pages claimed additional data was available", ErrorOutcome.FAILED_RESUMABLE),
                            malformed_records,
                        )
                if records_received > 0 and accepted == 0 and continue_pagination and consecutive_no_new >= self.max_consecutive_empty:
                    return self._finish(
                        PaginatorOutcome.PAGINATION_STALLED, PaginationState.PAGINATION_STALLED,
                        "no_new_comments_across_pages",
                        self._error("PAGINATION_ERROR", "Consecutive pages produced no new accepted comments", ErrorOutcome.FAILED_RESUMABLE),
                        malformed_records,
                    )
                if time.monotonic() - last_progress >= self.stalled_timeout_seconds:
                    return self._finish(PaginatorOutcome.PAGINATION_STALLED, PaginationState.PAGINATION_STALLED,
                                        "stalled_timeout",
                                        self._error("CURSOR_STALLED", "Pagination made no accepted progress before the stall deadline", ErrorOutcome.FAILED_RESUMABLE),
                                        malformed_records)
                if accepted > 0:
                    last_progress = time.monotonic()

                if not continue_pagination:
                    logger.info(
                        "TikTok pagination terminal candidate: video_id=%s cursor=%s "
                        "records=%d reported_total=%s; confirming same cursor",
                        self.video_id, cursor, records_received, reported_total,
                    )
                    if self.terminal_confirmation_interval_seconds > 0:
                        time.sleep(self.terminal_confirmation_interval_seconds)
                    continue

                cursor = next_cursor

        except KeyboardInterrupt:
            return self._finish(PaginatorOutcome.INTERRUPTED, PaginationState.INTERRUPTED,
                                "user_interrupt", malformed=malformed_records)
        except Exception as error:
            logger.exception("Unexpected pagination failure")
            return self._finish(
                PaginatorOutcome.FAILED_PERMANENT, PaginationState.FAILED_PERMANENT,
                "unexpected_pagination_error",
                self._error("UNEXPECTED_INTERNAL_ERROR", type(error).__name__),
                malformed_records,
            )

    def _schema_failure(
        self, reason: str, explanation: str, malformed: Optional[List[Any]] = None
    ) -> PaginatorResult:
        return self._finish(
            PaginatorOutcome.SCHEMA_CHANGED, PaginationState.SCHEMA_CHANGED, reason,
            self._error("API_SCHEMA_CHANGED", explanation, outcome=ErrorOutcome.SCHEMA_CHANGED),
            malformed,
        )

    def _fetch_batch(self, *, cursor: int, mstoken: str) -> Dict[str, Any]:
        """Delegate platform I/O and response translation to the selected adapter."""
        return self.adapter.fetch_page(cursor=cursor, token=mstoken)

    def _error(
        self, code: str, explanation: str, http_status: Optional[int] = None,
        outcome: ErrorOutcome = ErrorOutcome.FAILED_PERMANENT,
    ) -> ScraperError:
        return ScraperError(
            error_code=code, severity=ErrorSeverity.ERROR,
            component="comment_paginator", function="pagination",
            video_id=self.video_id, run_id=self.run_id,
            batch_number=self.metadata.batch_number,
            cursor=str(self.metadata.requested_cursor),
            http_status=http_status, explanation=explanation, outcome=outcome,
        )

    def _finish(
        self, outcome: PaginatorOutcome, state: PaginationState, reason: str,
        error: Optional[ScraperError] = None, malformed: Optional[List[Any]] = None,
    ) -> PaginatorResult:
        self.metadata = dataclasses.replace(
            self.metadata, outcome=state, stop_reason=reason,
            timestamp=datetime.now(timezone.utc),
        )
        return PaginatorResult(
            outcome=outcome, metadata=self.metadata, error=error,
            comments_returned=self.metadata.records_returned,
            malformed_comments=list(malformed or []),
        )


def paginate_comments(
    *, video_id: str, run_id: str, mstoken: Any,
    max_batches: int = DEFAULT_MAX_BATCHES, max_comments: int = DEFAULT_MAX_COMMENTS,
    max_consecutive_empty: int = DEFAULT_MAX_CONSECUTIVE_EMPTY,
    stalled_timeout_seconds: int = DEFAULT_STALLED_TIMEOUT_SECONDS,
    elapsed_timeout_seconds: int = DEFAULT_ELAPSED_TIMEOUT_SECONDS,
    terminal_confirmation_attempts: int = DEFAULT_TERMINAL_CONFIRMATION_ATTEMPTS,
    terminal_confirmation_interval_seconds: float = DEFAULT_TERMINAL_CONFIRMATION_INTERVAL_SECONDS,
    initial_cursor: int = 0, on_comment: Optional[Callable[[dict], Any]] = None,
    on_batch: Optional[Callable[[dict], Any]] = None,
    on_malformed_comment: Optional[Callable[[Any, str], Any]] = None,
    transport: Optional[HTTPTransport] = None, retry_budget: Any = None,
    stop_event: Any = None,
) -> PaginatorResult:
    paginator = CommentPaginator(
        video_id=video_id, run_id=run_id, max_batches=max_batches,
        max_comments=max_comments, max_consecutive_empty=max_consecutive_empty,
        stalled_timeout_seconds=stalled_timeout_seconds,
        elapsed_timeout_seconds=elapsed_timeout_seconds,
        terminal_confirmation_attempts=terminal_confirmation_attempts,
        terminal_confirmation_interval_seconds=terminal_confirmation_interval_seconds,
        transport=transport,
        retry_budget=retry_budget, stop_event=stop_event,
    )
    return paginator.run(
        initial_cursor=initial_cursor, mstoken=mstoken,
        on_comment=on_comment, on_batch=on_batch,
        on_malformed_comment=on_malformed_comment,
    )
