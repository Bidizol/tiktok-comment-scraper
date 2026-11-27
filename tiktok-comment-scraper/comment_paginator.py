"""Strict, bounded pagination and comment-record validation."""

from __future__ import annotations

import dataclasses
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlencode

from http_transport import HTTPTransport, HTTPTransportError
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
DEFAULT_MAX_CONSECUTIVE_EMPTY = 3
DEFAULT_STALLED_TIMEOUT_SECONDS = 300
DEFAULT_ELAPSED_TIMEOUT_SECONDS = 600
MAX_CURSOR_VALUE = (2 ** 63) - 1
MAX_MALFORMED_RECORDS_IN_MEMORY = 1_000


class PaginationStop(Exception):
    """A caller-requested resource boundary reached while processing a page."""


# A comment may legitimately omit cid; text is sufficient to preserve it as a
# comment record while the missing identifier is tracked independently.
COMMENT_SCHEMA_KEYS = frozenset({
    "cid", "text", "create_time", "user", "user_id", "aweme_id",
    "reply_id", "digg_count", "status", "stick_position", "reply_comment",
})

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
    """Only a validated has_more=False response can end pagination normally."""

    def __init__(
        self, *, video_id: str, run_id: str, worker_id: Optional[int] = None,
        max_batches: int = DEFAULT_MAX_BATCHES, max_comments: int = DEFAULT_MAX_COMMENTS,
        max_consecutive_empty: int = DEFAULT_MAX_CONSECUTIVE_EMPTY,
        stalled_timeout_seconds: int = DEFAULT_STALLED_TIMEOUT_SECONDS,
        elapsed_timeout_seconds: int = DEFAULT_ELAPSED_TIMEOUT_SECONDS,
        transport: Optional[HTTPTransport] = None, retry_budget: Any = None,
        stop_event: Any = None,
    ):
        limits = {
            "max_batches": max_batches, "max_comments": max_comments,
            "max_consecutive_empty": max_consecutive_empty,
            "stalled_timeout_seconds": stalled_timeout_seconds,
            "elapsed_timeout_seconds": elapsed_timeout_seconds,
        }
        invalid = [name for name, value in limits.items()
                   if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 1]
        if invalid:
            raise ValueError("Pagination limits must be positive: " + ", ".join(invalid))
        if not isinstance(video_id, (str, int)) or isinstance(video_id, bool):
            raise ValueError("video_id must be a non-empty TikTok video identifier")
        normalized_video_id = str(video_id).strip()
        if not normalized_video_id.isdigit() or len(normalized_video_id) > 32:
            raise ValueError("video_id must contain 1-32 decimal digits")
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
        self.transport = transport or HTTPTransport()
        self.retry_budget = retry_budget
        self.stop_event = stop_event
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

    @staticmethod
    def _comment_shape_is_recognized(comment: dict) -> bool:
        return bool(COMMENT_SCHEMA_KEYS.intersection(comment.keys()))

    @staticmethod
    def _has_comment_id(comment: dict) -> bool:
        cid = comment.get("cid")
        if isinstance(cid, bool):
            return False
        if isinstance(cid, int):
            return cid >= 0
        return isinstance(cid, str) and bool(cid.strip())

    @staticmethod
    def _has_minimum_content(comment: dict) -> bool:
        if "text" not in comment:
            return False
        value = comment.get("text")
        return isinstance(value, (str, int, float)) and not isinstance(value, bool)

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

                if has_more:
                    if isinstance(next_cursor, bool) or not isinstance(next_cursor, int) or not 0 <= next_cursor <= MAX_CURSOR_VALUE:
                        return self._finish(
                            PaginatorOutcome.PAGINATION_STALLED, PaginationState.PAGINATION_STALLED,
                            "missing_or_invalid_returned_cursor",
                            self._error("PAGINATION_ERROR", "API indicated more pages but returned no valid cursor", ErrorOutcome.FAILED_RESUMABLE),
                        )
                    can_continue, reason = self.cursor_checker.check_cursor_advancement(next_cursor)
                    if not can_continue:
                        code = "CURSOR_CYCLE" if "cycle" in reason else "CURSOR_STALLED"
                        return self._finish(
                            PaginatorOutcome.PAGINATION_STALLED, PaginationState.PAGINATION_STALLED,
                            reason, self._error(code, "Pagination cursor did not advance safely", ErrorOutcome.FAILED_RESUMABLE),
                        )
                else:
                    next_cursor = cursor

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
                    if not self._comment_shape_is_recognized(record):
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
                    if not self._has_minimum_content(record):
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
                    if not self._has_comment_id(record):
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

                if records_received == 0 and has_more:
                    if consecutive_empty >= self.max_consecutive_empty:
                        return self._finish(
                            PaginatorOutcome.PAGINATION_STALLED, PaginationState.PAGINATION_STALLED,
                            "empty_page_with_more",
                            self._error("EMPTY_PAGE_WITH_MORE", "Repeated empty pages claimed additional data was available", ErrorOutcome.FAILED_RESUMABLE),
                            malformed_records,
                        )
                if records_received > 0 and accepted == 0 and has_more and consecutive_no_new >= self.max_consecutive_empty:
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

                if not has_more:
                    if self.metadata.records_received == 0:
                        return self._finish(PaginatorOutcome.EMPTY_UNCONFIRMED, PaginationState.EMPTY_UNCONFIRMED,
                                            "empty_result_not_independently_confirmed", malformed=malformed_records)
                    if (consecutive_no_new >= self.max_consecutive_empty
                            or self.metadata.malformed_rejected > 0):
                        return self._finish(PaginatorOutcome.COMPLETE_SUSPECT, PaginationState.COMPLETE_SUSPECT,
                                            "api_end_reached_after_pages_without_new_comments", malformed=malformed_records)
                    return self._finish(PaginatorOutcome.COMPLETE, PaginationState.COMPLETE,
                                        "api_confirmed_end_of_data", malformed=malformed_records)

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
        query = urlencode({
            "aid": "1988", "aweme_id": self.video_id, "count": "50",
            "cursor": str(cursor), "msToken": mstoken,
        })
        url = "https://www.tiktok.com/api/comment/list/?" + query
        try:
            response = self.transport.request_with_retry(
                "GET", url,
                retry_budget=self.retry_budget,
                retry_component=("proxy" if self.transport.proxy_url else "http"),
                headers={
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "referer": "https://www.tiktok.com/",
                },
                expect_json=True,
                maximum_attempts=self.transport.max_retries,
            )
        except HTTPTransportError as transport_error:
            outcome = (ErrorOutcome.RETRY_EXHAUSTED if transport_error.error_code == "RETRY_EXHAUSTED"
                       else ErrorOutcome.FAILED_RESUMABLE if transport_error.retryable
                       else ErrorOutcome.FAILED_PERMANENT)
            raise self._error(
                transport_error.error_code, str(transport_error),
                http_status=transport_error.http_status, outcome=outcome,
            ) from transport_error

        data = response.json_data
        if not isinstance(data, dict):
            return {"schema_error": "response_not_object"}
        api_status = data.get("status_code")
        if api_status not in (None, 0, "0"):
            return {"error": True}
        if "comments" not in data:
            return {"schema_error": "comments_field_missing"}
        if "has_more" not in data:
            return {"schema_error": "has_more_field_missing"}
        if "cursor" not in data and data.get("has_more") in (True, 1, "1"):
            return {"schema_error": "cursor_field_missing"}
        return {
            "http_status": response.status_code,
            "comments": data.get("comments"),
            "has_more": data.get("has_more"),
            "next_cursor": data.get("cursor"),
            "error": None,
        }

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
        elapsed_timeout_seconds=elapsed_timeout_seconds, transport=transport,
        retry_budget=retry_budget, stop_event=stop_event,
    )
    return paginator.run(
        initial_cursor=initial_cursor, mstoken=mstoken,
        on_comment=on_comment, on_batch=on_batch,
        on_malformed_comment=on_malformed_comment,
    )
