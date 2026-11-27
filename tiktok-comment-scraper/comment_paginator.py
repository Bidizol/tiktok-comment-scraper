"""Strict, bounded pagination for TikTok comment extraction."""

from __future__ import annotations

import dataclasses
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, Callable, Dict, Optional
from urllib.parse import urlencode

from http_transport import HTTPTransport
from pagination_state import (
    CursorIntegrityChecker,
    PaginationMetadata,
    PaginationState,
    normalise_has_more,
)
from errors.model import ScraperError, ErrorOutcome, ErrorSeverity

logger = logging.getLogger(__name__)


class PaginatorOutcome(Enum):
    COMPLETED = auto()
    STALLED = auto()
    FAILED = auto()
    INTERRUPTED = auto()
    PARTIAL = auto()


DEFAULT_MAX_BATCHES = 500
DEFAULT_MAX_COMMENTS = 10_000
DEFAULT_MAX_CONSECUTIVE_EMPTY = 3
DEFAULT_STALLED_TIMEOUT_SECONDS = 300
DEFAULT_ELAPSED_TIMEOUT_SECONDS = 600


@dataclass
class PaginatorResult:
    outcome: PaginatorOutcome
    metadata: PaginationMetadata
    error: Optional[ScraperError] = None
    comments_returned: int = 0

    def to_dict(self) -> dict:
        return {
            "outcome": self.outcome.name,
            "metadata": self.metadata.to_dict(),
            "error": self.error.to_dict() if self.error else None,
            "comments_returned": self.comments_returned,
        }


class CommentPaginator:
    """Bounded pagination runner; only explicit has_more=False completes."""

    def __init__(
        self,
        *,
        video_id: str,
        run_id: str,
        worker_id: Optional[int] = None,
        max_batches: int = DEFAULT_MAX_BATCHES,
        max_comments: int = DEFAULT_MAX_COMMENTS,
        max_consecutive_empty: int = DEFAULT_MAX_CONSECUTIVE_EMPTY,
        stalled_timeout_seconds: int = DEFAULT_STALLED_TIMEOUT_SECONDS,
        elapsed_timeout_seconds: int = DEFAULT_ELAPSED_TIMEOUT_SECONDS,
        transport: Optional[HTTPTransport] = None,
    ):
        limits = {
            "max_batches": max_batches,
            "max_comments": max_comments,
            "max_consecutive_empty": max_consecutive_empty,
            "stalled_timeout_seconds": stalled_timeout_seconds,
            "elapsed_timeout_seconds": elapsed_timeout_seconds,
        }
        invalid = [name for name, value in limits.items() if value < 1]
        if invalid:
            raise ValueError("Pagination limits must be positive: " + ", ".join(invalid))

        self.video_id = str(video_id)
        self.run_id = run_id
        self.worker_id = worker_id
        self.max_batches = max_batches
        self.max_comments = max_comments
        self.max_consecutive_empty = max_consecutive_empty
        self.stalled_timeout_seconds = stalled_timeout_seconds
        self.elapsed_timeout_seconds = elapsed_timeout_seconds
        self.transport = transport or HTTPTransport()
        self.cursor_checker = CursorIntegrityChecker(
            max_cycle_detections=max_consecutive_empty * 2,
            max_consecutive_empty=max_consecutive_empty,
        )
        self.metadata = self._new_metadata(0)

    def _new_metadata(self, cursor: int) -> PaginationMetadata:
        return PaginationMetadata(
            video_id=self.video_id,
            run_id=self.run_id,
            worker_id=self.worker_id,
            requested_cursor=cursor,
            returned_cursor=cursor,
            batch_number=0,
            records_returned=0,
            valid_comments_extracted=0,
            duplicate_comments=0,
            malformed_rejected=0,
            has_more=True,
        )

    def run(
        self,
        *,
        initial_cursor: int = 0,
        mstoken: Any,
        on_comment: Optional[Callable[[dict], Any]] = None,
        on_batch: Optional[Callable[[dict], Any]] = None,
    ) -> PaginatorResult:
        if isinstance(initial_cursor, bool) or not isinstance(initial_cursor, int) or initial_cursor < 0:
            raise ValueError("initial_cursor must be a non-negative integer")

        self.metadata = self._new_metadata(initial_cursor)
        self.cursor_checker.reset()
        started = time.monotonic()
        last_progress = started
        cursor = initial_cursor
        consecutive_empty = 0

        try:
            while True:
                elapsed = time.monotonic() - started
                if elapsed >= self.elapsed_timeout_seconds:
                    return self._finish(
                        PaginatorOutcome.PARTIAL, PaginationState.PARTIAL,
                        "elapsed_timeout",
                    )

                if self.metadata.batch_number >= self.max_batches:
                    return self._finish(
                        PaginatorOutcome.PARTIAL, PaginationState.PARTIAL,
                        "max_batches_reached",
                    )

                remaining = self.max_comments - self.metadata.records_returned
                if remaining <= 0:
                    return self._finish(
                        PaginatorOutcome.PARTIAL, PaginationState.PARTIAL,
                        "max_comments_reached",
                    )

                try:
                    current_token = mstoken() if callable(mstoken) else mstoken
                except Exception as token_error:
                    return self._finish(
                        PaginatorOutcome.FAILED, PaginationState.FAILED,
                        "token_refresh_failed",
                        self._error("TOKEN_REFRESH_ERROR", type(token_error).__name__),
                    )
                if not isinstance(current_token, str) or not current_token:
                    return self._finish(
                        PaginatorOutcome.FAILED, PaginationState.FAILED,
                        "token_unavailable",
                        self._error("TOKEN_UNAVAILABLE", "Pagination token unavailable"),
                    )

                try:
                    batch = self._fetch_batch(cursor=cursor, mstoken=current_token)
                except ScraperError as error:
                    return self._finish(
                        PaginatorOutcome.FAILED, PaginationState.FAILED,
                        "transport_error:" + error.error_code, error,
                    )

                if not isinstance(batch, dict):
                    return self._finish(
                        PaginatorOutcome.FAILED, PaginationState.FAILED,
                        "invalid_batch_response",
                        self._error("INVALID_BATCH_RESPONSE", "Comment API returned no valid JSON object"),
                    )

                try:
                    has_more = normalise_has_more(batch.get("has_more"))
                except ValueError as error:
                    return self._finish(
                        PaginatorOutcome.FAILED, PaginationState.FAILED,
                        "malformed_has_more",
                        self._error("MALFORMED_METADATA", str(error)),
                    )

                comments = batch.get("comments")
                if not isinstance(comments, list):
                    return self._finish(
                        PaginatorOutcome.FAILED, PaginationState.FAILED,
                        "malformed_comments",
                        self._error("MALFORMED_COMMENTS", "Comment API comments field must be a list"),
                    )

                if batch.get("error"):
                    return self._finish(
                        PaginatorOutcome.FAILED, PaginationState.FAILED,
                        "api_error",
                        self._error("API_ERROR", str(batch.get("error"))[:160]),
                    )

                next_cursor = batch.get("next_cursor")
                if has_more:
                    if isinstance(next_cursor, bool) or not isinstance(next_cursor, int) or next_cursor < 0:
                        return self._finish(
                            PaginatorOutcome.STALLED, PaginationState.STALLED,
                            "missing_or_invalid_returned_cursor",
                        )
                    if next_cursor == cursor:
                        return self._finish(
                            PaginatorOutcome.STALLED, PaginationState.STALLED,
                            "cursor_not_advancing",
                        )
                    if next_cursor < cursor:
                        return self._finish(
                            PaginatorOutcome.STALLED, PaginationState.STALLED,
                            "cursor_regression",
                        )
                    can_continue, reason = self.cursor_checker.check_cursor_advancement(next_cursor)
                    if not can_continue:
                        return self._finish(
                            PaginatorOutcome.STALLED, PaginationState.STALLED,
                            "cursor_integrity:" + reason,
                        )
                else:
                    next_cursor = cursor

                retained = comments[:remaining]
                page_truncated = len(retained) < len(comments)
                accepted = 0
                try:
                    for comment in retained:
                        if on_comment is None:
                            accepted += 1
                        else:
                            result = on_comment(comment)
                            if result is not False:
                                accepted += 1
                except Exception as error:
                    return self._finish(
                        PaginatorOutcome.FAILED, PaginationState.FAILED,
                        "comment_callback_failed",
                        self._error("COMMENT_CALLBACK_ERROR", type(error).__name__),
                    )

                self.metadata = dataclasses.replace(
                    self.metadata,
                    requested_cursor=cursor,
                    returned_cursor=next_cursor,
                    batch_number=self.metadata.batch_number + 1,
                    records_returned=self.metadata.records_returned + accepted,
                    valid_comments_extracted=self.metadata.valid_comments_extracted + accepted,
                    has_more=has_more,
                    timestamp=datetime.now(timezone.utc),
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
                    "metadata": self.metadata,
                }
                if on_batch is not None:
                    try:
                        on_batch(batch_info)
                    except Exception as error:
                        return self._finish(
                            PaginatorOutcome.FAILED, PaginationState.FAILED,
                            "batch_callback_failed",
                            self._error("BATCH_CALLBACK_ERROR", type(error).__name__),
                        )

                if page_truncated:
                    return self._finish(
                        PaginatorOutcome.PARTIAL, PaginationState.PARTIAL,
                        "max_comments_reached",
                    )

                if not comments:
                    consecutive_empty += 1
                    if consecutive_empty >= self.max_consecutive_empty:
                        return self._finish(
                            PaginatorOutcome.STALLED, PaginationState.STALLED,
                            "consecutive_empty_batches_exceeded",
                        )
                    if time.monotonic() - last_progress >= self.stalled_timeout_seconds:
                        return self._finish(
                            PaginatorOutcome.STALLED, PaginationState.STALLED,
                            "stalled_timeout",
                        )
                else:
                    consecutive_empty = 0
                    last_progress = time.monotonic()

                if not has_more:
                    return self._finish(
                        PaginatorOutcome.COMPLETED, PaginationState.COMPLETED,
                        "api_confirmed_complete",
                    )

                cursor = next_cursor

        except KeyboardInterrupt:
            return self._finish(
                PaginatorOutcome.INTERRUPTED, PaginationState.INTERRUPTED,
                "user_interrupt",
            )
        except Exception as error:
            logger.exception("Unexpected pagination failure")
            return self._finish(
                PaginatorOutcome.FAILED, PaginationState.FAILED,
                "unexpected_pagination_error",
                self._error("UNEXPECTED_PAGINATION_ERROR", type(error).__name__),
            )

    def _fetch_batch(self, *, cursor: int, mstoken: str) -> Dict[str, Any]:
        query = urlencode({
            "aid": "1988",
            "aweme_id": self.video_id,
            "count": "50",
            "cursor": str(cursor),
            "msToken": mstoken,
        })
        url = "https://www.tiktok.com/api/comment/list/?" + query
        status, data = self.transport.request(
            "GET",
            url,
            headers={
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "referer": "https://www.tiktok.com/",
            },
        )
        if not (200 <= status < 300):
            raise self._error(
                "HTTP_ERROR",
                "Comment API request failed",
                http_status=status,
                outcome=ErrorOutcome.FAILED_RESUMABLE,
            )
        if not isinstance(data, dict):
            raise self._error("INVALID_JSON", "Comment API returned an invalid JSON object")
        return {
            "http_status": status,
            "comments": data.get("comments"),
            "has_more": data.get("has_more"),
            "next_cursor": data.get("cursor"),
            "error": (
                data.get("status_msg") or f"API status_code={data.get('status_code')}"
            ) if data.get("status_code") not in (None, 0, "0") else None,
        }

    def _error(
        self,
        code: str,
        explanation: str,
        http_status: Optional[int] = None,
        outcome: ErrorOutcome = ErrorOutcome.FAILED_PERMANENT,
    ) -> ScraperError:
        return ScraperError(
            error_code=code,
            severity=ErrorSeverity.ERROR,
            component="comment_paginator",
            function="pagination",
            video_id=self.video_id,
            run_id=self.run_id,
            batch_number=self.metadata.batch_number,
            cursor=str(self.metadata.requested_cursor),
            http_status=http_status,
            explanation=explanation,
            outcome=outcome,
        )

    def _finish(
        self,
        outcome: PaginatorOutcome,
        state: PaginationState,
        reason: str,
        error: Optional[ScraperError] = None,
    ) -> PaginatorResult:
        self.metadata = dataclasses.replace(
            self.metadata,
            outcome=state,
            stop_reason=reason,
            timestamp=datetime.now(timezone.utc),
        )
        return PaginatorResult(
            outcome=outcome,
            metadata=self.metadata,
            error=error,
            comments_returned=self.metadata.records_returned,
        )


def paginate_comments(
    *,
    video_id: str,
    run_id: str,
    mstoken: Any,
    max_batches: int = DEFAULT_MAX_BATCHES,
    max_comments: int = DEFAULT_MAX_COMMENTS,
    max_consecutive_empty: int = DEFAULT_MAX_CONSECUTIVE_EMPTY,
    stalled_timeout_seconds: int = DEFAULT_STALLED_TIMEOUT_SECONDS,
    elapsed_timeout_seconds: int = DEFAULT_ELAPSED_TIMEOUT_SECONDS,
    initial_cursor: int = 0,
    on_comment: Optional[Callable[[dict], Any]] = None,
    on_batch: Optional[Callable[[dict], Any]] = None,
    transport: Optional[HTTPTransport] = None,
) -> PaginatorResult:
    paginator = CommentPaginator(
        video_id=video_id,
        run_id=run_id,
        max_batches=max_batches,
        max_comments=max_comments,
        max_consecutive_empty=max_consecutive_empty,
        stalled_timeout_seconds=stalled_timeout_seconds,
        elapsed_timeout_seconds=elapsed_timeout_seconds,
        transport=transport,
    )
    return paginator.run(
        initial_cursor=initial_cursor,
        mstoken=mstoken,
        on_comment=on_comment,
        on_batch=on_batch,
    )
