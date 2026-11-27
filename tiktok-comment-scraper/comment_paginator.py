from __future__ import annotations

import dataclasses
import time

import dataclasses
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple

from http_transport import HTTPTransport, fetch_json

from pagination_state import PaginationState, PaginationMetadata, CursorIntegrityChecker, normalise_has_more

from errors.model import ScraperError, ErrorSeverity, ErrorOutcome

logger = __import__("logging").getLogger(__name__)


class PaginatorOutcome(Enum):
    COMPLETED = auto()
    STALLED = auto()
    FAILED = auto()
    INTERRUPTED = auto()
    PARTIAL = auto()


DEFAULT_MAX_BATCHES: int = 500
DEFAULT_MAX_COMMENTS: int = 10_000
DEFAULT_MAX_CONSECUTIVE_EMPTY: int = 3
DEFAULT_STALLED_TIMEOUT_SECONDS: int = 300
DEFAULT_ELAPSED_TIMEOUT_SECONDS: int = 600


# ---- pagination runner ----

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
    """Bounded pagination runner for TikTok comments."""

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
        self.video_id = video_id
        self.run_id = run_id
        self.worker_id = worker_id
        self.metadata = PaginationMetadata(
            video_id=video_id,
            run_id=run_id,
            worker_id=worker_id,
            requested_cursor=0,
            returned_cursor=None,
            batch_number=0,
            records_returned=0,
            valid_comments_extracted=0,
            duplicate_comments=0,
            malformed_rejected=0,
            has_more=True,
        )
        self.cursor_checker = CursorIntegrityChecker(
            max_cycle_detections=max_consecutive_empty * 2,
            max_consecutive_empty=max_consecutive_empty,
        )
        self.max_batches = max_batches
        self.max_comments = max_comments
        self.max_consecutive_empty = max_consecutive_empty
        self.stalled_timeout_seconds = stalled_timeout_seconds
        self.elapsed_timeout_seconds = elapsed_timeout_seconds
        self.transport = transport or HTTPTransport()
        self.batch_start_time: Optional[float] = None
        self.pagination_start_time: Optional[float] = None

    def run(
        self,
        *,
        initial_cursor: int = 0,
        mstoken: str,
        on_comment: Optional[callable] = None,
    ) -> PaginatorResult:
        self.metadata = dataclasses.replace(
            self.metadata,
            requested_cursor=initial_cursor,
            returned_cursor=initial_cursor,
            batch_number=0,
            records_returned=0,
            valid_comments_extracted=0,
            duplicate_comments=0,
            malformed_rejected=0,
            has_more=True,
            outcome=PaginationState.INITIAL,
            timestamp=datetime.now(timezone.utc),
            stop_reason=None,
        )
        self.cursor_checker.reset()
        self.batch_start_time = time.time()
        self.pagination_start_time = time.time()
        try:
            return self._paginate_loop(
                initial_cursor=initial_cursor, mstoken=mstoken, on_comment=on_comment
            )
        except KeyboardInterrupt:
            self.metadata = dataclasses.replace(
                self.metadata,
                outcome=PaginationState.INTERRUPTED,
                stop_reason="user_interrupt",
                timestamp=datetime.now(timezone.utc),
            )
            return PaginatorResult(
                outcome=PaginatorOutcome.INTERRUPTED,
                metadata=self.metadata,
                comments_returned=self.metadata.records_returned,
            )
        except Exception as unexpected:  # noqa: B001 – truly unexpected, surface as FAILED
            error = ScraperError(
                error_code="UNEXPECTED_PAGINATOR_ERROR",
                severity=ErrorSeverity.CRITICAL,
                timestamp_utc=datetime.now(timezone.utc).isoformat(),
                component="comment_paginator",
                function="run",
                video_id=self.video_id,
                run_id=self.run_id,
                explanation=f"Unexpected error during pagination: {unexpected}",
                outcome=ErrorOutcome.FAILED_PERMANENT,
            )
            self.metadata = dataclasses.replace(
                self.metadata,
                outcome=PaginationState.FAILED,
                stop_reason="unexpected_error",
                timestamp=datetime.now(timezone.utc),
            )
            return PaginatorResult(
                outcome=PaginatorOutcome.FAILED,
                metadata=self.metadata,
                error=error,
                comments_returned=self.metadata.records_returned,
            )

    def _paginate_loop(
        self,
        *,
        initial_cursor: int,
        mstoken: str,
        on_comment: Optional[callable],
    ) -> PaginatorResult:
        cursor = initial_cursor
        consecutive_empty = 0
        while True:
            # elapsed-time guard
            if self.pagination_start_time is not None:
                elapsed = time.time() - self.pagination_start_time
                if elapsed >= self.elapsed_timeout_seconds:
                    self.metadata = dataclasses.replace(
                        self.metadata,
                        outcome=PaginationState.COMPLETED,
                        stop_reason="elapsed_timeout",
                        has_more=False,
                        timestamp=datetime.now(timezone.utc),
                    )
                    return PaginatorResult(
                        outcome=PaginatorOutcome.PARTIAL,
                        metadata=self.metadata,
                        comments_returned=self.metadata.records_returned,
                    )
            # batch limit guard
            if self.metadata.batch_number >= self.max_batches:
                self.metadata = dataclasses.replace(
                    self.metadata,
                    outcome=PaginationState.PARTIAL,
                    stop_reason="max_batches_reached",
                    has_more=False,
                    timestamp=datetime.now(timezone.utc),
                )
                return PaginatorResult(
                    outcome=PaginatorOutcome.PARTIAL,
                    metadata=self.metadata,
                    comments_returned=self.metadata.records_returned,
                )
            # max-comments guard
            if self.metadata.records_returned >= self.max_comments:
                self.metadata = dataclasses.replace(
                    self.metadata,
                    outcome=PaginationState.PARTIAL,
                    stop_reason="max_comments_reached",
                    has_more=False,
                    timestamp=datetime.now(timezone.utc),
                )
                return PaginatorResult(
                    outcome=PaginatorOutcome.PARTIAL,
                    metadata=self.metadata,
                    comments_returned=self.metadata.records_returned,
                )
            # fetch a batch
            try:
                batch_result = self._fetch_batch(cursor=cursor, mstoken=mstoken)
            except ScraperError as transport_err:
                self.metadata = dataclasses.replace(
                    self.metadata,
                    outcome=PaginationState.FAILED,
                    stop_reason=f"transport_error:{transport_err.error_code}",
                    timestamp=datetime.now(timezone.utc),
                )
                return PaginatorResult(
                    outcome=PaginatorOutcome.FAILED,
                    metadata=self.metadata,
                    error=transport_err,
                    comments_returned=self.metadata.records_returned,
                )
            if batch_result is None:
                self.metadata = dataclasses.replace(
                    self.metadata,
                    outcome=PaginationState.FAILED,
                    stop_reason="null_batch_result",
                    timestamp=datetime.now(timezone.utc),
                )
                return PaginatorResult(
                    outcome=PaginatorOutcome.FAILED,
                    metadata=self.metadata,
                    comments_returned=self.metadata.records_returned,
                )
            # Normalize has_more - will raise ValueError for malformed metadata
            try:
                has_more = normalise_has_more(batch_result.get("has_more"))
            except ValueError as e:
                self.metadata = dataclasses.replace(
                    self.metadata,
                    outcome=PaginationState.FAILED,
                    stop_reason=f"malformed_has_more:{str(e)}",
                    timestamp=datetime.now(timezone.utc),
                )
                return PaginatorResult(
                    outcome=PaginatorOutcome.FAILED,
                    metadata=self.metadata,
                    error=ScraperError(
                        error_code="MALFORMED_METADATA",
                        severity=ErrorSeverity.ERROR,
                        timestamp_utc=datetime.now(timezone.utc).isoformat(),
                        component="comment_paginator",
                        function="_paginate_loop",
                        video_id=self.video_id,
                        run_id=self.run_id,
                        explanation=str(e),
                        outcome=ErrorOutcome.FAILED_PERMANENT,
                    ),
                    comments_returned=self.metadata.records_returned,
                )
            http_status = batch_result.get("http_status")
            error = batch_result.get("error")
            # Get next_cursor explicitly — do not substitute cursor as default
            # This allows the cursor checker to detect genuinely missing cursors
            next_cursor_raw = batch_result.get("next_cursor")
            if next_cursor_raw is None:
                # Key absent or explicitly None — treat as missing cursor
                # When has_more=True, a missing/None cursor means pagination is stalled
                if has_more:
                    self.metadata = dataclasses.replace(
                        self.metadata,
                        outcome=PaginationState.STALLED,
                        stop_reason="missing_returned_cursor",
                        has_more=False,
                        timestamp=datetime.now(timezone.utc),
                    )
                    return PaginatorResult(
                        outcome=PaginatorOutcome.STALLED,
                        metadata=self.metadata,
                        comments_returned=self.metadata.records_returned,
                    )
                next_cursor = None
            else:
                next_cursor = next_cursor_raw
            # error from API
            if error:
                self.metadata = dataclasses.replace(
                    self.metadata,
                    outcome=PaginationState.FAILED,
                    stop_reason=f"api_error:{error[:80]}",
                    timestamp=datetime.now(timezone.utc),
                )
                return PaginatorResult(
                    outcome=PaginatorOutcome.FAILED,
                    metadata=self.metadata,
                    error=ScraperError(
                        error_code="API_ERROR",
                        severity=ErrorSeverity.ERROR,
                        timestamp_utc=datetime.now(timezone.utc).isoformat(),
                        component="comment_paginator",
                        function="_fetch_and_process_batch",
                        video_id=self.video_id,
                        run_id=self.run_id,
                        batch_number=self.metadata.batch_number,
                        cursor=str(cursor),
                        http_status=http_status,
                        explanation=error,
                        outcome=ErrorOutcome.FAILED_PERMANENT,
                    ),
                    comments_returned=self.metadata.records_returned,
                )
            # no comments returned (empty page)
            if not batch_result.get("comments"):
                consecutive_empty += 1
                self.metadata = dataclasses.replace(
                    self.metadata,
                    consecutive_empty_batches=consecutive_empty,
                )
                # check cursor integrity for stagnation
                can_continue, reason = self.cursor_checker.check_cursor_advancement(
                    next_cursor
                )
                if not can_continue:
                    self.metadata = dataclasses.replace(
                        self.metadata,
                        outcome=PaginationState.STALLED,
                        stop_reason=f"cursor_stalled:{reason}",
                        has_more=False,
                        timestamp=datetime.now(timezone.utc),
                    )
                    return PaginatorResult(
                        outcome=PaginatorOutcome.STALLED,
                        metadata=self.metadata,
                        comments_returned=self.metadata.records_returned,
                    )
                if consecutive_empty >= self.max_consecutive_empty:
                    self.metadata = dataclasses.replace(
                        self.metadata,
                        outcome=PaginationState.STOPPED,
                        stop_reason="consecutive_empty_batches_exceeded",
                        has_more=False,
                        timestamp=datetime.now(timezone.utc),
                    )
                    return PaginatorResult(
                        outcome=PaginatorOutcome.STALLED,
                        metadata=self.metadata,
                        comments_returned=self.metadata.records_returned,
                    )
                # no progress but within empty-batch limit – advance cursor
                cursor = next_cursor
                continue
            # we have comments
            consecutive_empty = 0  # reset empty-counter
            # extract comments and update metadata
            comments = batch_result["comments"]
            comments_count = len(comments)
            # call user callback if provided
            if on_comment is not None:
                for comment in comments:
                    try:
                        on_comment(comment)
                    except Exception:  # noqa: B001 – user callback failure should not break pagination
                        pass
            # update metadata
            self.metadata = dataclasses.replace(
                self.metadata,
                batch_number=self.metadata.batch_number + 1,
                records_returned=self.metadata.records_returned + comments_count,
                valid_comments_extracted=self.metadata.valid_comments_extracted + comments_count,
                requested_cursor=cursor,
                returned_cursor=next_cursor,
                has_more=has_more,
            )
            # check if there are more pages
            if not has_more:
                # API confirmed completion, return COMPLETED
                self.metadata = dataclasses.replace(
                    self.metadata,
                    outcome=PaginationState.COMPLETED,
                    stop_reason="api_confirmed_complete",
                    timestamp=datetime.now(timezone.utc),
                )
                return PaginatorResult(
                    outcome=PaginatorOutcome.COMPLETED,
                    metadata=self.metadata,
                    comments_returned=self.metadata.records_returned,
                )
            # track cursor in integrity checker
            self.cursor_checker.check_cursor_advancement(next_cursor)
            # advance cursor for next iteration
            cursor = next_cursor

    def _fetch_batch(self, *, cursor: int, mstoken: str) -> Optional[Dict[str, Any]]:
        """Fetch a single comment batch using the existing HTTP transport."""
        try:
            status, data = fetch_json(
                url="https://www.tiktok.com/api/comment/list/?aid=1988&aweme_id={}&count=50&cursor={}&msToken={}".format(self.video_id, cursor, mstoken),
            )
        except Exception as exc:  # noqa: B001 – transport-level failure
            raise ScraperError(
                error_code="NETWORK_ERROR",
                severity=ErrorSeverity.ERROR,
                timestamp_utc=datetime.now(timezone.utc).isoformat(),
                component="comment_paginator",
                function="_fetch_batch",
                video_id=self.video_id,
                run_id=self.run_id,
                batch_number=self.metadata.batch_number,
                cursor=str(cursor),
                http_status=0,
                explanation=str(exc),
                outcome=ErrorOutcome.FAILED_PERMANENT,
            )
        if not (200 <= status < 300):
            raise ScraperError(
                error_code="HTTP_ERROR",
                severity=ErrorSeverity.ERROR,
                timestamp_utc=datetime.now(timezone.utc).isoformat(),
                component="comment_paginator",
                function="_fetch_batch",
                video_id=self.video_id,
                run_id=self.run_id,
                batch_number=self.metadata.batch_number,
                cursor=str(cursor),
                http_status=status,
                explanation=f"HTTP {status} from TikTok API",
                outcome=ErrorOutcome.FAILED_PERMANENT,
            )
        if not isinstance(data, dict):
            logger.warning(
                "Non-dict JSON response from TikTok comment API (status %s)", status
            )
            return None
        return data


def paginate_comments(
    *,
    video_id: str,
    run_id: str,
    mstoken: str,
    max_batches: int = DEFAULT_MAX_BATCHES,
    max_comments: int = DEFAULT_MAX_COMMENTS,
    max_consecutive_empty: int = DEFAULT_MAX_CONSECUTIVE_EMPTY,
    stalled_timeout_seconds: int = DEFAULT_STALLED_TIMEOUT_SECONDS,
    elapsed_timeout_seconds: int = DEFAULT_ELAPSED_TIMEOUT_SECONDS,
    initial_cursor: int = 0,
    on_comment: Optional[callable] = None,
    transport: Optional[HTTPTransport] = None,
) -> PaginatorResult:
    """High-level convenience wrapper around CommentPaginator.run()."""
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
    )
