from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, List, Optional, Tuple

logger = logging.getLogger(__name__)


class PaginationState(Enum):
    """Explicit, auditable states for a single pagination run."""
    INITIAL = "INITIAL"
    FETCHING = "FETCHING"
    BATCH_PROCESSED = "BATCH_PROCESSED"
    CONTINUING = "CONTINUING"
    COMPLETE = "COMPLETE"
    COMPLETE_SUSPECT = "COMPLETE_SUSPECT"
    EMPTY_UNCONFIRMED = "EMPTY_UNCONFIRMED"
    TRUNCATED_LIMIT = "TRUNCATED_LIMIT"
    FAILED_RESUMABLE = "FAILED_RESUMABLE"
    FAILED_PERMANENT = "FAILED_PERMANENT"
    INTERRUPTED = "INTERRUPTED"
    SCHEMA_CHANGED = "SCHEMA_CHANGED"
    PAGINATION_STALLED = "PAGINATION_STALLED"

    # Compatibility aliases for earlier callers.
    COMPLETED = "COMPLETE"
    PARTIAL = "TRUNCATED_LIMIT"
    STALLED = "PAGINATION_STALLED"
    FAILED = "FAILED_PERMANENT"


@dataclass
class PaginationMetadata:
    video_id: str
    run_id: str
    worker_id: Optional[int]
    requested_cursor: int
    returned_cursor: Optional[int]
    batch_number: int
    records_returned: int
    valid_comments_extracted: int
    duplicate_comments: int
    malformed_rejected: int
    has_more: bool
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    outcome: PaginationState = PaginationState.INITIAL
    stop_reason: Optional[str] = None
    consecutive_empty_batches: int = 0
    records_received: int = 0
    missing_comment_ids: int = 0
    pages_without_new_comments: int = 0

    def to_dict(self) -> dict:
        return {
            "video_id": self.video_id,
            "run_id": self.run_id,
            "worker_id": self.worker_id,
            "requested_cursor": self.requested_cursor,
            "returned_cursor": str(self.returned_cursor) if self.returned_cursor is not None else None,
            "batch_number": self.batch_number,
            "records_received": self.records_received,
            "records_returned": self.records_returned,
            "valid_comments_extracted": self.valid_comments_extracted,
            "duplicate_comments": self.duplicate_comments,
            "missing_comment_ids": self.missing_comment_ids,
            "malformed_rejected": self.malformed_rejected,
            "has_more": self.has_more,
            "timestamp": self.timestamp.isoformat(),
            "outcome": self.outcome.name,
            "stop_reason": self.stop_reason,
            "consecutive_empty_batches": self.consecutive_empty_batches,
            "pages_without_new_comments": self.pages_without_new_comments,
        }


def normalise_has_more(raw: Any) -> bool:
    """Accept only explicit boolean/0/1 representations from the API."""
    if raw is None:
        raise ValueError("has_more is None - malformed metadata")
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, int):
        if raw in (0, 1):
            return bool(raw)
        raise ValueError(f"Unsupported integer has_more value: {raw}")
    if isinstance(raw, str):
        if raw in ("0", "1"):
            return raw == "1"
        if raw == "":
            raise ValueError("has_more is empty string - malformed metadata")
        raise ValueError(f"Unknown has_more string value: {raw!r}")
    raise ValueError(f"Malformed has_more type: {type(raw).__name__}")


class CursorRegression(Enum):
    NONE = "NONE"
    REGRESSED = "REGRESSED"
    NON_ADVANCING = "NON_ADVANCING"
    INVALID_TYPE = "INVALID_TYPE"
    MISSING = "MISSING"
    CYCLE = "CYCLE"


class CursorIntegrityChecker:
    """Detect missing, invalid, repeated, cyclic and regressing cursors."""

    def __init__(self, max_cycle_detections: int = 5000, max_consecutive_empty: int = 3):
        if isinstance(max_cycle_detections, bool) or max_cycle_detections < 1:
            raise ValueError("max_cycle_detections must be positive")
        if isinstance(max_consecutive_empty, bool) or max_consecutive_empty < 1:
            raise ValueError("max_consecutive_empty must be positive")
        self.max_cycle_detections = max_cycle_detections
        self.max_consecutive_empty = max_consecutive_empty
        self.seen_cursors: List[int] = []
        self.consecutive_empty_batches = 0
        self.previous_cursor: Optional[int] = None

    def check_cursor_advancement(self, new_cursor: Optional[int]) -> Tuple[bool, str]:
        if new_cursor is None:
            return False, "missing_returned_cursor"
        if isinstance(new_cursor, bool) or not isinstance(new_cursor, int) or new_cursor < 0:
            return False, "invalid_cursor_type_or_value"
        if new_cursor == self.previous_cursor:
            return False, "cursor_not_advancing"
        if new_cursor in self.seen_cursors:
            return False, "cursor_cycle_detected"
        if self.previous_cursor is not None and new_cursor < self.previous_cursor:
            return False, "cursor_regression"
        self.seen_cursors.append(new_cursor)
        # A pagination run is bounded to at most 500 batches by default.
        # Keep a hard ceiling for callers that configure a larger page budget.
        if len(self.seen_cursors) > self.max_cycle_detections:
            return False, "cursor_history_limit_reached"
        self.previous_cursor = new_cursor
        return True, "ok"

    def reset(self, initial_cursor: Optional[int] = None) -> None:
        self.seen_cursors = []
        self.consecutive_empty_batches = 0
        self.previous_cursor = initial_cursor
        if initial_cursor is not None:
            self.seen_cursors.append(initial_cursor)
