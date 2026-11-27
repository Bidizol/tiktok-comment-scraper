from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class PaginationState(Enum):
    """Explicit pagination states that must be tracked and auditable."""

    INITIAL = auto()
    FETCHING = auto()
    BATCH_PROCESSED = auto()
    CONTINUING = auto()
    COMPLETED = auto()
    PARTIAL = auto()
    STALLED = auto()
    FAILED = auto()
    INTERRUPTED = auto()


@dataclass
class PaginationMetadata:
    """Metadata tracked for each pagination operation."""

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

    def to_dict(self) -> dict:
        """Convert the metadata to a dictionary for serialization."""
        return {
            "video_id": self.video_id,
            "run_id": self.run_id,
            "worker_id": self.worker_id,
            "requested_cursor": self.requested_cursor,
            "returned_cursor": str(self.returned_cursor) if self.returned_cursor is not None else None,
            "batch_number": self.batch_number,
            "records_returned": self.records_returned,
            "valid_comments_extracted": self.valid_comments_extracted,
            "duplicate_comments": self.duplicate_comments,
            "malformed_rejected": self.malformed_rejected,
            "has_more": self.has_more,
            "timestamp": self.timestamp.isoformat(),
            "outcome": self.outcome.name,
            "stop_reason": self.stop_reason,
            "consecutive_empty_batches": self.consecutive_empty_batches,
        }


def normalise_has_more(raw: Any) -> bool:
    """
    Normalize has_more value from API response.

    Required behavior:
    - True -> True
    - False -> False
    - 1 -> True
    - 0 -> False
    - "1" -> True
    - "0" -> False
    - Missing -> Malformed metadata error (caller must handle)
    - None -> Malformed metadata error
    - "" -> Malformed metadata error
    - Unknown string -> Malformed metadata error
    - Unsupported integer -> Malformed metadata error

    Returns True/False for valid inputs.
    Raises ValueError for malformed metadata.
    """
    if raw is None:
        raise ValueError("has_more is None - malformed metadata")
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, int):
        if raw == 1:
            return True
        if raw == 0:
            return False
        raise ValueError(f"Unsupported integer has_more value: {raw}")
    if isinstance(raw, str):
        if raw == "1":
            return True
        if raw == "0":
            return False
        if raw == "":
            raise ValueError("has_more is empty string - malformed metadata")
        raise ValueError(f"Unknown has_more string value: {raw!r}")
    # For any other type, raise error
    raise ValueError(f"Malformed has_more type: {type(raw).__name__}")


DEFAULT_MAX_BATCHES: int = 500
DEFAULT_MAX_COMMENTS: int = 10_000
DEFAULT_MAX_CONSECUTIVE_EMPTY: int = 3
DEFAULT_STALLED_TIMEOUT_SECONDS: int = 300
DEFAULT_ELAPSED_TIMEOUT_SECONDS: int = 600


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


class CursorRegression(Enum):
    """Enum representing cursor regression states."""
    NONE = auto()
    REGRESSED = auto()       # Cursor moved backwards (e.g., 100 -> 50)
    NON_ADVANCING = auto()   # Cursor stayed the same (e.g., 100 -> 100)
    INVALID_TYPE = auto()    # Cursor is not an integer
    MISSING = auto()         # Cursor is None


class CursorIntegrityChecker:
    """Safeguards against cursor integrity issues.

    Detects:
    - Cursor cycles (repeated same cursor)
    - Cursor regression (backwards movement)
    - Non-advancing cursors
    - Invalid cursor types
    """

    def __init__(self, max_cycle_detections: int = 10, max_consecutive_empty: int = 3):
        self.max_cycle_detections = max_cycle_detections
        self.max_consecutive_empty = max_consecutive_empty
        self.seen_cursors: List[int] = []
        self.consecutive_empty_batches: int = 0
        self.previous_cursor: Optional[int] = None

    def check_cursor_advancement(self, new_cursor: Optional[int]) -> Tuple[bool, str]:
        """Check if cursor is advancing properly.

        Returns:
            (can_continue, reason) tuple

        Reason codes:
            - "ok": cursor is advancing normally
            - "cursor_regression": cursor moved backwards
            - "cursor_not_advancing": cursor stayed the same
            - "cursor_cycle_detected": cursor repeated too many times
            - "missing_returned_cursor": cursor is None
            - "invalid_cursor_type": cursor is not an integer
        """
        if new_cursor is None:
            return False, "missing_returned_cursor"

        if not isinstance(new_cursor, int):
            return False, f"invalid_cursor_type:{type(new_cursor).__name__}"

        # Check for cursor cycle
        if new_cursor in self.seen_cursors:
            cycle_count = self.seen_cursors.count(new_cursor)
            if cycle_count >= self.max_cycle_detections:
                return False, f"cursor_cycle_detected:{new_cursor}"

        # Check for no advancement (same cursor) BEFORE regression check
        if new_cursor == self.previous_cursor:
            self.consecutive_empty_batches += 1
            if self.consecutive_empty_batches >= self.max_consecutive_empty:
                return False, "cursor_not_advancing:repeated_same_cursor"
        elif self.previous_cursor is not None and new_cursor < self.previous_cursor:
            # Regression detected - cursor moved backwards
            # But only if it's not the same value (handled above)
            self.consecutive_empty_batches = 0  # Reset on any new cursor value
            return False, "cursor_regression:backwards_movement"
        else:
            # Cursor advanced (new_cursor > previous_cursor) or first cursor
            # Reset empty counter on any new forward movement
            self.consecutive_empty_batches = 0

        self.seen_cursors.append(new_cursor)
        # Keep only recent cursors to avoid memory issues
        if len(self.seen_cursors) > self.max_cycle_detections * 2:
            self.seen_cursors = self.seen_cursors[-self.max_cycle_detections:]

        self.previous_cursor = new_cursor
        return True, "ok"

    def reset(self) -> None:
        """Reset checker state for new pagination run."""
        self.seen_cursors = []
        self.consecutive_empty_batches = 0
        self.previous_cursor = None
