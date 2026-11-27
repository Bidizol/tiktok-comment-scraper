"""Thread-safe cooperative shutdown signal shared by a scraper run."""
from __future__ import annotations

import threading
from typing import Optional


class RunControl:
    """One-way stop signal; safe to share across coordinator and worker threads."""

    def __init__(self, max_comments: int = 100_000) -> None:
        if isinstance(max_comments, bool) or not isinstance(max_comments, int) or not 1 <= max_comments <= 500_000:
            raise ValueError("max_comments must be between 1 and 500000")
        self.max_comments = max_comments
        self.stop_event = threading.Event()
        self._lock = threading.Lock()
        self._stop_reason: Optional[str] = None
        self._comments_reserved = 0

    def request_stop(self, reason: str = "stop_requested") -> bool:
        normalized = str(reason).strip() or "stop_requested"
        with self._lock:
            if self.stop_event.is_set():
                return False
            self._stop_reason = normalized
            self.stop_event.set()
            return True

    def is_stopped(self) -> bool:
        return self.stop_event.is_set()

    def reserve_comments(self, count: int = 1) -> bool:
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise ValueError("comment reservation must be a non-negative integer")
        with self._lock:
            if self.stop_event.is_set():
                return False
            if self._comments_reserved + count > self.max_comments:
                self._stop_reason = "run_comment_capacity_reached"
                self.stop_event.set()
                return False
            self._comments_reserved += count
            return True

    @property
    def comments_reserved(self) -> int:
        with self._lock:
            return self._comments_reserved

    @property
    def stop_reason(self) -> Optional[str]:
        with self._lock:
            return self._stop_reason
