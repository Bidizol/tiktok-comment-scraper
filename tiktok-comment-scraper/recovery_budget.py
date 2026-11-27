"""Shared per-video recovery limits and auditable retry accounting."""
from __future__ import annotations

import math
import threading
import time
from datetime import datetime, timezone


class RecoveryBudget:
    """Thread-safe budget shared by HTTP, token refresh and pagination layers."""
    def __init__(self, *, video_id, run_id, worker_id=None, maximum_retries=12,
                 maximum_elapsed_seconds=600.0, maximum_consecutive_failures=6,
                 maximum_token_refresh_retries=6, maximum_proxy_retries=2):
        limits = (maximum_retries, maximum_consecutive_failures,
                  maximum_token_refresh_retries, maximum_proxy_retries)
        if any(isinstance(v, bool) or not isinstance(v, int) or v < 0 for v in limits):
            raise ValueError("Recovery limits must be non-negative integers")
        if maximum_retries < 1 or maximum_consecutive_failures < 1:
            raise ValueError("Retry and consecutive-failure limits must be positive")
        if isinstance(maximum_elapsed_seconds, bool) or not isinstance(maximum_elapsed_seconds, (int, float)) or not math.isfinite(maximum_elapsed_seconds) or maximum_elapsed_seconds <= 0:
            raise ValueError("maximum_elapsed_seconds must be finite and positive")
        if not isinstance(video_id, str) or not video_id.strip() or not isinstance(run_id, str) or not run_id.strip():
            raise ValueError("video_id and run_id are required")
        self.video_id, self.run_id, self.worker_id = video_id, run_id, worker_id
        self.maximum_retries, self.maximum_elapsed_seconds = maximum_retries, float(maximum_elapsed_seconds)
        self.maximum_consecutive_failures = maximum_consecutive_failures
        self.maximum_token_refresh_retries, self.maximum_proxy_retries = maximum_token_refresh_retries, maximum_proxy_retries
        self._started = time.monotonic()
        self._total = 0
        self._consecutive = 0
        self._components = {}
        self._records = []
        self._exhausted = None
        self._lock = threading.RLock()

    def elapsed_seconds(self):
        return max(0.0, time.monotonic() - self._started)

    def remaining_seconds(self):
        return max(0.0, self.maximum_elapsed_seconds - self.elapsed_seconds())

    def _record(self, component, code, action, outcome):
        self._records.append({
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "video_id": self.video_id, "run_id": self.run_id, "worker_id": self.worker_id,
            "component": component, "error_code": code, "action": action, "outcome": outcome,
            "retries_used": self._total, "retries_remaining": max(0, self.maximum_retries-self._total),
            "consecutive_failures": self._consecutive, "elapsed_seconds": round(self.elapsed_seconds(), 3),
        })

    def check_deadline(self):
        with self._lock:
            if self._exhausted:
                return False
            if self.elapsed_seconds() >= self.maximum_elapsed_seconds:
                self._exhausted = "per_video_recovery_deadline_exceeded"
            elif self._consecutive >= self.maximum_consecutive_failures:
                self._exhausted = "per_video_circuit_open_after_consecutive_failures"
            return self._exhausted is None

    def consume_retry(self, component, error_code):
        with self._lock:
            if not self.check_deadline():
                self._record(component, error_code, "blocked", self._exhausted)
                return False
            limit = self.maximum_token_refresh_retries if component == "token" else self.maximum_proxy_retries if component == "proxy" else None
            used = self._components.get(component, 0)
            if self._total >= self.maximum_retries:
                self._exhausted = "per_video_retry_budget_exhausted"
            elif limit is not None and used >= limit:
                self._exhausted = component + "_retry_budget_exhausted"
            if self._exhausted:
                self._record(component, error_code, "blocked", self._exhausted)
                return False
            self._total += 1
            self._components[component] = used + 1
            self._consecutive += 1
            self._record(component, error_code, "retry_consumed", "retry_authorized")
            return True

    def record_success(self, component):
        with self._lock:
            self._consecutive = 0
            self._record(component, None, "success", "operation_succeeded")

    def record_failure(self, component, error_code):
        with self._lock:
            self._consecutive += 1
            self._record(component, error_code, "failure", "no_retry_performed")

    def snapshot(self):
        with self._lock:
            return {
                "video_id": self.video_id, "run_id": self.run_id, "worker_id": self.worker_id,
                "maximum_retries": self.maximum_retries, "retries_used": self._total,
                "retries_remaining": max(0, self.maximum_retries-self._total),
                "maximum_elapsed_seconds": self.maximum_elapsed_seconds,
                "elapsed_seconds": round(self.elapsed_seconds(), 3),
                "consecutive_failures": self._consecutive,
                "maximum_consecutive_failures": self.maximum_consecutive_failures,
                "component_retries": dict(self._components),
                "exhausted_reason": self._exhausted,
                "recovery_trace": list(self._records),
            }
