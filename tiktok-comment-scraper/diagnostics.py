"""Bounded, thread-safe run diagnostics and local alert recording."""
from __future__ import annotations

import json
import os
import re
import shutil
import threading
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


_SECRET_PATTERNS = (
    re.compile(r"(?i)(msToken|token|access_token|refresh_token)=([^&\s]+)"),
    re.compile(r"(?i)(authorization\s*[:=]\s*)([^,;\s]+)"),
    re.compile(r"(?i)((?:cookie|set-cookie)\s*[:=]\s*)([^\r\n]+)"),
    re.compile(r"(?i)(https?://)([^/@\s]+:[^/@\s]+)@"),
    re.compile(r"(?i)(password|passwd|proxy_pass|api_key|secret)\s*[:=]\s*([^\s,;]+)"),
)
_ALLOWED_EVENT_FIELDS = {
    "worker_id", "video_id", "status", "stop_reason", "error_code",
    "error_type", "http_status", "retry_attempt", "retries_used",
    "retries_remaining", "recovery_exhausted_reason", "duration_seconds",
    "total_comments", "total_batches", "records_received", "valid_comments",
    "new_comments", "duplicates_skipped", "malformed_records",
    "cache_error_count", "requested_video_jobs", "completed_videos",
    "partial_videos", "failed_videos", "max_workers", "max_pending_jobs",
    "max_run_comments", "comments_reserved", "disk_free_bytes",
    "disk_total_bytes", "disk_usage_percent", "active_workers",
    "peak_workers", "event_count", "alert_count", "error_count",
    "warning_count", "pagination_outcome", "component", "function",
    "output_dir", "summary_path", "json_path", "csv_path",
}


def _clean(value: Any, depth: int = 0) -> Any:
    if depth > 4:
        return "[TRUNCATED]"
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        return value if value == value and abs(value) != float("inf") else None
    if isinstance(value, str):
        result = value[:1000]
        for pattern in _SECRET_PATTERNS:
            if pattern.pattern.startswith("(?i)(https?"):
                result = pattern.sub(r"\1[REDACTED]@", result)
            else:
                result = pattern.sub(r"\1[REDACTED]", result)
        return result
    if isinstance(value, dict):
        return {
            str(key)[:80]: _clean(item, depth + 1)
            for key, item in list(value.items())[:64]
            if str(key).lower() not in {
                "token", "mstoken", "cookie", "set-cookie", "authorization",
                "password", "passwd", "proxy_pass", "api_key", "secret",
                "credentials", "headers",
            }
        }
    if isinstance(value, (list, tuple)):
        return [_clean(item, depth + 1) for item in value[:64]]
    return _clean(type(value).__name__, depth + 1)


class DiagnosticsRecorder:
    """Append-only JSONL events plus a separate alert stream.

    A run is single-process by design; the lock serializes concurrent worker
    threads. Each line is independently appended and files rotate at a fixed
    size, so diagnostics storage does not grow without a bound.
    """

    def __init__(
        self,
        run_id: str | None = None,
        jsonl_path: str | Path | None = None,
        alert_path: str | Path | None = None,
        max_bytes: int = 10 * 1024 * 1024,
        backup_count: int = 5,
    ) -> None:
        if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or not 64 * 1024 <= max_bytes <= 100 * 1024 * 1024:
            raise ValueError("diagnostics max_bytes must be between 65536 and 104857600")
        if isinstance(backup_count, bool) or not isinstance(backup_count, int) or not 1 <= backup_count <= 20:
            raise ValueError("diagnostics backup_count must be between 1 and 20")
        self.run_id = run_id or "unassigned"
        self.jsonl_path = Path(jsonl_path) if jsonl_path else None
        self.alert_path = Path(alert_path) if alert_path else (
            self.jsonl_path.with_name(self.jsonl_path.stem.replace(".events", "") + ".alerts.jsonl")
            if self.jsonl_path else None
        )
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self._lock = threading.RLock()
        self._events = 0
        self._alerts = 0
        self._errors = 0
        self._warnings = 0
        self._event_counts: Counter[str] = Counter()
        self._active_workers = 0
        self._peak_workers = 0

    def _rotate_if_needed(self, path: Path, incoming: int) -> None:
        try:
            current_size = path.stat().st_size
        except FileNotFoundError:
            current_size = 0
        if current_size + incoming <= self.max_bytes:
            return
        oldest = path.with_name(path.name + f".{self.backup_count}")
        oldest.unlink(missing_ok=True)
        for index in range(self.backup_count - 1, 0, -1):
            source = path.with_name(path.name + f".{index}")
            if source.exists():
                source.replace(path.with_name(path.name + f".{index + 1}"))
        if path.exists():
            path.replace(path.with_name(path.name + ".1"))

    def _append(self, path: Path | None, payload: dict[str, Any]) -> None:
        if path is None:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(path.parent, 0o700)
        except OSError:
            pass
        encoded = (json.dumps(_clean(payload), ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")
        if len(encoded) > 64 * 1024:
            raise ValueError("diagnostic record exceeds 64 KiB")
        with self._lock:
            self._rotate_if_needed(path, len(encoded))
            descriptor = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            try:
                if hasattr(os, "fchmod"):
                    try:
                        os.fchmod(descriptor, 0o600)
                    except OSError:
                        pass
                with os.fdopen(descriptor, "ab", closefd=False) as stream:
                    stream.write(encoded)
                    stream.flush()
                    os.fsync(stream.fileno())
            finally:
                os.close(descriptor)

    def record_event(self, event: str, severity: str = "INFO", component: str = "scraper", **fields: Any) -> dict[str, Any]:
        if not isinstance(event, str) or not event.strip():
            raise ValueError("diagnostic event name is required")
        level = str(severity).upper()
        if level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError("invalid diagnostic severity")
        safe_fields = {key: value for key, value in fields.items() if key in _ALLOWED_EVENT_FIELDS}
        payload = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "run_id": self.run_id,
            "event": event[:100],
            "severity": level,
            "component": component[:80],
            **safe_fields,
        }
        payload = _clean(payload)
        self._append(self.jsonl_path, payload)
        with self._lock:
            self._events += 1
            self._event_counts[event[:100]] += 1
            if level in {"ERROR", "CRITICAL"}:
                self._errors += 1
            elif level == "WARNING":
                self._warnings += 1
            if event == "video_started":
                self._active_workers += 1
                self._peak_workers = max(self._peak_workers, self._active_workers)
            elif event == "video_finished":
                self._active_workers = max(0, self._active_workers - 1)
        return payload

    def alert(self, code: str, severity: str, message: str, **fields: Any) -> dict[str, Any]:
        level = str(severity).upper()
        if level not in {"WARNING", "ERROR", "CRITICAL"}:
            raise ValueError("alerts must use WARNING, ERROR or CRITICAL severity")
        safe_fields = {key: value for key, value in fields.items() if key in _ALLOWED_EVENT_FIELDS}
        payload = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "run_id": self.run_id,
            "alert_code": str(code)[:100],
            "severity": level,
            "message": str(message)[:500],
            **safe_fields,
        }
        payload = _clean(payload)
        self._append(self.alert_path, payload)
        with self._lock:
            self._alerts += 1
        event_fields = dict(safe_fields)
        event_fields.setdefault("error_code", code)
        event_component = event_fields.pop("component", "alerting")
        self.record_event("alert_raised", severity=level, component=event_component, **event_fields)
        return payload

    def record_error(
        self, component: str, function: str, error: Exception,
        retry_attempt: int = 0, recovery_action: str | None = None,
        final_outcome: str | None = None,
    ) -> dict[str, Any]:
        code = getattr(error, "error_code", "UNEXPECTED_INTERNAL_ERROR")
        status = getattr(error, "http_status", None)
        payload = self.record_event(
            "operation_failed", severity="ERROR", component=component,
            function=function, error_code=str(code), error_type=type(error).__name__,
            http_status=status if isinstance(status, int) and 100 <= status <= 599 else None,
            retry_attempt=max(0, int(retry_attempt or 0)),
            stop_reason=final_outcome or recovery_action or "inspect_error_report",
            video_id=getattr(error, "video_id", None),
            worker_id=getattr(error, "worker_id", None),
        )
        self.alert(
            str(code), "ERROR", "An operation failed; inspect the structured event record.",
            component=component, function=function,
            error_code=str(code), error_type=type(error).__name__,
            http_status=status if isinstance(status, int) and 100 <= status <= 599 else None,
            video_id=getattr(error, "video_id", None),
            worker_id=getattr(error, "worker_id", None),
        )
        return payload

    def record_from_exception(self, component: str, function: str, error: Exception, retry_attempt: int = 0, recovery_action: str | None = None) -> dict[str, Any]:
        return self.record_error(component, function, error, retry_attempt, recovery_action)

    def record_video_result(self, result: dict[str, Any], duration_seconds: float) -> None:
        status = str(result.get("status", "unknown"))
        recovery = result.get("recovery_budget") or {}
        metrics = result.get("pagination_metrics") or {}
        malformed = int(metrics.get("malformed_rejected") or 0)
        retries = int(recovery.get("retries_used") or 0)
        self.record_event(
            "video_finished",
            severity="INFO" if status == "completed" else "WARNING",
            component="video_worker",
            video_id=result.get("video_id"),
            worker_id=result.get("worker_id"),
            status=status,
            stop_reason=result.get("stop_reason"),
            duration_seconds=round(max(0.0, duration_seconds), 3),
            total_comments=result.get("total_comments", 0),
            total_batches=result.get("total_batches", 0),
            records_received=result.get("records_received", 0),
            valid_comments=result.get("valid_comments", 0),
            duplicates_skipped=result.get("duplicates_skipped", 0),
            malformed_records=malformed,
            cache_error_count=len(result.get("cache_errors") or []),
            pagination_outcome=result.get("pagination_outcome"),
            retries_used=retries,
            recovery_exhausted_reason=recovery.get("exhausted_reason"),
        )
        if status != "completed":
            self.alert(
                "VIDEO_INCOMPLETE", "ERROR" if status == "failed" else "WARNING",
                "Video did not reach a completed pagination state.",
                video_id=result.get("video_id"), worker_id=result.get("worker_id"),
                status=status, stop_reason=result.get("stop_reason"),
                pagination_outcome=result.get("pagination_outcome"),
            )
        if malformed:
            self.alert(
                "MALFORMED_RECORDS_QUARANTINED", "WARNING",
                "Malformed source records were quarantined.",
                video_id=result.get("video_id"), worker_id=result.get("worker_id"),
                malformed_records=malformed,
            )
        if result.get("cache_errors"):
            self.alert(
                "CHECKPOINT_PERSISTENCE_ISSUE", "ERROR",
                "One or more checkpoint persistence operations reported an error.",
                video_id=result.get("video_id"), worker_id=result.get("worker_id"),
                cache_error_count=len(result.get("cache_errors") or []),
            )
        if recovery.get("exhausted_reason"):
            self.alert(
                "RECOVERY_BUDGET_EXHAUSTED", "WARNING",
                "The per-video recovery budget was exhausted.",
                video_id=result.get("video_id"), worker_id=result.get("worker_id"),
                retries_used=retries,
                recovery_exhausted_reason=recovery.get("exhausted_reason"),
            )
        elif retries >= 8:
            self.alert(
                "RECOVERY_BUDGET_NEAR_LIMIT", "WARNING",
                "The video consumed at least two-thirds of its retry allowance.",
                video_id=result.get("video_id"), worker_id=result.get("worker_id"),
                retries_used=retries,
                retries_remaining=recovery.get("retries_remaining"),
            )

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            snapshot = {
                "events_written": self._events,
                "alerts_written": self._alerts,
                "errors": self._errors,
                "warnings": self._warnings,
                "active_workers": self._active_workers,
                "peak_workers": self._peak_workers,
                "event_counts": dict(self._event_counts),
                "events_path": str(self.jsonl_path) if self.jsonl_path else None,
                "alerts_path": str(self.alert_path) if self.alert_path else None,
            }
        if self.jsonl_path:
            try:
                usage = shutil.disk_usage(self.jsonl_path.parent)
                snapshot.update({
                    "disk_free_bytes": usage.free,
                    "disk_total_bytes": usage.total,
                    "disk_usage_percent": round((usage.used / usage.total) * 100, 2) if usage.total else None,
                })
            except OSError:
                snapshot["disk_usage_unavailable"] = True
        return snapshot
