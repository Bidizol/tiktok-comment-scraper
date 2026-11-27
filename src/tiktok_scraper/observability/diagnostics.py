"""Append-only, machine-readable operational diagnostic events."""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any

from ..reliability.errors import ErrorRecord, redact_secrets


class DiagnosticRecorder:
    """Persist sanitized JSONL events without rewriting prior history."""

    def __init__(self, report_dir: Path) -> None:
        self._report_dir = report_dir
        self._path = report_dir / "errors.jsonl"
        self._lock = threading.Lock()

    @property
    def path(self) -> Path:
        return self._path

    def record_error(self, error: ErrorRecord) -> None:
        self._append(error.to_dict())

    def record_event(self, event: str, **fields: Any) -> None:
        if not event.strip():
            raise ValueError("event name is required")
        payload = {
            "event": redact_secrets(event),
            **{key: redact_secrets(value) for key, value in fields.items()},
        }
        self._append(payload)

    def _append(self, payload: dict[str, Any]) -> None:
        self._report_dir.mkdir(parents=True, exist_ok=True)
        encoded = (
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n"
        ).encode("utf-8")
        with self._lock:
            descriptor = os.open(
                self._path,
                os.O_WRONLY | os.O_CREAT | os.O_APPEND,
                0o600,
            )
            try:
                with os.fdopen(descriptor, "ab", closefd=False) as stream:
                    stream.write(encoded)
                    stream.flush()
                    os.fsync(stream.fileno())
            finally:
                os.close(descriptor)
