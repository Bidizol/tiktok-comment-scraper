"""Atomic, run-scoped JSON and CSV exports for normalized comment records."""
from __future__ import annotations

import csv
import json
import os
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CSV_FIELDS = (
    "platform", "video_id", "comment_id", "user_id", "username", "nickname",
    "text", "likes", "reply_count", "created_at_epoch", "created_at_utc",
    "parent_comment_id", "is_reply", "collected_at_utc", "dedupe_key",
)


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, float):
        import math
        return value if math.isfinite(value) else str(value)
    if value is None or isinstance(value, (str, int, bool)):
        return value
    return str(value)


def _csv_safe(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    candidate = value.lstrip(" \t\r\n\x00")
    if candidate.startswith(("=", "+", "-", "@")):
        return "'" + value
    return value


def _atomic_replace(path: Path, writer) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    temp_path = Path(temp_name)
    try:
        try:
            os.chmod(temp_path, 0o600)
        except OSError:
            pass
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as stream:
            writer(stream)
            stream.flush()
            os.fsync(stream.fileno())
        # Run directories are unique. Never replace an existing artifact.
        if path.exists():
            raise FileExistsError(f"Refusing to overwrite existing run artifact: {path.name}")
        os.replace(temp_path, path)
        if os.name != "nt":
            directory_fd = os.open(str(path.parent), os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    except Exception:
        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def write_run_outputs(results: list[dict], output_root: Path, *, run_id: str | None = None,
                      run_metadata: dict | None = None) -> dict:
    """Write immutable run-scoped exports; return artifact paths and aggregate counts."""
    if not isinstance(results, list):
        raise TypeError("results must be a list")
    run_id = run_id or str(uuid.uuid4())
    if not run_id or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for ch in run_id):
        raise ValueError("run_id contains unsafe path characters")
    run_dir = Path(output_root) / f"run_{run_id}"
    run_dir.mkdir(parents=True, exist_ok=False)
    try:
        os.chmod(run_dir, 0o700)
    except OSError:
        pass
    comments = []
    video_summaries = []
    for result in results:
        if not isinstance(result, dict):
            raise TypeError("each video result must be an object")
        video_comments = result.get("comments", [])
        if not isinstance(video_comments, list):
            raise TypeError("video result comments must be a list")
        for comment in video_comments:
            if not isinstance(comment, dict):
                raise TypeError("only normalized comment objects can be exported")
            if not isinstance(comment.get("platform"), str) or not comment.get("platform"):
                raise ValueError("normalized comment is missing its platform")
            if str(comment.get("video_id", "")) != str(result.get("video_id", "")):
                raise ValueError("comment video identity does not match its video result")
            if not isinstance(comment.get("text"), str):
                raise ValueError("normalized comment text must be a string")
            comments.append(comment)
        video_summaries.append({
            key: result.get(key) for key in (
                "video_id", "author", "worker_id", "run_id", "status", "stop_reason",
                "total_comments", "total_batches", "records_received", "valid_comments", "new_comments",
                "duplicates_skipped", "invalid_missing_ids", "normalization_anomalies",
                "error_code", "error_type", "cache_recovery_error",
                "malformed_comments_path", "pagination_metrics", "cache_errors",
                "pagination_outcome", "stop_reason", "recovery_budget",
            )
        })
    created = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    summary = _json_safe({
        "run_id": run_id,
        "created_at_utc": created,
        "total_videos": len(video_summaries),
        "completed_videos": sum(1 for item in video_summaries if item.get("status") == "completed"),
        "partial_videos": sum(1 for item in video_summaries if item.get("status") != "completed"),
        "total_comments": len(comments),
        "valid_comments": sum(int(item.get("valid_comments") or 0) for item in video_summaries),
        "new_comments": sum(int(item.get("new_comments") or 0) for item in video_summaries),
        "records_received": sum(int(item.get("records_received") or 0) for item in video_summaries),
        "duplicates_skipped": sum(int(item.get("duplicates_skipped") or 0) for item in video_summaries),
        "missing_comment_ids": sum(int(item.get("invalid_missing_ids") or 0) for item in video_summaries),
        "malformed_records": sum(int((item.get("pagination_metrics") or {}).get("malformed_rejected") or 0) for item in video_summaries),
        "successful_pages": sum(int((item.get("pagination_metrics") or {}).get("batch_number") or 0) for item in video_summaries),
        "videos": video_summaries,
        "metadata": run_metadata or {},
    })
    json_path = run_dir / "comments.json"
    csv_path = run_dir / "comments.csv"
    summary_path = run_dir / "summary.json"

    def write_json(stream):
        json.dump({"run": summary, "comments": comments}, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")

    def write_csv(stream):
        output = csv.DictWriter(stream, fieldnames=CSV_FIELDS, extrasaction="ignore", lineterminator="\n")
        output.writeheader()
        for comment in comments:
            output.writerow({key: _csv_safe(comment.get(key)) for key in CSV_FIELDS})

    def write_summary(stream):
        json.dump(summary, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")

    try:
        _atomic_replace(json_path, write_json)
        _atomic_replace(csv_path, write_csv)
        _atomic_replace(summary_path, write_summary)
    except Exception:
        # Keep already-created artifacts; a later run uses a new directory.
        raise
    return {
        "run_id": run_id, "output_dir": str(run_dir),
        "json_path": str(json_path), "csv_path": str(csv_path),
        "summary_path": str(summary_path), "total_comments": len(comments),
        "total_videos": len(video_summaries),
    }
