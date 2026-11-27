"""Versioned, portable JSON checkpoints with atomic replacement and preservation."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Tuple

try:
    import fcntl
except ImportError:  # Windows
    fcntl = None
try:
    import msvcrt
except ImportError:  # POSIX
    msvcrt = None

SCHEMA_VERSION = 2
PLATFORM = "tiktok"
DEFAULT_DISK_SPACE_THRESHOLD = 500 * 1024 * 1024
_LOCKS_GUARD = threading.Lock()
_RUN_LOCK = threading.Lock()
_THREAD_LOCKS: dict[int, threading.RLock] = {index: threading.RLock() for index in range(256)}
_HELD_LOCKS = threading.local()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_json(value: dict) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _checksum(value: dict) -> str:
    payload = dict(value)
    payload.pop("integrity_sha256", None)
    return hashlib.sha256(_canonical_json(payload)).hexdigest()


def _safe_video_id(video_id: str) -> str:
    value = str(video_id)
    if (not value or len(value) > 64
            or any(character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for character in value)):
        raise ValueError("video_id must contain 1-64 safe filename characters")
    return value


def _canonical_path(cache_dir: Path, video_id: str) -> Path:
    return cache_dir / f"tiktok_{_safe_video_id(video_id)}.json"


def _legacy_path(cache_dir: Path, worker_id: int, video_id: str) -> Path:
    return cache_dir / f"worker_{worker_id}_{_safe_video_id(video_id)}.json"


def _resolve_checkpoint_path(cache_dir: Path, worker_id: int, video_id: str):
    canonical = _canonical_path(cache_dir, video_id)
    if canonical.exists():
        return canonical, None
    legacy_candidates = sorted(cache_dir.glob(f"worker_*_{_safe_video_id(video_id)}.json"))
    if len(legacy_candidates) > 1:
        return None, "multiple_legacy_checkpoints_for_video_require_manual_reconciliation"
    if len(legacy_candidates) == 1:
        return legacy_candidates[0], None
    return _legacy_path(cache_dir, worker_id, video_id), None


def _lock_path(path: Path) -> Path:
    return path.with_name(path.name + ".lock")


def _thread_lock(path: Path) -> threading.Lock:
    # Fixed lock striping avoids retaining one Python lock for every video
    # ever processed during a long-running session.
    key = str(path.resolve())
    return _THREAD_LOCKS[hash(key) % len(_THREAD_LOCKS)]


def _os_lock(fd: int, deadline: float) -> bool:
    while True:
        try:
            if fcntl is not None:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            elif msvcrt is not None:
                os.lseek(fd, 0, os.SEEK_SET)
                if os.fstat(fd).st_size == 0:
                    os.write(fd, b"0")
                    os.fsync(fd)
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:
                raise RuntimeError("No supported cross-process file-lock backend")
            return True
        except (BlockingIOError, OSError):
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.05)


def _os_unlock(fd: int) -> None:
    if fcntl is not None:
        fcntl.flock(fd, fcntl.LOCK_UN)
    elif msvcrt is not None:
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)


def acquire_path_lock(path: Path, timeout: float = 30.0) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = _lock_path(path)
    thread_lock = _thread_lock(lock_path)
    if not thread_lock.acquire(timeout=max(0.0, timeout)):
        return False
    fd = None
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o600)
        if not _os_lock(fd, time.monotonic() + max(0.0, timeout)):
            os.close(fd)
            thread_lock.release()
            return False
        held = getattr(_HELD_LOCKS, "stack", [])
        held.append((str(path.resolve()), fd, thread_lock))
        _HELD_LOCKS.stack = held
        return True
    except Exception:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
        thread_lock.release()
        raise


def release_path_lock(path: Path) -> None:
    key = str(path.resolve())
    held = getattr(_HELD_LOCKS, "stack", [])
    for index in range(len(held) - 1, -1, -1):
        held_key, fd, thread_lock = held[index]
        if held_key == key:
            held.pop(index)
            try:
                _os_unlock(fd)
            finally:
                os.close(fd)
                thread_lock.release()
            return


@contextmanager
def scraper_run_lock(cache_dir: Path, timeout: float = 0.0):
    """Allow only one scraper run per cache root across threads and processes.

    This lock deliberately uses a dedicated in-process mutex rather than the
    checkpoint lock stripes: holding a run lock must never block a worker's
    unrelated per-video lock because of a hash-stripe collision.
    """
    root = Path(cache_dir)
    root.mkdir(parents=True, exist_ok=True)
    lock_path = root / ".scraper-run.lock"
    if not _RUN_LOCK.acquire(timeout=max(0.0, timeout)):
        raise RuntimeError("Another scraper run already owns this project cache")
    descriptor = None
    acquired_os_lock = False
    try:
        descriptor = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            os.chmod(lock_path, 0o600)
        except OSError:
            pass
        acquired_os_lock = _os_lock(descriptor, time.monotonic() + max(0.0, timeout))
        if not acquired_os_lock:
            raise RuntimeError("Another process already owns this project cache")
        try:
            yield
        finally:
            _os_unlock(descriptor)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        _RUN_LOCK.release()


@contextmanager
def video_checkpoint_lock(cache_dir: Path, video_id: str, timeout: float = 30.0):
    lock_target = cache_dir / f".video_{_safe_video_id(video_id)}"
    if not acquire_path_lock(lock_target, timeout):
        raise TimeoutError("Another worker or process already owns this video's checkpoint")
    try:
        yield
    finally:
        release_path_lock(lock_target)


def acquire_cache_lock(timeout: float = 30.0, cache_dir: Optional[Path] = None) -> bool:
    root = Path(cache_dir) if cache_dir is not None else Path(__file__).resolve().parent / "cache"
    target = root / ".cache-global"
    key = str(target.resolve())
    if any(held_key == key for held_key, _, _ in getattr(_HELD_LOCKS, "stack", [])):
        return False
    return acquire_path_lock(target, timeout)


def release_cache_lock(cache_dir: Optional[Path] = None) -> None:
    root = Path(cache_dir) if cache_dir is not None else Path(__file__).resolve().parent / "cache"
    release_path_lock(root / ".cache-global")


def _read_raw(path: Path) -> bytes:
    return path.read_bytes()


def _quarantine(path: Path, reason: str) -> Optional[Path]:
    if not path.exists():
        return None
    quarantine_dir = path.parent / "quarantine"
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
    target = quarantine_dir / f"{path.name}.{stamp}.corrupt"
    try:
        with path.open("rb") as source, target.open("xb") as destination:
            shutil.copyfileobj(source, destination)
            destination.flush()
            os.fsync(destination.fileno())
        os.chmod(target, 0o600)
        _fsync_directory(quarantine_dir)
        path.unlink()
        _fsync_directory(path.parent)
        return target
    except OSError:
        try:
            target.unlink(missing_ok=True)
        except OSError:
            pass
        return None


def _fsync_directory(directory: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(str(directory), os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _validate_payload(data: Any, video_id: str, worker_id: int) -> Tuple[bool, str]:
    if not isinstance(data, dict):
        return False, "checkpoint_root_not_object"
    if data.get("platform", PLATFORM) != PLATFORM or str(data.get("video_id", "")) != str(video_id):
        return False, "checkpoint_identity_mismatch"
    stored_worker_id = data.get("worker_id")
    if stored_worker_id is not None and (
            isinstance(stored_worker_id, bool) or not isinstance(stored_worker_id, int) or stored_worker_id < 0):
        return False, "invalid_checkpoint_worker_provenance"
    if not isinstance(data.get("comments"), list):
        return False, "comments_not_list"
    if not isinstance(data.get("seen_cids", []), list):
        return False, "seen_cids_not_list"
    cursor = data.get("cursor", 0)
    batch = data.get("batch_number", 0)
    if isinstance(cursor, bool) or not isinstance(cursor, int) or not 0 <= cursor <= (2**63 - 1):
        return False, "invalid_checkpoint_cursor"
    if isinstance(batch, bool) or not isinstance(batch, int) or batch < 0:
        return False, "invalid_checkpoint_batch_number"
    total_comments = data.get("total_comments", len(data["comments"]))
    if isinstance(total_comments, bool) or not isinstance(total_comments, int) or total_comments != len(data["comments"]):
        return False, "checkpoint_comment_count_mismatch"
    timestamp = data.get("timestamp_utc", data.get("timestamp"))
    if not isinstance(timestamp, str) or not timestamp.strip():
        return False, "checkpoint_timestamp_missing"
    try:
        parsed_timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError:
        return False, "checkpoint_timestamp_invalid"
    if parsed_timestamp.tzinfo is None and data.get("schema_version") == SCHEMA_VERSION:
        return False, "checkpoint_timestamp_timezone_missing"
    seen_ids = data.get("seen_cids", [])
    if any(isinstance(cid, bool) or not isinstance(cid, (str, int)) or not str(cid).strip() for cid in seen_ids):
        return False, "checkpoint_seen_id_invalid"
    if not isinstance(data.get("is_complete", False), bool):
        return False, "invalid_completion_flag"
    if data.get("schema_version") == SCHEMA_VERSION:
        expected = data.get("integrity_sha256")
        if not isinstance(expected, str) or expected != _checksum(data):
            return False, "checkpoint_integrity_checksum_mismatch"
        if data.get("completion_state") not in {"IN_PROGRESS", "COMPLETE"}:
            return False, "invalid_completion_state"
        if data["is_complete"] != (data["completion_state"] == "COMPLETE"):
            return False, "completion_state_flag_mismatch"
    elif data.get("schema_version", data.get("version", "0")) not in (0, "0", 1, "1"):
        return False, "unsupported_checkpoint_schema"
    return True, "valid"


def validate_cache(cache_data: dict, cache_filename: str = "") -> Tuple[str, str]:
    if not isinstance(cache_data, dict):
        return "corrupted", "Checkpoint root must be an object"
    ok, reason = _validate_payload(cache_data, str(cache_data.get("video_id", "")), cache_data.get("worker_id", 0))
    return ("valid", "") if ok else ("corrupted", reason)


def is_cache_corrupted(cache_data: dict) -> bool:
    return validate_cache(cache_data)[0] == "corrupted"


def _build_cache_data(worker_id: int, video_id: str, comments: list, seen_cids: set,
                      batch_number: int, cursor: int, is_final: bool = False,
                      completion_state: Optional[str] = None, stop_reason: Optional[str] = None,
                      run_id: Optional[str] = None) -> dict:
    if not isinstance(comments, list):
        raise TypeError("comments must be a list; refusing to replace checkpoint")
    if not isinstance(is_final, bool):
        raise TypeError("is_final must be a boolean")
    if isinstance(worker_id, bool) or not isinstance(worker_id, int) or worker_id < 0:
        raise ValueError("worker_id must be a non-negative integer")
    if isinstance(cursor, bool) or not isinstance(cursor, int) or not 0 <= cursor <= (2**63 - 1):
        raise ValueError("cursor must be a non-negative 64-bit integer")
    if isinstance(batch_number, bool) or not isinstance(batch_number, int) or batch_number < 0:
        raise ValueError("batch_number must be a non-negative integer")
    ids = sorted({str(cid).strip() for cid in seen_cids if isinstance(cid, (str, int)) and not isinstance(cid, bool) and str(cid).strip()})
    complete = is_final
    state = completion_state or ("COMPLETE" if complete else "IN_PROGRESS")
    if state not in {"IN_PROGRESS", "COMPLETE"}:
        raise ValueError("completion_state must be IN_PROGRESS or COMPLETE")
    if complete != (state == "COMPLETE"):
        raise ValueError("is_final and completion_state must describe the same state")
    payload = {
        "schema_version": SCHEMA_VERSION,
        "platform": PLATFORM,
        "video_id": _safe_video_id(video_id),
        "worker_id": worker_id,
        "run_id": run_id,
        "timestamp_utc": _utc_now(),
        "batch_number": batch_number,
        "successful_pages": batch_number,
        "cursor": cursor,
        "total_comments": len(comments),
        "is_complete": complete,
        "completion_state": state,
        "stop_reason": stop_reason,
        "seen_cids": ids,
        "comments": comments,
    }
    payload["integrity_sha256"] = _checksum(payload)
    return payload


def _disk_space_error(directory: Path) -> Optional[str]:
    try:
        free = shutil.disk_usage(directory).free
    except OSError as exc:
        return f"Could not inspect checkpoint storage capacity: {type(exc).__name__}"
    threshold_raw = os.environ.get("DISK_SPACE_THRESHOLD_BYTES")
    try:
        threshold = int(threshold_raw) if threshold_raw is not None else DEFAULT_DISK_SPACE_THRESHOLD
    except ValueError:
        return "DISK_SPACE_THRESHOLD_BYTES must be an integer"
    if threshold < 0:
        return "DISK_SPACE_THRESHOLD_BYTES cannot be negative"
    if free < threshold:
        return f"Insufficient free space for checkpoint write ({free} bytes available; {threshold} required)"
    return None


def _atomic_write(path: Path, data: dict) -> None:
    encoded = json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False).encode("utf-8")
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    temp_path = Path(temp_name)
    try:
        os.fchmod(fd, 0o600) if hasattr(os, "fchmod") else None
        with os.fdopen(fd, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_path, path)
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass
        _fsync_directory(path.parent)
    except Exception:
        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def _preserve_previous(path: Path) -> Optional[Path]:
    """Keep one durable immediately-previous checkpoint without unbounded growth."""
    if not path.exists():
        return None
    previous = path.with_name(path.name + ".previous.json")
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{previous.name}.", suffix=".tmp", dir=str(path.parent))
    temporary = Path(temporary_name)
    try:
        with path.open("rb") as source, os.fdopen(descriptor, "wb") as destination:
            shutil.copyfileobj(source, destination)
            destination.flush()
            os.fsync(destination.fileno())
        try:
            os.chmod(temporary, 0o600)
        except OSError:
            pass
        os.replace(temporary, previous)
        _fsync_directory(path.parent)
        return previous
    except Exception:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def _load_unlocked(path: Path, worker_id: int, video_id: str):
    if not path.exists():
        return None, "checkpoint_missing"
    try:
        raw = _read_raw(path)
        data = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        preserved = _quarantine(path, f"unreadable:{type(exc).__name__}")
        if preserved:
            return {"_quarantined": True, "_quarantine_path": str(preserved)}, "checkpoint_unreadable_quarantined"
        return None, "checkpoint_unreadable_and_preservation_failed"
    ok, reason = _validate_payload(data, video_id, worker_id)
    if not ok:
        preserved = _quarantine(path, reason)
        if preserved:
            return {"_quarantined": True, "_quarantine_path": str(preserved), "_corruption_reason": reason}, f"{reason}_quarantined"
        return None, f"{reason}_preservation_failed"
    # Migrate legacy cache metadata in memory; disk is updated on the next checkpoint.
    if data.get("schema_version") != SCHEMA_VERSION:
        data = dict(data)
        data["schema_version"] = 1
        data["platform"] = PLATFORM
        data["timestamp_utc"] = data.get("timestamp") or _utc_now()
        data["successful_pages"] = data.get("batch_number", 0)
        data["completion_state"] = "COMPLETE" if data.get("is_complete") else "IN_PROGRESS"
        data["stop_reason"] = None
    return data, "valid" if data.get("schema_version") == SCHEMA_VERSION else "legacy_cache_migrated_in_memory"


def load_cache_safe(cache_dir: Path, worker_id: int, video_id: str):
    cache_dir = Path(cache_dir)
    path, resolution_error = _resolve_checkpoint_path(cache_dir, worker_id, video_id)
    if resolution_error:
        return None, resolution_error
    if not acquire_cache_lock(timeout=30.0):
        return None, "global_cache_lock_timeout"
    try:
        if not acquire_path_lock(path, timeout=30.0):
            return None, "checkpoint_lock_timeout"
        try:
            return _load_unlocked(path, worker_id, str(video_id))
        finally:
            release_path_lock(path)
    finally:
        release_cache_lock()


def save_and_load_cache_safe(cache_dir: Path, worker_id: int, video_id: str, comments: list,
                             seen_cids: set, batch_number: int, cursor: int,
                             is_final: bool = False, completion_state: Optional[str] = None,
                             stop_reason: Optional[str] = None, run_id: Optional[str] = None):
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = _canonical_path(cache_dir, video_id)
    # The global lock preserves the legacy lock contract; the per-video lock
    # additionally prevents concurrent writes to this video's checkpoint.
    if not acquire_cache_lock(timeout=5.0):
        return None, "Global cache lock is already held by another writer"
    try:
        if not acquire_path_lock(path, timeout=0.05):
            return None, "Per-video checkpoint lock is already held by another writer"
        try:
            space_error = _disk_space_error(cache_dir)
            if space_error:
                return None, space_error
            existing = None
            source_path, resolution_error = _resolve_checkpoint_path(cache_dir, worker_id, video_id)
            if resolution_error:
                return None, resolution_error
            if source_path.exists():
                existing, status = _load_unlocked(source_path, worker_id, str(video_id))
                if existing is None:
                    return None, status
                if existing.get("_quarantined"):
                    # The damaged bytes are preserved in quarantine. The
                    # caller may only continue from its current in-memory data.
                    existing = None
                elif existing.get("is_complete"):
                    return None, "Refusing to overwrite an already completed video checkpoint"
                else:
                    previous_batch = int(existing.get("batch_number", 0))
                    previous_cursor = int(existing.get("cursor", 0))
                    if batch_number < previous_batch:
                        return None, "Refusing checkpoint regression: batch number moved backwards"
                    if cursor < previous_cursor:
                        return None, "Refusing checkpoint regression: cursor moved backwards"
                    if batch_number == previous_batch and comments != existing.get("comments", []):
                        return None, "Checkpoint lock conflict: divergent data at the same batch position"
                    if len(comments) < len(existing.get("comments", [])):
                        return None, "Refusing checkpoint regression: collected comment count decreased"
            new_data = _build_cache_data(worker_id, str(video_id), comments, seen_cids,
                                         batch_number, cursor, is_final, completion_state,
                                         stop_reason, run_id)
            if existing is not None:
                try:
                    _preserve_previous(source_path)
                except OSError as exc:
                    return None, f"Could not preserve previous checkpoint: {type(exc).__name__}"
            try:
                _atomic_write(path, new_data)
            except (OSError, TypeError, ValueError) as exc:
                return None, f"Checkpoint write failed: {type(exc).__name__}: {exc}"
            return path, None
        finally:
            release_path_lock(path)
    finally:
        release_cache_lock()


def save_cache_safe(cache_dir: Path, worker_id: int, video_id: str, comments: list,
                    seen_cids: set, batch_number: int, cursor: int, is_final: bool = False):
    return save_and_load_cache_safe(cache_dir, worker_id, video_id, comments, seen_cids,
                                    batch_number, cursor, is_final)


def restore_previous_checkpoint(cache_dir: Path, video_id: str, worker_id: int = 0):
    """Restore the immediately previous valid checkpoint, preserving current data."""
    cache_dir = Path(cache_dir)
    current = _canonical_path(cache_dir, video_id)
    previous = current.with_name(current.name + ".previous.json")
    if not acquire_path_lock(current):
        return None, "checkpoint_lock_timeout"
    try:
        if not previous.exists():
            return None, "previous_checkpoint_missing"
        try:
            data = json.loads(previous.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            return None, f"previous_checkpoint_unreadable:{type(exc).__name__}"
        backup_worker_id = data.get("worker_id", worker_id) if isinstance(data, dict) else worker_id
        valid, reason = _validate_payload(data, str(video_id), backup_worker_id)
        if not valid:
            return None, f"previous_checkpoint_invalid:{reason}"
        if current.exists():
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
            archive = current.with_name(f"{current.name}.pre_restore.{stamp}")
            try:
                with current.open("rb") as source, archive.open("xb") as destination:
                    shutil.copyfileobj(source, destination)
                    destination.flush()
                    os.fsync(destination.fileno())
                os.chmod(archive, 0o600)
                _fsync_directory(current.parent)
            except OSError as exc:
                return None, f"current_checkpoint_archive_failed:{type(exc).__name__}"
        try:
            _atomic_write(current, data)
        except (OSError, TypeError, ValueError) as exc:
            return None, f"checkpoint_restore_failed:{type(exc).__name__}"
        return current, None
    finally:
        release_path_lock(current)


def cleanup_orphan_temp_files(cache_dir: Path):
    """Remove legacy temp files only when their own OS lock is available."""
    removed = []
    root = Path(cache_dir)
    if not root.exists():
        return removed
    # Older POSIX releases used this filename pattern and held an OS lock
    # while writing. Windows does not permit unlinking an open locked file.
    for candidate in (root.glob(".tmp_*.json") if os.name != "nt" else ()):
        descriptor = None
        acquired = False
        try:
            descriptor = os.open(str(candidate), os.O_CREAT | os.O_RDWR, 0o600)
            acquired = _os_lock(descriptor, time.monotonic())
            if acquired:
                candidate.unlink()
                removed.append(candidate.name)
        except OSError:
            continue
        finally:
            if descriptor is not None:
                try:
                    if acquired:
                        _os_unlock(descriptor)
                finally:
                    os.close(descriptor)
    # New-format temporary files are uniquely named. Only old files are
    # eligible for cleanup; active writers never leave a seven-day temp.
    cutoff = time.time() - 7 * 24 * 60 * 60
    for candidate in root.glob(".*.tmp"):
        try:
            if candidate.stat().st_mtime < cutoff:
                candidate.unlink()
                removed.append(candidate.name)
        except OSError:
            continue
    return removed


def check_disk_space(min_free_bytes: int):
    try:
        free = shutil.disk_usage(Path.cwd()).free
    except OSError as exc:
        return False, f"Could not inspect disk space: {type(exc).__name__}"
    return (free >= min_free_bytes, f"{free} bytes free; {min_free_bytes} required")
