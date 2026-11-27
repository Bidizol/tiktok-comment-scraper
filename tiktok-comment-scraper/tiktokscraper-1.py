import sys
import os
import logging
import threading
import uuid
import json
import time
import shutil
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import signal

# Add the parent directory to path for imports
# This allows importing from the tiktok_comment_scraper package
sys.path.insert(0, str(Path(__file__).parent.parent))

# New Phase 2/3 imports - centralized handling
from http_transport import HTTPTransport, fetch_json
from token_manager import TokenManager, MAX_REFRESH_ATTEMPTS
from proxy_manager import ProxyManager
from retry import BoundedRetry, RetryPolicy, RetryRecord
from coordinated_retry import CoordinatedRetryBudget, ErrorClassifier, AttemptRecord, make_classifier_for_operation, CoordinatedRetryContext
from recovery_budget import RecoveryBudget
from run_control import RunControl
from diagnostics import DiagnosticsRecorder
from comment_schema import normalize_comment_record, safe_source_payload
from platform_adapters import TikTokAdapter
from output_writer import write_run_outputs

from errors.model import ScraperError, ErrorSeverity, ErrorOutcome

from comment_paginator import (
    CommentPaginator,
    PaginatorResult,
    PaginatorOutcome,
    PaginationStop,
    DEFAULT_MAX_BATCHES,
    DEFAULT_MAX_COMMENTS,
    DEFAULT_MAX_CONSECUTIVE_EMPTY,
    DEFAULT_STALLED_TIMEOUT_SECONDS,
    DEFAULT_ELAPSED_TIMEOUT_SECONDS,
)

from pagination_state import normalise_has_more, PaginationState, PaginationMetadata, CursorIntegrityChecker

# Cache safety – single shared read-modify-write implementation.
# Imported from cache_safety so there is no second competing cache implementation.
sys.path.insert(0, str(Path(__file__).parent))
from cache_safety import (
    save_and_load_cache_safe, cleanup_orphan_temp_files, load_cache_safe,
    video_checkpoint_lock, scraper_run_lock,
)  # noqa: E402

# Configuration
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
MAX_RETRIES = 3
from config import CACHE_DIR, MAX_COMMENTS as CONFIG_MAX_COMMENTS
MAX_COMMENTS = CONFIG_MAX_COMMENTS
COMMENTS_PER_BATCH = 50

# Cache version for backward compatibility
CACHE_VERSION = "2"

# Save cache every N batches (operational batch persistence interval)
# Phase 5 Block 3.1: checkpoint every successfully processed batch to minimize
# data loss after unexpected termination.
SAVE_EVERY_N_BATCHES: int = 1

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# Lock para print thread-safe
print_lock = threading.Lock()


def safe_print(message):
    with print_lock:
        print(message)

def validate_cache_data(cached: dict, video_id: str) -> tuple:
    """Validate without silently dropping any previously collected records.

    Returns (comments, seen_cids, cursor, batch_number, is_complete, is_valid).
    Invalid structures are rejected as a whole and remain available in quarantine.
    """
    if not isinstance(cached, dict) or str(cached.get("video_id", "")) != str(video_id):
        return [], set(), 0, 0, False, False
    comments = cached.get("comments")
    seen_raw = cached.get("seen_cids")
    cursor = cached.get("cursor")
    batch_number = cached.get("batch_number")
    total = cached.get("total_comments", len(comments) if isinstance(comments, list) else None)
    is_complete = cached.get("is_complete", False)
    if not isinstance(comments, list) or not isinstance(seen_raw, list):
        return [], set(), 0, 0, False, False
    if isinstance(cursor, bool) or not isinstance(cursor, int) or not 0 <= cursor <= (2**63 - 1):
        return [], set(), 0, 0, False, False
    if isinstance(batch_number, bool) or not isinstance(batch_number, int) or batch_number < 0:
        return [], set(), 0, 0, False, False
    if isinstance(total, bool) or not isinstance(total, int) or total != len(comments):
        return [], set(), 0, 0, False, False
    if not isinstance(is_complete, bool):
        return [], set(), 0, 0, False, False
    seen_cids = set()
    for cid in seen_raw:
        if isinstance(cid, bool) or not isinstance(cid, (str, int)) or not str(cid).strip():
            return [], set(), 0, 0, False, False
        seen_cids.add(str(cid).strip())
    return list(comments), seen_cids, cursor, batch_number, is_complete, True


def validate_cache_identity(cached: dict, video_id: str, worker_id: int) -> bool:
    """Validate that a loaded cache matches the requested identity and data structures.

    Every cache loaded for a scraping operation must match the TikTok platform
    and requested video_id. Worker ID is provenance, not part of the identity.
    Comment, seen-ID, cursor and batch structures are validated without dropping records.

    Returns True if the cache is valid and safe to resume from.
    Returns False if the cache is missing fields, has mismatched identity, or
    contains malformed data structures. Invalid caches must not be silently loaded;
    recoverable data is preserved via the quarantine mechanism in save_and_load_cache_safe.
    """
    if not isinstance(cached, dict):
        return False

    # Exact video_id match required
    if cached.get("video_id") != video_id:
        return False

    # New checkpoints use (platform, video_id) as identity. Legacy
    # worker-keyed files retain their historical worker ownership during migration.
    if cached.get("platform") == "tiktok":
        pass
    elif cached.get("worker_id") != worker_id:
        return False

    # comments must be a list
    comments = cached.get("comments")
    if not isinstance(comments, list):
        return False

    # seen_cids must be a list (serialized form of the set)
    seen_cids = cached.get("seen_cids")
    if not isinstance(seen_cids, list):
        return False

    # cursor must be an int (or coercible to one)
    cursor = cached.get("cursor")
    if cursor is None:
        return False
    try:
        int(cursor)
    except (ValueError, TypeError):
        return False

    # batch_number must be an int (or coercible to one)
    batch_number = cached.get("batch_number")
    if batch_number is None:
        return False
    try:
        int(batch_number)
    except (ValueError, TypeError):
        return False

    return True


def load_cache(worker_id: int, video_id: str) -> dict | None:
    """Cargar cache existente para un video.

    Valida:
    - Que el archivo exista y sea JSON válido
    - Que el video_id coincida con el solicitado
    - Que el campo version sea compatible (formato backward-compatible)
    - Que is_complete y otros campos sean coherentes
    - Que comment IDs y cursor sean válidos

    Returns None si el cache está corrupto, pertenece a otro video
    o no puede ser validado de forma segura.

    If an existing cache cannot be safely migrated, preserve it and report
    the problem rather than silently overwriting it.
    """
    cache_file = CACHE_DIR / f"tiktok_{video_id}.json"
    legacy_cache_files = list(CACHE_DIR.glob(f"worker_*_{video_id}.json"))
    if not cache_file.exists() and not legacy_cache_files:
        return None

    cached, cache_status = load_cache_safe(CACHE_DIR, worker_id, video_id)
    if cached is None:
        safe_print(f"[W{worker_id}] Cache no disponible o ilegible: {cache_file}: {cache_status}")
        return None

    if cached.get("_quarantined"):
        safe_print(f"[W{worker_id}] Cache preservado en cuarentena: {cache_status}")
        return None

    # Phase 5 Block 3.1: Verify identity and structure after the locked read.
    if not validate_cache_identity(cached, video_id, worker_id):
        safe_print(f"[W{worker_id}] Cache inválido: {cache_file} ({cache_status})")
        return None

    # Validate the cached data
    comments, seen_cids, cursor, batch_number, is_complete, data_valid = validate_cache_data(cached, video_id)
    if not data_valid:
        safe_print(f"[W{worker_id}] Cache structure rejected and preserved: {cache_file}")
        return None

    if not comments and not cached.get("video_id"):
        # No valid data to recover
        return None

    cached_version = cached.get("schema_version", cached.get("version", "0"))
    safe_print(f"[W{worker_id}] Cache validado: version={cached_version}, is_complete={is_complete}, {len(comments)} comentarios, cursor={cursor}")

    # Return structured data with validated values
    return {
        "version": str(cached.get("schema_version", cached_version)),
        "platform": "tiktok",
        "video_id": video_id,
        "worker_id": cached.get("worker_id", worker_id),
        "timestamp": cached.get("timestamp_utc", cached.get("timestamp", datetime.now(timezone.utc).isoformat())),
        "batch_number": batch_number,
        "cursor": cursor,
        "total_comments": len(comments),
        "is_complete": is_complete,
        "completion_state": cached.get("completion_state", "COMPLETE" if is_complete else "IN_PROGRESS"),
        "successful_pages": cached.get("successful_pages", batch_number),
        "stop_reason": cached.get("stop_reason"),
        "run_id": cached.get("run_id"),
        "seen_cids": list(seen_cids),
        "comments": comments,
    }


def save_cache(worker_id: int, video_id: str, comments: list, seen_cids: set, batch_number: int, cursor: int, is_final: bool = False):
    """Compatibility wrapper for the single versioned checkpoint writer."""
    saved_path, save_error = save_and_load_cache_safe(
        cache_dir=CACHE_DIR, worker_id=worker_id, video_id=video_id,
        comments=comments, seen_cids=seen_cids, batch_number=batch_number,
        cursor=cursor, is_final=is_final,
        completion_state="COMPLETE" if is_final else "IN_PROGRESS",
    )
    if save_error:
        raise RuntimeError(f"Checkpoint persistence failed: {save_error}")
    return saved_path


def export_run_results(results: list, run_id: str | None = None, run_metadata: dict | None = None) -> dict:
    """Create immutable JSON/CSV/summary artifacts for one completed scraper run."""
    from config import OUTPUT_DIR
    return write_run_outputs(results, OUTPUT_DIR, run_id=run_id, run_metadata=run_metadata)


def scrape_videos_and_export(video_jobs: list, max_workers: int = 3, run_id: str | None = None, run_control: RunControl | None = None) -> dict:
    """Run bounded video work and persist structured operational telemetry."""
    if not isinstance(video_jobs, list):
        raise TypeError("video_jobs must be a list")
    if isinstance(max_workers, bool) or not isinstance(max_workers, int) or not 1 <= max_workers <= 16:
        raise ValueError("max_workers must be an integer between 1 and 16")
    if isinstance(MAX_COMMENTS, bool) or not isinstance(MAX_COMMENTS, int) or not 1 <= MAX_COMMENTS <= 100_000:
        raise ValueError("MAX_COMMENTS must be an integer between 1 and 100000")
    if len(video_jobs) > 1000:
        raise ValueError("A single run is limited to 1000 video jobs")
    run_id = run_id or str(uuid.uuid4())
    if not run_id or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for ch in run_id):
        raise ValueError("run_id contains unsafe path characters")

    normalized_jobs = []
    seen_video_ids = set()
    for job in video_jobs:
        if not isinstance(job, (tuple, list)) or len(job) != 2:
            raise ValueError("Each video job must be a (video_id, author) pair")
        video_id, author = str(job[0]).strip(), str(job[1]).strip()
        if not video_id.isdigit() or not 1 <= len(video_id) <= 32:
            raise ValueError("Each video_id must contain 1-32 decimal digits")
        if video_id in seen_video_ids:
            raise ValueError("Duplicate video_id in one run: " + video_id)
        seen_video_ids.add(video_id)
        normalized_jobs.append((video_id, author))

    from config import (
        ERROR_REPORT_DIR, MAX_RUN_COMMENTS, MINIMUM_FREE_BYTES,
        DIAGNOSTICS_MAX_BYTES, DIAGNOSTICS_BACKUP_COUNT,
    )
    if run_control is None:
        run_control = RunControl(max_comments=MAX_RUN_COMMENTS)
    events_path = ERROR_REPORT_DIR / f"run_{run_id}.events.jsonl"
    alerts_path = ERROR_REPORT_DIR / f"run_{run_id}.alerts.jsonl"
    monitor = DiagnosticsRecorder(
        run_id=run_id, jsonl_path=events_path, alert_path=alerts_path,
        max_bytes=DIAGNOSTICS_MAX_BYTES, backup_count=DIAGNOSTICS_BACKUP_COUNT,
    )
    run_started = time.monotonic()
    results_by_index = {}
    next_index = 0
    max_pending = max_workers * 2

    def interrupted_result(index, reason):
        video_id, author = normalized_jobs[index]
        return {
            "video_id": video_id, "author": author, "worker_id": index + 1,
            "run_id": run_id, "total_comments": 0, "total_batches": 0,
            "status": "partial", "stop_reason": reason, "comments": [],
        }

    health = monitor.snapshot()
    low_disk_alerted = health.get("disk_free_bytes") is not None and health["disk_free_bytes"] < MINIMUM_FREE_BYTES
    if low_disk_alerted:
        monitor.alert(
            "LOW_DISK_SPACE", "ERROR",
            "Available disk space is below the configured minimum threshold.",
            disk_free_bytes=health["disk_free_bytes"],
            disk_total_bytes=health.get("disk_total_bytes"),
        )
    monitor.record_event(
        "run_started", component="run_coordinator",
        requested_video_jobs=len(normalized_jobs), max_workers=max_workers,
        max_pending_jobs=max_pending, max_run_comments=run_control.max_comments,
        disk_free_bytes=health.get("disk_free_bytes"),
        disk_total_bytes=health.get("disk_total_bytes"),
        disk_usage_percent=health.get("disk_usage_percent"),
    )

    try:
        with scraper_run_lock(CACHE_DIR, timeout=0.0):
            monitor.record_event("run_lock_acquired", component="run_coordinator")
            with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="tiktok-video") as executor:
                pending = {}
    
                def submit_until_bounded():
                    nonlocal next_index
                    while (not run_control.is_stopped()
                           and next_index < len(normalized_jobs)
                           and len(pending) < max_pending):
                        index = next_index
                        next_index += 1
                        video_id, author = normalized_jobs[index]
                        monitor.record_event(
                            "video_queued", component="run_coordinator",
                            video_id=video_id, worker_id=index + 1,
                        )
                        future = executor.submit(
                            scrape_video_comments, video_id, author, index + 1,
                            run_id, run_control, monitor,
                        )
                        pending[future] = index
    
                submit_until_bounded()
                while pending:
                    completed, _ = wait(tuple(pending), return_when=FIRST_COMPLETED)
                    for future in completed:
                        index = pending.pop(future)
                        if future.cancelled():
                            results_by_index[index] = interrupted_result(index, "run_stopped_before_worker_start")
                            continue
                        try:
                            results_by_index[index] = future.result()
                        except Exception as error:
                            video_id, author = normalized_jobs[index]
                            recovered_comments = []
                            recovery_error = None
                            try:
                                recovered_cache = load_cache(index + 1, video_id)
                                if recovered_cache:
                                    recovered_comments = [
                                        normalize_comment_record(video_id, item)
                                        for item in recovered_cache.get("comments", [])
                                    ]
                            except Exception as cache_error:
                                recovery_error = type(cache_error).__name__
                            results_by_index[index] = {
                                "video_id": video_id, "author": author, "worker_id": index + 1,
                                "run_id": run_id, "total_comments": len(recovered_comments), "total_batches": 0,
                                "status": "failed", "stop_reason": "unhandled_video_worker_exception",
                                "error_code": getattr(error, "error_code", "UNEXPECTED_INTERNAL_ERROR"),
                                "error_type": type(error).__name__, "cache_recovery_error": recovery_error,
                                "comments": recovered_comments,
                            }
                            monitor.record_event(
                                "worker_result_recovered", severity="ERROR", component="run_coordinator",
                                video_id=video_id, worker_id=index + 1, status="failed",
                                stop_reason="unhandled_video_worker_exception",
                                error_code=getattr(error, "error_code", "UNEXPECTED_INTERNAL_ERROR"),
                                error_type=type(error).__name__,
                                cache_error_count=1 if recovery_error else 0,
                            )
    
                    if run_control.is_stopped():
                        for future, index in list(pending.items()):
                            if future.cancel():
                                pending.pop(future)
                                results_by_index[index] = interrupted_result(index, "run_stopped_before_worker_start")
                    else:
                        submit_until_bounded()
    except RuntimeError as error:
        monitor.record_error("run_coordinator", "scraper_run_lock_or_executor", error)
        raise

    for index in range(next_index, len(normalized_jobs)):
        results_by_index[index] = interrupted_result(index, "run_stopped_before_worker_start")

    results = [
        results_by_index.get(index, interrupted_result(index, "worker_result_unavailable"))
        for index in range(len(normalized_jobs))
    ]
    completed_count = sum(1 for item in results if item.get("status") == "completed")
    failed_count = sum(1 for item in results if item.get("status") == "failed")
    partial_count = len(results) - completed_count - failed_count
    total_comments = sum(int(item.get("total_comments") or 0) for item in results)
    monitor.record_event(
        "run_results_aggregated",
        severity="INFO" if completed_count == len(results) else "WARNING",
        component="run_coordinator",
        requested_video_jobs=len(results), completed_videos=completed_count,
        partial_videos=partial_count, failed_videos=failed_count,
        total_comments=total_comments, comments_reserved=run_control.comments_reserved,
        max_run_comments=run_control.max_comments,
        duration_seconds=round(time.monotonic() - run_started, 3),
        stop_reason=run_control.stop_reason,
    )
    if run_control.is_stopped():
        monitor.alert(
            "RUN_STOPPED", "WARNING",
            "The run stopped before all requested work completed.",
            stop_reason=run_control.stop_reason,
            requested_video_jobs=len(results), completed_videos=completed_count,
            partial_videos=partial_count, failed_videos=failed_count,
        )
    if partial_count or failed_count:
        monitor.alert(
            "RUN_INCOMPLETE", "WARNING",
            "One or more video jobs did not reach a completed state.",
            requested_video_jobs=len(results), completed_videos=completed_count,
            partial_videos=partial_count, failed_videos=failed_count,
        )

    final_health = monitor.snapshot()
    final_disk_free = final_health.get("disk_free_bytes")
    if (not low_disk_alerted and final_disk_free is not None
            and final_disk_free < MINIMUM_FREE_BYTES):
        monitor.alert(
            "LOW_DISK_SPACE", "ERROR",
            "Available disk space fell below the configured minimum during this run.",
            disk_free_bytes=final_disk_free,
            disk_total_bytes=final_health.get("disk_total_bytes"),
        )
    monitor_snapshot = monitor.snapshot()
    try:
        outputs = export_run_results(
            results, run_id=run_id,
            run_metadata={
                "max_workers": max_workers, "max_pending_jobs": max_pending,
                "requested_video_jobs": len(normalized_jobs),
                "interrupted": run_control.is_stopped(),
                "stop_reason": run_control.stop_reason,
                "max_run_comments": run_control.max_comments,
                "comments_reserved": run_control.comments_reserved,
                "monitoring": monitor_snapshot,
            },
        )
    except Exception as error:
        monitor.record_error("output_writer", "export_run_results", error)
        raise

    monitor.record_event(
        "run_outputs_written", component="output_writer",
        output_dir=outputs.get("output_dir"), summary_path=outputs.get("summary_path"),
        json_path=outputs.get("json_path"), csv_path=outputs.get("csv_path"),
        total_comments=outputs.get("total_comments"),
    )
    monitor.record_event(
        "run_finished", component="run_coordinator",
        severity="INFO" if completed_count == len(results) else "WARNING",
        requested_video_jobs=len(results), completed_videos=completed_count,
        partial_videos=partial_count, failed_videos=failed_count,
        total_comments=total_comments,
        duration_seconds=round(time.monotonic() - run_started, 3),
        stop_reason=run_control.stop_reason,
    )
    return {
        "run_id": run_id, "results": results, "outputs": outputs,
        "interrupted": run_control.is_stopped(), "stop_reason": run_control.stop_reason,
        "monitoring": monitor.snapshot(),
    }


def scrape_video_comments(
    video_id: str, video_author: str, worker_id: int,
    run_id: str | None = None, run_control: RunControl | None = None,
    monitor: DiagnosticsRecorder | None = None,
) -> dict:
    """Serialize checkpoint ownership and emit one bounded lifecycle record."""
    run_control = run_control or RunControl()
    started = time.monotonic()
    if monitor:
        monitor.record_event("video_started", component="video_worker", video_id=str(video_id), worker_id=worker_id)
    try:
        with video_checkpoint_lock(CACHE_DIR, str(video_id), timeout=30.0):
            result = _scrape_video_comments_locked(
                video_id, video_author, worker_id, run_id=run_id,
                run_control=run_control,
            )
    except TimeoutError as lock_error:
        result = {
            "video_id": str(video_id), "author": video_author, "worker_id": worker_id,
            "total_comments": 0, "total_batches": 0, "tokens_used": 0,
            "proxy_rotations": 0, "duplicates_skipped": 0, "invalid_missing_ids": 0,
            "status": "partial", "stop_reason": "video_checkpoint_lock_unavailable",
            "cache_errors": [type(lock_error).__name__], "pagination_outcome": "LOCKED",
            "pagination_metrics": {}, "recovery_budget": {},
            "malformed_comments": [], "malformed_comments_path": None,
            "records_received": 0, "valid_comments": 0, "new_comments": 0,
            "normalization_anomalies": {}, "comments": [],
        }
    except Exception as error:
        if monitor:
            monitor.record_error("video_worker", "scrape_video_comments", error)
            monitor.record_event(
                "video_finished", severity="ERROR", component="video_worker",
                video_id=str(video_id), worker_id=worker_id, status="failed",
                stop_reason="unhandled_video_worker_exception",
                error_code=getattr(error, "error_code", "UNEXPECTED_INTERNAL_ERROR"),
                error_type=type(error).__name__,
                duration_seconds=round(time.monotonic() - started, 3),
            )
        raise
    if monitor:
        monitor.record_video_result(result, time.monotonic() - started)
    return result


def _scrape_video_comments_locked(video_id: str, video_author: str, worker_id: int, run_id: str | None = None, run_control: RunControl | None = None) -> dict:
    """Scrape one video while holding its cross-process checkpoint lease."""
    from http_transport import HTTPTransport
    from config import (
        PROXY_HOST, PROXY_PORT, PROXY_USER, PROXY_PASS, PROXY_REQUIRED, PROXY_MODE,
        HTTP_CONNECT_TIMEOUT_SECONDS, HTTP_READ_TIMEOUT_SECONDS,
        HTTP_TOTAL_TIMEOUT_SECONDS, HTTP_MAX_RESPONSE_BYTES, HTTP_MAX_RETRIES,
    )

    if isinstance(MAX_COMMENTS, bool) or not isinstance(MAX_COMMENTS, int) or not 1 <= MAX_COMMENTS <= 100_000:
        raise ValueError("MAX_COMMENTS must be an integer between 1 and 100000")
    if PROXY_MODE == "disabled" and (PROXY_HOST or PROXY_PORT):
        raise ValueError("Proxy settings are present while PROXY_MODE is disabled")
    if PROXY_MODE not in {"disabled", "optional", "required"}:
        raise ValueError("PROXY_MODE must be disabled, optional or required")

    proxy_manager = ProxyManager(
        proxy_host=PROXY_HOST,
        proxy_port=PROXY_PORT,
        proxy_user=PROXY_USER or None,
        proxy_pass=PROXY_PASS or None,
    )
    proxy_valid, proxy_error = proxy_manager.validate()
    if not proxy_valid:
        raise ValueError("Invalid proxy configuration: " + str(proxy_error))

    transport = HTTPTransport(
        connect_timeout=HTTP_CONNECT_TIMEOUT_SECONDS,
        read_timeout=HTTP_READ_TIMEOUT_SECONDS,
        overall_timeout=HTTP_TOTAL_TIMEOUT_SECONDS,
        proxy_url=proxy_manager.proxy_url,
        proxy_required=PROXY_REQUIRED,
        max_response_bytes=HTTP_MAX_RESPONSE_BYTES,
        max_retries=HTTP_MAX_RETRIES,
    )
    run_id = run_id or str(uuid.uuid4())
    recovery_budget = RecoveryBudget(
        video_id=str(video_id), run_id=run_id, worker_id=worker_id,
        maximum_retries=48, maximum_elapsed_seconds=DEFAULT_ELAPSED_TIMEOUT_SECONDS,
        maximum_consecutive_failures=12, maximum_token_refresh_retries=12,
        maximum_proxy_retries=12,
    )
    token_manager = TokenManager(worker_id, transport=transport, retry_budget=recovery_budget)
    platform_adapter = TikTokAdapter(
        transport=transport, video_id=str(video_id), run_id=run_id,
        retry_budget=recovery_budget,
    )

    cached = load_cache(worker_id, video_id)
    if cached and cached.get("is_complete", False):
        # Completed checkpoint data is immutable and is included in the run
        # output even when it exhausts the new-collection budget.
        cached_comments = [
            normalize_comment_record(video_id, item)
            for item in cached.get("comments", [])
        ]
        if not run_control.reserve_comments(len(cached_comments)):
            safe_print(f"[W{worker_id}] Existing completed checkpoint exceeds the remaining run comment budget.")
        # A completed checkpoint is never truncated to satisfy a run-level cap.
        return {
            "video_id": video_id, "author": video_author, "worker_id": worker_id,
            "total_comments": len(cached.get("comments", [])),
            "total_batches": int(cached.get("batch_number", 0)),
            "tokens_used": 0, "proxy_rotations": 0, "duplicates_skipped": 0,
            "invalid_missing_ids": 0, "status": "completed",
            "stop_reason": "existing_completed_checkpoint",
            "cache_errors": [], "pagination_outcome": "COMPLETE",
            "pagination_metrics": {}, "recovery_budget": recovery_budget.snapshot(),
            "malformed_comments": [], "malformed_comments_path": None,
            "records_received": 0, "valid_comments": 0, "new_comments": 0,
            "normalization_anomalies": {},
            "comments": cached_comments,
        }
    if cached and not cached.get("is_complete", False):
        all_comments = [normalize_comment_record(video_id, item) for item in cached.get("comments", [])]
        if not run_control.reserve_comments(len(all_comments)):
            safe_print(f"[W{worker_id}] Run resource boundary reached before resuming this video.")
        seen_cids = set(cached.get("seen_cids", []))
        # Migrate legacy raw-ID dedup keys to the normalized key namespace.
        for existing_comment in all_comments:
            if existing_comment.get("dedupe_key"):
                seen_cids.add(existing_comment["dedupe_key"])
        cursor = cached.get("cursor", 0)
        base_batch_number = int(cached.get("batch_number", 0))
        safe_print(
            f"\n [W{worker_id}] RESUMING desde cache: "
            f"{len(all_comments)} comentarios, batch {base_batch_number}"
        )
    else:
        all_comments = []
        seen_cids = set()
        cursor = 0
        base_batch_number = 0

    if isinstance(cursor, bool) or not isinstance(cursor, int) or cursor < 0:
        cursor = 0
    if len(all_comments) > MAX_COMMENTS:
        all_comments = all_comments[:MAX_COMMENTS]

    duplicates_skipped = 0
    invalid_missing_ids = 0
    last_checkpoint_cursor = cursor
    last_checkpoint_batch = base_batch_number
    cache_errors = []
    quarantine_dir = CACHE_DIR / "quarantine"
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    malformed_path = quarantine_dir / f"worker_{worker_id}_{video_id}.jsonl"

    def quarantine_malformed(record, reason):
        """Append malformed source records to a per-video, restricted sidecar."""
        entry = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "video_id": video_id,
            "worker_id": worker_id,
            "reason": reason,
            "record": safe_source_payload(record),
        }
        encoded = (json.dumps(entry, ensure_ascii=False, default=str) + "\n").encode("utf-8")
        descriptor = os.open(
            str(malformed_path),
            os.O_WRONLY | os.O_CREAT | os.O_APPEND,
            0o600,
        )
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "ab") as quarantine_file:
                quarantine_file.write(encoded)
                quarantine_file.flush()
                os.fsync(quarantine_file.fileno())
        except Exception:
            # fdopen owns the descriptor after it succeeds; otherwise close it.
            try:
                os.close(descriptor)
            except OSError:
                pass
            raise

    normalization_anomalies = {"invalid_timestamp": 0, "invalid_like_count": 0, "invalid_reply_count": 0}
    def retain_comment(comment):
        nonlocal duplicates_skipped, invalid_missing_ids
        if run_control.stop_event.is_set():
            raise PaginationStop(run_control.stop_reason or "run_stop_requested")
        if not isinstance(comment, dict):
            invalid_missing_ids += 1
            return False
        normalized = platform_adapter.normalize_comment(video_id, comment)
        if normalized["comment_id"] is None:
            invalid_missing_ids += 1
        for anomaly, count in platform_adapter.normalization_anomalies(comment, normalized).items():
            if anomaly in normalization_anomalies:
                normalization_anomalies[anomaly] += count
        dedupe_key = normalized.get("dedupe_key")
        legacy_id_key = normalized.get("comment_id")
        if dedupe_key is not None and (
                dedupe_key in seen_cids
                or (legacy_id_key is not None and legacy_id_key in seen_cids)):
            duplicates_skipped += 1
            return False
        if not run_control.reserve_comments(1):
            raise PaginationStop(run_control.stop_reason or "run_comment_capacity_reached")
        if dedupe_key is not None:
            seen_cids.add(dedupe_key)
        all_comments.append(normalized)
        return True

    def persist_batch(batch_info):
        nonlocal last_checkpoint_cursor, last_checkpoint_batch
        candidate_cursor = batch_info["checkpoint_cursor"]
        candidate_batch = base_batch_number + batch_info["batch_number"]
        _, save_error = save_and_load_cache_safe(
            cache_dir=CACHE_DIR,
            worker_id=worker_id,
            video_id=video_id,
            comments=all_comments,
            seen_cids=seen_cids,
            batch_number=candidate_batch,
            cursor=candidate_cursor,
            is_final=False,
            completion_state="IN_PROGRESS",
            stop_reason=batch_info.get("stop_reason"),
            run_id=run_id,
        )
        if save_error:
            cache_errors.append(str(save_error))
            safe_print(
                f"      [W{worker_id}][CACHE] Save error; stopping this video "
                f"to preserve the last durable checkpoint: {save_error}"
            )
            raise RuntimeError("Checkpoint persistence failed; current page must not advance")
        # Advance the in-memory durable position only after the atomic write
        # has returned successfully.
        last_checkpoint_cursor = candidate_cursor
        last_checkpoint_batch = candidate_batch

    safe_print(f"\n [W{worker_id}] INICIANDO: {video_id} (@{video_author})")
    safe_print(f" [W{worker_id}] {'─' * 55}")

    remaining_comments = max(0, MAX_COMMENTS - len(all_comments))
    pagination_result = None
    if remaining_comments > 0 and base_batch_number < DEFAULT_MAX_BATCHES:
        paginator = CommentPaginator(
            video_id=video_id,
            run_id=run_id,
            worker_id=worker_id,
            max_batches=DEFAULT_MAX_BATCHES - base_batch_number,
            max_comments=remaining_comments,
            transport=transport,
            retry_budget=recovery_budget,
            stop_event=run_control.stop_event,
            adapter=platform_adapter,
        )
        pagination_result = paginator.run(
            initial_cursor=cursor,
            mstoken=token_manager.get_token,
            on_comment=retain_comment,
            on_batch=persist_batch,
            on_malformed_comment=quarantine_malformed,
        )
        naturally_completed = pagination_result.outcome == PaginatorOutcome.COMPLETED
        stop_reason = pagination_result.metadata.stop_reason or pagination_result.outcome.name.lower()
        total_batches = base_batch_number + pagination_result.metadata.batch_number
        # The checkpoint callback owns the durable cursor. Never copy the
        # paginator's in-memory cursor here: the last page may not have saved.
    else:
        naturally_completed = False
        stop_reason = (
            "max_comments_reached"
            if remaining_comments <= 0
            else "max_batches_reached"
        )
        total_batches = base_batch_number

    # A partial/failed pagination result must never be saved as complete.
    saved_path, save_error = save_and_load_cache_safe(
        cache_dir=CACHE_DIR,
        worker_id=worker_id,
        video_id=video_id,
        comments=all_comments,
        seen_cids=seen_cids,
        batch_number=last_checkpoint_batch,
        cursor=last_checkpoint_cursor,
        is_final=naturally_completed,
        completion_state="COMPLETE" if naturally_completed else "IN_PROGRESS",
        stop_reason=stop_reason,
        run_id=run_id,
    )
    if save_error:
        cache_errors.append(str(save_error))
        naturally_completed = False
        stop_reason = "final_checkpoint_save_failed"
        safe_print(f"      [W{worker_id}][CACHE] Final save error: {save_error}")

    result = {
        "video_id": video_id,
        "author": video_author,
        "worker_id": worker_id,
        "total_comments": len(all_comments),
        "total_batches": total_batches,
        "tokens_used": getattr(token_manager, "total_tokens_used", 0),
        "proxy_rotations": 0,
        "duplicates_skipped": duplicates_skipped,
        "invalid_missing_ids": invalid_missing_ids,
        "normalization_anomalies": dict(normalization_anomalies),
        "records_received": pagination_result.metadata.records_received if pagination_result else 0,
        "valid_comments": pagination_result.metadata.valid_comments_extracted if pagination_result else 0,
        "new_comments": pagination_result.metadata.valid_comments_extracted if pagination_result else 0,
        "status": "completed" if naturally_completed else "partial",
        "stop_reason": stop_reason,
        "cache_errors": cache_errors,
        "pagination_outcome": pagination_result.outcome.name if pagination_result else "NOT_STARTED",
        "pagination_metrics": pagination_result.metadata.to_dict() if pagination_result else {},
        "recovery_budget": recovery_budget.snapshot(),
        "malformed_comments": pagination_result.malformed_comments if pagination_result else [],
        "malformed_comments_path": str(malformed_path) if pagination_result and pagination_result.metadata.malformed_rejected and malformed_path.exists() else None,
        "comments": all_comments,
    }

    terminal_label = "COMPLETADO" if naturally_completed else "INCOMPLETO"
    safe_print(f"\n [W{worker_id}] {terminal_label}: {video_id} ({stop_reason})")
    safe_print(
        f" [W{worker_id}] → {result['total_comments']} comentarios | "
        f"{duplicates_skipped} duplicados omitidos | "
        f"{invalid_missing_ids} IDs inválidos/faltantes"
    )
    return result



def main() -> int:
    """Run the configured video list and create immutable run exports."""
    from config import MAX_WORKERS, TEST_VIDEOS
    if not TEST_VIDEOS:
        safe_print("No video jobs are configured; nothing was started.")
        return 2
    from config import MAX_RUN_COMMENTS
    try:
        run_control = RunControl(max_comments=MAX_RUN_COMMENTS)
    except ValueError as error:
        logger.error("Invalid run resource configuration (%s)", type(error).__name__)
        safe_print(f"Invalid run resource configuration: {type(error).__name__}")
        return 2
    previous_sigint_handler = signal.getsignal(signal.SIGINT)
    previous_sigterm_handler = signal.getsignal(signal.SIGTERM)

    def request_graceful_stop(signum, frame):
        reason = "termination_signal" if signum == signal.SIGTERM else "keyboard_interrupt"
        run_control.request_stop(reason)
        logger.warning("Graceful shutdown requested (%s)", reason)

    signal.signal(signal.SIGINT, request_graceful_stop)
    signal.signal(signal.SIGTERM, request_graceful_stop)
    try:
        run_result = scrape_videos_and_export(TEST_VIDEOS, max_workers=MAX_WORKERS, run_id=None, run_control=run_control)
    except KeyboardInterrupt:
        run_control.request_stop("keyboard_interrupt")
        logger.warning("Run interrupted during shutdown")
        return 130
    except Exception as error:
        logger.error("Scraper run could not produce its output artifacts (%s)", type(error).__name__)
        safe_print(f"Run failed: {type(error).__name__}")
        return 1
    finally:
        signal.signal(signal.SIGINT, previous_sigint_handler)
        signal.signal(signal.SIGTERM, previous_sigterm_handler)
    outputs = run_result["outputs"]
    safe_print(f"Run ID: {run_result['run_id']}")
    safe_print(f"Videos: {outputs['total_videos']} | Comments: {outputs['total_comments']}")
    safe_print(f"JSON: {outputs['json_path']}")
    safe_print(f"CSV: {outputs['csv_path']}")
    safe_print(f"Summary: {outputs['summary_path']}")
    monitoring = run_result.get("monitoring") or {}
    if monitoring.get("events_path"):
        safe_print(f"Events: {monitoring['events_path']}")
    if monitoring.get("alerts_path"):
        safe_print(f"Alerts: {monitoring['alerts_path']}")
    if run_result.get("interrupted"):
        stop_reason = run_result.get("stop_reason")
        safe_print(f"Run stopped: {stop_reason}")
        return 130 if stop_reason == "keyboard_interrupt" else 2
    return 0 if all(item.get("status") == "completed" for item in run_result["results"]) else 2


if __name__ == "__main__":
    raise SystemExit(main())
