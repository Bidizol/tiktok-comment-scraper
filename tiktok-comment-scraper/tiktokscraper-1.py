import sys
import os
import logging
import threading
import uuid
import json
import time
import fcntl
import shutil
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

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
from diagnostics import DiagnosticsRecorder

from errors.model import ScraperError, ErrorSeverity, ErrorOutcome

from comment_paginator import (
    CommentPaginator,
    PaginatorResult,
    PaginatorOutcome,
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
from cache_safety import save_and_load_cache_safe, cleanup_orphan_temp_files, load_cache_safe  # noqa: E402

# Configuration
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
MAX_RETRIES = 3
MAX_COMMENTS = 10000
COMMENTS_PER_BATCH = 50

# Cache version for backward compatibility
CACHE_VERSION = "0"

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

# Directorio para cache
CACHE_DIR = Path(__file__).parent / "cache"
CACHE_DIR.mkdir(exist_ok=True)

# Lock para print thread-safe
print_lock = threading.Lock()


def safe_print(message):
    with print_lock:
        print(message)

# Videos de prueba
TEST_VIDEOS = [
    ("7676572010415230221", "erickie20"),
]

def validate_cache_data(cached: dict, video_id: str) -> tuple:
    """Validate and sanitize cached data for backward compatibility.

    Returns (validated_comments, validated_seen_cids, validated_cursor, validated_batch_number, is_valid)
    If cache is invalid, returns ([], set(), 0, 0, False)
    """
    if not isinstance(cached, dict):
        return [], set(), 0, 0, False

    # Validate video_id
    if cached.get("video_id") != video_id:
        return [], set(), 0, 0, False

    # Validate version compatibility (backward compatible: "0" or "1")
    cached_version = cached.get("version", "0")
    if cached_version not in ("0", "1"):
        # Legacy cache without version field - still accept but mark
        cached_version = "0"

    # Validate comments is a list
    comments = cached.get("comments", [])
    if not isinstance(comments, list):
        comments = []

    # Validate seen_cids is a list
    seen_cids_raw = cached.get("seen_cids", [])
    if not isinstance(seen_cids_raw, list):
        seen_cids_raw = []

    # Convert seen_cids to set, validating each CID
    seen_cids = set()
    for cid in seen_cids_raw:
        if isinstance(cid, str) and cid.strip():
            seen_cids.add(cid)

    # Validate cursor
    cursor = cached.get("cursor", 0)
    try:
        cursor = int(cursor)
    except (ValueError, TypeError):
        cursor = 0

    # Validate batch_number
    batch_number = cached.get("batch_number", 0)
    try:
        batch_number = int(batch_number)
    except (ValueError, TypeError):
        batch_number = 0

    # Validate total_comments consistency
    total_comments = cached.get("total_comments", len(comments))
    try:
        total_comments = int(total_comments)
    except (ValueError, TypeError):
        total_comments = len(comments)

    # Trim comments to match total_comments if there's discrepancy
    if total_comments < len(comments):
        comments = comments[:total_comments]

    # Validate completion state: is_complete should only be True if we have
    # comments and the data is consistent
    is_complete = cached.get("is_complete", False)
    if is_complete and not comments:
        # Cache marked complete but has no comments - this is inconsistent
        # Don't mark as complete, but try to recover what we can
        is_complete = False

    # Validate that if is_complete is True, we have reasonable data
    if is_complete and total_comments > 0 and len(comments) == 0:
        is_complete = False

    # Validate comment IDs within comments
    valid_comments = []
    for comment in comments:
        if not isinstance(comment, dict):
            continue
        cid = comment.get("cid")
        if cid is not None and isinstance(cid, str) and cid.strip():
            # Ensure comment has essential fields
            if "text" in comment or "unique_id" in comment:
                valid_comments.append(comment)
        else:
            # Comment without valid CID - still include but track it
            if "text" in comment:
                valid_comments.append(comment)

    # Update total_comments to match valid comments
    total_comments = len(valid_comments)

    return valid_comments, seen_cids, cursor, batch_number, is_complete


def validate_cache_identity(cached: dict, video_id: str, worker_id: int) -> bool:
    """Validate that a loaded cache matches the requested identity and data structures.

    Phase 5 Block 3.1: Every cache loaded for a scraping operation must have:
      - The exact requested video_id
      - The exact requested worker_id
      - Valid expected data structures for comments, seen_cids, cursor and batch_number.

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

    # Exact worker_id match required
    if cached.get("worker_id") != worker_id:
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
    cache_file = CACHE_DIR / f"worker_{worker_id}_{video_id}.json"
    if not cache_file.exists():
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
    comments, seen_cids, cursor, batch_number, is_complete = validate_cache_data(cached, video_id)

    if not comments and not cached.get("video_id"):
        # No valid data to recover
        return None

    # Report if version was incompatible or data was sanitized
    cached_version = cached.get("version", "0")
    if cached_version not in ("0", "1"):
        safe_print(f"[W{worker_id}] Versión de cache no compatible: {cached_version}, datos sanitizados")

    # Check if data was significantly modified during sanitization
    original_comment_count = len(cached.get("comments", []))
    if original_comment_count != len(comments):
        safe_print(f"[W{worker_id}] Cache sanitizado: {original_comment_count} -> {len(comments)} comentarios")

    safe_print(f"[W{worker_id}] Cache validado: version={cached_version}, is_complete={is_complete}, {len(comments)} comentarios, cursor={cursor}")

    # Return structured data with validated values
    return {
        "version": cached_version,
        "video_id": video_id,
        "worker_id": worker_id,
        "timestamp": cached.get("timestamp", datetime.now().isoformat()),
        "batch_number": batch_number,
        "cursor": cursor,
        "total_comments": len(comments),
        "is_complete": is_complete,
        "seen_cids": list(seen_cids),
        "comments": comments,
    }


def save_cache(worker_id: int, video_id: str, comments: list, seen_cids: set, batch_number: int, cursor: int, is_final: bool = False):
    """Guardar cache para resume work.

    Validates data before saving and ensures that is_complete is only True
    when the scrape is truly complete.
    """
    # Validate inputs before saving
    if not isinstance(comments, list):
        safe_print(f"[W{worker_id}] Error: comments no es una lista, guardando vacío")
        comments = []

    # Validate and sanitize seen_cids
    valid_seen_cids = set()
    for cid in seen_cids:
        if isinstance(cid, str) and cid.strip():
            valid_seen_cids.add(cid)

    # Validate cursor
    try:
        cursor = int(cursor)
    except (ValueError, TypeError):
        cursor = 0

    # Validate batch_number
    try:
        batch_number = int(batch_number)
    except (ValueError, TypeError):
        batch_number = 0

    # is_final should only be True if the scrape actually completed naturally
    # or reached a definitive end point. Never mark incomplete scrapes as completed.
    # The caller must ensure this invariant.

    cache_file = CACHE_DIR / f"worker_{worker_id}_{video_id}.json"
    cache_data = {
        "version": CACHE_VERSION,
        "video_id": video_id,
        "worker_id": worker_id,
        "timestamp": datetime.now().isoformat(),
        "batch_number": batch_number,
        "cursor": cursor,
        "total_comments": len(comments),
        "is_complete": is_final,
        "seen_cids": list(valid_seen_cids),
        "comments": comments,
    }

    # Atomic write: write to temp file then rename
    try:
        temp_file = CACHE_DIR / f"worker_{worker_id}_{video_id}.json.tmp"
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, ensure_ascii=False)
        temp_file.rename(cache_file)
        if not is_final:
            safe_print(f"      [W{worker_id}][CACHE] Guardado: {len(comments)} comentarios (batch {batch_number})")
    except Exception as e:
        safe_print(f"[W{worker_id}] Error guardando cache: {e}")



def scrape_video_comments(video_id: str, video_author: str, worker_id: int) -> dict:
    """Scrape one video through the shared bounded pagination state machine."""
    from http_transport import HTTPTransport
    from config import (
        PROXY_HOST, PROXY_PORT, PROXY_USER, PROXY_PASS, PROXY_REQUIRED, PROXY_MODE,
        HTTP_CONNECT_TIMEOUT_SECONDS, HTTP_READ_TIMEOUT_SECONDS,
        HTTP_TOTAL_TIMEOUT_SECONDS, HTTP_MAX_RESPONSE_BYTES, HTTP_MAX_RETRIES,
    )

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
    run_id = str(uuid.uuid4())
    recovery_budget = RecoveryBudget(
        video_id=str(video_id), run_id=run_id, worker_id=worker_id,
        maximum_retries=12, maximum_elapsed_seconds=DEFAULT_ELAPSED_TIMEOUT_SECONDS,
        maximum_consecutive_failures=6, maximum_token_refresh_retries=6,
        maximum_proxy_retries=2,
    )
    token_manager = TokenManager(worker_id, transport=transport, retry_budget=recovery_budget)

    cached = load_cache(worker_id, video_id)
    if cached and not cached.get("is_complete", False):
        all_comments = list(cached.get("comments", []))
        seen_cids = set(cached.get("seen_cids", []))
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
            "record": record,
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

    def normalize_cid(cid):
        if cid is None or isinstance(cid, bool):
            return None
        if isinstance(cid, int):
            return str(cid)
        if isinstance(cid, str) and cid.strip():
            return cid.strip()
        return None

    def retain_comment(comment):
        nonlocal duplicates_skipped, invalid_missing_ids
        if not isinstance(comment, dict):
            invalid_missing_ids += 1
            return False
        cid = normalize_cid(comment.get("cid"))
        if cid is None:
            invalid_missing_ids += 1
            all_comments.append(comment)
            return True
        if cid in seen_cids:
            duplicates_skipped += 1
            return False
        seen_cids.add(cid)
        all_comments.append(comment)
        return True

    def persist_batch(batch_info):
        nonlocal last_checkpoint_cursor, last_checkpoint_batch
        last_checkpoint_cursor = batch_info["checkpoint_cursor"]
        last_checkpoint_batch = base_batch_number + batch_info["batch_number"]
        _, save_error = save_and_load_cache_safe(
            cache_dir=CACHE_DIR,
            worker_id=worker_id,
            video_id=video_id,
            comments=all_comments,
            seen_cids=seen_cids,
            batch_number=last_checkpoint_batch,
            cursor=last_checkpoint_cursor,
            is_final=False,
        )
        if save_error:
            cache_errors.append(str(save_error))
            safe_print(
                f"      [W{worker_id}][CACHE] Save error; stopping this video "
                f"to preserve the last durable checkpoint: {save_error}"
            )
            raise RuntimeError("Checkpoint persistence failed; current page must not advance")

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
        last_checkpoint_cursor = (
            last_checkpoint_cursor
            if pagination_result.metadata.stop_reason == "max_comments_reached"
            else pagination_result.metadata.returned_cursor
        )
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
        batch_number=total_batches,
        cursor=last_checkpoint_cursor,
        is_final=naturally_completed,
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
