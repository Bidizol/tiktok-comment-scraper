import sys
import os
import logging
import threading
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
from cache_safety import save_and_load_cache_safe, cleanup_orphan_temp_files  # noqa: E402

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

    try:
        with open(cache_file, "r", encoding="utf-8") as f:
            cached = json.load(f)
    except (json.JSONDecodeError, Exception) as e:
        safe_print(f"[W{worker_id}] Cache corrupto o ilegible: {cache_file}: {e}")
        # Preserve the corrupt cache file - don't overwrite it
        return None

    # Phase 5 Block 3.1: Cache identity validation — reject invalid/mismatched
    # caches before they are loaded. Recoverable data is preserved on disk
    # (and quarantined on the next write via save_and_load_cache_safe).
    if not validate_cache_identity(cached, video_id, worker_id):
        safe_print(f"[W{worker_id}] Cache inválido o identidad no coincidente: {cache_file} (video_id={video_id}, worker_id={worker_id})")
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
    """Scrape comments for a video with cache resume support.

    Uses terminal pagination evidence (has_more=False) for completion,
    not merely total_comments == len(comments).
    An incomplete or failed operation must never become completed because
    the counts happen to match.

    Returns dict with video_id, author, worker_id, total_comments,
    total_batches, tokens_used, proxy_rotations, duplicates_skipped,
    and comments list.
    """
    from http_transport import HTTPTransport

    transport = HTTPTransport()
    token_manager = TokenManager(worker_id, transport=transport)

    # Attempt to load existing cache for resume
    cached = load_cache(worker_id, video_id)
    if cached and not cached.get("is_complete", False):
        all_comments = cached.get("comments", [])
        seen_cids = set(cached.get("seen_cids", []))
        cursor = cached.get("cursor", 0)
        batch_number = cached.get("batch_number", 0)
        safe_print(f"\n [W{worker_id}] RESUMING desde cache: {len(all_comments)} comentarios, batch {batch_number}")
    else:
        all_comments = []
        seen_cids = set()
        cursor = 0
        batch_number = 0

    consecutive_failures = 0
    proxy_rotations = 0
    duplicates_skipped = 0
    invalid_missing_ids = 0
    naturally_completed = False

    def _normalize_cid(cid):
        """Normalize a comment ID for consistent deduplication.

        Accepts string or integer IDs. Returns a canonical string form for
        valid IDs, or None for missing/invalid IDs (which must not be treated
        as duplicates and must not be invented).
        """
        if cid is None:
            return None
        if isinstance(cid, bool):
            # bool is a subclass of int in Python; reject it explicitly
            return None
        if isinstance(cid, int):
            return str(cid)
        if isinstance(cid, str):
            cid = cid.strip()
            if cid == "":
                return None
            return cid
        # Any other type (float, list, dict, etc.) is invalid
        return None

    safe_print(f"\n [W{worker_id}] INICIANDO: {video_id} (@{video_author})")
    safe_print(f" [W{worker_id}] {'\u2500'*55}")

    while True:
        batch_number += 1

        mstoken = token_manager.get_token()

        # Never expose token values or prefixes in terminal output.
        safe_print(f"      [W{worker_id}][BATCH {batch_number:3d}] cursor={cursor:<6} total={len(all_comments):<5}")

        url = f"https://www.tiktok.com/api/comment/list/?aid=1988&aweme_id={video_id}&count={COMMENTS_PER_BATCH}&cursor={cursor}&msToken={mstoken}"
        http_status, api_response = transport.request("GET", url, headers={"user-agent": USER_AGENT, "referer": "https://www.tiktok.com/"})

        if http_status == 200 and api_response is not None:
            # Success - process comments
            page_comments = api_response.get("comments") or []
            if not isinstance(page_comments, list):
                page_comments = []

            try:
                has_more = normalise_has_more(api_response.get("has_more"))
            except ValueError as error:
                safe_print(f"      [W{worker_id}][PAGINATION] Invalid has_more metadata: {error}")
                break

            next_cursor = api_response.get("cursor")
            if has_more:
                if isinstance(next_cursor, bool) or not isinstance(next_cursor, int) or next_cursor <= cursor:
                    safe_print(f"      [W{worker_id}][PAGINATION] Missing, invalid, or non-advancing cursor; stopping safely")
                    break
            else:
                # No next cursor is needed after the API explicitly marks the
                # final page. Keep the last confirmed cursor in the checkpoint.
                next_cursor = cursor

            # Stage candidates first. Only IDs belonging to comments actually
            # retained below are added to seen_cids.
            candidates = []
            for comment in page_comments:
                if not isinstance(comment, dict):
                    continue
                raw_cid = comment.get("cid")
                cid = _normalize_cid(raw_cid)
                if cid is None:
                    invalid_missing_ids += 1
                    candidates.append((comment, None))
                elif cid not in seen_cids:
                    candidates.append((comment, cid))
                else:
                    duplicates_skipped += 1

            remaining = max(0, MAX_COMMENTS - len(all_comments))
            retained = candidates[:remaining]
            page_was_truncated = len(retained) < len(candidates)
            for comment, cid in retained:
                all_comments.append(comment)
                if cid is not None:
                    seen_cids.add(cid)
            consecutive_failures = 0

            # If a page was only partly retained, do not advance its cursor:
            # resumption must refetch that page rather than skip its remainder.
            checkpoint_cursor = cursor if page_was_truncated else next_cursor

            if batch_number % SAVE_EVERY_N_BATCHES == 0:
                saved_path, save_error = save_and_load_cache_safe(
                    cache_dir=CACHE_DIR,
                    worker_id=worker_id,
                    video_id=video_id,
                    comments=all_comments,
                    seen_cids=seen_cids,
                    batch_number=batch_number,
                    cursor=checkpoint_cursor,
                    is_final=False,
                )
                if save_error:
                    safe_print(f"      [W{worker_id}][CACHE] Save error (data preserved in memory): {save_error}")
                elif saved_path:
                    safe_print(f"      [W{worker_id}][CACHE] Guardado: {len(all_comments)} comentarios (batch {batch_number})")

            # Verificar límite — partial/limited result, never natural completion
            if len(all_comments) >= MAX_COMMENTS:
                safe_print(f"      [W{worker_id}][LIMITE] Alcanzado {MAX_COMMENTS} comentarios")
                break

            # Check for terminal pagination evidence - completion requires
            # valid terminal pagination (has_more=False received from the API),
            # not just count matching or missing/malformed has_more.
            # A missing (None) has_more means we cannot confirm completion;
            # treat it as continued pagination unless we hit another stop condition.
            if page_was_truncated:
                safe_print(f"      [W{worker_id}][LIMITE] Página parcialmente retenida; cursor no avanzado")
                break

            if has_more is False:
                naturally_completed = True
                safe_print(f"      [W{worker_id}][FIN] No hay más comentarios")
                break

            cursor = checkpoint_cursor

        else:
            # API error or no response - treat as failure
            consecutive_failures += 1
            safe_print(f"      [W{worker_id}][BATCH {batch_number:3d}] \u2717 retry {consecutive_failures}/{MAX_RETRIES}")

            if consecutive_failures >= MAX_RETRIES:
                safe_print(f"      [W{worker_id}] Max retries - budget exhausted")
                break

        time.sleep(0.3)

    # Trim a exactly MAX_COMMENTS
    if len(all_comments) > MAX_COMMENTS:
        all_comments = all_comments[:MAX_COMMENTS]

    # Guardar cache final - is_final only if scrape completed naturally via has_more=False
    # Never mark incomplete scrapes as completed (invariant maintenance)
    is_final = naturally_completed

    saved_path, save_error = save_and_load_cache_safe(
        cache_dir=CACHE_DIR,
        worker_id=worker_id,
        video_id=video_id,
        comments=all_comments,
        seen_cids=seen_cids,
        batch_number=batch_number,
        cursor=cursor,
        is_final=is_final,
    )
    if save_error:
        safe_print(f"      [W{worker_id}][CACHE] Final save error: {save_error}")

    # Track tokens used (TokenManager in this version doesn't track total_tokens_used)
    tokens_used = getattr(token_manager, 'total_tokens_used', 0)

    result = {
        "video_id": video_id,
        "author": video_author,
        "worker_id": worker_id,
        "total_comments": len(all_comments),
        "total_batches": batch_number,
        "tokens_used": tokens_used,
        "proxy_rotations": proxy_rotations,
        "duplicates_skipped": duplicates_skipped,
        "invalid_missing_ids": invalid_missing_ids,
        "comments": all_comments,
    }

    safe_print(f"\n [W{worker_id}] COMPLETADO: {video_id}")
    safe_print(f" [W{worker_id}] \u2192 {result['total_comments']} comentarios | {duplicates_skipped} duplicados omitidos | {invalid_missing_ids} IDs inválidos/faltantes | {result['tokens_used']} tokens")

    return result
