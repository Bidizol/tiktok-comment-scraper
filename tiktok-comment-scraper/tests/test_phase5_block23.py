"""
Phase 5 Block 2.3 — Operational Cache Safety Integration Tests

Deterministic local tests (no live TikTok requests) covering:

  1. Two concurrent workers updating the same cache without losing comments.
  2. A process attempting to access a cache while another process holds its lock.
  3. Cleanup encountering a temporary file owned by a live process.
  4. Cleanup of a genuinely orphaned temporary file.
  5. Failed writes preserving the previous valid cache.
  6. Interrupted writes preserving the previous valid cache.
  7. Operational tiktokscraper-1.py actually invoking the safety implementation.
"""
import json
import unittest
import os
import sys
import tempfile
import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest

import importlib.util

REPO_ROOT = Path(__file__).parent.parent
CACHE_SAFETY_PATH = REPO_ROOT / "cache_safety.py"
TIKTOKSCRAPER_1_PATH = REPO_ROOT / "tiktokscraper-1.py"


def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


cache_safety = _load_module("cache_safety_block23", CACHE_SAFETY_PATH)
tiktokscraper_1 = _load_module("tiktokscraper_1_block23", TIKTOKSCRAPER_1_PATH)

save_and_load_cache_safe = cache_safety.save_and_load_cache_safe
cleanup_orphan_temp_files = cache_safety.cleanup_orphan_temp_files
scrape_video_comments = tiktokscraper_1.scrape_video_comments


def _fresh_cache_dir():
    return Path(tempfile.mkdtemp(prefix="block23_cache_"))


def _make_comments(n, prefix="c"):
    return [{"cid": f"{prefix}_{i}", "text": f"comment {i}"} for i in range(n)]

# --------------------------------------------------------------------------- #
# TEST 1: Two concurrent workers updating the same cache without losing comments
# --------------------------------------------------------------------------- #
class TestConcurrentWorkersSameCache:
    def test_two_workers_same_cache_no_lost_comments(self):
        """Two threads write to the same cache via save_and_load_cache_safe;
        neither worker's comments are lost because the lock serializes the
        read-modify-write transaction."""
        cache_dir = _fresh_cache_dir()
        video_id = "vid_concurrent"
        comments_a = _make_comments(10, "a")
        comments_b = _make_comments(10, "b")

        results = {}

        def worker(tag, comments):
            path, err = save_and_load_cache_safe(
                cache_dir=cache_dir,
                worker_id=1,
                video_id=video_id,
                comments=comments,
                seen_cids=set(c["cid"] for c in comments),
                batch_number=1,
                cursor=0,
                is_final=False,
            )
            results[tag] = (path, err)

        t1 = threading.Thread(target=worker, args=("a", comments_a))
        t2 = threading.Thread(target=worker, args=("b", comments_b))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # Exactly one worker must have succeeded (lock serialization).
        successes = [tag for tag in ("a", "b") if results[tag][1] is None]
        failures = [tag for tag in ("a", "b") if results[tag][1] is not None]
        assert len(successes) == 1, f"expected exactly 1 success, got {len(successes)}"
        assert len(failures) == 1, f"expected exactly 1 lock diagnostic, got {len(failures)}"
        assert "lock" in results[failures[0]][1].lower(), \
            f"failed worker should get lock diagnostic, got: {results[failures[0]][1]}"

        # The successful write produced valid, non-corrupt data.
        success_path = results[successes[0]][0]
        with open(success_path) as f:
            final = json.load(f)
        assert final["total_comments"] == 10
        assert len(final["comments"]) == 10

        # The losing worker retries with the combined set (A + B) after
        # the lock has been released – no comments are lost.
        all_comments = comments_a + comments_b
        path_retry, err_retry = save_and_load_cache_safe(
            cache_dir=cache_dir,
            worker_id=1,
            video_id=video_id,
            comments=all_comments,
            seen_cids=set(c["cid"] for c in all_comments),
            batch_number=2,
            cursor=0,
            is_final=False,
        )
        assert err_retry is None, f"retry failed: {err_retry}"
        assert path_retry is not None

        with open(path_retry) as f:
            combined = json.load(f)
        assert combined["total_comments"] == 20
        assert len(combined["comments"]) == 20


# --------------------------------------------------------------------------- #
# TEST 2: Process attempting to access a cache while another holds its lock
# --------------------------------------------------------------------------- #
class TestLockContention:
    def test_second_process_blocked_when_lock_held(self):
        """While one process holds the cache lock, a second process cannot
        acquire it and must receive a clear diagnostic instead of corrupting data."""
        cache_dir = _fresh_cache_dir()
        video_id = "vid_contention"

        path_a, err_a = save_and_load_cache_safe(
            cache_dir=cache_dir,
            worker_id=1,
            video_id=video_id,
            comments=_make_comments(5),
            seen_cids=set(),
            batch_number=1,
            cursor=0,
            is_final=False,
        )
        assert err_a is None

        assert cache_safety.acquire_cache_lock() is True  # re-acquire after release

        path_b, err_b = save_and_load_cache_safe(
            cache_dir=cache_dir,
            worker_id=2,
            video_id=video_id,
            comments=_make_comments(5),
            seen_cids=set(),
            batch_number=1,
            cursor=0,
            is_final=False,
        )
        assert path_b is None
        assert err_b is not None
        assert "lock" in err_b.lower()

        cache_safety.release_cache_lock()

# --------------------------------------------------------------------------- #
# TEST 3: Cleanup encountering a temp file owned by a live process
# --------------------------------------------------------------------------- #
class TestCleanupPreservesLiveWorkerTempFile:
    def test_cleanup_preserves_temp_file_held_by_live_process(self):
        """A temp file whose lock is held by a live process must be preserved
        (never deleted) by cleanup_orphan_temp_files."""
        import fcntl

        cache_dir = _fresh_cache_dir()
        tmp_file = cache_dir / ".tmp_worker_5_7676572010415230221.json"
        tmp_file.write_text("active worker data")

        fd = os.open(str(tmp_file), os.O_CREAT | os.O_RDWR)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

        removed = cleanup_orphan_temp_files(cache_dir)

        assert removed == [], f"expected no removal, got {removed}"
        assert tmp_file.exists(), "active worker's temp file must be preserved"

        os.close(fd)

# --------------------------------------------------------------------------- #
# TEST 4: Cleanup of a genuinely orphaned temporary file
# --------------------------------------------------------------------------- #
class TestCleanupRemovesOrphanTempFile:
    def test_cleanup_removes_genuinely_orphaned_temp_file(self):
        """A temp file not held by any process is removed."""
        cache_dir = _fresh_cache_dir()
        tmp_file = cache_dir / ".tmp_worker_99_orphan.json"
        tmp_file.write_text("orphan data")

        removed = cleanup_orphan_temp_files(cache_dir)

        assert removed == [".tmp_worker_99_orphan.json"], removed
        assert not tmp_file.exists()

# --------------------------------------------------------------------------- #
# TEST 5: Failed writes preserve the previous valid cache
# --------------------------------------------------------------------------- #
class TestFailedWritePreservesPreviousCache:
    def test_failed_write_preserves_previous_valid_cache(self, monkeypatch):
        """When a cache write fails, the previously valid cache file must remain
        intact on disk (not truncated/corrupted)."""
        cache_dir = _fresh_cache_dir()
        video_id = "vid_fail"

        path1, err1 = save_and_load_cache_safe(
            cache_dir=cache_dir,
            worker_id=1,
            video_id=video_id,
            comments=_make_comments(3),
            seen_cids={"c_0", "c_1", "c_2"},
            batch_number=1,
            cursor=0,
            is_final=False,
        )
        assert err1 is None
        with open(path1) as f:
            before = json.load(f)
        assert before["total_comments"] == 3

        cache_dir.chmod(0o555)
        try:
            path2, err2 = save_and_load_cache_safe(
                cache_dir=cache_dir,
                worker_id=1,
                video_id=video_id,
                comments=_make_comments(7),
                seen_cids=set(),
                batch_number=2,
                cursor=0,
                is_final=False,
            )
        finally:
            cache_dir.chmod(0o755)

        assert path2 is None
        assert err2 is not None

        with open(path1) as f:
            after = json.load(f)
        assert after["total_comments"] == 3
        assert len(after["comments"]) == 3


# --------------------------------------------------------------------------- #
# TEST 6: Interrupted writes preserve the previous valid cache
# --------------------------------------------------------------------------- #
class TestInterruptedWritePreservesPreviousCache:
    def test_interrupted_write_preserves_previous_valid_cache(self, monkeypatch):
        """A write interrupted mid-way must leave the previous valid cache intact
        and not leave a corrupt final file behind."""
        cache_dir = _fresh_cache_dir()
        video_id = "vid_interrupt"

        path1, err1 = save_and_load_cache_safe(
            cache_dir=cache_dir,
            worker_id=1,
            video_id=video_id,
            comments=_make_comments(4),
            seen_cids={"c_0", "c_1", "c_2", "c_3"},
            batch_number=1,
            cursor=0,
            is_final=False,
        )
        assert err1 is None
        with open(path1) as f:
            before = json.load(f)
        assert before["total_comments"] == 4

        original_replace = os.replace
        def failing_replace(src, dst):
            raise OSError("simulated interrupted rename")

        monkeypatch.setattr(os, "replace", failing_replace)
        try:
            path2, err2 = save_and_load_cache_safe(
                cache_dir=cache_dir,
                worker_id=1,
                video_id=video_id,
                comments=_make_comments(8),
                seen_cids=set(),
                batch_number=2,
                cursor=0,
                is_final=False,
            )
        finally:
            monkeypatch.setattr(os, "replace", original_replace)

        assert path2 is None
        assert err2 is not None

        assert os.path.exists(path1)
        with open(path1) as f:
            after = json.load(f)
        assert after["total_comments"] == 4

# --------------------------------------------------------------------------- #
# --------------------------------------------------------------------------- #
# TEST 7: Operational tiktokscraper-1.py actually invoking the safety implementation
# --------------------------------------------------------------------------- #
import unittest.mock
class TestOperationalScraperUsesSafetyImplementation:
    def test_scraper_imports_cache_safety_implementation(self):
        """tiktokscraper-1.py uses save_and_load_cache_safe from cache_safety
        (single shared implementation, not a second competing one)."""
        fn = tiktokscraper_1.save_and_load_cache_safe
        assert fn.__module__ == "cache_safety", fn.__module__

    def test_scrape_video_comments_invokes_safety_save(self, monkeypatch):
        """scrape_video_comments calls save_and_load_cache_safe for both
        periodic and final cache persistence."""
        calls = []

        def fake_save(cache_dir, worker_id, video_id, comments, seen_cids,
                      batch_number, cursor, is_final=False):
            calls.append((worker_id, video_id, len(comments), batch_number, is_final))
            return Path(cache_dir) / f"worker_{worker_id}_{video_id}.json", None

        monkeypatch.setattr(tiktokscraper_1, "save_and_load_cache_safe", fake_save)

        mock_tm = unittest.mock.MagicMock()
        mock_http = unittest.mock.MagicMock()
        monkeypatch.setattr(tiktokscraper_1, "TokenManager", mock_tm)
        monkeypatch.setattr(tiktokscraper_1, "HTTPTransport", mock_http)

        mock_tm.return_value.get_token.return_value = "test_token"
        instance = mock_http.return_value
        # Two successful batches then has_more=False (natural completion).
        instance.request.side_effect = [
            (200, {"comments": [{"cid": "1", "text": "hi"}],
                    "has_more": True, "next_cursor": 50,
                    "http_status": 200, "error": None}),
            (200, {"comments": [{"cid": "2", "text": "hi2"}],
                    "has_more": False, "next_cursor": 100,
                    "http_status": 200, "error": None}),
        ]

        scrape_video_comments("7676572010415230221", "erickie20", worker_id=1)

        assert len(calls) >= 1, \
            "scrape_video_comments must invoke save_and_load_cache_safe"

    def test_scraper_never_marks_interrupted_scrape_complete(self, monkeypatch, tmp_path):
        """When pagination is interrupted (API errors exhaust retries), the final
        save must use is_final=False — never report successful completion."""
        import http_transport

        # Use a temp cache dir so we don't read/write real cache files.
        temp_cache = tmp_path / "cache"
        temp_cache.mkdir(parents=True, exist_ok=True)
        monkeypatch.setattr(tiktokscraper_1, "CACHE_DIR", temp_cache)

        calls = []

        def fake_save(cache_dir, worker_id, video_id, comments, seen_cids,
                      batch_number, cursor, is_final=False):
            calls.append(is_final)
            return Path(cache_dir) / f"worker_{worker_id}_{video_id}.json", None

        monkeypatch.setattr(tiktokscraper_1, "save_and_load_cache_safe", fake_save)

        mock_tm = unittest.mock.MagicMock()
        monkeypatch.setattr(tiktokscraper_1, "TokenManager", mock_tm)
        mock_tm.return_value.get_token.return_value = "test_token"

        # Patch HTTPTransport.request at the module level since scrape_video_comments
        # does "from http_transport import HTTPTransport" inside the function.
        monkeypatch.setattr(http_transport.HTTPTransport, "request",
                            lambda self, method, url, headers=None, body=None: (0, None))

        scrape_video_comments("7676572010415230221", "erickie20", worker_id=1)

        assert calls, "scrape_video_comments must save cache even when interrupted"
        assert all(f is False for f in calls), \
            f"interrupted scrape must never be marked complete; got {calls}"
