"""
Phase 5 Block 3.1 — Data Integrity and Recovery Corrections

Deterministic local tests (no live TikTok requests).
"""
import json
import tempfile
from pathlib import Path
import importlib.util

REPO_ROOT = Path(__file__).parent.parent
TIKTOKSCRAPER_1_PATH = REPO_ROOT / "tiktokscraper-1.py"
HTTP_TRANSPORT_PATH = REPO_ROOT / "http_transport.py"


def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


tiktokscraper_1 = _load_module("tiktokscraper_1_block31", TIKTOKSCRAPER_1_PATH)

scrape_video_comments = tiktokscraper_1.scrape_video_comments
validate_cache_identity = tiktokscraper_1.validate_cache_identity
load_cache = tiktokscraper_1.load_cache
SAVE_EVERY_N_BATCHES = tiktokscraper_1.SAVE_EVERY_N_BATCHES
MAX_COMMENTS = tiktokscraper_1.MAX_COMMENTS

http_transport = _load_module("http_transport_block31", HTTP_TRANSPORT_PATH)


def _make_comment(cid, text=None):
    return {"cid": cid, "text": text if text is not None else f"comment {cid}"}


def _fresh_cache_dir():
    return Path(tempfile.mkdtemp(prefix="block31_cache_"))


def _write_cache(cache_dir, worker_id, video_id, comments, seen_cids, cursor, batch_number, is_complete=False):
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"worker_{worker_id}_{video_id}.json"
    cache_data = {
        "version": "0",
        "video_id": video_id,
        "worker_id": worker_id,
        "timestamp": "2026-01-01T00:00:00",
        "batch_number": batch_number,
        "cursor": cursor,
        "total_comments": len(comments),
        "is_complete": is_complete,
        "seen_cids": list(seen_cids),
        "comments": comments,
    }
    cache_file.write_text(json.dumps(cache_data), encoding="utf-8")
    return cache_file


def _install_mock_transport(monkeypatch, responses):
    call_order = {"n": 0}

    def fake_request(self, method, url, headers=None, body=None):
        idx = call_order["n"]
        call_order["n"] += 1
        if idx < len(responses):
            return responses[idx]
        return (200, {"comments": [], "has_more": False, "next_cursor": 0, "http_status": 200, "error": None})

    monkeypatch.setattr(http_transport.HTTPTransport, "request", fake_request)
    return call_order


# ============================================================================
# 1. CACHE IDENTITY VALIDATION
# ============================================================================

class TestCacheIdentityValidation:
    def test_correct_video_and_worker_ids_accepted(self):
        cached = {
            "video_id": "vid_correct",
            "worker_id": 1,
            "comments": [{"cid": "1", "text": "hi"}],
            "seen_cids": ["1"],
            "cursor": 50,
            "batch_number": 1,
        }
        assert validate_cache_identity(cached, "vid_correct", 1) is True

    def test_wrong_video_id_rejected(self):
        cached = {
            "video_id": "other_video",
            "worker_id": 1,
            "comments": [],
            "seen_cids": [],
            "cursor": 0,
            "batch_number": 0,
        }
        assert validate_cache_identity(cached, "vid_correct", 1) is False

    def test_wrong_worker_id_rejected(self):
        cached = {
            "video_id": "vid_correct",
            "worker_id": 2,
            "comments": [],
            "seen_cids": [],
            "cursor": 0,
            "batch_number": 0,
        }
        assert validate_cache_identity(cached, "vid_correct", 1) is False

    def test_missing_video_id_rejected(self):
        cached = {
            "worker_id": 1,
            "comments": [],
            "seen_cids": [],
            "cursor": 0,
            "batch_number": 0,
        }
        assert validate_cache_identity(cached, "vid_correct", 1) is False

    def test_missing_worker_id_rejected(self):
        cached = {
            "video_id": "vid_correct",
            "comments": [],
            "seen_cids": [],
            "cursor": 0,
            "batch_number": 0,
        }
        assert validate_cache_identity(cached, "vid_correct", 1) is False

    def test_malformed_comments_not_a_list_rejected(self):
        cached = {
            "video_id": "vid_correct",
            "worker_id": 1,
            "comments": "not a list",
            "seen_cids": [],
            "cursor": 0,
            "batch_number": 0,
        }
        assert validate_cache_identity(cached, "vid_correct", 1) is False

    def test_malformed_seen_cids_not_a_list_rejected(self):
        cached = {
            "video_id": "vid_correct",
            "worker_id": 1,
            "comments": [],
            "seen_cids": "not a list",
            "cursor": 0,
            "batch_number": 0,
        }
        assert validate_cache_identity(cached, "vid_correct", 1) is False

    def test_missing_cursor_rejected(self):
        cached = {
            "video_id": "vid_correct",
            "worker_id": 1,
            "comments": [],
            "seen_cids": [],
            "batch_number": 0,
        }
        assert validate_cache_identity(cached, "vid_correct", 1) is False

    def test_missing_batch_number_rejected(self):
        cached = {
            "video_id": "vid_correct",
            "worker_id": 1,
            "comments": [],
            "seen_cids": [],
            "cursor": 0,
        }
        assert validate_cache_identity(cached, "vid_correct", 1) is False

    def test_non_dict_cache_rejected(self):
        assert validate_cache_identity("not a dict", "vid_correct", 1) is False
        assert validate_cache_identity(None, "vid_correct", 1) is False
        assert validate_cache_identity([], "vid_correct", 1) is False

    def test_resume_path_invokes_validation_rejects_wrong_video(self, monkeypatch, tmp_path):
        """The actual resume path (load_cache) must reject a cache with the wrong video_id."""
        cache_dir = _fresh_cache_dir()
        monkeypatch.setattr(tiktokscraper_1, "CACHE_DIR", cache_dir)

        _write_cache(cache_dir, 1, "other_video", [_make_comment("1")], ["1"], 50, 1)

        cached = load_cache(1, "vid_requested")
        assert cached is None, "Wrong video_id cache must not be loaded"

    def test_resume_path_invokes_validation_rejects_wrong_worker(self, monkeypatch, tmp_path):
        cache_dir = _fresh_cache_dir()
        monkeypatch.setattr(tiktokscraper_1, "CACHE_DIR", cache_dir)

        _write_cache(cache_dir, 99, "vid_requested", [_make_comment("1")], ["1"], 50, 1)

        cached = load_cache(1, "vid_requested")
        assert cached is None, "Wrong worker_id cache must not be loaded"

    def test_resume_path_invokes_validation_rejects_malformed_structure(self, monkeypatch, tmp_path):
        cache_dir = _fresh_cache_dir()
        monkeypatch.setattr(tiktokscraper_1, "CACHE_DIR", cache_dir)

        cache_file = cache_dir / "worker_1_vid_requested.json"
        cache_file.write_text("not valid json {{{", encoding="utf-8")

        cached = load_cache(1, "vid_requested")
        assert cached is None, "Malformed cache structure must not be loaded"


# ============================================================================
# 2. MISSING AND MALFORMED COMMENT IDS
# ============================================================================

class TestMissingAndMalformedCommentIds:
    def test_missing_cid_not_classified_as_duplicate(self, monkeypatch, tmp_path):
        cache_dir = _fresh_cache_dir()
        monkeypatch.setattr(tiktokscraper_1, "CACHE_DIR", cache_dir)
        responses = [
            (200, {
                "comments": [
                    _make_comment("1"),
                    {"text": "no cid here"},
                    _make_comment("2"),
                    {"text": "also no cid"},
                ],
                "has_more": False,
                "next_cursor": 50,
                "http_status": 200,
                "error": None,
            }),
        ]
        _install_mock_transport(monkeypatch, responses)

        result = scrape_video_comments("vid", "author", worker_id=1)

        cids = [c.get("cid") for c in result["comments"] if c.get("cid") is not None]
        assert "1" in cids
        assert "2" in cids
        no_cid = [c for c in result["comments"] if c.get("cid") is None]
        assert len(no_cid) == 2, f"Expected 2 comments without cid, got {len(no_cid)}"
        assert result["duplicates_skipped"] == 0
        assert result["invalid_missing_ids"] == 2

    def test_malformed_cid_not_classified_as_duplicate(self, monkeypatch, tmp_path):
        cache_dir = _fresh_cache_dir()
        monkeypatch.setattr(tiktokscraper_1, "CACHE_DIR", cache_dir)
        responses = [
            (200, {
                "comments": [
                    _make_comment("1"),
                    {"cid": "", "text": "empty cid"},
                    {"cid": None, "text": "null cid"},
                    {"cid": True, "text": "bool cid"},
                    {"cid": 3.5, "text": "float cid"},
                ],
                "has_more": False,
                "next_cursor": 50,
                "http_status": 200,
                "error": None,
            }),
        ]
        _install_mock_transport(monkeypatch, responses)

        result = scrape_video_comments("vid", "author", worker_id=1)

        assert result["duplicates_skipped"] == 0
        assert result["invalid_missing_ids"] == 4
        valid = [c for c in result["comments"] if c.get("cid") == "1"]
        assert len(valid) == 1

    def test_valid_string_and_integer_ids_normalized_before_dedup(self, monkeypatch, tmp_path):
        cache_dir = _fresh_cache_dir()
        monkeypatch.setattr(tiktokscraper_1, "CACHE_DIR", cache_dir)
        responses = [
            (200, {
                "comments": [
                    _make_comment("7"),
                    _make_comment(7),
                ],
                "has_more": False,
                "next_cursor": 50,
                "http_status": 200,
                "error": None,
            }),
        ]
        _install_mock_transport(monkeypatch, responses)

        result = scrape_video_comments("vid", "author", worker_id=1)

        assert result["duplicates_skipped"] == 1, f"Expected 1 duplicate, got {result['duplicates_skipped']}"
        assert result["total_comments"] == 1

    def test_no_false_dedup_claims_for_comments_without_ids(self, monkeypatch, tmp_path):
        cache_dir = _fresh_cache_dir()
        monkeypatch.setattr(tiktokscraper_1, "CACHE_DIR", cache_dir)
        responses = [
            (200, {
                "comments": [
                    {"text": "no id a"},
                    {"text": "no id b"},
                ],
                "has_more": False,
                "next_cursor": 50,
                "http_status": 200,
                "error": None,
            }),
        ]
        _install_mock_transport(monkeypatch, responses)

        result = scrape_video_comments("vid", "author", worker_id=1)

        assert result["total_comments"] == 2
        assert result["duplicates_skipped"] == 0
        assert result["invalid_missing_ids"] == 2

    def test_accurate_counts_new_duplicates_invalid(self, monkeypatch, tmp_path):
        cache_dir = _fresh_cache_dir()
        monkeypatch.setattr(tiktokscraper_1, "CACHE_DIR", cache_dir)
        responses = [
            (200, {
                "comments": [
                    _make_comment("1"),
                    _make_comment("2"),
                    _make_comment("1"),
                    {"text": "no cid"},
                    {"cid": "", "text": "empty"},
                ],
                "has_more": False,
                "next_cursor": 50,
                "http_status": 200,
                "error": None,
            }),
        ]
        _install_mock_transport(monkeypatch, responses)

        result = scrape_video_comments("vid", "author", worker_id=1)

        assert result["total_comments"] == 4
        assert result["duplicates_skipped"] == 1
        assert result["invalid_missing_ids"] == 2


# ============================================================================
# 3. RESUME AND DUPLICATE INTEGRITY
# ============================================================================

class TestResumeAndDuplicateIntegrity:
    def test_overlapping_comments_no_duplicate_valid_cids(self, monkeypatch, tmp_path):
        cache_dir = _fresh_cache_dir()
        monkeypatch.setattr(tiktokscraper_1, "CACHE_DIR", cache_dir)

        responses_run1 = [
            (200, {
                "comments": [_make_comment("1"), _make_comment("2")],
                "has_more": True,
                "next_cursor": 50,
                "http_status": 200,
                "error": None,
            }),
        ]
        _install_mock_transport(monkeypatch, responses_run1)
        result1 = scrape_video_comments("vid", "author", worker_id=1)
        assert result1["total_comments"] == 2
        assert result1["total_batches"] == 1

        cache_file = cache_dir / "worker_1_vid.json"
        assert cache_file.exists()
        cached = json.loads(cache_file.read_text(encoding="utf-8"))
        assert set(cached["seen_cids"]) == {"1", "2"}
        assert cached["cursor"] == 50
        assert cached["batch_number"] == 1
        assert len(cached["comments"]) == 2

        responses_run2 = [
            (200, {
                "comments": [_make_comment("2"), _make_comment("3")],
                "has_more": False,
                "next_cursor": 100,
                "http_status": 200,
                "error": None,
            }),
        ]
        _install_mock_transport(monkeypatch, responses_run2)
        result2 = scrape_video_comments("vid", "author", worker_id=1)

        assert result2["total_batches"] == 2
        assert result2["duplicates_skipped"] == 1
        final_cids = [c.get("cid") for c in result2["comments"] if c.get("cid") is not None]
        assert final_cids.count("2") == 1, f"Duplicate valid CID '2' found: {final_cids}"
        assert set(final_cids) == {"1", "2", "3"}

    def test_termination_before_next_checkpoint(self, monkeypatch, tmp_path):
        cache_dir = _fresh_cache_dir()
        monkeypatch.setattr(tiktokscraper_1, "CACHE_DIR", cache_dir)

        responses = [
            (200, {
                "comments": [_make_comment("1")],
                "has_more": True,
                "next_cursor": 50,
                "http_status": 200,
                "error": None,
            }),
        ]
        _install_mock_transport(monkeypatch, responses)
        result = scrape_video_comments("vid", "author", worker_id=1)

        assert result["total_comments"] == 1
        assert result["total_batches"] == 1
        cache_file = cache_dir / "worker_1_vid.json"
        assert cache_file.exists()
        cached = json.loads(cache_file.read_text(encoding="utf-8"))
        assert cached["batch_number"] == 1
        assert cached["cursor"] == 50

    def test_cursor_advances_only_after_page_successfully_processed(self, monkeypatch, tmp_path):
        cache_dir = _fresh_cache_dir()
        monkeypatch.setattr(tiktokscraper_1, "CACHE_DIR", cache_dir)
        responses = [
            (200, {
                "comments": [_make_comment("1")],
                "has_more": True,
                "next_cursor": 50,
                "http_status": 200,
                "error": None,
            }),
            (0, None),
            (0, None),
            (0, None),
        ]
        _install_mock_transport(monkeypatch, responses)
        result = scrape_video_comments("vid", "author", worker_id=1)

        assert result["total_comments"] == 1
        cache_file = cache_dir / "worker_1_vid.json"
        cached = json.loads(cache_file.read_text(encoding="utf-8"))
        assert cached["cursor"] == 50, f"Cursor should remain 50, got {cached['cursor']}"


# ============================================================================
# 4. CHECKPOINT FREQUENCY
# ============================================================================

class TestCheckpointFrequency:
    def test_save_every_n_batches_is_one(self):
        assert SAVE_EVERY_N_BATCHES == 1

    def test_checkpoint_saved_after_each_batch(self, monkeypatch, tmp_path):
        cache_dir = _fresh_cache_dir()
        monkeypatch.setattr(tiktokscraper_1, "CACHE_DIR", cache_dir)
        call_count = {"n": 0}

        def fake_request(self, method, url, headers=None, body=None):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return (200, {
                    "comments": [_make_comment("1")],
                    "has_more": True,
                    "next_cursor": 50,
                    "http_status": 200,
                    "error": None,
                })
            if call_count["n"] == 2:
                return (200, {
                    "comments": [_make_comment("2")],
                    "has_more": False,
                    "next_cursor": 100,
                    "http_status": 200,
                    "error": None,
                })
            return (200, {"comments": [], "has_more": False, "next_cursor": 0, "http_status": 200, "error": None})

        monkeypatch.setattr(http_transport.HTTPTransport, "request", fake_request)
        scrape_video_comments("vid", "author", worker_id=1)

        cache_file = cache_dir / "worker_1_vid.json"
        assert cache_file.exists(), "Checkpoint must be saved after each batch"
        cached = json.loads(cache_file.read_text(encoding="utf-8"))
        assert cached["batch_number"] == 2
        assert len(cached["comments"]) == 2


# ============================================================================
# 5. MAX_COMMENTS ENFORCEMENT
# ============================================================================

class TestMaxCommentsEnforcement:
    def test_never_appends_more_than_max_comments(self, monkeypatch, tmp_path):
        cache_dir = _fresh_cache_dir()
        monkeypatch.setattr(tiktokscraper_1, "CACHE_DIR", cache_dir)
        monkeypatch.setattr(tiktokscraper_1, "MAX_COMMENTS", 3)

        call_count = {"n": 0}

        def fake_request(self, method, url, headers=None, body=None):
            call_count["n"] += 1
            return (200, {
                "comments": [_make_comment(str(call_count["n"] + 10)), _make_comment(str(call_count["n"] + 11))],
                "has_more": True,
                "next_cursor": 50 * call_count["n"],
                "http_status": 200,
                "error": None,
            })

        monkeypatch.setattr(http_transport.HTTPTransport, "request", fake_request)
        result = scrape_video_comments("vid", "author", worker_id=1)

        assert len(result["comments"]) <= 3, f"Exceeded MAX_COMMENTS: {len(result['comments'])}"
        assert result["total_comments"] <= 3

    def test_limit_reached_preserves_comments_saves_checkpoint_partial_result(self, monkeypatch, tmp_path):
        cache_dir = _fresh_cache_dir()
        monkeypatch.setattr(tiktokscraper_1, "CACHE_DIR", cache_dir)
        monkeypatch.setattr(tiktokscraper_1, "MAX_COMMENTS", 2)

        call_count = {"n": 0}

        def fake_request(self, method, url, headers=None, body=None):
            call_count["n"] += 1
            return (200, {
                "comments": [_make_comment(str(i)) for i in range(call_count["n"] * 2 - 1, call_count["n"] * 2 + 1)],
                "has_more": True,
                "next_cursor": 50,
                "http_status": 200,
                "error": None,
            })

        monkeypatch.setattr(http_transport.HTTPTransport, "request", fake_request)
        result = scrape_video_comments("vid", "author", worker_id=1)

        assert len(result["comments"]) <= 2
        cache_file = cache_dir / "worker_1_vid.json"
        assert cache_file.exists()
        cached = json.loads(cache_file.read_text(encoding="utf-8"))
        assert len(cached["comments"]) <= 2

    def test_never_marks_pagination_complete_without_has_more_false(self, monkeypatch, tmp_path):
        cache_dir = _fresh_cache_dir()
        monkeypatch.setattr(tiktokscraper_1, "CACHE_DIR", cache_dir)
        monkeypatch.setattr(tiktokscraper_1, "MAX_COMMENTS", 1)

        responses = [
            (200, {
                "comments": [_make_comment("1")],
                "has_more": True,
                "next_cursor": 50,
                "http_status": 200,
                "error": None,
            }),
        ]
        _install_mock_transport(monkeypatch, responses)
        result = scrape_video_comments("vid", "author", worker_id=1)

        assert len(result["comments"]) <= 1
        cache_file = cache_dir / "worker_1_vid.json"
        cached = json.loads(cache_file.read_text(encoding="utf-8"))
        assert cached["is_complete"] is False, "Must not mark complete when limit reached without has_more=False"


# ============================================================================
# 6. FINAL OUTPUT INVESTIGATION
# ============================================================================

class TestFinalOutputInvestigation:
    def test_no_csv_writing_function_in_operational_version(self):
        source = TIKTOKSCRAPER_1_PATH.read_text(encoding="utf-8")
        assert "def save_to_csv" not in source, "Operational version must not define save_to_csv"
        assert "import csv" not in source, "Operational version must not import csv"

    def test_no_final_json_csv_output_path_in_operational_version(self):
        source = TIKTOKSCRAPER_1_PATH.read_text(encoding="utf-8")
        assert "RESULTADOS" not in source, "No final output path (RESULTADOS) in operational version"
        assert "def main(" not in source, "No main() entry point in operational version"
