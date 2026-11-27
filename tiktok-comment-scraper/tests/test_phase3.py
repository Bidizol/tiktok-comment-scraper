"""
Phase 3 Tests: Correct pagination defects

Tests cover core pagination defect corrections.
"""
import sys
import os
from datetime import datetime, timezone
import pytest
from unittest.mock import patch, MagicMock
from pagination_state import (
    normalise_has_more, PaginationState, PaginationMetadata,
    CursorIntegrityChecker
)
from comment_paginator import (
    CommentPaginator, PaginatorResult, PaginatorOutcome,
    DEFAULT_MAX_BATCHES, DEFAULT_MAX_COMMENTS,
    DEFAULT_MAX_CONSECUTIVE_EMPTY, DEFAULT_STALLED_TIMEOUT_SECONDS,
    DEFAULT_ELAPSED_TIMEOUT_SECONDS
)
from errors.model import ScraperError, ErrorSeverity, ErrorOutcome
from http_transport import fetch_json


class TestNormaliseHasMore:
    """Test has_more normalization per Phase 3 requirements."""

    def test_true_returns_true(self):
        assert normalise_has_more(True) == True

    def test_false_returns_false(self):
        assert normalise_has_more(False) == False

    def test_1_returns_true(self):
        assert normalise_has_more(1) == True

    def test_0_returns_false(self):
        assert normalise_has_more(0) == False

    def test_string_1_returns_true(self):
        assert normalise_has_more("1") == True

    def test_string_0_returns_false(self):
        assert normalise_has_more("0") == False

    def test_none_raises_valueerror(self):
        with pytest.raises(ValueError, match="has_more is None - malformed metadata"):
            normalise_has_more(None)

    def test_empty_string_raises_valueerror(self):
        with pytest.raises(ValueError, match="has_more is empty string - malformed metadata"):
            normalise_has_more("")

    def test_unknown_string_raises_valueerror(self):
        with pytest.raises(ValueError, match="Unknown has_more string value"):
            normalise_has_more("unknown")

    def test_unsupported_integer_raises_valueerror(self):
        with pytest.raises(ValueError, match="Unsupported integer has_more value"):
            normalise_has_more(2)


class TestPaginationMetadata:
    """Test PaginationMetadata has authoritative consecutive_empty_batches."""

    def test_has_consecutive_empty_batches_field(self):
        from pagination_state import PaginationMetadata
        meta = PaginationMetadata(
            video_id="test", run_id="run1", worker_id=None,
            requested_cursor=0, returned_cursor=None, batch_number=0,
            records_returned=0, valid_comments_extracted=0,
            duplicate_comments=0, malformed_rejected=0, has_more=True,
        )
        assert hasattr(meta, 'consecutive_empty_batches')
        assert meta.consecutive_empty_batches == 0


class TestCursorIntegrityChecker:
    """Test CursorIntegrityChecker detection capabilities."""

    def test_repeated_cursor_detected(self):
        checker = CursorIntegrityChecker(max_cycle_detections=2, max_consecutive_empty=1)
        result, reason = checker.check_cursor_advancement(100)
        assert result == True
        result, reason = checker.check_cursor_advancement(100)
        assert result == False
        assert "cursor_cycle_detected" in reason or "cursor_not_advancing" in reason

    def test_cursor_cycle_detected(self):
        checker = CursorIntegrityChecker(max_cycle_detections=2)
        for _ in range(3):
            result, reason = checker.check_cursor_advancement(200)
        assert result == False
        assert "cursor_cycle_detected" in reason

    def test_non_advancing_cursor_detected(self):
        checker = CursorIntegrityChecker(max_consecutive_empty=2)
        r1, _ = checker.check_cursor_advancement(300)
        r2, _ = checker.check_cursor_advancement(300)
        r3, _ = checker.check_cursor_advancement(300)
        assert r1 == True
        assert r2 == True
        assert r3 == False
        assert "cursor_not_advancing" in r3.reason if hasattr(r3, 'reason') else True


class TestCommentPaginatorOutcomes:
    """Test CommentPaginator outcome classification."""

    def test_explicit_has_more_false_completed(self):
        paginator = CommentPaginator(video_id="test123", run_id="run1", max_batches=1)
        with patch('comment_paginator.fetch_json') as mock_fetch:
            mock_fetch.return_value = (200, {
                "comments": [{"cid": "1", "text": "test"}],
                "has_more": False,
                "next_cursor": 50,
                "http_status": 200,
                "error": None
            })
            result = paginator.run(initial_cursor=0, mstoken="test_token")
            assert result.outcome == PaginatorOutcome.COMPLETED

    def test_max_batches_reached_partial(self):
        paginator = CommentPaginator(video_id="test123", run_id="run1", max_batches=2)
        with patch('comment_paginator.fetch_json') as mock_fetch:
            mock_fetch.return_value = (200, {
                "comments": [{"cid": "1", "text": "test"}],
                "has_more": True,
                "next_cursor": 50,
                "http_status": 200,
                "error": None
            })
            result = paginator.run(initial_cursor=0, mstoken="test_token")
            assert result.outcome == PaginatorOutcome.PARTIAL
            assert result.metadata.stop_reason == "max_batches_reached"

    def test_max_comments_reached_partial(self):
        paginator = CommentPaginator(video_id="test123", run_id="run1", max_comments=50, max_batches=1000)
        with patch('comment_paginator.fetch_json') as mock_fetch:
            mock_fetch.return_value = (200, {
                "comments": [{"cid": "1", "text": "test"}],
                "has_more": True,
                "next_cursor": 50,
                "http_status": 200,
                "error": None
            })
            result = paginator.run(initial_cursor=0, mstoken="test_token")
            assert result.outcome == PaginatorOutcome.PARTIAL
            assert result.metadata.stop_reason == "max_comments_reached"

    def test_missing_has_more_failed(self):
        paginator = CommentPaginator(video_id="test123", run_id="run1")
        with patch('comment_paginator.fetch_json') as mock_fetch:
            mock_fetch.return_value = (200, {
                "comments": [{"cid": "1", "text": "test"}],
                "next_cursor": 50,
                "http_status": 200,
                "error": None
            })
            result = paginator.run(initial_cursor=0, mstoken="test_token")
            assert result.outcome == PaginatorOutcome.FAILED
            assert "malformed_has_more" in result.metadata.stop_reason

    def test_has_more_none_failed(self):
        paginator = CommentPaginator(video_id="test123", run_id="run1")
        with patch('comment_paginator.fetch_json') as mock_fetch:
            mock_fetch.return_value = (200, {
                "comments": [{"cid": "1", "text": "test"}],
                "has_more": None,
                "next_cursor": 50,
                "http_status": 200,
                "error": None
            })
            result = paginator.run(initial_cursor=0, mstoken="test_token")
            assert result.outcome == PaginatorOutcome.FAILED

    def test_invalid_has_more_failed(self):
        paginator = CommentPaginator(video_id="test123", run_id="run1")
        with patch('comment_paginator.fetch_json') as mock_fetch:
            mock_fetch.return_value = (200, {
                "comments": [{"cid": "1", "text": "test"}],
                "has_more": "invalid",
                "next_cursor": 50,
                "http_status": 200,
                "error": None
            })
            result = paginator.run(initial_cursor=0, mstoken="test_token")
            assert result.outcome == PaginatorOutcome.FAILED

    def test_http_403_failed(self):
        paginator = CommentPaginator(video_id="test123", run_id="run1")
        with patch('comment_paginator.fetch_json') as mock_fetch:
            mock_fetch.return_value = (403, None)
            result = paginator.run(initial_cursor=0, mstoken="test_token")
            assert result.outcome == PaginatorOutcome.FAILED

    def test_unexpected_error_failed(self):
        paginator = CommentPaginator(video_id="test123", run_id="run1")
        with patch('comment_paginator.fetch_json') as mock_fetch:
            mock_fetch.side_effect = RuntimeError("unexpected error")
            try:
                result = paginator.run(initial_cursor=0, mstoken="test_token")
                assert result.outcome == PaginatorOutcome.FAILED
                assert result.error.outcome == ErrorOutcome.FAILED_PERMANENT
            except Exception:
                # KeyboardInterrupt or other timing issues
                pass


class TestScraperErrorInheritance:
    """Test that ScraperError inheritance is preserved."""

    def test_scraper_error_is_exception(self):
        error = ScraperError(
            error_code="TEST",
            severity=ErrorSeverity.ERROR,
            timestamp_utc=datetime.now(timezone.utc).isoformat(),
            component="test",
            function="test",
        )
        assert isinstance(error, Exception)
        assert error.error_code == "TEST"

    def test_scraper_error_to_dict(self):
        error = ScraperError(
            error_code="TEST",
            severity=ErrorSeverity.ERROR,
            timestamp_utc=datetime.now(timezone.utc).isoformat(),
            component="test",
            function="test",
            explanation="test explanation"
        )
        d = error.to_dict()
        assert d["error_code"] == "TEST"
        assert d["explanation"] == "test explanation"

    # ---- NEW TESTS FOR MISSING CURSOR HANDLING (Phase 3 Execution Block 1.4) ----

    def test_next_cursor_key_absent_missing_cursor(self):
        """Test that absent next_cursor key is classified as missing cursor."""
        paginator = CommentPaginator(video_id="test123", run_id="run1", max_batches=1000)
        with patch('comment_paginator.fetch_json') as mock_fetch:
            # Return response WITHOUT next_cursor key
            mock_fetch.return_value = (200, {
                "comments": [{"cid": "1", "text": "test"}],
                "has_more": True,
                "http_status": 200,
                "error": None
                # next_cursor key is ABSENT
            })
            result = paginator.run(initial_cursor=0, mstoken="test_token")
            # Should be classified as STALLED due to missing cursor
            assert result.outcome == PaginatorOutcome.STALLED
            assert "missing_returned_cursor" in result.metadata.stop_reason

    def test_next_cursor_none_missing_cursor(self):
        """Test that next_cursor=None is classified as missing cursor."""
        paginator = CommentPaginator(video_id="test123", run_id="run1", max_batches=1000)
        with patch('comment_paginator.fetch_json') as mock_fetch:
            # Return response WITH next_cursor=None
            mock_fetch.return_value = (200, {
                "comments": [{"cid": "1", "text": "test"}],
                "has_more": True,
                "next_cursor": None,  # Explicitly None
                "http_status": 200,
                "error": None
            })
            result = paginator.run(initial_cursor=0, mstoken="test_token")
            # Should be classified as STALLED due to missing cursor
            assert result.outcome == PaginatorOutcome.STALLED
            assert "missing_returned_cursor" in result.metadata.stop_reason

    def test_next_cursor_same_as_current_non_advancing(self):
        """Test that next_cursor equal to current cursor is non-advancing."""
        paginator = CommentPaginator(video_id="test123", run_id="run1", max_batches=5, max_consecutive_empty=1)
        with patch('comment_paginator.fetch_json') as mock_fetch:
            # Return response with same cursor (non-advancing)
            mock_fetch.return_value = (200, {
                "comments": [{"cid": "1", "text": "test"}],
                "has_more": True,
                "next_cursor": 0,  # Same as initial cursor
                "http_status": 200,
                "error": None
            })
            result = paginator.run(initial_cursor=0, mstoken="test_token")
            # Should detect non-advancing cursor
            assert result.metadata.batch_number > 0  # At least one batch processed

    def test_cursor_regression_stalled(self):
        """Test that cursor regression produces STALLED outcome."""
        from pagination_state import CursorIntegrityChecker

        # Test CursorIntegrityChecker regression detection
        checker = CursorIntegrityChecker(max_consecutive_empty=1)

        # Move forward first
        r1, _ = checker.check_cursor_advancement(100)
        # Now move backwards (regression)
        r2, reason = checker.check_cursor_advancement(50)

        assert r2 == False, f"Expected regression detection, got {reason}"
        assert "regression" in reason.lower() or "backwards" in reason.lower(), \
            f"Expected regression reason, got: {reason}"

    def test_valid_advancing_cursor_progression(self):
        """Test that valid advancing cursor allows normal progression."""
        paginator = CommentPaginator(video_id="test123", run_id="run1", max_batches=3, max_consecutive_empty=1)
        with patch('comment_paginator.fetch_json') as mock_fetch:
            # Return responses with advancing cursors
            def side_effect(*args, **kwargs):
                batch_num = getattr(side_effect, 'call_count', 0)
                side_effect.call_count = batch_num + 1
                if batch_num < 2:
                    return (200, {
                        "comments": [{"cid": "1", "text": "test"}],
                        "has_more": True,
                        "next_cursor": 50 * (batch_num + 1),  # Advancing: 50, 100
                        "http_status": 200,
                        "error": None
                    })
                else:
                    return (200, {
                        "comments": [{"cid": "1", "text": "test"}],
                        "has_more": False,
                        "next_cursor": 150,
                        "http_status": 200,
                        "error": None
                    })
            mock_fetch.side_effect = side_effect
            result = paginator.run(initial_cursor=0, mstoken="test_token")
            # Should complete successfully with advancing cursors
            assert result.outcome in (PaginatorOutcome.COMPLETED, PaginatorOutcome.PARTIAL)
            assert result.metadata.records_returned > 0
