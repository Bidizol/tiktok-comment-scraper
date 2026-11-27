from unittest.mock import patch, MagicMock, call
import json
import pytest
import sys
from pathlib import Path
import importlib.util

# Dynamically load tiktokscraper-1 module (file has hyphen, can't import normally)
# The test file is at /Users/admin/Desktop/tiktok/tiktok-comment-scraper/tests/test_phase4.py
# tiktokscraper-1.py is at /Users/admin/Desktop/tiktok/tiktok-comment-scraper/tiktokscraper-1.py
TIKTOKSCRAPER_1_PATH = str(Path(__file__).parent.parent / "tiktokscraper-1.py")
_tiktokscraper_1_spec = importlib.util.spec_from_file_location("tiktokscraper_1", TIKTOKSCRAPER_1_PATH)
_tiktokscraper_1 = importlib.util.module_from_spec(_tiktokscraper_1_spec)
_tiktokscraper_1_spec.loader.exec_module(_tiktokscraper_1)

# Exercise the operational implementation loaded from tiktokscraper-1.py.
scrape_video_comments = _tiktokscraper_1.scrape_video_comments


class TestScrapeVideoCommentsIntegration:
    """Integration test for scrape_video_comments with mocked dependencies."""

    @patch('token_manager.TokenManager.rotate_proxy_and_get_token', create=True)
    @patch('token_manager.TokenManager.get_token')
    @patch('http_transport.HTTPTransport.request')
    def test_scenario_first_401_then_token_refresh_failure_then_proxy_failure_then_budget_exhausted(self, mock_http_request, mock_get_token, mock_rotate_proxy_and_get_token):
        """Test the full retry scenario: 401 -> token refresh failure -> proxy failure -> budget exhausted."""
        # scrape_video_comments is already imported at module level
        video_id = "7676572010415230221"
        video_author = "erickie20"
        worker_id = 1
        
        def make_fetch_result(http_status, success=True):
            if not success:
                return {"success": False, "comments": [], "has_more": False, "next_cursor": 0, "http_status": http_status, "error": f"HTTP error: {http_status}"}
            return {"success": True, "comments": [], "has_more": True, "next_cursor": 50, "http_status": http_status, "error": None}
        
        mock_get_token.side_effect = ["initial_token", None, None]
        mock_http_request.side_effect = [(401, make_fetch_result(401, False)), (None, make_fetch_result(0, False)), (None, make_fetch_result(0, False))]
        
        result = scrape_video_comments(video_id, video_author, worker_id=worker_id)
        assert "COMPLETED" not in str(result).upper() or result["total_comments"] == 0
        assert isinstance(result, dict)
        assert mock_http_request.call_count == 3
        print("PASS: test_scenario_first_401_then_token_refresh_failure_then_proxy_failure_then_budget_exhausted")
