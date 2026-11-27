"""Regression tests for the centralized HTTP transport.

All network calls are mocked; these tests never contact TikTok or the internet.
"""
import io
import json
from urllib.error import HTTPError, URLError
from unittest.mock import MagicMock, patch

from http_transport import HTTPTransport


def test_request_returns_parsed_json_object():
    response = MagicMock()
    response.getcode.return_value = 200
    response.read.return_value = json.dumps({"ok": True}).encode("utf-8")

    with patch("http_transport.urllib.request.urlopen", return_value=response):
        status, payload = HTTPTransport().request("GET", "https://example.invalid")

    assert status == 200
    assert payload == {"ok": True}
    response.close.assert_called_once()


def test_network_error_returns_zero_and_none():
    with patch(
        "http_transport.urllib.request.urlopen",
        side_effect=URLError("connection timed out"),
    ):
        status, payload = HTTPTransport().request("GET", "https://example.invalid")

    assert status == 0
    assert payload is None


def test_http_error_preserves_status_and_returns_no_payload():
    error = HTTPError(
        "https://example.invalid", 429, "Too Many Requests", {}, io.BytesIO(b"{}")
    )
    with patch("http_transport.urllib.request.urlopen", side_effect=error):
        status, payload = HTTPTransport().request("GET", "https://example.invalid")

    assert status == 429
    assert payload is None


def test_invalid_json_returns_status_and_none():
    response = MagicMock()
    response.getcode.return_value = 200
    response.read.return_value = b"{not-json"

    with patch("http_transport.urllib.request.urlopen", return_value=response):
        status, payload = HTTPTransport().request("GET", "https://example.invalid")

    assert status == 200
    assert payload is None


def test_non_object_json_is_rejected():
    response = MagicMock()
    response.getcode.return_value = 200
    response.read.return_value = b"[1, 2, 3]"

    with patch("http_transport.urllib.request.urlopen", return_value=response):
        status, payload = HTTPTransport().request("GET", "https://example.invalid")

    assert status == 200
    assert payload is None


def test_fetch_json_forwards_overall_timeout():
    transport = HTTPTransport(overall_timeout=17)
    with patch.object(transport, "request", return_value=(200, {})) as request:
        assert transport.fetch_json("https://example.invalid") == (200, {})
    assert request.call_args.kwargs["overall_timeout"] == 17
