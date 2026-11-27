"""TikTok API adapter; TikTok wire-format knowledge lives here."""
from __future__ import annotations
import random
import time
from typing import Any, Dict
from urllib.parse import urlencode

from errors.model import ScraperError, ErrorOutcome, ErrorSeverity
from http_transport import HTTPTransport, HTTPTransportError
from comment_schema import normalize_tiktok_comment
from .base import PlatformAdapter


class TikTokAdapter(PlatformAdapter):
    platform_id = "tiktok"
    DEFAULT_API_RESPONSE_RETRIES = 6
    MAX_API_RETRY_DELAY_SECONDS = 30.0

    def __init__(self, *, transport: HTTPTransport, video_id: str,
                 run_id: str, retry_budget: Any = None,
                 max_api_response_retries: int = DEFAULT_API_RESPONSE_RETRIES):
        self.validate_resource_id(video_id)
        self.transport = transport
        self.video_id = video_id
        self.run_id = run_id
        if isinstance(max_api_response_retries, bool) or not isinstance(max_api_response_retries, int) or max_api_response_retries < 1:
            raise ValueError("max_api_response_retries must be a positive integer")
        self.retry_budget = retry_budget
        self.max_api_response_retries = max_api_response_retries

    def validate_resource_id(self, resource_id: str) -> None:
        if not resource_id.isdigit() or not 1 <= len(resource_id) <= 32:
            raise ValueError("TikTok video_id must contain 1-32 decimal digits")

    def fetch_page(self, *, cursor: int, token: str) -> Dict[str, Any]:
        query = urlencode({
            "aid": "1988", "aweme_id": self.video_id, "count": "50",
            "cursor": str(cursor), "msToken": token,
        })
        url = "https://www.tiktok.com/api/comment/list/?" + query
        data = None
        response = None
        for attempt in range(1, self.max_api_response_retries + 1):
            try:
                response = self.transport.request_with_retry(
                    "GET", url, retry_budget=self.retry_budget,
                    retry_component=("proxy" if self.transport.proxy_url else "http"),
                    headers={
                        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                        "referer": "https://www.tiktok.com/",
                    },
                    expect_json=True, maximum_attempts=self.transport.max_retries,
                )
            except HTTPTransportError as error:
                outcome = (ErrorOutcome.RETRY_EXHAUSTED if error.error_code == "RETRY_EXHAUSTED"
                           else ErrorOutcome.FAILED_RESUMABLE if error.retryable
                           else ErrorOutcome.FAILED_PERMANENT)
                raise ScraperError(
                    error_code=error.error_code, severity=ErrorSeverity.ERROR,
                    component="tiktok_adapter", function="fetch_page",
                    video_id=self.video_id, run_id=self.run_id,
                    http_status=error.http_status, explanation=str(error), outcome=outcome,
                ) from error

            data = response.json_data
            if not isinstance(data, dict):
                return {"schema_error": "response_not_object"}

            api_status = data.get("status_code")
            if api_status in (None, 0, "0"):
                break

            logger.warning(
                "TikTok application response was not successful: video_id=%s cursor=%s "
                "status_code=%s attempt=%d/%d",
                self.video_id, cursor, str(api_status)[:32],
                attempt, self.max_api_response_retries,
            )
            if attempt >= self.max_api_response_retries:
                return {"error": True, "api_status_code": str(api_status)[:32]}

            if self.retry_budget is not None and not self.retry_budget.consume_retry(
                "api_response", "TIKTOK_APPLICATION_STATUS"
            ):
                return {"error": True, "api_status_code": str(api_status)[:32], "retry_exhausted": True}

            ceiling = min(self.MAX_API_RETRY_DELAY_SECONDS, 2.0 ** (attempt - 1))
            delay = random.uniform(0.5, max(0.5, ceiling))
            if self.retry_budget is not None:
                delay = min(delay, self.retry_budget.remaining_seconds())
            if delay > 0:
                time.sleep(delay)

        if not isinstance(data, dict):
            return {"schema_error": "response_not_object"}
        if "comments" not in data:
            return {"schema_error": "comments_field_missing"}
        if "has_more" not in data:
            return {"schema_error": "has_more_field_missing"}
        if "cursor" not in data and data.get("has_more") in (True, 1, "1"):
            return {"schema_error": "cursor_field_missing"}
        return {
            "http_status": response.status_code, "comments": data.get("comments"),
            "has_more": data.get("has_more"), "next_cursor": data.get("cursor"),
            "error": None,
        }

    def is_comment_record(self, record: Any) -> bool:
        if not isinstance(record, dict):
            return False
        keys = {
            "cid", "text", "create_time", "user", "user_id", "aweme_id",
            "reply_id", "digg_count", "status", "stick_position", "reply_comment",
        }
        return bool(keys.intersection(record.keys()))

    def has_minimum_content(self, record: Dict[str, Any]) -> bool:
        value = record.get("text")
        return isinstance(value, (str, int, float)) and not isinstance(value, bool)

    def has_comment_id(self, record: Dict[str, Any]) -> bool:
        value = record.get("cid")
        if isinstance(value, bool):
            return False
        if isinstance(value, int):
            return value >= 0
        return isinstance(value, str) and bool(value.strip())

    def normalization_anomalies(self, source_record: Dict[str, Any],
                                normalized_record: Dict[str, Any]) -> Dict[str, int]:
        raw_timestamp = source_record.get("create_time", source_record.get("createTime"))
        raw_likes = source_record.get("digg_count", source_record.get("like_count"))
        raw_replies = source_record.get("reply_comment_total", source_record.get("reply_count"))
        return {
            "invalid_timestamp": int(normalized_record.get("created_at_epoch") is None and raw_timestamp is not None),
            "invalid_like_count": int(normalized_record.get("likes") is None and raw_likes is not None),
            "invalid_reply_count": int(normalized_record.get("reply_count") is None and raw_replies is not None),
        }

    def normalize_comment(self, video_id: str, record: Dict[str, Any]) -> Dict[str, Any]:
        return normalize_tiktok_comment(video_id, record).to_dict()
