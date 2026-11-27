"""Stable comment record model and TikTok-to-normalized conversion."""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Optional


@dataclass(frozen=True)
class NormalizedComment:
    platform: str
    video_id: str
    comment_id: Optional[str]
    user_id: Optional[str]
    username: Optional[str]
    nickname: Optional[str]
    text: str
    likes: Optional[int]
    reply_count: Optional[int]
    created_at_epoch: Optional[int]
    created_at_utc: Optional[str]
    parent_comment_id: Optional[str]
    is_reply: bool
    collected_at_utc: str
    dedupe_key: Optional[str]
    source_payload: dict

    def to_dict(self) -> dict:
        return asdict(self)


def _optional_string(value: Any) -> Optional[str]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (str, int)):
        value = str(value).strip()
        return value or None
    return None


def _optional_parent_comment_id(value: Any) -> Optional[str]:
    """Normalize TikTok's zero-valued root-comment sentinel to no parent."""
    parent_id = _optional_string(value)
    return None if parent_id == "0" else parent_id


def _is_reply_flag(value: Any) -> bool:
    return value is True or (isinstance(value, int) and not isinstance(value, bool) and value == 1)


def _optional_count(value: Any) -> Optional[int]:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float):
        return int(value) if math.isfinite(value) and value >= 0 and value.is_integer() else None
    if isinstance(value, str):
        try:
            parsed = int(value.strip())
            return parsed if parsed >= 0 else None
        except (ValueError, TypeError):
            return None
    return None


def _epoch_seconds(value: Any) -> Optional[int]:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            if not math.isfinite(value) or value < 0:
                return None
            number = float(value)
        except (OverflowError, TypeError, ValueError):
            return None
        if number >= 1e17:
            number /= 1e9
        elif number >= 1e14:
            number /= 1e6
        elif number >= 1e11:
            number /= 1e3
        try:
            parsed = datetime.fromtimestamp(number, tz=timezone.utc)
            return int(parsed.timestamp())
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str) and value.strip():
        raw = value.strip()
        try:
            number = float(raw)
        except ValueError:
            number = None
        if number is not None:
            return _epoch_seconds(number)
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        try:
            return int(parsed.astimezone(timezone.utc).timestamp())
        except (OverflowError, OSError, ValueError):
            return None
    return None


def safe_source_payload(value: Any) -> Any:
    """Make source fields JSON-safe and omit fields that could carry credentials."""
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            key_text = str(key)
            lowered = key_text.lower().replace("-", "_")
            sensitive_names = {
                "token", "mstoken", "accesstoken", "access_token",
                "refreshtoken", "refresh_token", "authtoken",
                "cookie", "cookies", "session", "session_id", "sessionid",
                "sessioncookie", "authorization", "auth", "password",
                "secret", "clientsecret", "client_secret", "proxy",
                "proxy_url", "api_key", "apikey", "credentials",
            }
            if (lowered in sensitive_names or lowered.startswith("proxy_") or lowered.endswith(
                    ("_token", "_cookie", "_password", "_secret", "_api_key"))):
                continue
            result[key_text] = safe_source_payload(item)
        return result
    if isinstance(value, (list, tuple)):
        return [safe_source_payload(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def normalize_comment_record(video_id: str, record: dict) -> dict:
    """Normalize either a legacy TikTok payload or an existing normalized cache row."""
    if isinstance(record, dict) and record.get("platform") == "tiktok" and "comment_id" in record:
        text_value = record.get("text")
        if not isinstance(text_value, str):
            raise ValueError("Normalized comment text must be a string")
        created_epoch = _epoch_seconds(
            record.get("created_at_epoch")
            if record.get("created_at_epoch") is not None
            else record.get("created_at_utc")
        )
        created_utc = (
            datetime.fromtimestamp(created_epoch, tz=timezone.utc).isoformat().replace("+00:00", "Z")
            if created_epoch is not None else None
        )
        comment_id = _optional_string(record.get("comment_id"))
        user_id = _optional_string(record.get("user_id"))
        username = _optional_string(record.get("username"))
        nickname = _optional_string(record.get("nickname"))
        parent_id = _optional_parent_comment_id(record.get("parent_comment_id"))
        source_payload = record.get("source_payload", {})
        if isinstance(source_payload, dict) and source_payload:
            raw_reply_flag = source_payload.get("reply_comment")
            is_reply = parent_id is not None or _is_reply_flag(raw_reply_flag)
        else:
            # Older normalized rows may not retain the source payload needed to
            # distinguish an explicit reply flag from a legacy zero parent ID.
            is_reply = parent_id is not None or _is_reply_flag(record.get("is_reply"))
        dedupe_key = "cid:" + comment_id if comment_id else record.get("dedupe_key")
        if comment_id is None and dedupe_key is None and created_epoch is not None:
            fingerprint = json.dumps({
                "video_id": str(video_id), "user_id": user_id, "username": username,
                "text": text_value, "created_at_epoch": created_epoch,
                "parent_comment_id": parent_id,
            }, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            dedupe_key = "fingerprint:" + hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()
        return NormalizedComment(
            platform="tiktok", video_id=str(video_id), comment_id=comment_id,
            user_id=user_id, username=username, nickname=nickname, text=text_value,
            likes=_optional_count(record.get("likes")),
            reply_count=_optional_count(record.get("reply_count")),
            created_at_epoch=created_epoch, created_at_utc=created_utc,
            parent_comment_id=parent_id,
            is_reply=is_reply,
            collected_at_utc=_optional_string(record.get("collected_at_utc")) or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            dedupe_key=dedupe_key, source_payload=safe_source_payload(record.get("source_payload", {}))
            if isinstance(record.get("source_payload", {}), dict) else {},
        ).to_dict()
    return normalize_tiktok_comment(video_id, record).to_dict()


def normalize_tiktok_comment(video_id: str, record: dict) -> NormalizedComment:
    if not isinstance(record, dict):
        raise TypeError("Comment record must be an object")
    user = record.get("user")
    user = user if isinstance(user, dict) else {}
    comment_id = _optional_string(record.get("cid"))
    user_id = _optional_string(user.get("uid", user.get("id", record.get("user_id"))))
    username = _optional_string(user.get("unique_id", user.get("username", record.get("unique_id"))))
    nickname = _optional_string(user.get("nickname", record.get("nickname")))
    text_value = record.get("text")
    if not isinstance(text_value, (str, int, float)) or isinstance(text_value, bool):
        raise ValueError("Comment text is missing or invalid")
    comment_text = str(text_value)
    created_epoch = _epoch_seconds(record.get("create_time", record.get("createTime")))
    created_utc = (
        datetime.fromtimestamp(created_epoch, tz=timezone.utc).isoformat().replace("+00:00", "Z")
        if created_epoch is not None else None
    )
    likes = _optional_count(record.get("digg_count", record.get("like_count")))
    replies = _optional_count(record.get("reply_comment_total", record.get("reply_count")))
    parent_id = _optional_string(record.get("reply_id", record.get("reply_to_comment_id")))
    raw_reply_flag = record.get("reply_comment", False)
    is_reply = parent_id is not None or raw_reply_flag is True or raw_reply_flag == 1
    collected = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    stable_parts = {
        "video_id": str(video_id), "user_id": user_id, "username": username,
        "text": comment_text, "created_at_epoch": created_epoch,
        "parent_comment_id": parent_id,
    }
    dedupe_key = "cid:" + comment_id if comment_id is not None else None
    if comment_id is None and created_epoch is not None:
        fingerprint = json.dumps(stable_parts, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        dedupe_key = "fingerprint:" + hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()
    return NormalizedComment(
        platform="tiktok", video_id=str(video_id), comment_id=comment_id,
        user_id=user_id, username=username, nickname=nickname, text=comment_text,
        likes=likes, reply_count=replies, created_at_epoch=created_epoch,
        created_at_utc=created_utc, parent_comment_id=parent_id,
        is_reply=is_reply, collected_at_utc=collected, dedupe_key=dedupe_key,
        source_payload=safe_source_payload(record),
    )
