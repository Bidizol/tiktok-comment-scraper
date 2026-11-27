"""Contracts shared by platform-specific collection adapters."""
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict


class PlatformAdapter(ABC):
    """Platform boundary consumed by the generic pagination engine."""
    platform_id: str

    @abstractmethod
    def validate_resource_id(self, resource_id: str) -> None:
        """Raise ValueError when an identifier is invalid for this platform."""

    @abstractmethod
    def fetch_page(self, *, cursor: int, token: str) -> Dict[str, Any]:
        """Return a canonical page mapping: comments, has_more, next_cursor."""

    @abstractmethod
    def is_comment_record(self, record: Any) -> bool:
        """Recognize a source-platform comment record."""

    @abstractmethod
    def has_minimum_content(self, record: Dict[str, Any]) -> bool:
        """Determine whether a source record has enough content to retain."""

    @abstractmethod
    def has_comment_id(self, record: Dict[str, Any]) -> bool:
        """Determine whether a source record has a usable stable identifier."""

    @abstractmethod
    def normalization_anomalies(self, source_record: Dict[str, Any],
                                normalized_record: Dict[str, Any]) -> Dict[str, int]:
        """Return platform-specific normalization anomaly counters."""

    @abstractmethod
    def normalize_comment(self, video_id: str, record: Dict[str, Any]) -> Dict[str, Any]:
        """Translate a source record into the shared normalized comment schema."""
