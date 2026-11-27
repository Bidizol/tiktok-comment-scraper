"""Platform-specific adapters for the generic collection engine."""
from .base import PlatformAdapter
from .tiktok import TikTokAdapter

__all__ = ["PlatformAdapter", "TikTokAdapter"]
