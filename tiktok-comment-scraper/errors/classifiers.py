"""Error classifier for the TikTok Comment Scraper.

Provides functionality to classify exceptions and raw API responses
into the centralized error taxonomy (Section 5.1).

The classifier determines the appropriate error code, severity, and
recovery action based on the type of failure encountered.
"""

from typing import Any, Optional

from .model import ScraperError, ErrorSeverity, ErrorOutcome