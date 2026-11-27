"""Configuration management for the TikTok Comment Scraper.

Provides file-based configuration with validation, default values,
and secret redaction. All configuration is loaded from environment
variables with defaults, and validated on access.

DO NOT modify tiktokscraper.py or tiktokscraper.py.backup-*. These are
the original baseline files.
"""
import os
from pathlib import Path

# Base directory
BASE_DIR = Path(__file__).parent

# Load .env file from the project root if it exists
ENV_PATH = BASE_DIR / ".env"
if ENV_PATH.exists():
    with open(ENV_PATH) as env_file:
        for env_line in env_file:
            if '=' in env_line:
                env_key, env_value = env_line.strip().split('=', 1)
                os.environ[env_key] = env_value

# Proxy configuration
PROXY_HOST: str = os.environ.get("PROXY_HOST", "")
PROXY_PORT: str = os.environ.get("PROXY_PORT", "")
PROXY_USER: str = os.environ.get("PROXY_USER", "")
PROXY_PASS: str = os.environ.get("PROXY_PASS", "")

# Derived proxy URL — constructed only when needed, never logged directly
def get_proxy_url() -> str:
    """Construct the proxy URL from component parts when needed."""
    if PROXY_HOST and PROXY_PORT:
        user_part = f"{PROXY_USER}@" if PROXY_USER else ""
        return f"http://{user_part}{PROXY_HOST}:{PROXY_PORT}"
    return ""

PROXY_URL = property(get_proxy_url)

# User agent string
USER_AGENT: str = os.environ.get(
    "USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
)

# Pagination configuration
COMMENTS_PER_BATCH: int = int(os.environ.get("COMMENTS_PER_BATCH", "50"))
BATCHES_BEFORE_TOKEN_REFRESH: int = int(
    os.environ.get("BATCHES_BEFORE_TOKEN_REFRESH", "15")
)
MAX_COMMENTS: int = int(os.environ.get("MAX_COMMENTS", "10000"))
SAVE_EVERY_N_BATCHES: int = int(os.environ.get("SAVE_EVERY_N_BATCHES", "5"))

# Runtime configuration
MAX_WORKERS: int = int(os.environ.get("MAX_WORKERS", "3"))

# Output configuration
OUTPUT_DIR: Path = BASE_DIR / "results"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Cache configuration — transitioning from JSON cache to SQLite
CACHE_DIR: Path = BASE_DIR / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Database configuration
DATABASE_PATH: Path = BASE_DIR / "scraper.db"

# Error reporting configuration
ERROR_REPORT_DIR: Path = BASE_DIR / "error_reports"
ERROR_REPORT_DIR.mkdir(parents=True, exist_ok=True)

# Testing configuration
TEST_MODE: bool = os.environ.get("TEST_MODE", "0") == "1"

# Video configuration
TEST_VIDEOS: list = [
    (os.environ.get("TEST_VIDEO_ID", "7676572010415230221"),
     os.environ.get("TEST_VIDEO_AUTHOR", "erickie20"))
]

# ---------------------------------------------------------------------------
# Configuration validation
# ---------------------------------------------------------------------------


def validate_config() -> list[str]:
    """Validate configuration values and return a list of warnings."""
    warnings: list[str] = []

    # Validate proxy configuration
    if (PROXY_HOST and not PROXY_PORT) or (PROXY_PORT and not PROXY_HOST):
        warnings.append(
            "PROXY_HOST and PROXY_PORT must both be set or both be unset"
        )

    # Validate comment counts
    if COMMENTS_PER_BATCH < 1:
        warnings.append("COMMENTS_PER_BATCH must be >= 1, using default 50")
        COMMENTS_PER_BATCH = 50

    if BATCHES_BEFORE_TOKEN_REFRESH < 1:
        warnings.append("BATCHES_BEFORE_TOKEN_REFRESH must be >= 1, using default 15")
        BATCHES_BEFORE_TOKEN_REFRESH = 15

    if MAX_COMMENTS < 1:
        warnings.append("MAX_COMMENTS must be >= 1, using default 10000")
        MAX_COMMENTS = 10000

    if MAX_WORKERS < 1:
        warnings.append("MAX_WORKERS must be >= 1, using default 3")
        MAX_WORKERS = 3

    if SAVE_EVERY_N_BATCHES < 1:
        warnings.append("SAVE_EVERY_N_BATCHES must be >= 1, using default 5")
        SAVE_EVERY_N_BATCHES = 5

    # Validate output directories are writable
    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        warnings.append(f"Output directory not writable: {error}")

    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        warnings.append(f"Cache directory not writable: {error}")

    try:
        DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        warnings.append(f"Database directory not writable: {error}")

    return warnings


def print_config_summary() -> None:
    """Print a sanitized configuration summary (no secrets)."""
    import logging
    logger = logging.getLogger(__name__)
    warnings = validate_config()

    logger.info("Configuration summary")
    logger.info(f"  Proxy: {'enabled' if PROXY_HOST and PROXY_PORT else 'disabled'}")
    logger.info(f"  User agent: {USER_AGENT}")
    logger.info(f"  Comments per batch: {COMMENTS_PER_BATCH}")
    logger.info(f"  Batches before token refresh: {BATCHES_BEFORE_TOKEN_REFRESH}")
    logger.info(f"  Max comments: {MAX_COMMENTS}")
    logger.info(f"  Max workers: {MAX_WORKERS}")
    logger.info(f"  Save every N batches: {SAVE_EVERY_N_BATCHES}")
    logger.info(f"  Test mode: {TEST_MODE}")
    logger.info(f"  Output directory: {OUTPUT_DIR}")
    logger.info(f"  Cache directory: {CACHE_DIR}")

    if warnings:
        logger.warning("Configuration warnings:")
        for warning in warnings:
            logger.warning(f"  - {warning}")


# ---------------------------------------------------------------------------
# Secret redaction — ensure no secrets appear in logs
# ---------------------------------------------------------------------------

def redact_proxy_url(url: str) -> str:
    """Redact credentials from a proxy URL for logging."""
    if not url:
        return "disabled"
    import re
    redacted = re.sub(r'://[^:@]+:[^@]*@', '://***@', url)
    return redacted


def is_test_mode() -> bool:
    """Return whether the application is running in test mode."""
    return TEST_MODE