"""Configuration management for the TikTok Comment Scraper.

Provides file-based configuration with validation, default values,
and secret redaction. All configuration is loaded from environment
variables with defaults, and validated on access.

DO NOT modify tiktokscraper.py or tiktokscraper.py.backup-*. These are
the original baseline files.
"""
import os
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

# Base directory
BASE_DIR = Path(__file__).parent

# Load .env file from the project root if it exists
ENV_PATH = BASE_DIR / ".env"
if ENV_PATH.exists():
    with open(ENV_PATH, encoding="utf-8") as env_file:
        for env_line in env_file:
            line = env_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            env_key, env_value = line.split("=", 1)
            env_key = env_key.strip()
            if env_key.startswith("export "):
                env_key = env_key[7:].strip()
            if not env_key:
                continue
            env_value = env_value.strip()
            if len(env_value) >= 2 and env_value[0] == env_value[-1] and env_value[0] in {"'", '"'}:
                env_value = env_value[1:-1]
            else:
                env_value = env_value.split(" #", 1)[0].strip()
            # Process environment always takes precedence over the .env file.
            if env_key not in os.environ:
                os.environ[env_key] = env_value

# Proxy configuration
PROXY_HOST: str = os.environ.get("PROXY_HOST", "")
PROXY_PORT: str = os.environ.get("PROXY_PORT", "")
PROXY_USER: str = os.environ.get("PROXY_USER", "")
PROXY_PASS: str = os.environ.get("PROXY_PASS", "")
PROXY_URL: str = os.environ.get("PROXY_URL", "").strip()
PROXY_MODE: str = os.environ.get("PROXY_MODE", "optional").strip().lower()
if PROXY_URL and not (PROXY_HOST and PROXY_PORT):
    _proxy_parts = urlsplit(PROXY_URL)
    if (
        _proxy_parts.scheme.lower() != "http"
        or not _proxy_parts.hostname
        or _proxy_parts.path not in {"", "/"}
        or _proxy_parts.query
        or _proxy_parts.fragment
    ):
        raise ValueError("PROXY_URL must be a valid HTTP proxy endpoint without a path or query")
    try:
        _proxy_port = _proxy_parts.port
    except ValueError as exc:
        raise ValueError("PROXY_URL contains an invalid port") from exc
    if _proxy_port is None:
        raise ValueError("PROXY_URL must include an explicit port")
    if bool(_proxy_parts.username) != bool(_proxy_parts.password):
        raise ValueError("PROXY_URL must include both username and password, or neither")
    PROXY_HOST = _proxy_parts.hostname
    PROXY_PORT = str(_proxy_port)
    PROXY_USER = unquote(_proxy_parts.username or "")
    PROXY_PASS = unquote(_proxy_parts.password or "")
PROXY_REQUIRED: bool = (
    PROXY_MODE == "required"
    or os.environ.get("PROXY_REQUIRED", "0").strip().lower() in {"1", "true", "yes"}
)
HTTP_CONNECT_TIMEOUT_SECONDS: float = float(os.environ.get("HTTP_CONNECT_TIMEOUT_SECONDS", "15"))
HTTP_READ_TIMEOUT_SECONDS: float = float(os.environ.get("HTTP_READ_TIMEOUT_SECONDS", "20"))
HTTP_TOTAL_TIMEOUT_SECONDS: float = float(os.environ.get("HTTP_TOTAL_TIMEOUT_SECONDS", "30"))
HTTP_MAX_RESPONSE_BYTES: int = int(os.environ.get("HTTP_MAX_RESPONSE_BYTES", str(5 * 1024 * 1024)))
HTTP_MAX_RETRIES: int = int(os.environ.get("HTTP_MAX_RETRIES", "3"))

# Derived proxy URL — constructed only when needed, never logged directly
def get_proxy_url() -> str:
    """Construct the proxy URL from component parts when needed."""
    if PROXY_HOST and PROXY_PORT:
        user_part = ""
        if PROXY_USER:
            credentials = quote(PROXY_USER, safe="")
            if PROXY_PASS:
                credentials += ":" + quote(PROXY_PASS, safe="")
            user_part = credentials + "@"
        return f"http://{user_part}{PROXY_HOST}:{PROXY_PORT}"
    return ""

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
    """Validate loaded configuration and return actionable warnings.

    This function deliberately does not assign to module-level settings:
    validation must not create local-variable shadowing or pretend that an
    invalid setting has been replaced when callers still read the original.
    """
    warnings: list[str] = []

    if (PROXY_HOST and not PROXY_PORT) or (PROXY_PORT and not PROXY_HOST):
        warnings.append(
            "PROXY_HOST and PROXY_PORT must both be set or both be unset"
        )

    for name, value in (
        ("HTTP_CONNECT_TIMEOUT_SECONDS", HTTP_CONNECT_TIMEOUT_SECONDS),
        ("HTTP_READ_TIMEOUT_SECONDS", HTTP_READ_TIMEOUT_SECONDS),
        ("HTTP_TOTAL_TIMEOUT_SECONDS", HTTP_TOTAL_TIMEOUT_SECONDS),
    ):
        if not __import__("math").isfinite(value) or value <= 0:
            warnings.append(f"{name} must be a finite positive number")
    if HTTP_MAX_RESPONSE_BYTES < 1024 or HTTP_MAX_RESPONSE_BYTES > 50 * 1024 * 1024:
        warnings.append("HTTP_MAX_RESPONSE_BYTES must be between 1024 and 52428800")
    if HTTP_MAX_RETRIES < 1 or HTTP_MAX_RETRIES > 10:
        warnings.append("HTTP_MAX_RETRIES must be between 1 and 10")
    if PROXY_MODE not in {"disabled", "optional", "required"}:
        warnings.append("PROXY_MODE must be disabled, optional or required")
    if PROXY_REQUIRED and not (PROXY_HOST and PROXY_PORT):
        warnings.append("PROXY_REQUIRED is enabled but no proxy endpoint is configured")

    if COMMENTS_PER_BATCH < 1:
        warnings.append("COMMENTS_PER_BATCH must be >= 1")
    if BATCHES_BEFORE_TOKEN_REFRESH < 1:
        warnings.append("BATCHES_BEFORE_TOKEN_REFRESH must be >= 1")
    if MAX_COMMENTS < 1:
        warnings.append("MAX_COMMENTS must be >= 1")
    if MAX_WORKERS < 1:
        warnings.append("MAX_WORKERS must be >= 1")
    if SAVE_EVERY_N_BATCHES < 1:
        warnings.append("SAVE_EVERY_N_BATCHES must be >= 1")

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