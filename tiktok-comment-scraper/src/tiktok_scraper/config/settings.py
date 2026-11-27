"""Typed, side-effect-free application configuration."""

from __future__ import annotations

import math
import os
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit


class ConfigurationError(ValueError):
    """Raised when application configuration is invalid."""


def _read_env_file(path: Path) -> dict[str, str]:
    """Read a deliberately small .env format without overriding process env."""
    values: dict[str, str] = {}
    if not path.is_file():
        return values

    for line_number, raw_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            raise ConfigurationError(
                f"Invalid environment assignment in {path.name}:{line_number}"
            )
        key, raw_value = line.split("=", 1)
        key = key.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            raise ConfigurationError(
                f"Invalid environment key in {path.name}:{line_number}"
            )
        value = raw_value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        else:
            value = value.split(" #", 1)[0].strip()
        values[key] = value
    return values


def _integer(env: dict[str, str], key: str, default: int, minimum: int) -> int:
    raw = env.get(key, str(default))
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{key} must be an integer") from exc
    if value < minimum:
        raise ConfigurationError(f"{key} must be at least {minimum}")
    return value


@dataclass(frozen=True, slots=True)
class Settings:
    """Immutable runtime settings; loading them performs no filesystem writes."""

    project_dir: Path
    data_dir: Path
    log_dir: Path
    report_dir: Path
    database_path: Path
    http_backend: str
    proxy_mode: str
    proxy_url: str | None
    max_workers: int
    comments_per_page: int
    max_comments_per_video: int
    max_pages_per_video: int
    request_timeout_seconds: float
    connect_timeout_seconds: float
    read_timeout_seconds: float
    max_response_bytes: int
    max_http_retries: int
    minimum_free_bytes: int
    log_level: str

    def validate(self) -> None:
        if self.http_backend != "urllib":
            raise ConfigurationError("Only the urllib HTTP backend is currently supported")
        if self.proxy_mode not in {"disabled", "optional", "required"}:
            raise ConfigurationError(
                "PROXY_MODE must be 'disabled', 'optional' or 'required'"
            )
        if self.proxy_mode == "required" and not self.proxy_url:
            raise ConfigurationError(
                "PROXY_MODE is required but no proxy endpoint is configured"
            )
        if self.proxy_mode == "disabled" and self.proxy_url:
            raise ConfigurationError(
                "A proxy endpoint is configured while PROXY_MODE is disabled"
            )
        if self.proxy_url:
            parsed = urlsplit(self.proxy_url)
            if parsed.scheme != "http" or not parsed.hostname:
                raise ConfigurationError(
                    "PROXY_URL must be an absolute HTTP proxy URL"
                )
            if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
                raise ConfigurationError("PROXY_URL must not contain a path, query or fragment")
            try:
                if parsed.port is None or not 1 <= parsed.port <= 65535:
                    raise ValueError
            except ValueError as exc:
                raise ConfigurationError("PROXY_URL must include a valid explicit port") from exc
            if bool(parsed.username) != bool(parsed.password):
                raise ConfigurationError("PROXY_URL must provide both username and password, or neither")
        if self.log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ConfigurationError("LOG_LEVEL is not a supported severity")
        for name, value in (
            ("REQUEST_TIMEOUT_SECONDS", self.request_timeout_seconds),
            ("HTTP_CONNECT_TIMEOUT_SECONDS", self.connect_timeout_seconds),
            ("HTTP_READ_TIMEOUT_SECONDS", self.read_timeout_seconds),
        ):
            if not math.isfinite(value) or value <= 0 or value > 600:
                raise ConfigurationError(f"{name} must be finite and between 0 and 600 seconds")
        if self.max_http_retries > 10:
            raise ConfigurationError("HTTP_MAX_RETRIES must not exceed 10")
        if self.max_response_bytes > 50 * 1024 * 1024:
            raise ConfigurationError("HTTP_MAX_RESPONSE_BYTES must not exceed 52428800")

    @classmethod
    def from_environment(
        cls,
        project_dir: Path | None = None,
        environ: dict[str, str] | None = None,
    ) -> "Settings":
        root = (project_dir or Path(__file__).resolve().parents[3]).resolve()
        file_values = _read_env_file(root / ".env")
        merged = {**file_values, **dict(os.environ if environ is None else environ)}

        def path_value(key: str, default: Path) -> Path:
            candidate = Path(merged.get(key, str(default))).expanduser()
            return (root / candidate).resolve() if not candidate.is_absolute() else candidate.resolve()

        data_dir = path_value("DATA_DIR", root / "data")
        proxy_url = merged.get("PROXY_URL") or None
        settings = cls(
            project_dir=root,
            data_dir=data_dir,
            log_dir=path_value("LOG_DIR", data_dir / "logs"),
            report_dir=path_value("REPORT_DIR", data_dir / "reports"),
            database_path=path_value("DATABASE_PATH", data_dir / "scraper.sqlite3"),
            http_backend=merged.get("HTTP_BACKEND", "urllib").strip().lower(),
            proxy_mode=merged.get("PROXY_MODE", "disabled").strip().lower(),
            proxy_url=proxy_url,
            max_workers=_integer(merged, "MAX_WORKERS", 3, 1),
            comments_per_page=_integer(merged, "COMMENTS_PER_PAGE", 50, 1),
            max_comments_per_video=_integer(
                merged, "MAX_COMMENTS_PER_VIDEO", 10000, 1
            ),
            max_pages_per_video=_integer(merged, "MAX_PAGES_PER_VIDEO", 250, 1),
            request_timeout_seconds=float(merged.get("REQUEST_TIMEOUT_SECONDS", "30")),
            connect_timeout_seconds=float(merged.get("HTTP_CONNECT_TIMEOUT_SECONDS", "15")),
            read_timeout_seconds=float(merged.get("HTTP_READ_TIMEOUT_SECONDS", "20")),
            max_response_bytes=_integer(merged, "HTTP_MAX_RESPONSE_BYTES", 5242880, 1024),
            max_http_retries=_integer(merged, "HTTP_MAX_RETRIES", 3, 1),
            minimum_free_bytes=_integer(
                merged, "MINIMUM_FREE_BYTES", 100 * 1024 * 1024, 0
            ),
            log_level=merged.get("LOG_LEVEL", "INFO").strip().upper(),
        )
        settings.validate()
        return settings


def load_settings(
    project_dir: Path | None = None,
    environ: dict[str, str] | None = None,
) -> Settings:
    """Load and validate application settings."""
    return Settings.from_environment(project_dir=project_dir, environ=environ)
