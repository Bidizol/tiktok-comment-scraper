"""Explicit startup diagnostics for the new application foundation."""

from __future__ import annotations

import importlib.util
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.parse import urlsplit

from .config.settings import Settings


@dataclass(frozen=True, slots=True)
class DiagnosticCheck:
    name: str
    status: str
    detail: str


def _directory_check(name: str, path: Path) -> DiagnosticCheck:
    try:
        path.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            prefix=".startup-", dir=path, delete=True
        ):
            pass
    except OSError:
        return DiagnosticCheck(name, "FAILED", "Directory is not writable")
    return DiagnosticCheck(name, "OK", "Directory is writable")


def collect_startup_diagnostics(settings: Settings) -> list[DiagnosticCheck]:
    """Collect sanitized environment and resource information."""
    checks: list[DiagnosticCheck] = []

    version_ok = sys.version_info >= (3, 10)
    checks.append(
        DiagnosticCheck(
            "python_version",
            "OK" if version_ok else "FAILED",
            f"Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        )
    )

    for module_name in ("sqlite3", "logging", "json", "urllib", "concurrent.futures"):
        available = importlib.util.find_spec(module_name) is not None
        checks.append(
            DiagnosticCheck(
                f"dependency:{module_name}",
                "OK" if available else "FAILED",
                "Available" if available else "Unavailable",
            )
        )

    checks.extend(
        (
            _directory_check("data_directory", settings.data_dir),
            _directory_check("log_directory", settings.log_dir),
            _directory_check("report_directory", settings.report_dir),
            _directory_check("database_directory", settings.database_path.parent),
        )
    )

    try:
        free_bytes = shutil.disk_usage(settings.data_dir).free
        enough_space = free_bytes >= settings.minimum_free_bytes
        checks.append(
            DiagnosticCheck(
                "disk_space",
                "OK" if enough_space else "WARNING",
                f"{free_bytes} bytes free",
            )
        )
    except OSError:
        checks.append(
            DiagnosticCheck("disk_space", "WARNING", "Disk capacity unavailable")
        )

    checks.append(
        DiagnosticCheck(
            "sqlite_runtime",
            "OK" if sqlite3.sqlite_version_info >= (3, 24, 0) else "WARNING",
            f"SQLite {sqlite3.sqlite_version}",
        )
    )

    if settings.database_path.exists():
        try:
            connection = sqlite3.connect(
                f"file:{settings.database_path.as_posix()}?mode=rw",
                uri=True,
                timeout=1.0,
            )
            try:
                connection.execute("PRAGMA schema_version").fetchone()
            finally:
                connection.close()
            db_status, db_detail = "OK", "Existing database is accessible"
        except sqlite3.Error:
            db_status, db_detail = "FAILED", "Existing database is inaccessible"
    else:
        db_status, db_detail = "OK", "Database will be initialized by the storage phase"
    checks.append(DiagnosticCheck("database", db_status, db_detail))

    if settings.http_backend == "curl":
        curl_path = shutil.which("curl")
        if curl_path is None:
            checks.append(
                DiagnosticCheck("curl", "FAILED", "curl executable was not found")
            )
        else:
            try:
                completed = subprocess.run(
                    [curl_path, "--version"],
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=3,
                )
                version_line = completed.stdout.splitlines()[0].strip()
                checks.append(DiagnosticCheck("curl", "OK", version_line[:160]))
            except (OSError, subprocess.SubprocessError, IndexError):
                checks.append(
                    DiagnosticCheck("curl", "FAILED", "curl version information is unavailable")
                )
    else:
        checks.append(
            DiagnosticCheck("http_backend", "OK", "Python urllib backend selected")
        )

    if settings.proxy_mode == "required":
        parsed = urlsplit(settings.proxy_url or "")
        valid = parsed.scheme in {"http", "https"} and bool(parsed.hostname)
        checks.append(
            DiagnosticCheck(
                "proxy_configuration",
                "OK" if valid else "FAILED",
                "Mandatory proxy endpoint is configured"
                if valid
                else "Mandatory proxy endpoint is invalid",
            )
        )
    elif settings.proxy_mode == "optional":
        checks.append(
            DiagnosticCheck(
                "proxy_configuration",
                "OK",
                "Proxy is optional; direct transport is permitted by configuration",
            )
        )
    else:
        checks.append(
            DiagnosticCheck(
                "proxy_configuration",
                "OK",
                "Proxy use is disabled by configuration",
            )
        )

    return checks


def diagnostics_to_dict(checks: list[DiagnosticCheck]) -> list[dict[str, str]]:
    return [asdict(check) for check in checks]


def startup_is_healthy(checks: list[DiagnosticCheck]) -> bool:
    return not any(check.status == "FAILED" for check in checks)
