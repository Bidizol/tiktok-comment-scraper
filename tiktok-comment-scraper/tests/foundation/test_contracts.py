"""Contract-level examples for the isolated application foundation."""

from datetime import datetime, timezone

from tiktok_scraper.config.settings import ConfigurationError, Settings
from tiktok_scraper.reliability.errors import (
    ErrorCode,
    ErrorOutcome,
    ErrorRecord,
    Severity,
    redact_secrets,
)


def test_error_record_uses_canonical_enums_and_redacts_credentials() -> None:
    record = ErrorRecord(
        error_code=ErrorCode.NETWORK_ERROR,
        severity=Severity.ERROR,
        component="transport",
        operation="request",
        explanation="Request failed with msToken=secret-value",
        run_id="run-123",
        timestamp_utc=datetime.now(timezone.utc).isoformat(),
        final_outcome=ErrorOutcome.FAILED_RESUMABLE,
    )

    assert record.error_code is ErrorCode.NETWORK_ERROR
    assert "secret-value" not in record.explanation
    assert record.to_dict()["final_outcome"] == "FAILED_RESUMABLE"


def test_redactor_removes_proxy_credentials() -> None:
    assert "user:password" not in redact_secrets(
        "proxy=http://user:password@proxy.example:8080"
    )


def test_required_proxy_rejects_missing_endpoint(tmp_path) -> None:
    settings = Settings(
        project_dir=tmp_path,
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
        report_dir=tmp_path / "reports",
        database_path=tmp_path / "data" / "scraper.sqlite3",
        http_backend="urllib",
        proxy_mode="required",
        proxy_url=None,
        max_workers=1,
        comments_per_page=50,
        max_comments_per_video=1000,
        max_pages_per_video=100,
        request_timeout_seconds=10,
        minimum_free_bytes=0,
        log_level="INFO",
    )

    try:
        settings.validate()
    except ConfigurationError:
        return
    raise AssertionError("required proxy without an endpoint must be rejected")
