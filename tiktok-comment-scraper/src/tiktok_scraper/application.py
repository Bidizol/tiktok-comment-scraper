"""Application lifecycle and command-line entry point."""

from __future__ import annotations

import argparse
import json
import sys
import uuid

from .config import ConfigurationError, load_settings
from .observability import DiagnosticRecorder, configure_logging
from .startup import (
    collect_startup_diagnostics,
    diagnostics_to_dict,
    startup_is_healthy,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tiktok-scraper",
        description="Production-oriented TikTok scraper foundation",
    )
    parser.add_argument(
        "--diagnostics",
        action="store_true",
        help="Display sanitized startup diagnostics and exit",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        settings = load_settings()
    except (ConfigurationError, OSError, ValueError) as exc:
        print(f"Startup configuration failed: {exc}", file=sys.stderr)
        return 2

    logger = configure_logging(settings.log_dir, settings.log_level)
    recorder = DiagnosticRecorder(settings.report_dir)
    run_id = str(uuid.uuid4())
    startup_context = {
        "run_id": run_id,
        "component": "application",
        "operation": "startup",
    }
    logger.info("Application startup", extra=startup_context)

    checks = collect_startup_diagnostics(settings)
    for check in checks:
        log_method = logger.error if check.status == "FAILED" else (
            logger.warning if check.status == "WARNING" else logger.info
        )
        log_method(
            "Startup diagnostic: %s — %s",
            check.name,
            check.detail,
            extra={
                "run_id": run_id,
                "component": "startup",
                "operation": "diagnostics",
            },
        )

    if args.diagnostics:
        print(json.dumps(diagnostics_to_dict(checks), indent=2))
        return 0 if startup_is_healthy(checks) else 1

    if not startup_is_healthy(checks):
        recorder.record_event(
            "startup_blocked",
            failed_checks=[check.name for check in checks if check.status == "FAILED"],
        )
        logger.critical(
            "Startup blocked by failed prerequisite",
            extra=startup_context,
        )
        return 1

    logger.info(
        "Foundation initialized; scraping engine integration is scheduled for a later phase",
        extra=startup_context,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
