# TikTok Comment Scraper

Python 3.10+ scraper with bounded pagination, resumable per-video checkpoints, normalized comment records, and run-scoped JSON/CSV exports.

## Current output

Each invocation of the versioned scraper entry point creates a unique directory:

```text
results/
  run_<run_id>/
    comments.json
    comments.csv
    summary.json
```

- `comments.json` contains run metadata and normalized comments, including a sanitized `source_payload` for source-field traceability.
- `comments.csv` contains the stable normalized columns and protects spreadsheet-imported text fields against formula injection.
- `summary.json` contains per-video outcomes and aggregate counts for records received, accepted comments, duplicates, missing IDs, malformed records, and successful pages.
- Each run writes to a new directory. Existing run output is not overwritten.
- Files are written through same-directory temporary files and atomic replacement. The run summary is written last and acts as the output-set completion marker.

## Normalized comment fields

The stable record includes:

| Field | Meaning |
|---|---|
| `platform`, `video_id` | Source platform and video identity |
| `comment_id` | Stable source comment ID, or null when absent |
| `user_id`, `username`, `nickname` | Available author identity fields |
| `text` | Comment text, preserved as Unicode |
| `likes`, `reply_count` | Non-negative integer counts, otherwise null |
| `created_at_epoch`, `created_at_utc` | UTC timestamp fields, or null when unavailable/invalid |
| `parent_comment_id`, `is_reply` | Reply relationship |
| `collected_at_utc` | Collection timestamp |
| `dedupe_key` | Source ID key or stable missing-ID fingerprint |
| `source_payload` | Sanitized source fields retained for traceability |

Missing comment IDs do not cause a comment to be discarded. When a usable timestamp exists, a deterministic fingerprint is used for deduplication. This is a heuristic: two genuinely distinct comments with identical author, text, timestamp and parent may share a fingerprint. Records without a usable timestamp are retained without fingerprint-based deduplication.

Malformed source records are retained in restricted JSONL files under `cache/quarantine/`; they are not silently converted into empty comments. Each video's quarantine file is capped at 50 MiB, and the in-memory malformed-record sample is capped at 1,000 entries per video.

## Configuration

The versioned scraper reads settings from environment variables and the project `.env` file. Process environment values take precedence. Common settings include:

- `TEST_VIDEO_ID` and `TEST_VIDEO_AUTHOR`
- `MAX_WORKERS` (1–16 workers; at most twice the worker count is queued)
- `MAX_COMMENTS` (per video)
- `MAX_RUN_COMMENTS` (run-level new-collection budget; defaults to 100,000; existing completed checkpoints are preserved even if they exceed the remaining budget)
- `PROXY_MODE`, proxy endpoint and credentials
- HTTP connection, read and total timeouts

Start from `.env.example`. Do not commit real credentials or paste them into issue reports.

## Run

From this directory:

```bash
python tiktokscraper-1.py
```

The entry point reads the configured video list, processes videos independently, and writes the run-scoped JSON, CSV and summary files. A run accepts at most 1,000 jobs and permits only one active scraper process per project cache. Ctrl+C requests a graceful stop: no new jobs are submitted, queued jobs are cancelled where possible, and active workers stop between pagination pages while preserving the last durable checkpoint.

## Checkpoints and recovery

Per-video checkpoints are stored under `cache/`. They contain a versioned schema, a SHA-256 integrity checksum, pagination position and normalized comment records. Corrupt or mismatched checkpoint files are preserved in quarantine. The immediately previous valid checkpoint is retained for recovery.

## Scope and limitations

TikTok's internal API is not a stable public contract. Response schema or access changes may require adapter updates. Missing timestamps remain null; the scraper does not invent dates. Missing-ID fingerprints are heuristic and cannot guarantee semantic identity.


## Monitoring, diagnostics and alerts

Each run creates two append-only JSONL files under `error_reports/`:

- `run_<run_id>.events.jsonl` records run lifecycle, queued/active/completed video work, outcomes, pagination totals, retry-budget summaries, resource snapshots and output artifact locations.
- `run_<run_id>.alerts.jsonl` records actionable warnings and errors, including incomplete videos, malformed-record quarantine, checkpoint persistence issues, recovery-budget exhaustion and unexpected worker failures.

Event and alert files rotate independently at the configured size. Defaults are 10 MiB per file with five retained backups. Records are sanitized, bounded in size and written with restricted file permissions. The run summary also embeds a monitoring snapshot with worker concurrency, event/alert counts and available disk-space metrics.

Monitoring is local-file based; it does not send email, SMS or external notifications. Inspect the alert JSONL file for the run's actionable conditions.

Optional settings:
- `DIAGNOSTICS_MAX_BYTES` (64 KiB–100 MiB; defaults to 10 MiB)
- `DIAGNOSTICS_BACKUP_COUNT` (1–20 retained backups; defaults to 5)
- `MINIMUM_FREE_BYTES` (minimum free-disk threshold; defaults to 100 MiB and raises a run alert below the threshold)
