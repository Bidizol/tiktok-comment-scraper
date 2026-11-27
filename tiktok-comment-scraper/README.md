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


## Platform adapter boundary

The pagination engine delegates source-specific work through the `PlatformAdapter` contract in `platform_adapters/base.py`. The contract covers resource-ID rules, page retrieval, source-record recognition, minimum-content rules, comment identity, normalization and normalization-anomaly reporting. The current implementation is `TikTokAdapter` in `platform_adapters/tiktok.py`; it owns the TikTok endpoint, query parameters, response envelope, source field names and normalized-record conversion.

The active command and checkpoint identity remain TikTok-specific. No other platform adapter is currently included or advertised as supported. A future platform integration must implement the adapter contract and separately define its authentication, pagination semantics, durable identity and operational limits before it can be enabled.


## Phase 10 — deployment and long-run operation

### Runtime requirements

- Python 3.10 or newer
- No third-party Python packages are required by the active scraper entry point
- No external `curl` executable is required by the active scraper entry point
- Outbound HTTPS access to TikTok is required
- Persistent writable storage is required for checkpoints, exports and diagnostics

The supported scraper command is `python tiktokscraper-1.py` from this directory. The separate `src/tiktok_scraper` foundation package currently provides startup diagnostics; its `tiktok-scraper` command is not the collection command.

### Native installation — macOS, Linux or Windows

1. Install Python 3.10 or newer.
2. Open this project directory.
3. Copy `.env.example` to `.env`.
4. Set both `TEST_VIDEO_ID` and `TEST_VIDEO_AUTHOR` in `.env`. The scraper does not select a built-in example video.
5. Run the scraper.

macOS/Linux:

```bash
if [ ! -f .env ]; then cp .env.example .env; fi
python3 -m venv .venv
source .venv/bin/activate
python tiktokscraper-1.py
```

Windows PowerShell:

```powershell
py -3.10 -m venv .venv
if (!(Test-Path .env)) { Copy-Item .env.example .env }
.\.venv\Scripts\Activate.ps1
python tiktokscraper-1.py
```

The virtual environment is optional because the active scraper currently has no third-party package dependencies. The `.env` file is local configuration and must not be committed. If either target-video value is missing, the command starts no collection run.

### Persistent storage locations

The active scraper accepts these directory overrides. Relative paths resolve from the project directory; absolute paths are supported for external disks and container mounts.

| Variable | Default | Contents |
|---|---|---|
| `CACHE_DIR` | `cache/` | Checkpoints, lock files and quarantined cache data |
| `OUTPUT_DIR` | `results/` | Immutable per-run JSON, CSV and summary exports |
| `ERROR_REPORT_DIR` | `error_reports/` | Run events and alert JSONL files |
| `DATABASE_PATH` | `scraper.db` | Reserved database path; the active scraper currently uses file checkpoints |

Keep all three active data directories on persistent storage. Do not place the cache on an ephemeral container filesystem. Avoid network filesystems for the checkpoint directory unless their file-lock and atomic-rename semantics are known to be reliable.

### Docker deployment

Docker Compose is provided at the repository root. Create the local configuration file first:

```bash
if [ ! -f tiktok-comment-scraper/.env ]; then cp tiktok-comment-scraper/.env.example tiktok-comment-scraper/.env; fi
```

Edit the file and set the target video ID and author. Then, from the repository root:

```bash
docker compose build
docker compose run --rm scraper
```

The container runs as an unprivileged user. A named persistent volume, `tiktok-scraper-data`, stores checkpoints, exports and diagnostics across container invocations. The service deliberately has no automatic restart policy: a scheduler or operator controls when a new collection run begins, preventing an unexpected failure from creating an endless restart loop.

The attached command streams the scraper's console output directly. Press Ctrl+C to request a cooperative stop. For a detached run:

```bash
docker compose run -d --name tiktok-scraper-run scraper
docker stop --time 90 tiktok-scraper-run
docker rm tiktok-scraper-run
```

The application handles SIGINT and SIGTERM, stops scheduling new work and lets active workers stop between pages so the last durable checkpoint can be retained. The container stop grace period is 90 seconds; a forced termination after that interval may interrupt a write in progress, although checkpoint replacement is atomic.

### Backup and restore

Stop the container before taking a consistent backup. The Docker named volume contains all persistent application data:

```bash
mkdir -p backups
docker run --rm \
  -v tiktok-scraper-data:/data:ro \
  -v "$PWD/backups:/backup" \
  alpine:3.20 \
  sh -c 'tar -czf /backup/tiktok-scraper-$(date -u +%Y%m%dT%H%M%SZ).tar.gz -C /data .'
```

To restore, stop all scraper containers using the volume, preserve a copy of the current volume first, and extract the selected archive into the same volume:

```bash
docker run --rm \
  -v tiktok-scraper-data:/data \
  -v "$PWD/backups:/backup:ro" \
  alpine:3.20 \
  sh -c 'tar -xzf /backup/REPLACE_WITH_BACKUP_FILENAME.tar.gz -C /data'
```

For native deployments, stop the process and back up the configured `CACHE_DIR`, `OUTPUT_DIR` and `ERROR_REPORT_DIR` together. Keep backups outside the project runtime directories and periodically retain a copy on separate storage. A checkpoint backup without the corresponding exports is recoverable, but it is not a complete historical archive.

### Scheduling and retention

Use the host scheduler (for example, cron, launchd or Windows Task Scheduler) to invoke `docker compose run --rm scraper` or the native Python command. The scraper itself does not include a scheduler or discover new TikTok videos. Its current command-line configuration processes the single video specified by `TEST_VIDEO_ID` and `TEST_VIDEO_AUTHOR`; an hourly schedule will therefore revisit that configured video unless an external process updates the configuration before each run.

The application prevents overlapping runs that share the same cache directory. If a prior run is still active, a new invocation cannot acquire the run lock.

Run output directories are intentionally immutable and are not automatically deleted. Define a retention policy that fits your archive requirements, back up the data first, and remove old run exports only when they are no longer required. Diagnostics rotate automatically; checkpoints and quarantined data should be retained until their recovery value is no longer needed.

### Operational limits

- TikTok's internal API is not a stable public interface; platform changes can stop collection.
- Local JSONL alerts do not send email, SMS or external notifications.
- The current command-line interface processes one configured video per invocation.
- No automatic video discovery, multi-host coordination, cloud backup or database-backed storage is included.
- A single persistent storage volume is the recovery boundary. Back it up before moving the scraper to another machine.
