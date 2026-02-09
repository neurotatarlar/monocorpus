# Monocorpus

Utilities for syncing, extracting, and managing a Tatar/Crimean Tatar document corpus. The
project connects Yandex.Disk storage, a database, and Yandex Cloud (S3-compatible) buckets to:

- synchronize document metadata with a database
- extract content and metadata from documents
- upload processed artifacts to cloud storage
- perform maintenance tasks (public links, deduplication, layout analysis)

## Architecture Overview

High-level flow:

1) **Source storage (Yandex.Disk)**  
   Raw documents live in Yandex.Disk under configured entry points.

2) **Database (metadata + state)**  
   Document records track MD5, storage locations, language, metadata, and processing state.

3) **Processing pipelines**  
   - Content extraction (`src/content/*`) for PDFs and non-PDFs  
   - Metadata extraction (`src/metadata/*`) using Gemini prompts  
   - Optional layout detection (`src/layout/dispatch.py`)

4) **Artifact storage (Yandex Cloud S3)**  
   Extracted content, images, and metadata are uploaded to buckets.

5) **Maintenance**  
   Sync, dedup, and public link verification keep the dataset clean and accessible.

In short: **Yandex.Disk → DB → extraction → S3**, with maintenance tools keeping everything aligned.

## Quick Start

1) Create a virtual environment and install dependencies:

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt
```

2) Create a local config at `~/.monocorpus/config.yaml` (see template below).

3) Run CLI commands via the entrypoint:

```bash
python src/main.py --help
```

## Configuration

The project expects a local config file in `~/.monocorpus/config.yaml` and a few optional
credential files (see below). Keep secrets out of the repo.

Minimal template (fill with your own values):

```yaml
database_url: "postgresql+psycopg2://USER:PASSWORD@HOST:PORT/DBNAME"
encryption_key: "BASE64_URLSAFE_KEY"

proxy: null

yandex:
  disk:
    oauth_token: "YANDEX_DISK_OAUTH_TOKEN"
    hidden: "/path/segment/used/for/sharing_restricted"
    entry_points:
      tt: "/path/to/tatar/entry_point"
      crh: "/path/to/crimean_tatar/entry_point"
    filtered_out: "/path/to/filtered_out"
  cloud:
    aws_access_key_id: "YANDEX_CLOUD_ACCESS_KEY"
    aws_secret_access_key: "YANDEX_CLOUD_SECRET"
    bucket:
      document: "ttdoc"
      content: "ttcontent"
      content_chunks: "ttcontent_chunks"
      image: "ttimg"
      metadata: "ttmeta"
      upstream_metadata: "ttupstream"

gemini_api_keys:
  - "GEMINI_API_KEY_1"
  - "GEMINI_API_KEY_2"

google_api_key:
  free: "GEMINI_API_KEY_FOR_CLI"
```

Optional local files (kept out of git):

- `client_secret.json` and `personal_token.json` for Google APIs
- any extra tokens required by your workflow

## Common Commands

Run all commands via `python src/main.py <command>`:

- `sync`: sync Yandex.Disk and database, handle filtering and deduplication
- `extract`: extract content from documents (PDF and non-PDF)
- `meta`: extract metadata from documents
- `hf`: assemble structured dataset into parquet
- `layouts`: run layout detection on PDFs
- `pps`: postpostprocess extracted markdown in `~/.monocorpus/1_result` and re-upload updated archives
- `dedup`: scan near-full duplicate extracted documents and produce JSON report
- `match-limited`: reconcile limited vs full document variants
- `sharing-restricted`: check restricted sharing docs
- `check-pub-links`: verify/restore public links
- `dump-state`: export database state to CSV and Google Drive/Sheets
- `upload-to-s3`: upload missing Crimean Tatar documents

Use `--help` for command options:

```bash
python src/main.py extract --help
```

### Command Details & Examples

Below are the most commonly used commands and typical flows.

**sync**  
Synchronizes Yandex.Disk with the database, applies filtering rules, and updates links.

```bash
python src/main.py sync
```

**extract**  
Extracts content from documents. Use `--md5` or `--path` to scope work.  
`--workers` controls Gemini parallelism; `--batch-size` controls queue size.

```bash
python src/main.py extract --workers 4
python src/main.py extract --md5 <MD5>
python src/main.py extract --path "/path/in/yadisk"
```

**meta**  
Extracts structured metadata from documents.

```bash
python src/main.py meta
```

**hf**  
Builds a parquet dataset from extracted content.

```bash
python src/main.py hf
```

**layouts**  
Runs PDF layout detection (YOLO + Surya) and produces annotated outputs.

```bash
python src/main.py layouts --md5 <MD5>
```

**check-pub-links**  
Verifies public links and restores missing ones.

```bash
python src/main.py check-pub-links
```

**pps**  
Runs postpostprocessing on extracted markdown archives in `~/.monocorpus/1_result`
and re-uploads updated archives to S3.

```bash
python src/main.py pps
```

**dedup**  
Scans extracted archives for near-full duplicate documents and writes a report with
recommended keeper documents using format priority (`epub > fb2 > docx > pdf`).

```bash
python src/main.py dedup --threshold 0.98
```

**dump-state**  
Exports DB state into CSV, ZIP, and Google Sheets/Drive.

```bash
python src/main.py dump-state
```

## Workdir Layout

The local workdir is `~/.monocorpus` and is organized into subfolders like:

- `0_entry_point`: local copies of documents
- `1_result`: extracted content
- `2_metadata`: extracted metadata
- `misc/`: supporting artifacts (slices, upstream metadata, logs, etc.)

See `src/dirs.py` for the full list of subdirectories.

### Example Layout

```
~/.monocorpus/
  0_entry_point/
    <md5>.pdf
    <md5>.docx
  1_result/
    <md5>-formatted.md
    <md5>.zip
  2_metadata/
    <md5>.json
  misc/
    doc_slices/
    upstream_metadata/
    page_images/
    clips/
    prompts/
    logs/
  parquet/
```

## Security Notes

- Keep secrets in `~/.monocorpus/config.yaml` or other local files, not in the repo.
- This repository's `.gitignore` already ignores common secret files, but ensure
  sensitive files are not committed.

## Development Notes

- Main CLI entrypoint: `src/main.py`
- Core utilities: `src/utils.py`
- Content pipeline: `src/content/*`
- Metadata pipeline: `src/metadata/*`
- Sync/maintenance: `src/sync.py`, `src/check_pub_links.py`, `src/match_limited.py`

## Tests

Run unit tests:

```bash
.venv/bin/python -m unittest discover -s tests -v
```

Run lint checks:

```bash
make lint
make lint-fix
```
