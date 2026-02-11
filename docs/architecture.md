# Architecture

## Modules

- `src/core/`: runtime primitives (config, db sessions, paths, encryption, worker state)
- `src/integrations/`: adapters for external systems (Gemini, S3, Yandex Disk)
- `src/content/`: content extraction pipeline
- `src/metadata/`: metadata extraction and applicability evaluation
- `src/layout/`: layout-specific processing
- `src/cli/`: command registration and CLI argument mapping
- `src/maintenance/`: operational and maintenance workflows
- `src/prompts/`: content/metadata prompt templates and prompt helpers

## Import Boundaries

- Domain modules (`content`, `metadata`, `layout`, top-level workflows) should import shared runtime behavior from `core/*`.
- External APIs should be consumed via `integrations/*`.
- `utils.py`, `gemini.py`, and `s3.py` are compatibility shims and should not be imported directly from new domain code.
- `prompt.py`, `prepare_shots.py`, and maintenance root modules (`check_pub_links.py`, etc.) are compatibility shims.
- Deprecated modules:
  - `meta_fields` → use `metadata.fields`
  - `meta` package → use `metadata`

## Enforcement

`make lint` runs:

1. Ruff lint checks
2. `scripts/check_architecture.py` boundary checks
