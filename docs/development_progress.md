# ades Development Progress

## Overview

This file records the implementation progress of `ades` as the project moves toward the local-tool `v0.1.0` release. The current product track is the local tool. The later production server tool is planned separately and remains a future delivery track.

## Completed So Far

### 1. Project identity and repository baseline

- Normalized the product identity to `ades` across the active repository docs.
- Added the first authoritative project documents for guidance, implementation planning, and `v0.1.0` decisions.
- Created the git repository, connected it to `git@github.com:bekirdag/ades.git`, and pushed the initial branch.
- Added a real `README.md` that explains the product direction, local-first runtime, pack model, and current usage surface.

### 2. Python package and runtime scaffold

- Created the Python package layout under `src/ades/`.
- Added `pyproject.toml` for packaging, dependencies, and test configuration.
- Added the Typer-based CLI entrypoint and the FastAPI app factory.
- Added versioning, config loading, and storage-root resolution.
- Kept the packaging direction Python-first, with npm reserved as a thin CLI wrapper around the Python runtime.

### 3. Pack system foundation

- Implemented the bundled pack registry under `src/ades/resources/registry`.
- Added checksum-verified pack artifacts and manifest loading.
- Implemented `ades pull` with dependency-aware installation.
- Added starter packs for `general-en`, `finance-en`, and `medical-en`.
- Established the rule that large downloaded data for the real runtime belongs under `/mnt/githubActions/ades_big_data`, not inside the repo.

### 4. Local metadata and backend seam

- Added the local metadata store on `SQLite`.
- Implemented installed-pack metadata persistence, dependency persistence, and alias/rule/label storage.
- Added activation and deactivation support for installed packs.
- Added a runtime/backend selector so the current local tool uses `runtime_target=local` and `metadata_backend=sqlite`.
- Added placeholder modules for the later production server path that will use PostgreSQL.

### 5. Lookup and tagging pipeline

- Added deterministic metadata lookup over aliases and rules.
- Wired lookup into the tagging pipeline so local extraction reuses indexed metadata.
- Added lightweight linking and explicit provenance on entity matches.
- Kept the first tagging flow deterministic and local-first instead of introducing heavy distributed infrastructure.

### 6. File tagging and result persistence

- Added local file tagging support for plain text and HTML documents.
- Added CLI and service entrypoints for file-based tagging.
- Added stable JSON result persistence for CLI, library, and service flows.
- Added pack-aware output filenames such as `report.finance-en.ades.json`.
- Added collision-safe persisted output naming for batch file tagging so repeated stems from different directories do not overwrite each other.

### 7. Batch file tagging

- Added batch file tagging through the public Python API with `tag_files`.
- Added the CLI command `ades tag-files`.
- Added the local service endpoint `POST /v0/tag/files`.
- Preserved the local-only runtime behavior and kept the feature scoped to local file paths for this milestone.

### 8. Test structure and execution

- Added categorized test layers under `tests/unit`, `tests/component`, `tests/integration`, and `tests/api`.
- Added unit, component, integration, and API coverage for the runtime seam, lookup-driven tagging, provenance, file tagging, persistence, and batch tagging.
- Added `__init__.py` markers to the test directories so pytest can collect duplicate test basenames without import collisions.
- Verified the current repo state with `python -m pytest -q`, which now passes successfully.

### 9. Directory and glob discovery

- Extended batch tagging so local users can provide explicit file paths, directories, and glob patterns in a single run.
- Added shared source discovery logic in the local file helper layer.
- Kept directory/glob discovery additive to the existing batch API, CLI, and local-service endpoint instead of creating a second batch surface.
- Filtered generated `.ades.json` outputs out of directory and glob discovery to avoid accidental self-reprocessing in local corpus runs.

### 10. Include/exclude filtering and batch discovery summaries

- Added include and exclude glob filters for batch local tagging across the shared file-discovery layer, public Python API, CLI, and local-service endpoint.
- Kept filtering additive to the existing local batch surface instead of creating a second corpus-run interface.
- Added machine-readable summary metadata for corpus discovery runs, including discovered counts, included counts, excluded counts, duplicate suppression counts, generated-output skip counts, and the active filter patterns.
- Preserved the current local-only execution model while making larger local corpus runs easier to control and audit.

### 11. Batch manifest export and aggregated warnings

- Added run-level manifest export for batch local tagging so a corpus run can write one stable audit artifact alongside per-file outputs.
- Added aggregated batch warnings on the local Python API, CLI, and local-service response shape.
- Kept manifest export additive to the existing batch output flow by requiring the existing output-directory path instead of introducing a second persistence mode.
- Added manifest persistence helpers that write a compact run summary, aggregated warnings, and per-item output references without duplicating full extraction payloads.

## Current Local Tool Capabilities

- `ades pull <pack>`
- `ades serve`
- `ades status`
- `ades tag <text>`
- `ades tag --file <path>`
- `ades tag-files <path...>`
- `ades tag-files --directory <dir>`
- `ades tag-files --glob <pattern>`
- `ades tag-files --include <pattern>`
- `ades tag-files --exclude <pattern>`
- `ades tag-files --write-manifest`
- `ades packs list`
- `ades packs activate`
- `ades packs deactivate`
- `ades packs lookup`

The local service currently exposes:

- `GET /healthz`
- `GET /v0/status`
- `GET /v0/packs`
- `GET /v0/packs/{pack}`
- `POST /v0/packs/{pack}/activate`
- `POST /v0/packs/{pack}/deactivate`
- `GET /v0/lookup`
- `POST /v0/tag`
- `POST /v0/tag/file`
- `POST /v0/tag/files`

## Architectural State

### Local tool

- This is the active delivery target for `v0.1.0`.
- It runs on one machine.
- It uses `SQLite` for local metadata and pack-related lookup state.
- It exposes the CLI, library, and localhost service surfaces.

### Production server tool

- This is intentionally deferred until after the local tool is complete.
- It is expected to use `PostgreSQL` instead of `SQLite`.
- The current codebase already keeps a seam for that future backend swap, but the production server is not yet implemented.

## Workflow Rules In Force

- Every completed development task must include unit, component, integration, and API tests where applicable.
- Progress must be kept in this markdown file under `docs/`.
- Every completed task must end with a git commit and push to the remote repository.

## Current Next Step

- Add corpus input-size accounting and skip reporting so larger local runs can explain exactly what was processed, skipped, or rejected.
