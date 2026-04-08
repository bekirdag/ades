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

### 12. Corpus input-size accounting and skip reporting

- Added per-file input-size accounting to local file and batch tagging responses so callers can see how many source bytes were actually processed.
- Extended batch local tagging to report deterministic skipped and rejected inputs instead of failing or silently dropping discovery edge cases.
- Added batch summary fields for discovered, included, and processed byte totals plus processed, skipped, and rejected counts.
- Kept the batch manifest aligned with the runtime response so audit artifacts now include per-item input sizes and the full skipped/rejected input lists.

### 13. Corpus-run guardrails

- Added explicit `max_files` and `max_input_bytes` guardrails to local batch tagging across the shared discovery layer, public Python API, CLI, and local-service endpoint.
- Applied the guardrails in deterministic discovery order after filtering, so once a configured limit is reached the remaining corpus inputs are skipped with explicit limit reasons instead of being processed opportunistically.
- Extended batch summaries to carry the configured limits plus a `limit_skipped_count` counter for auditability.
- Preserved manifest parity with runtime responses so batch audit artifacts now include the configured guardrails and the exact inputs skipped because of them.

### 14. Resumable corpus runs and manifest replay

- Added manifest replay as a first-class batch source across the local Python API, CLI, and local-service endpoint.
- Added deterministic replay modes `resume`, `processed`, and `all` so a saved batch manifest can resume deferred work, re-run processed items, or replay the entire recorded corpus without rediscovering directories and globs.
- Reused the existing batch pipeline for replayed sources so filtering, guardrails, persistence, and audit reporting remain consistent with normal local corpus runs.
- Extended batch summaries with manifest replay metadata such as the manifest path, replay mode, and manifest candidate/selected counts.

### 15. Source fingerprinting and unchanged-file skip detection

- Added stable source fingerprints to file-based tag results and batch manifest items using file size, modification time, and SHA-256 content hash.
- Added `skip_unchanged` support for repeated local corpus runs, so a saved batch manifest can act as a fingerprint baseline while the current corpus is rediscovered normally.
- Kept unchanged detection deterministic and local-only by comparing the newly loaded file fingerprint with the prior manifest fingerprint before running the tagger.
- Extended batch summaries with an `unchanged_skipped_count` counter so audit output explains how many files were intentionally not re-tagged.

### 16. Unchanged-output reuse on reruns

- Added optional unchanged-output reuse for repeated local corpus runs so skipped unchanged files can carry forward their prior manifest item metadata instead of appearing only in the skipped list.
- Kept `items` scoped to newly processed files in the current run and added a separate `reused_items` collection for carried-forward output metadata.
- Extended batch summaries with an `unchanged_reused_count` counter so rerun audit output distinguishes skipped unchanged files from unchanged files whose prior output metadata was intentionally reused.
- Extended saved batch manifests and replay planning so reused manifest items remain valid baseline coverage for future reruns.

### 17. Reused-output verification and missing-output warnings

- Added reused-output verification so carried-forward manifest items now record whether their `saved_output_path` still exists on disk.
- Extended reused batch items with `saved_output_exists` and deterministic warning strings when a carried-forward output path is missing or was never recorded.
- Extended batch summaries with a `reused_output_missing_count` counter so rerun audit output shows how many reused outputs are unavailable.
- Kept manifest export self-consistent by re-verifying reused items and normalizing missing-output warnings during manifest persistence.

### 18. Rerun repair mode for missing reused outputs

- Added opt-in rerun repair mode so a local batch rerun can regenerate missing reused JSON outputs instead of only reporting them as unavailable.
- Kept repaired files in the existing `reused_items` path so `items` still represent newly processed changed or new inputs only.
- Extended batch summaries with a `repaired_reused_output_count` counter so rerun audit output records how many reused outputs were regenerated during the run.
- Added deterministic `repaired_reused_output:<path>` warnings on repaired reused items so callers can distinguish repaired outputs from reused outputs that were already present.

### 19. Rerun diff reporting

- Added rerun diff reporting so local batch reruns now emit a structured `rerun_diff` payload whenever they are driven from a prior manifest.
- Categorized rerun outcomes into `changed`, `newly_processed`, `reused`, `repaired`, and `skipped` so callers can audit the effective delta without re-deriving it from the full response.
- Persisted the same rerun diff structure into saved batch manifests so replayed local runs keep one stable audit artifact for later inspection or chained reruns.
- Kept top-level `items`, `reused_items`, and `skipped` semantics unchanged so the new diff layer is additive rather than a breaking response rewrite.

### 20. Manifest lineage metadata and stable run identifiers

- Added stable batch `run_id` generation so every local corpus run now carries a durable identifier in both runtime responses and saved manifests.
- Added lineage metadata with `root_run_id`, `parent_run_id`, `source_manifest_path`, and `created_at` so reruns can express parent-child relationships across repeated manifest-driven executions.
- Kept the lineage block additive and backward-compatible by making it optional in the persisted manifest schema, so older saved manifests can still be loaded for replay.
- Fixed the batch manifest persistence path handling so an explicit manifest output path can coexist with the existing per-file output directory flow, which is required for preserving parent and child manifests in the same workspace.

### 21. Static pack publication and remote registry tooling

- Added a static registry builder that publishes local pack directories into a file-based registry with generated `index.json`, published `manifest.json` files, and checksum-verified `.tar.zst` artifacts.
- Kept the published registry contract identical to the existing installer read path, so generated registries can be consumed immediately through `file://` or any simple static HTTP host without introducing a second registry format.
- Added local CLI and public Python API support for registry publication, plus per-command `--registry-url` overrides so pack discovery and pulls can target external registries without changing the default bundled source.
- Added a local HTTP admin endpoint for registry builds so the localhost service can drive publication workflows when direct filesystem automation is preferred.

### 22. npm wrapper and local install bootstrap flow

- Added the `npm/ades-cli` wrapper package so the Node distribution target now exists as a real installable npm surface instead of documentation only.
- Implemented first-run bootstrap logic in the npm bin wrapper so `ades-cli` creates a user-local Python virtual environment, installs the matching `ades` Python package, writes a runtime marker, and then delegates all CLI arguments to the Python `ades` command.
- Added canonical npm-bootstrap metadata on the Python side plus a local service endpoint so the npm package, public API, docs, and local service all agree on package name, version, runtime directory defaults, and bootstrap environment variables.
- Kept the local tool Python-first by making the npm package a thin launcher around the Python runtime rather than re-implementing the pipeline in Node.

### 23. Packaging release verification for Python and npm

- Added a shared local release-verification flow that builds the Python wheel, Python sdist, and npm tarball into one caller-selected output directory.
- Added stable verification metadata for each artifact, including absolute path, file name, byte size, and SHA-256 digest.
- Added explicit Python/npm version alignment checks so release verification now reports whether both distributions are still locked to the same `v0.1.0` version before publication.
- Exposed the verifier consistently through the public Python API, the new `ades release verify` CLI command, and a local HTTP endpoint for localhost automation.

## Current Local Tool Capabilities

- `ades pull <pack>`
- `ades pull <pack> --registry-url <url>`
- `ades serve`
- `ades release verify --output-dir <dir>`
- `ades status`
- `npm install -g ades-cli`
- `ades tag <text>`
- `ades tag --file <path>`
- `ades tag-files <path...>`
- `ades tag-files --directory <dir>`
- `ades tag-files --glob <pattern>`
- `ades tag-files --include <pattern>`
- `ades tag-files --exclude <pattern>`
- `ades tag-files --write-manifest`
- `ades tag-files --reuse-unchanged-outputs`
- `ades registry build <pack-dir...> --output-dir <dir>`
- `ades packs list`
- `ades packs list --available --registry-url <url>`
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
- `POST /v0/registry/build`
- `GET /v0/installers/npm`
- `POST /v0/release/verify`
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

- Add coordinated release version synchronization and release-manifest generation so Python and npm version bumps and publication metadata stay locked together before the first public `v0.1.0` release.
