# ades Implementation Plan

Target release: `v0.1.0`

## Current Understanding

- The existing docs describe a semantic enrichment pipeline with ingestion, NLP, NER, entity linking, topic tagging, relation extraction, and scoring.
- The old material was framed as an “Open Calais clone” and used a temporary placeholder CLI name.
- The current product name is `ades`, and the product direction is a reusable local service with tiered downloadable packs.
- The current release target is `v0.1.0`.
- The first priority is not distributed scale. It is a strong local runtime, a reliable pack system, and a clean developer-facing interface.

## Local Tool vs Production Server Tool

- The current implementation track is the `local tool`.
- The later `production server tool` is a separate track that will use `PostgreSQL` instead of `SQLite`.
- Code written during local-tool development must preserve a clear seam for that future server build.
- Placeholders for the production-server path are allowed now, but real delivery work remains focused on the local tool until that track is complete.

## Detailed Local Tool Scope

- Runtime model: single-machine local execution.
- Metadata backend: `SQLite`.
- Service style: localhost `FastAPI` service plus direct in-process Python library use.
- Primary user flows:
  - pull one or more packs locally
  - serve the runtime on localhost
  - tag inline text
  - tag local files
  - tag batches of local files
  - inspect installed packs and metadata
- Storage model:
  - code and packaging stay in the repo
  - large downloaded datasets and runtime assets live under `/mnt/githubActions/ades_big_data`
  - local runtime metadata and installed-pack state remain lightweight
- Quality bar:
  - every feature must add or update unit, component, integration, and API tests
  - local-tool changes should preserve a stable JSON contract for downstream tools such as Docdex and mcoda

## Detailed Production Server Tool Scope

- Runtime model: hosted or shared multi-user service after the local tool is complete.
- Metadata backend: `PostgreSQL`.
- Expected additions beyond the local tool:
  - real server-grade persistence and migrations
  - multi-user state and operational controls
  - production-specific deployment and health strategy
  - production-only test coverage for migrations, concurrency, and server behavior
- Compatibility goal:
  - reuse the pack model, result schema, and as much of the public API surface as possible
  - avoid baking local SQLite assumptions into public contracts
- Current state:
  - the server track is not being delivered in `v0.1.0`
  - codebase placeholders may exist, but production-server behavior is not yet implemented

## What We Keep From The Existing Docs

- `FastAPI` remains the service interface for the local tool.
- Pack-driven deterministic extraction, alias lookup, and structured JSON output remain central to the first release.
- A staged enrichment pipeline is still the long-term design, with room for topic classification, entity linking, relation extraction, and optional heavier NLP components later.
- `spaCy`, `GLiNER`, and `FlashText` remain candidate upgrades, but they are not part of the current implemented `v0.1.0` baseline.

## What We Change For ades v0.1.0

- Replace the original server-heavy default stack with a local-first stack.
- Start with `SQLite` and local indexes instead of `Postgres`, `OpenSearch`, and `Redis`.
- Avoid `Celery` for the first milestone and keep execution in-process unless background jobs are clearly needed.
- Treat `REL`, `REBEL`, `Qdrant`, and other heavy components as later upgrades rather than day-one requirements.
- Make the pack manager a first-class subsystem instead of treating the knowledge base as a one-off build artifact.

## Tooling We Will Use

### Core Runtime

- `Python 3.11+`
- `Typer` for the CLI
- `FastAPI` + `Uvicorn` for the local service
- `Pydantic v2` for config, manifests, and API models
- `httpx` for downloads and remote registry access
- `orjson` for fast payload handling
- regex and pack-provided alias metadata for the current deterministic tagging baseline
- `build` and `twine` for Python release packaging and publication

### Candidate Additions After v0.1.0

- `spaCy` for a richer NLP base layer if deterministic tagging stops being sufficient after `v0.1.0`
- `GLiNER` for flexible entity extraction if a later release accepts a model-backed extraction step
- `FlashText` if a dedicated keyword-extraction layer is still warranted beyond the current alias lookup path

### Local Data And Search

- `SQLite` for local metadata, installed packs, aliases, and enrichment state
- deterministic alias/rule lookup over installed-pack metadata for the current local baseline
- `SQLite FTS5` or a similarly lightweight local index remains a candidate next step for broader lookup
- File-based pack storage under `/mnt/githubActions/ades_big_data`
- Checksum validation for pulled pack artifacts

### Packaging And Quality

- `pyproject.toml` with a modern build backend such as `hatchling`
- `pytest` for tests
- `ruff` for linting and formatting
- `mypy` for type checking
- npm package as a thin CLI distribution layer around the Python runtime
- every development step must add or update unit, component, integration, and API tests

## Parts We Will Build

1. Package scaffold
   - `src/ades/`
   - CLI entrypoint
   - service entrypoint
   - config models

2. Pack registry and pack manager
   - pack manifest schema
   - local install registry
   - download, verify, unpack, activate flow
   - pack versioning and checksums

3. Local storage layer
   - local installed-pack registry
   - alias and KB storage
   - cache layout
   - local state and metadata tables
   - backend selector and future production placeholder seam

4. Enrichment pipeline core
   - text normalization
   - deterministic alias and rule extractors
   - relevance scoring
   - topic tagging with evidence summaries
   - optional linking hooks
   - result shaping plus a future NLP seam

5. Local HTTP service
   - `/healthz`
   - `/v0/status`
   - `/v0/tag`
   - `/v0/packs`
   - `/v0/packs/{pack}`

6. CLI commands
   - `ades pull <pack>`
   - `ades serve`
   - `ades tag <text>`
   - `ades packs list`
   - `ades status`

7. Distribution
   - `pip install ades`
   - `npm install -g ades-cli`
   - release flow for pack manifests and pack artifacts

8. Integrations
   - direct Python import for Docdex and mcoda
   - local HTTP mode for shared agent workflows
   - stable result schema for downstream AI systems

## Proposed Phases

### Phase 0

- Normalize naming in docs and establish `ades` as the canonical product name.
- Add authoritative repo docs: guidance, implementation plan, and README.
- Define the storage-root rule for all large downloadable assets.

### Phase 1

- Scaffold the Python package and service layout.
- Add config loading and storage-root resolution.
- Implement a placeholder `ades serve` and `ades status`.

### Phase 2

- Design the pack manifest schema.
- Implement `ades pull`.
- Add local pack install metadata and artifact verification.
- Create a minimal local registry format for `finance-en` and `medical-en`.

### Phase 3

- Deliver the first deterministic tag pipeline with plain-text and file input.
- Reuse pack-provided aliases and rules for high-value deterministic extraction.
- Return a stable JSON result structure through the CLI, library, and local service.

### Phase 4

- Keep `v0.1.0` deterministic-only and local-first for the default runtime.
- Add explicit deterministic relevance scoring and stronger topic output on the existing entity/topic response surface.
- Treat lightweight candidate search as the remaining tagging-related release-scope decision; defer `spaCy` / `GLiNER` / `FlashText` unless the release target changes after `v0.1.0`.

### Phase 5

- Add library mode APIs for direct import.
- Publish the Python package shape.
- Add the npm wrapper and installation docs.
- Keep the npm distribution thin by bootstrapping a user-local Python runtime instead of rebuilding the enrichment pipeline in Node.

### Phase 6

- Preserve the local/public interfaces while preparing the production-server seam.
- Keep the PostgreSQL-backed production-server path as an explicit placeholder, not an accidental SQLite assumption spread across the codebase.
- Extend the release workflow through verify, manifest, validate, smoke-install, and coordinated publish steps with categorized test coverage.

### Phase 7

- Add richer pack tooling and pack publication scripts beyond the current local registry builder.
- Evaluate heavier optional components such as REL, OpenTapioca, or relation extraction after the local-tool tagging scope is settled.
- Decide which server-scale pieces are still necessary after the local stack is proven.

### Later Production Track

- Introduce the real `production server tool`.
- Replace the local metadata backend with `PostgreSQL` for that track.
- Reuse the same pack model and public API contracts where possible.
- Add production-specific deployment, migration, and multi-user test coverage after the local tool is complete.
- Split the server-track implementation plan into:
  - backend persistence and migration work
  - hosted service/runtime operations
  - server-only API and operational endpoints
  - production integration and rollout testing

## Locked Decisions For v0.1.0

- Release scope and technical decisions are fixed in [`v0.1.0_decisions.md`](/home/wodo/apps/ades/docs/v0.1.0_decisions.md).
- `v0.1.0` is a local-first release, not a distributed deployment release.
- `v0.1.0` delivers the local tool only; the production server is deferred.
- The canonical service API for this release is versioned under `/v0`.
- Pack pulling is CLI-first; the local service exposes installed-pack visibility, not full remote pack management.
- Heavy linking, vector databases, and relation extraction are deferred unless they become necessary to make the first local product useful.

## First Deliverables

- A runnable `ades` Python package scaffold
- A minimal CLI
- A minimal FastAPI service
- A pack manifest format
- A working `ades pull` against a local or file-backed registry
- A first thin `finance-en` pack
- A first thin `medical-en` pack

## Decisions Beyond v0.1.0

- Whether later local search should stay on `SQLite FTS5` or move to a second lightweight index
- Whether post-`v0.1.0` local enrichment should add `spaCy`, `GLiNER`, `FlashText`, or stay deterministic-first
- Whether future larger packs should include heavier dedicated domain models
- Whether the first stable API after `v0.x` should remain HTTP-only or add another transport

## Deferred For Now

- Full distributed worker architecture
- Large external search clusters
- Heavy vector databases
- Commercially risky relation extraction dependencies
- Multilingual support beyond the first English packs
