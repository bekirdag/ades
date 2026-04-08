# ades Implementation Plan

## Current Understanding

- The existing docs describe a semantic enrichment pipeline with ingestion, NLP, NER, entity linking, topic tagging, relation extraction, and scoring.
- The old material was framed as an “Open Calais clone” and used a temporary placeholder CLI name.
- The current product name is `ades`, and the product direction is a reusable local service with tiered downloadable packs.
- The first priority is not distributed scale. It is a strong local runtime, a reliable pack system, and a clean developer-facing interface.

## What We Keep From The Existing Docs

- `FastAPI` for the service interface.
- `spaCy` for tokenization, sentence splitting, and syntax.
- `GLiNER` for flexible entity extraction.
- `FlashText` and pattern rules for deterministic extraction.
- A staged enrichment pipeline with room for topic classification, entity linking, and relation extraction.

## What We Change For ades v1

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
- `spaCy` for the NLP base layer
- `GLiNER` for entity extraction
- `FlashText` plus regex/rule packs for deterministic entities
- `httpx` for downloads and remote registry access
- `orjson` for fast payload handling

### Local Data And Search

- `SQLite` for local metadata, installed packs, aliases, and enrichment state
- `SQLite FTS5` or a similarly lightweight local index for early candidate search
- File-based pack storage under `/mnt/githubActions/ades_big_data`
- Checksum validation for pulled pack artifacts

### Packaging And Quality

- `pyproject.toml` with a modern build backend such as `hatchling`
- `pytest` for tests
- `ruff` for linting and formatting
- `mypy` for type checking
- npm package as a thin CLI distribution layer around the Python runtime

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
   - installed-pack registry
   - alias and KB storage
   - cache layout
   - local state and metadata tables

4. Enrichment pipeline core
   - text normalization
   - NLP base layer
   - NER and deterministic extractors
   - topic tagging
   - optional linking hooks
   - scoring and result shaping

5. Local HTTP service
   - `/healthz`
   - `/v1/tag`
   - `/v1/packs`
   - `/v1/packs/{pack}`

6. CLI commands
   - `ades pull <pack>`
   - `ades serve`
   - `ades tag <text>`
   - `ades packs list`
   - `ades status`

7. Distribution
   - `pip install ades`
   - `npm install -g ades`
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

- Implement the first tag pipeline with plain-text input.
- Add `spaCy` and `GLiNER`.
- Add deterministic rule extraction for high-value patterns.
- Return a stable JSON result structure.

### Phase 4

- Add topic tagging and lightweight candidate lookup.
- Introduce pack-provided alias tables and label sets.
- Add score fields for confidence and relevance.

### Phase 5

- Add library mode APIs for direct import.
- Publish the Python package shape.
- Add the npm wrapper and installation docs.

### Phase 6

- Add richer pack tooling and pack publication scripts.
- Evaluate heavier optional components such as REL, OpenTapioca, or relation extraction.
- Decide which server-scale pieces are still necessary after the local stack is proven.

## First Deliverables

- A runnable `ades` Python package scaffold
- A minimal CLI
- A minimal FastAPI service
- A pack manifest format
- A working `ades pull` against a local or file-backed registry
- A first thin `finance-en` pack
- A first thin `medical-en` pack

## Decisions To Lock Early

- Exact pack manifest format and versioning rules
- The default on-disk directory layout under `/mnt/githubActions/ades_big_data`
- Whether v1 local search uses `SQLite FTS5` alone or a second lightweight index
- The initial JSON response schema for tags and entity matches
- How the npm installer will locate or bootstrap the Python runtime

## Deferred For Now

- Full distributed worker architecture
- Large external search clusters
- Heavy vector databases
- Commercially risky relation extraction dependencies
- Multilingual support beyond the first English packs
