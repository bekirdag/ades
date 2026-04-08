# ades Project Guidance

## Canonical Identity

- Product name: `ades`
- Product type: local-first semantic enrichment service and Python library
- Primary commands: `ades pull <pack>`, `ades serve`, `ades tag <text>`
- Primary package language: Python
- Distribution targets: PyPI package and npm CLI package

## Mission

- `ades` provides reusable entity extraction, entity linking, topic tagging, and enrichment for AI systems that need grounded structured context.
- The first-class consumers are Docdex, mcoda, quantitative trading workflows, LLM refinement flows, Codali, and domain research workloads such as medical and finance.
- The system must run locally after packs are installed and should not require a hosted dependency for core enrichment.

## Operating Rules

- Large downloads, dataset packs, model packs, and generated KB assets must live under `/mnt/githubActions/ades_big_data`.
- Do not store large runtime data, downloaded corpora, or model checkpoints inside this repo.
- Keep the implementation Python-first. The npm package should distribute or launch the Python-based runtime instead of re-implementing the pipeline in Node.
- Treat the older “Open Calais clone” wording as historical research context, not the current product identity.

## Product Modes

- Library mode: import `ades` directly from Python for in-process use.
- CLI mode: pull packs, inspect local state, tag text, and manage the service from the terminal.
- Local service mode: run `ades serve` to expose a localhost API for shared use by multiple agents or tools.
- Pack-builder mode: internal tooling for creating and publishing tiered domain/language packs.

## Architecture Direction

- Build the first usable version as a local service before designing a distributed deployment.
- Start with a simple local stack: `Typer` CLI, `FastAPI` service, `Pydantic v2`, `spaCy`, `GLiNER`, rule-based extractors, and local storage.
- Prefer lightweight local persistence for v1 such as `SQLite` plus local indexes over `Postgres` and `OpenSearch`.
- Keep heavier components such as `Celery`, `Redis`, `OpenSearch`, `Qdrant`, and large REL indexes as optional upgrades after the local pack/runtime model is stable.
- Design every stage so packs can selectively add data, models, aliases, and rules by domain and language.

## Pack Strategy

- Packs must be tiered from smallest to biggest so users can install only what they need.
- Initial examples: `finance-en`, `medical-en`, `general-en`.
- Pack contents may include aliases, KB snapshots, entity dictionaries, topic labels, rules, and model overrides.
- Packs should be versioned, checksummed, and pulled through a manifest-based registry.

## Repo Conventions

- Put source code under `src/ades/`.
- Put tests under `tests/`.
- Keep build or pack scripts under `scripts/`.
- Keep planning, design, and operating guidance under `docs/`.
- When a new document or sample refers to the product, use `ades` explicitly.

## Immediate Priorities

1. Scaffold the Python package, CLI entrypoint, and local service entrypoint.
2. Define the pack manifest schema and on-disk layout rooted at `/mnt/githubActions/ades_big_data`.
3. Implement `ades pull` for downloading and activating packs.
4. Implement `ades serve` with a minimal health endpoint and text-tagging endpoint.
5. Add a first finance and medical pack shape, even if initially thin, so the pack system is real from day one.
