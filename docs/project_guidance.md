# ades Project Guidance

## Canonical Identity

- Product name: `ades`
- Product type: local-first semantic enrichment service and Python library
- Primary commands: `ades pull <pack>`, `ades serve`, `ades tag <text>`
- Primary package language: Python
- Distribution targets: PyPI package `ades` and npm CLI wrapper `ades-cli`
- Current delivery target: `v0.1.0`

## Mission

- `ades` provides reusable entity extraction, entity linking, topic tagging, and enrichment for AI systems that need grounded structured context.
- The first-class consumers are Docdex, mcoda, quantitative trading workflows, LLM refinement flows, Codali, and domain research workloads such as medical and finance.
- The system must run locally after packs are installed and should not require a hosted dependency for core enrichment.

## Operating Rules

- Large downloads, dataset packs, model packs, and generated KB assets must live under `/mnt/githubActions/ades_big_data`.
- Do not store large runtime data, downloaded corpora, or model checkpoints inside this repo.
- Keep the implementation Python-first. The npm package should distribute or launch the Python-based runtime instead of re-implementing the pipeline in Node.
- The npm wrapper should bootstrap a user-local Python virtual environment on first use and then delegate to the Python `ades` command.
- Treat the older “Open Calais clone” wording as historical research context, not the current product identity.
- Every development step must include unit, component, integration, and API tests.

## Product Distinctions

- `local tool`: the current delivery track. It must remain the default runtime until `v0.1.0` local functionality is complete.
- `production server tool`: a later delivery track for server deployment. It will use `PostgreSQL` instead of `SQLite`.
- The current code structure should make that later production-server path explicit, but only the local SQLite tool is implemented today.

## Product Modes

- Library mode: import `ades` directly from Python for in-process use.
- CLI mode: pull packs, inspect local state, tag text, and manage the service from the terminal.
- Local service mode: run `ades serve` to expose a localhost API for shared use by multiple agents or tools.
- Pack-builder mode: internal tooling for creating and publishing tiered domain/language packs.

## Architecture Direction

- Build the first usable version as a local service before designing a distributed deployment.
- Start with a simple local stack: `Typer` CLI, `FastAPI` service, `Pydantic v2`, `spaCy`, `GLiNER`, rule-based extractors, and local storage.
- Prefer lightweight local persistence for `v0.1.0` such as `SQLite` plus local indexes over `Postgres` and `OpenSearch`.
- Add backend seams and placeholders early so the later production-server build can swap `SQLite` for `PostgreSQL` without rewriting the public CLI, API, or pack model.
- Keep heavier components such as `Celery`, `Redis`, `OpenSearch`, `Qdrant`, and large REL indexes as optional upgrades after the local pack/runtime model is stable.
- Design every stage so packs can selectively add data, models, aliases, and rules by domain and language.
- Use commercially safe default dependencies in `v0.1.0`; avoid non-commercial relation extraction models in the initial runtime.

## Pack Strategy

- Packs must be tiered from smallest to biggest so users can install only what they need.
- Initial examples: `finance-en`, `medical-en`, `general-en`.
- Packs should be layered where useful: a domain pack may depend on a smaller shared core language pack.
- Pack contents may include aliases, KB snapshots, entity dictionaries, topic labels, rules, and model overrides.
- Packs should be versioned, checksummed, and pulled through a manifest-based registry.

## Repo Conventions

- Put source code under `src/ades/`.
- Put tests under `tests/`.
- Organize new test coverage under `tests/unit/`, `tests/component/`, `tests/integration/`, and `tests/api/`.
- Keep build or pack scripts under `scripts/`.
- Keep planning, design, and operating guidance under `docs/`.
- Keep ongoing implementation progress in [`development_progress.md`](/home/wodo/apps/ades/docs/development_progress.md).
- When a new document or sample refers to the product, use `ades` explicitly.
- Keep release-scope decisions for the current milestone in [`v0.1.0_decisions.md`](/home/wodo/apps/ades/docs/v0.1.0_decisions.md).

## Development Workflow

- Complete the implementation work, including the required unit, component, integration, and API tests, before closing a task.
- After every completed task, commit the changes and push them to the configured remote repository.
- Update [`development_progress.md`](/home/wodo/apps/ades/docs/development_progress.md) at each task completion so the current repo state is visible without reconstructing history from commits alone.

## Immediate Priorities

1. Scaffold the Python package, CLI entrypoint, and local service entrypoint.
2. Define the pack manifest schema and on-disk layout rooted at `/mnt/githubActions/ades_big_data`.
3. Implement `ades pull` for downloading and activating packs.
4. Implement `ades serve` with a minimal health endpoint and text-tagging endpoint.
5. Add a first finance and medical pack shape, even if initially thin, so the pack system is real from day one.
