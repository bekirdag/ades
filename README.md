# ades

`ades` is a local-first semantic enrichment tool for AI systems. It is being designed as both a Python library and a localhost service so tools such as Docdex, mcoda, quantitative trading workflows, LLM refinement pipelines, Codali, and domain research systems can share the same entity extraction and tagging runtime.

The core idea is simple: users install the runtime, pull the domain packs they need, and run everything locally. Example packs include `finance-en` and `medical-en`. Large downloaded data for this project must live under `/mnt/githubActions/ades_big_data`, not inside this repository.

The current delivery target is `v0.1.0`.

## Runtime Modes

- `local tool`: the active product track for `v0.1.0`. It runs on one machine, stores runtime metadata in `SQLite`, and is the only supported execution target today.
- `production server tool`: a later product track for multi-user or hosted deployment. It is planned to use `PostgreSQL` instead of `SQLite` and now has code/documentation placeholders only.

## Testing Policy

Every development step in this repository must ship four test layers:

- unit tests
- component tests
- integration tests
- API tests

## Planned Usage

```bash
pip install ades
npm install -g ades-cli

ades pull finance-en
ades pull medical-en
ades serve
ades tag "Apple CEO Tim Cook announced quarterly earnings."
ades tag --file ./notes/report.html --pack finance-en
ades tag --file ./notes/report.html --pack finance-en --output-dir ./outputs
ades tag-files ./notes/report-a.html ./notes/report-b.html --pack finance-en --output-dir ./outputs
ades tag-files --directory ./corpus --glob "./extras/*.html" --pack finance-en --output-dir ./outputs
ades tag-files --directory ./corpus --include "*earnings*.html" --exclude "*draft*" --pack finance-en
ades tag-files ./notes/report-a.html ./notes/report-b.html --pack finance-en --output-dir ./outputs --write-manifest
ades tag-files --directory ./corpus --pack finance-en --max-files 100 --max-bytes 5000000
ades tag-files --manifest-input ./outputs/batch.finance-en.ades-manifest.json --manifest-mode resume
```

## Current Usage

- Library mode: import `ades` directly from Python for in-process enrichment.
- CLI mode: pull packs, tag text, inspect installed packs, and run the local service.
- Local service mode: run `ades serve` and expose a localhost API that multiple tools or agents can share.
- Future production server mode: reserved for a later PostgreSQL-backed server build and not available in `v0.1.0`.

```python
from ades import pull_pack, tag, tag_file, tag_files

pull_pack("finance-en")
response = tag(
    "AAPL rallied on NASDAQ after USD 12.5 guidance from Apple.",
    pack="finance-en",
)
print(response.model_dump())

file_response = tag_file("./notes/report.html", pack="finance-en")
print(file_response.model_dump())

batch_response = tag_files(
    ["./notes/report-a.html", "./notes/report-b.html"],
    pack="finance-en",
    output_dir="./outputs",
)
print(batch_response.model_dump())

discovered_response = tag_files(
    pack="finance-en",
    directories=["./corpus"],
    glob_patterns=["./extras/*.html"],
    output_dir="./outputs",
)
print(discovered_response.model_dump())
print(discovered_response.summary.model_dump())

saved_response = tag(
    "Apple said AAPL traded on NASDAQ.",
    pack="finance-en",
    output_dir="./outputs",
)
print(saved_response.saved_output_path)
```

## Current Runtime Surface

- CLI commands: `ades pull`, `ades serve`, `ades tag`, `ades tag-files`, `ades status`, `ades packs list`, `ades packs activate`, `ades packs deactivate`, `ades packs lookup`
- Local API endpoints: `GET /healthz`, `GET /v0/status`, `GET /v0/packs`, `GET /v0/packs/{pack}`, `POST /v0/packs/{pack}/activate`, `POST /v0/packs/{pack}/deactivate`, `GET /v0/lookup`, `POST /v0/tag`, `POST /v0/tag/file`, `POST /v0/tag/files`
- Local runtime status now reports `runtime_target=local` and `metadata_backend=sqlite`
- Public Python helpers: `pull_pack`, `list_packs`, `get_pack`, `activate_pack`, `deactivate_pack`, `lookup_candidates`, `status`, `tag`, `tag_file`, `tag_files`, `create_service_app`
- Bundled development packs: `general-en`, `finance-en`, `medical-en`

## Initial Product Direction

- Python-first implementation
- Tiered downloadable packs from smallest to biggest
- Local pack and model storage under `/mnt/githubActions/ades_big_data`
- Explicit separation between the current local tool and the future production server tool
- Reusable structured enrichment for entity extraction, linking, topic tagging, and related AI workflows
- Packaging for both PyPI and npm for easy installation
- First release target: `v0.1.0`

## Current Repo Status

This repository now contains a working `v0.1.0` scaffold with:

- an installable Python package layout under [`src/ades`](/home/wodo/apps/ades/src/ades)
- a Typer CLI
- a FastAPI app factory for local service mode
- a bundled development registry with checksum-verified pack artifacts
- a local SQLite metadata store for installed packs, dependencies, labels, rules, and aliases
- a runtime/backend selector that keeps the local SQLite tool active and reserves a PostgreSQL production-server path for later work
- deterministic pack-aware tagging for plain text and HTML with lookup-driven alias extraction
- lightweight entity linking and match provenance on extracted entities
- file-oriented tagging entrypoints for local document paths
- stable JSON persistence for tag results from CLI, library, and local-service entrypoints
- batch tagging entrypoints for multiple local files with collision-safe persisted output names
- directory and glob discovery for batch local tagging so corpus runs do not require enumerating every file path
- include/exclude filtering plus batch summary metadata for local corpus runs so discovery, filtering, and audit counts are visible in one response
- run-level manifest export plus aggregated batch warnings so corpus jobs can emit one stable audit artifact alongside per-file outputs
- corpus input-size accounting plus explicit skipped/rejected input reporting so batch local runs can explain what was processed, skipped, or rejected
- corpus-run guardrails with explicit max-file and max-byte limits so local jobs stop in deterministic discovery order with explicit limit skip reasons
- resumable corpus runs and manifest replay so saved batch manifests can resume deferred work or selectively replay processed corpus inputs
- initial tests for installer, tagger, lookup, public API, and service behavior
- categorized test coverage under `tests/unit`, `tests/component`, `tests/integration`, and `tests/api`

The next local-tool step is to add source fingerprinting and unchanged-file skip detection so repeated local corpus runs can avoid re-tagging files that have not changed since the last saved manifest.
