# ades

`ades` is a local-first semantic enrichment tool for AI systems. It is being designed as both a Python library and a localhost service so tools such as Docdex, mcoda, quantitative trading workflows, LLM refinement pipelines, Codali, and domain research systems can share the same entity extraction and tagging runtime.

The core idea is simple: users install the runtime, pull the domain packs they need, and run everything locally. Example packs include `finance-en` and `medical-en`. Large downloaded data for this project must live under `/mnt/githubActions/ades_big_data`, not inside this repository.

The current delivery target is `v0.1.0`.

## Runtime Modes

- `local tool`: the active product track for `v0.1.0`. It runs on one machine, stores runtime metadata in `SQLite`, and is the only supported execution target today.
- `production server tool`: the hosted deployment track. It uses `PostgreSQL`, serves the API behind `nginx`, and publishes downloadable packs from `repo.adestool.com`.

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
ades pull finance-en --registry-url file:///tmp/ades-registry/index.json
ades list packs --registry-url https://repo.adestool.com/index.json
ades serve
ades release versions
ades release sync-version 0.1.0
ades release verify --output-dir ./dist/release
ades release manifest --output-dir ./dist/release
ades release validate --output-dir ./.docdex/artifacts/release-validation
ades release publish --manifest-path ./dist/release/release-manifest.0.1.0.json --dry-run
ades registry build ./src/ades/resources/registry/packs/general-en ./src/ades/resources/registry/packs/finance-en --output-dir /tmp/ades-registry
ades tag "Apple CEO Tim Cook announced quarterly earnings."
ades tag --file ./notes/report.html --pack finance-en
ades tag --file ./notes/report.html --pack finance-en --output-dir ./outputs
ades tag-files ./notes/report-a.html ./notes/report-b.html --pack finance-en --output-dir ./outputs
ades tag-files --directory ./corpus --glob "./extras/*.html" --pack finance-en --output-dir ./outputs
ades tag-files --directory ./corpus --include "*earnings*.html" --exclude "*draft*" --pack finance-en
ades tag-files ./notes/report-a.html ./notes/report-b.html --pack finance-en --output-dir ./outputs --write-manifest
ades tag-files --directory ./corpus --pack finance-en --max-files 100 --max-bytes 5000000
ades tag-files --manifest-input ./outputs/batch.finance-en.ades-manifest.json --manifest-mode resume
ades tag-files --directory ./corpus --manifest-input ./outputs/batch.finance-en.ades-manifest.json --skip-unchanged --pack finance-en
ades tag-files --directory ./corpus --manifest-input ./outputs/batch.finance-en.ades-manifest.json --skip-unchanged --reuse-unchanged-outputs --pack finance-en
ades tag-files --directory ./corpus --manifest-input ./outputs/batch.finance-en.ades-manifest.json --skip-unchanged --reuse-unchanged-outputs --repair-missing-reused-outputs --pack finance-en
```

## Current Usage

- Library mode: import `ades` directly from Python for in-process enrichment.
- CLI mode: pull packs, tag text, inspect installed packs, and run the local service.
- npm wrapper mode: install `ades-cli`, then let the wrapper bootstrap a user-local Python runtime on first execution before delegating to the Python `ades` command.
- Local service mode: run `ades serve` and expose a localhost API that multiple tools or agents can share.
- Hosted production-server mode: documented in [`docs/production_deployment.md`](/home/wodo/apps/ades/docs/production_deployment.md) with a PostgreSQL-backed deployment and public registry hosting.

Coordinated release publication is manifest-driven. `ades release publish` reads a validated manifest from `ades release validate` and then publishes the recorded wheel, sdist, and npm tarball without rebuilding them. The publish flow reads credentials and registry overrides from `ADES_RELEASE_TWINE_USERNAME`, `ADES_RELEASE_TWINE_PASSWORD`, `ADES_RELEASE_TWINE_TOKEN`, `ADES_RELEASE_TWINE_REPOSITORY_URL`, `ADES_RELEASE_NPM_TOKEN`, `ADES_RELEASE_NPM_REGISTRY`, and `ADES_RELEASE_NPM_ACCESS`.

```python
from ades import (
    build_registry,
    publish_release,
    pull_pack,
    release_versions,
    sync_release_version,
    tag,
    tag_file,
    tag_files,
    validate_release,
    verify_release,
    write_release_manifest,
)

pull_pack("finance-en")
registry = build_registry(
    [
        "./src/ades/resources/registry/packs/general-en",
        "./src/ades/resources/registry/packs/finance-en",
    ],
    output_dir="./published-registry",
)
pull_pack("finance-en", registry_url=registry.index_url)
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

release = verify_release(output_dir="./dist/release")
print(release.model_dump())

validated = validate_release(output_dir="./.docdex/artifacts/release-validation")
print(validated.model_dump())

publish = publish_release(manifest_path=validated.manifest_path, dry_run=True)
print(publish.model_dump())

versions = release_versions()
print(versions.model_dump())

sync_release_version("0.1.0")
manifest = write_release_manifest(output_dir="./dist/release")
print(manifest.manifest_path)
```

## Current Runtime Surface

- CLI commands: `ades pull`, `ades pull --registry-url`, `ades registry build`, `ades release versions`, `ades release sync-version`, `ades release verify`, `ades release manifest`, `ades release validate`, `ades release publish`, `ades serve`, `ades tag`, `ades tag-files`, `ades status`, `ades list packs`, `ades packs list`, `ades packs list --available --registry-url`, `ades packs activate`, `ades packs deactivate`, `ades packs lookup`
- npm wrapper package: `npm/ades-cli`, installed command `ades`, first-run bootstrap into a user-local Python virtual environment
- Local API endpoints: `GET /healthz`, `GET /v0/status`, `GET /v0/packs`, `GET /v0/packs/{pack}`, `POST /v0/packs/{pack}/activate`, `POST /v0/packs/{pack}/deactivate`, `POST /v0/registry/build`, `GET /v0/installers/npm`, `GET /v0/release/versions`, `POST /v0/release/sync-version`, `POST /v0/release/verify`, `POST /v0/release/manifest`, `POST /v0/release/validate`, `POST /v0/release/publish`, `GET /v0/lookup`, `POST /v0/tag`, `POST /v0/tag/file`, `POST /v0/tag/files`
- Local runtime status now reports `runtime_target=local` and `metadata_backend=sqlite`
- Public Python helpers: `build_registry`, `npm_installer_info`, `publish_release`, `release_versions`, `sync_release_version`, `validate_release`, `verify_release`, `write_release_manifest`, `pull_pack`, `list_packs`, `get_pack`, `activate_pack`, `deactivate_pack`, `lookup_candidates`, `status`, `tag`, `tag_file`, `tag_files`, `create_service_app`
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
- deterministic entity relevance scoring plus topic evidence metadata on the local tag response surface
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
- source fingerprinting plus unchanged-file skip detection so repeated local corpus runs can avoid re-tagging files whose content has not changed since the last saved manifest
- unchanged-output reuse so skipped unchanged files can optionally carry forward prior manifest output metadata in a dedicated `reused_items` collection during reruns
- reused-output verification so carried-forward manifest items now report `saved_output_exists` and emit deterministic warnings when the referenced output file is unavailable
- rerun repair mode so missing reused outputs can be regenerated automatically and recorded with `repaired_reused_output:<path>` warnings plus `repaired_reused_output_count`
- rerun diff reporting so manifest-driven corpus reruns now emit a structured `rerun_diff` summary with changed, newly processed, reused, repaired, and skipped inputs
- manifest lineage metadata and stable run identifiers so saved batch manifests now record `run_id`, `root_run_id`, `parent_run_id`, `source_manifest_path`, and `created_at` across rerun chains
- static pack publication and remote registry tooling so local pack directories can be exported as a file-based registry and consumed immediately by `ades pull` through `--registry-url`
- a real `ades-cli` npm wrapper package with first-run Python runtime bootstrap into a user-local virtual environment
- local release verification for Python wheel/sdist plus npm tarball outputs with artifact hashes, file sizes, and version-alignment checks before publication
- coordinated release version synchronization across `src/ades/version.py`, `pyproject.toml`, and `npm/ades-cli/package.json`, plus persisted release-manifest generation for one verified local build
- one-command release validation through `ades release validate`, which runs the local pytest suite, writes a coordinated release manifest, and powers the committed `.docdex/run-tests.json` configuration
- manifest-driven coordinated release publication through `ades release publish`, the public Python API, and `POST /v0/release/publish`, with dry-run support and explicit env-var credential handling for Python and npm publication
- initial tests for installer, tagger, lookup, public API, and service behavior
- categorized test coverage under `tests/unit`, `tests/component`, `tests/integration`, and `tests/api`

The remaining local-tool priorities are tracked in [`docs/next_work_items.md`](/home/wodo/apps/ades/docs/next_work_items.md). The current queue now starts with lightweight local candidate search, while heavier NLP-backed enrichment is explicitly deferred past the deterministic `v0.1.0` baseline.
