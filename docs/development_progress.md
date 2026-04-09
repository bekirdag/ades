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

### 24. Coordinated release version synchronization and release-manifest generation

- Added coordinated release version inspection so the local tool can report whether `src/ades/version.py`, `pyproject.toml`, and `npm/ades-cli/package.json` are still synchronized before a publication step.
- Added coordinated release version synchronization so one command can update the Python runtime version file, the Python package version, and the npm wrapper version together.
- Added persisted release-manifest generation so a local release build now writes one stable JSON artifact that captures the coordinated version state, optional sync response, and the verified wheel/sdist/npm tarball metadata.
- Exposed the coordinated release workflow consistently through the public Python API, the CLI commands `ades release versions`, `ades release sync-version`, and `ades release manifest`, plus matching local HTTP endpoints.

### 25. Repo-local Docdex test runner and one-command release validation

- Added a shared release-validation flow that runs the local pytest suite first and only writes the coordinated release manifest when tests pass.
- Exposed that flow consistently through the public Python API, the CLI command `ades release validate`, and the local HTTP endpoint `POST /v0/release/validate`.
- Replaced the placeholder `.docdex/run-tests.json` with a committed repo-local runner configuration that executes `ades release validate` through `python -m ades` with `PYTHONPATH=src`.
- Updated the repo ignore rules so `.docdex/run-tests.json` is versioned while generated `.docdex` artifacts remain ignored.

### 26. Clean-environment install smoke validation for release artifacts

- Extended release verification so the built Python wheel is installed into a fresh virtual environment and the installed `ades status` command is executed outside the source checkout.
- Extended release verification so the built npm tarball is installed under a clean npm prefix, bootstraps the matching Python wheel into its runtime directory, and then executes `ades status` through the installed wrapper.
- Added explicit smoke-validation metadata to the public Python API, CLI, and localhost service release responses, including per-command stdout/stderr, reported version, and deterministic warnings when install or invocation checks fail.
- Added categorized unit, component, integration, and API coverage for smoke-install success paths, smoke-install disable flags, and propagated manifest/validate response shapes.

### 27. Coordinated release publication from validated manifests

- Added a manifest-driven publish workflow that reads a validated release manifest and publishes the recorded Python wheel/sdist plus npm tarball without rebuilding them.
- Gated publication on the manifest carrying explicit validation metadata from `ades release validate`, so release publication now fails fast when callers try to publish from a manifest that did not come from the validated workflow.
- Exposed the coordinated publish flow consistently through the public Python API, the CLI command `ades release publish`, and the localhost service endpoint `POST /v0/release/publish`.
- Added dry-run support plus explicit environment-variable credential and registry handling for `twine` and `npm publish`, and added categorized unit, component, integration, and API coverage for the new workflow.

### 28. Deterministic relevance scoring and stronger topic evidence

- Closed the open `v0.1.0` tagging-scope decision in favor of the current deterministic-only runtime instead of widening the release to a new `spaCy` / `GLiNER` / `FlashText` dependency stack.
- Extended entity matches with an explicit deterministic `relevance` score so the local tagger now emits both confidence and rank-oriented match weight on the same stable response surface.
- Extended topic matches with evidence counts and contributing entity-label summaries so topic output now carries more than a bare label-plus-score pair.
- Added categorized unit, component, integration, and API coverage for deterministic relevance ordering, topic evidence metadata, and the public response contract.

### 29. Production-ready module completion standard

- Updated the repo requirements so completed modules are now expected to be production-ready and functionally usable when closed, not merely scaffolded or partially wired.
- Aligned the implementation plan with that rule by making module completion explicitly require operational behavior, stable contracts, validation, and categorized tests.
- Kept placeholders limited to explicitly deferred seams such as the future PostgreSQL-backed production server path rather than treating scaffolds as completed delivery for active local-tool modules.

### 30. Lightweight local candidate search/index

- Added broader metadata lookup in the local SQLite store so `ades` can resolve multi-term candidate searches across alias text, labels, and source-domain metadata instead of only whole-query substring matching.
- Implemented `SQLite FTS5` search tables for aliases and rules when the local SQLite build supports them, while preserving deterministic fallback behavior through standard SQLite queries when FTS is unavailable.
- Kept exact-alias lookup unchanged for the tagger hot path and exposed the broader search through the existing public API, `ades packs lookup`, and `GET /v0/lookup` rather than introducing a second lookup surface.
- Added categorized unit, component, integration, and API coverage for the broader candidate-search behavior and verified the repo through `docdexd run-tests --repo /home/wodo/apps/ades`, including full `pytest -q` with `142 passed`.

### 31. Rollback-safe pack install recovery

- Hardened `src/ades/packs/installer.py` so fresh installs extract into hidden staging directories and pack updates move the current installed pack into a hidden backup directory before replacement.
- Added rollback logic so checksum-adjacent or post-replacement failures now clean up temporary directories, remove broken fresh installs, and restore the previous installed pack when an update fails after replacement begins.
- Kept the existing public Python API, CLI `ades pull`, and local service pack surfaces unchanged while improving the underlying pack lifecycle safety.
- Added categorized unit, component, integration, and API coverage for failed fresh installs, failed update recovery, and real static-registry artifact corruption paths.

### 32. Same-version pull metadata repair for installed packs

- Hardened repeated same-version pulls so `src/ades/packs/installer.py` now refreshes installed pack metadata from the on-disk pack directory before deciding to skip or reactivate a pack.
- Extended `src/ades/storage/registry_db.py` and `src/ades/packs/registry.py` so pack metadata resync can preserve the current active state while repairing stale alias, rule, label, and dependency rows in the local SQLite store.
- Kept the existing `ades pull`, public Python API, CLI lookup, and local service lookup surfaces unchanged while making repeated pulls heal stale same-version metadata drift instead of leaving SQLite search state behind the filesystem state.
- Added categorized unit, component, integration, and API coverage for repeated same-version pulls that repair stale installed-pack alias metadata before returning a skip result.
- Verified the item through focused lifecycle tests plus the repo-standard `docdexd run-tests --repo /home/wodo/apps/ades` release-validation flow, including `161 passed` under `pytest -q` and successful Python/npm smoke-install checks.

### 33. Dependency-aware activation and deactivation guardrails

- Hardened `src/ades/api.py` so pack activation now recursively reactivates installed dependency chains, while pack deactivation rejects requests that would leave active dependent packs pointing at an inactive dependency.
- Updated the CLI and local service adapters in `src/ades/cli.py` and `src/ades/service/app.py` so dependency-guardrail failures surface cleanly as CLI errors and HTTP `400` responses instead of partially mutating pack state.
- Kept the existing `ades packs activate`, `ades packs deactivate`, public Python API, and local service routes unchanged while preventing users from deactivating shared dependencies out from under active packs and while making activation heal inactive dependency chains without a repull.
- Added categorized unit, component, integration, and API coverage for blocked dependency deactivation and recursive dependency reactivation behavior.
- Verified the item through focused lifecycle/API tests plus the repo-standard `docdexd run-tests --repo /home/wodo/apps/ades` release-validation flow, including `169 passed` under `pytest -q` and successful Python/npm smoke-install checks.

### 34. Clean-environment pull-and-tag smoke validation for release artifacts

- Extended `src/ades/release.py` so release smoke verification now runs the installed CLI through `ades status`, `ades pull general-en`, and `ades tag "Contact OpenAI via smoke@example.com"` instead of stopping at `status` only.
- Added deterministic pull/tag parsing and warning logic so release verification now proves bundled-registry pull and post-pull tagging behavior for both the installed Python wheel and the installed npm wrapper in clean environments.
- Extended the shared release smoke response model in `src/ades/service/models.py` so callers can inspect the pull command result, tag command result, pulled pack ids, and tagged labels for each smoke-install artifact.
- Updated the fake release runner and categorized unit, component, integration, and API coverage so the release verify/validate surfaces exercise the new pull/tag smoke path and explicit tag-failure warnings.
- Verified the item through focused release verification/validation tests plus `python -m compileall src/ades`.

### 35. Installed-pack list repair for missing SQLite metadata rows

- Hardened `src/ades/packs/registry.py` so `PackRegistry.list_installed_packs()` now reconciles on-disk `packs/*/manifest.json` directories back into the metadata store even when other valid installed-pack rows still exist in SQLite.
- Kept the existing public `list_packs`, CLI `ades packs list`, and local service `/v0/packs` contracts unchanged while making those list-driven surfaces repair missing alias, rule, label, and dependency metadata for packs that still exist on disk.
- Added a shared SQLite test helper in `tests/pack_registry_helpers.py` plus categorized unit, component, integration, and API coverage for the recovery path where a single installed-pack row is deleted from `storage_root/registry/ades.db` while sibling pack rows remain.
- Verified the item through focused list-repair lifecycle tests with `21 passed`, `python -m compileall src/ades`, and the repo-standard `docdexd run-tests --repo /home/wodo/apps/ades` release-validation flow, including `174 passed` under `pytest -q` plus successful Python/npm smoke-install checks.

### 36. Clean-environment serve smoke validation for release artifacts

- Extended `src/ades/release.py` so clean-environment release smoke verification now launches installed-artifact `ades serve` processes for both the built wheel and npm wrapper on free loopback ports, probes `/healthz` and `/v0/status`, then shuts the service down again.
- Extended the shared smoke response model in `src/ades/service/models.py` so each artifact now exposes additive `serve`, `serve_healthz`, and `serve_status` command results plus `served_pack_ids` alongside the existing install, status, pull, and tag results.
- Added deterministic fake serve-smoke support in `tests/release_helpers.py` and extended categorized unit, component, integration, and API release-verify coverage so successful serve startup and explicit serve-failure warnings are exercised without changing the public release command contracts.
- Verified the item through focused release verification/validation tests with `23 passed`, `python -m compileall src/ades`, and the repo-standard `docdexd run-tests --repo /home/wodo/apps/ades` release-validation flow, including `175 passed` under `pytest -q` plus successful wheel/npm clean-environment serve probes.

### 37. Clean-environment bootstrap recovery smoke validation

- Extended `src/ades/release.py` so clean-environment release smoke verification now deletes `storage_root/registry/ades.db` after pull/tag, proves `ades status` can bootstrap `general-en` back from on-disk pack manifests, deletes the registry metadata again, and then starts `ades serve`.
- Extended the shared smoke response model in `src/ades/service/models.py` so each artifact now exposes additive `recovery_status` command results plus `recovered_pack_ids` alongside the existing install, status, pull, tag, and serve results.
- Updated `tests/release_helpers.py` and the categorized release verify tests so successful metadata-bootstrap recovery and explicit recovery-status failures are covered across unit, component, integration, and API layers without changing the public release command contracts.
- Verified the item through focused release verification/validation tests with `24 passed`, `python -m compileall src/ades`, and the repo-standard `docdexd run-tests --repo /home/wodo/apps/ades` release-validation flow, including `176 passed` under `pytest -q` plus successful wheel/npm clean-environment bootstrap recovery checks.

### 38. Lookup-driven repair for missing installed-pack SQLite rows

- Hardened `src/ades/packs/registry.py` so `PackRegistry.lookup_candidates()` now retries on lookup misses after reconciling on-disk `packs/*/manifest.json` directories back into the metadata store, instead of requiring a prior `list` call to repair a missing installed-pack row.
- Kept the existing public Python API, CLI `ades packs lookup`, and local service `GET /v0/lookup` contracts unchanged while making direct alias lookup recover a pack like `finance-en` when its SQLite row is missing but its pack files still exist on disk.
- Updated the categorized pack-reactivation coverage in `tests/unit/test_pack_install_reactivation.py`, `tests/component/test_cli_pack_reactivation.py`, `tests/integration/test_pack_reactivation_api.py`, and `tests/api/test_pack_reactivation_endpoint.py` so those surfaces now prove lookup-driven repair and verify the restored row remains visible to list calls as a side effect.
- Verified the item through focused lookup-recovery tests with `21 passed`, `python -m compileall src/ades`, and the repo-standard `docdexd run-tests --repo /home/wodo/apps/ades` release-validation flow, including `176 passed` under `pytest -q` plus successful wheel/npm clean-environment smoke validation.

### 39. Tagger regression coverage for partial installed-pack row loss

- Added explicit alias-driven regression coverage so the tagger surfaces now prove `finance-en` entity extraction still works after the installed-pack SQLite row is deleted while the on-disk pack files remain available.
- Kept product code unchanged because the runtime already recovered through the current registry/runtime seam; this item closes the coverage gap that previously left the tagger path unproven under the same partial metadata-loss condition.
- Extended the categorized tagger tests in `tests/test_tagger.py`, `tests/component/test_lookup_driven_tagger.py`, `tests/integration/test_lookup_driven_tag_api.py`, and `tests/api/test_tag_lookup_aliases.py` with alias-only assertions on `AAPL` and `NASDAQ` after deleting the `finance-en` row from `storage_root/registry/ades.db`.
- Verified the item through focused tagger-regression tests with `9 passed`, `python -m compileall src/ades`, and the repo-standard `docdexd run-tests --repo /home/wodo/apps/ades` release-validation flow, including `180 passed` under `pytest -q` plus successful wheel/npm clean-environment smoke validation.

### 40. Dependency-bearing release smoke validation for installed artifacts

- Hardened `src/ades/release.py` so clean-environment release smoke verification now pulls `finance-en`, tags with `--pack finance-en`, and requires both `finance-en` and its dependency `general-en` to remain visible through pull, recovery, and serve status checks instead of only proving a single `general-en` pack path.
- Tightened the smoke contract so the installed wheel and npm wrapper now must produce `organization`, `ticker`, `exchange`, and `currency_amount` labels from the pulled finance pack chain, which proves dependency aliases plus finance-specific alias/rule behavior in the packaged artifacts.
- Updated `tests/release_helpers.py` and the categorized release verification tests in `tests/unit/test_release_verification.py`, `tests/component/test_cli_release_verify.py`, `tests/integration/test_release_verify_api.py`, and `tests/api/test_release_verify_endpoint.py` so the finance-pack dependency chain is covered consistently across fake-runner and real release-validation flows.
- Verified the item through focused release verification tests with `17 passed`, `python -m compileall src/ades`, and the repo-standard `docdexd run-tests --repo /home/wodo/apps/ades` release-validation flow, including `180 passed` under `pytest -q` plus successful clean-environment wheel/npm finance-pack pull, tag, recovery, and serve checks.

### 41. Live service-tag smoke validation for installed artifacts

- Extended `src/ades/release.py` so clean-environment release smoke verification now sends a real `POST /v0/tag` request through the served wheel and npm wrapper after finance-pack pull and metadata-bootstrap recovery, instead of stopping at `/healthz` and `/v0/status`.
- Extended the shared smoke response model in `src/ades/service/models.py` so each packaged-artifact smoke result now exposes additive `serve_tag` command output plus `serve_tagged_labels`, which keeps the release verify/validate contracts additive while surfacing the live service-tag result explicitly.
- Updated `tests/release_helpers.py` and the categorized release verification tests in `tests/unit/test_release_verification.py`, `tests/component/test_cli_release_verify.py`, `tests/integration/test_release_verify_api.py`, and `tests/api/test_release_verify_endpoint.py` so successful served tagging and explicit live `/v0/tag` failure warnings are covered across unit, component, integration, and API layers.
- Verified the item through focused release verification tests with `18 passed`, `python -m compileall src/ades`, and the repo-standard `docdexd run-tests --repo /home/wodo/apps/ades` release-validation flow, including `181 passed` under `pytest -q` plus successful clean-environment wheel/npm live `/v0/tag` smoke checks.

### 42. Live file-tag smoke validation for installed artifacts

- Extended `src/ades/release.py` so clean-environment release smoke verification now writes a deterministic local HTML file and sends a real `POST /v0/tag/file` request through the served wheel and npm wrapper after finance-pack pull, metadata-bootstrap recovery, and serve startup.
- Extended the shared smoke response model in `src/ades/service/models.py` so each packaged-artifact smoke result now exposes additive `serve_tag_file` command output plus `serve_tag_file_labels`, which keeps the release verify/validate contracts additive while surfacing the live file-tag result explicitly.
- Updated `tests/release_helpers.py` and the categorized release verification tests in `tests/unit/test_release_verification.py`, `tests/component/test_cli_release_verify.py`, `tests/integration/test_release_verify_api.py`, and `tests/api/test_release_verify_endpoint.py` so successful served file tagging and explicit live `/v0/tag/file` failure warnings are covered across unit, component, integration, and API layers.
- Verified the item through focused release verification tests with `19 passed`, `python -m compileall src/ades`, and the repo-standard `docdexd run-tests --repo /home/wodo/apps/ades` release-validation flow, including `182 passed` under `pytest -q` plus successful clean-environment wheel/npm live `/v0/tag/file` smoke checks.

### 43. Live batch file-tag smoke validation for installed artifacts

- Extended `src/ades/release.py` so clean-environment release smoke verification now writes two deterministic local HTML inputs and sends a real `POST /v0/tag/files` request through the served wheel and npm wrapper after finance-pack pull, metadata-bootstrap recovery, and serve startup.
- Extended the shared smoke response model in `src/ades/service/models.py` so each packaged-artifact smoke result now exposes additive `serve_tag_files` command output plus `serve_tag_files_labels`, which keeps the release verify/validate contracts additive while surfacing the live batch file-tag result explicitly.
- Updated `tests/release_helpers.py` and the categorized release verification tests in `tests/unit/test_release_verification.py`, `tests/component/test_cli_release_verify.py`, `tests/integration/test_release_verify_api.py`, and `tests/api/test_release_verify_endpoint.py` so successful served batch tagging and explicit live `/v0/tag/files` failure warnings are covered across unit, component, integration, and API layers.
- Verified the item through focused release verification tests with `20 passed`, `python -m compileall src/ades`, and the repo-standard `docdexd run-tests --repo /home/wodo/apps/ades` release-validation flow, including `183 passed` under `pytest -q` plus successful clean-environment wheel/npm live `/v0/tag/files` smoke checks.

### 44. Live batch-manifest write smoke validation for installed artifacts

- Extended `src/ades/release.py` so the live `/v0/tag/files` release smoke now requests persisted batch outputs with `output.directory` plus `write_manifest=true`, then requires the served wheel and npm wrapper to return one `saved_manifest_path` and both per-item `saved_output_path` values after finance-pack pull, metadata-bootstrap recovery, and serve startup.
- Kept the release verify/validate surface additive by reusing the existing `serve_tag_files` command result while tightening the internal smoke pass/fail checks and warning generation for missing manifest paths, invalid manifest-path suffixes, and missing persisted batch outputs.
- Updated `tests/release_helpers.py` and the categorized release verification tests in `tests/unit/test_release_verification.py`, `tests/component/test_cli_release_verify.py`, `tests/integration/test_release_verify_api.py`, and `tests/api/test_release_verify_endpoint.py` so successful served batch-manifest writes and explicit missing-manifest warnings are covered across unit, component, integration, and API layers.
- Verified the item through focused release verification tests with `21 passed`, `python -m compileall src/ades`, and the repo-standard `docdexd run-tests --repo /home/wodo/apps/ades` release-validation flow, including `184 passed` under `pytest -q` plus successful clean-environment wheel/npm live `/v0/tag/files` manifest-write smoke checks.

### 45. Live batch-manifest path override smoke validation for installed artifacts

- Extended `src/ades/release.py` so the live `/v0/tag/files` release smoke now provides an explicit `output.manifest_path` override instead of relying on the default manifest filename under the batch output directory, then requires the served wheel and npm wrapper to return that exact saved manifest path.
- Kept the release verify/validate surface unchanged by reusing the existing `serve_tag_files` result while tightening the internal smoke pass/fail checks and warning generation so a packaged artifact now fails validation if the service ignores the requested manifest path and falls back to the default filename.
- Updated `tests/release_helpers.py` and the categorized release verification tests in `tests/unit/test_release_verification.py`, `tests/component/test_cli_release_verify.py`, `tests/integration/test_release_verify_api.py`, and `tests/api/test_release_verify_endpoint.py` so successful explicit manifest-path overrides and explicit invalid-manifest-path warnings are covered across unit, component, integration, and API layers.
- Verified the item through focused release verification tests with `22 passed`, `python -m compileall src/ades`, and the repo-standard `docdexd run-tests --repo /home/wodo/apps/ades` release-validation flow, including `185 passed` under `pytest -q` plus successful clean-environment wheel/npm live `/v0/tag/files` manifest-path override smoke checks.

### 46. Live batch-manifest replay smoke validation for installed artifacts

- Extended `src/ades/release.py` so the live `/v0/tag/files` release smoke now replays the manifest it just wrote through the served wheel and npm wrapper, writing an explicit child replay manifest and requiring replay-summary plus lineage fields instead of validating only the initial batch persistence call.
- Extended the shared smoke response model in `src/ades/service/models.py` so each packaged-artifact smoke result now exposes additive `serve_tag_files_replay` command output plus `serve_tag_files_replay_labels`, while the warning flow now reports replay-specific manifest-input, replay-mode, lineage, and child-manifest-path failures deterministically.
- Updated `tests/release_helpers.py` and the categorized release verification tests in `tests/unit/test_release_verification.py`, `tests/component/test_cli_release_verify.py`, `tests/integration/test_release_verify_api.py`, and `tests/api/test_release_verify_endpoint.py` so successful manifest replay plus explicit invalid replay-manifest-input warnings are covered across unit, component, integration, and API layers.
- Verified the item through focused release verification tests with `23 passed`, `python -m compileall src/ades`, and the repo-standard `docdexd run-tests --repo /home/wodo/apps/ades` release-validation flow, including `186 passed` under `pytest -q` plus successful clean-environment wheel/npm live `/v0/tag/files` manifest replay and lineage smoke checks.

### 47. Live batch-manifest replay summary-count smoke validation for installed artifacts

- Extended `src/ades/release.py` so the replayed live `/v0/tag/files` release smoke now requires `summary.manifest_candidate_count` and `summary.manifest_selected_count` to match the two deterministic finance-pack smoke files through both the served wheel and the npm wrapper.
- Kept the release verify/validate surface additive by reusing the existing `serve_tag_files_replay` result while tightening the internal smoke pass/fail checks and warning generation for missing or invalid replay summary counts.
- Updated `tests/release_helpers.py` and the categorized release verification tests in `tests/unit/test_release_verification.py`, `tests/component/test_cli_release_verify.py`, `tests/integration/test_release_verify_api.py`, and `tests/api/test_release_verify_endpoint.py` so successful replay summary-count validation and explicit invalid replay-count warnings are covered across unit, component, integration, and API layers.
- Verified the item through focused release verification tests with `25 passed`, `python -m compileall src/ades`, and the repo-standard `docdexd run-tests --repo /home/wodo/apps/ades` release-validation flow, including `188 passed` under `pytest -q` plus successful clean-environment wheel/npm live `/v0/tag/files` replay summary-count smoke checks.

## Current Local Tool Capabilities

- `ades pull <pack>`
- `ades pull <pack> --registry-url <url>`
- `ades serve`
- `ades release verify --output-dir <dir>`
- `ades release versions`
- `ades release sync-version <version>`
- `ades release manifest --output-dir <dir>`
- `ades release validate --output-dir <dir>`
- `ades release publish --manifest-path <path>`
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
- `GET /v0/release/versions`
- `POST /v0/release/sync-version`
- `POST /v0/release/verify`
- `POST /v0/release/manifest`
- `POST /v0/release/validate`
- `POST /v0/release/publish`
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

- Extend installed-artifact live-service smoke so `POST /v0/tag/files` replays the manifest it just wrote through the served wheel and npm wrapper, proving packaged clean-environment manifest replay and lineage behavior after finance-pack pull and metadata recovery.
