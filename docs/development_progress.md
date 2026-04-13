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

### 31. Bounded real-source `general-en` follow-on expansion

- Widened the bounded live `general-en` Wikidata seed set with `Sam Altman` and `Google DeepMind` while preserving the zero-ambiguity dependency contract required by `medical-en`.
- Added a follow-on generated-pack quality case that proves the new organization through the low-noise alias `DeepMind Technologies`, which avoids the existing standalone `Google` entity from producing an unexpected extra hit in the quality gate.
- Updated the raw snapshot fixtures plus the categorized unit, component, integration, and API coverage so the synthetic builder/source/quality surfaces stay aligned with the widened live seed set.
- Revalidated the bounded live `general-en` slice on `2026-04-10`, which now passes cleanly at `wikidata_entity_count=11`, `entity_record_count=15`, `alias_count=226`, `ambiguous_alias_count=0`, `expected_recall=1.0`, and `precision=1.0`.

### 32. Bounded real-source `general-en` person-alias pruning

- Added a conservative alias-pruning rule in `src/ades/packs/general_bundle.py` that drops plain single-token aliases for multi-token person entities before normalized bundle generation.
- Used that new headroom to widen the bounded live `general-en` Wikidata seed set with `Sundar Pichai` and `Demis Hassabis` in `src/ades/packs/general_sources.py` without weakening the zero-unexpected-hit quality gate.
- Added a generated-pack quality case that proves the retained multi-token alias path through `Sundara Pichai`, while the builder tests now assert the noisy single-token aliases `Pichai` and `Hassabis` are absent from the normalized bundle.
- Revalidated the bounded live `general-en` slice on `2026-04-10`, which now passes cleanly at `wikidata_entity_count=13`, `entity_record_count=17`, `alias_count=232`, `ambiguous_alias_count=0`, `expected_recall=1.0`, and `precision=1.0`.

### 33. Bounded real-source `general-en` retained-alias person expansion

- Widened the bounded live `general-en` Wikidata seed set with `Mira Murati` and `Ilya Sutskever` in `src/ades/packs/general_sources.py`, choosing the next pair from the same low-noise AI/tech cluster so the bounded dependency pack could keep growing without opening a new organization-pruning seam.
- Added a generated-pack quality case in `src/ades/packs/general_quality.py` that proves the live retained-alias path through `Ermira Murati` and `Ilya Efimovich Sutskever`, rather than only relying on fetch-count deltas.
- Updated the raw snapshot fixtures plus the categorized unit, component, integration, and API coverage so the builder/source/quality surfaces stay aligned with the widened live seed set.
- Revalidated the bounded live `general-en` slice on `2026-04-10`, which now passes cleanly at `wikidata_entity_count=15`, `entity_record_count=19`, `alias_count=236`, `ambiguous_alias_count=0`, `expected_recall=1.0`, and `precision=1.0`.

### 34. Bounded real-source `general-en` curated location expansion

- Extended the curated source slice in `src/ades/packs/general_sources.py` with `Tokyo`, using the low-noise retained alias `Tokio` instead of widening the raw GeoNames row set and inheriting additional short-code alias noise.
- Added a generated-pack quality case in `src/ades/packs/general_quality.py` that proves the curated location alias path through `Tokio`.
- Updated the raw snapshot fixtures plus the categorized unit, component, integration, and API coverage so the bundle/source/quality surfaces stay aligned with the expanded curated slice.
- Revalidated the bounded live `general-en` slice on `2026-04-10`, which now passes cleanly at `wikidata_entity_count=15`, `curated_entity_count=3`, `entity_record_count=20`, `alias_count=238`, `ambiguous_alias_count=0`, `expected_recall=1.0`, and `precision=1.0`.

### 31. Real finance-source ingestion and live bundle tuning

- Added a reproducible finance-source fetcher in `src/ades/packs/finance_sources.py` plus public Python, CLI, and localhost service surfaces so dated SEC and Nasdaq source snapshots can be downloaded directly into `/mnt/githubActions/ades_big_data/pack_sources/raw/finance-en/<snapshot>/` instead of being staged by hand.
- Added immutable snapshot metadata through `sources.fetch.json`, including exact upstream URLs, fetched paths, SHA-256 digests, byte sizes, and the fetch user agent, while keeping the raw-source layout outside the repo.
- Extended generated-pack alias pruning with per-record `blocked_aliases` support and tuned the finance bundle inputs so live issuer aliases no longer suppress exchange aliases and noisy real-world ticker aliases like `ON` and `USD` are dropped before they reach the runtime pack.
- Validated the first live finance snapshot dated `2026-04-10`: the real bundle now emits `22881` normalized entity records, the quality gate passes at `expected_recall=1.0` and `precision=1.0`, and `refresh-generated-packs` produces a publishable finance-only registry release under `/mnt/githubActions/ades_big_data/pack_releases/finance-en-2026-04-10`.

### 31. Real medical-source and general-source live bundle tuning

- Added a reproducible medical-source fetcher in `src/ades/packs/medical_sources.py` plus public Python, CLI, and localhost service surfaces so dated Disease Ontology, HGNC, UniProt, and ClinicalTrials.gov snapshots can be downloaded directly into `/mnt/githubActions/ades_big_data/pack_sources/raw/medical-en/<snapshot>/` with immutable `sources.fetch.json` metadata.
- Added a reproducible general-source fetcher in `src/ades/packs/general_sources.py` plus public Python, CLI, and localhost service surfaces so bounded live Wikidata and GeoNames inputs can be downloaded directly into `/mnt/githubActions/ades_big_data/pack_sources/raw/general-en/<snapshot>/` with matching immutable `sources.fetch.json` metadata.
- Tightened `src/ades/packs/medical_bundle.py` so compact symbolic disease aliases and compact symbolic protein aliases are pruned before generation, while the earlier HGNC lowercase-alias guard keeps obvious gene noise out of the normalized bundle.
- Generalized refresh-time ambiguity handling in `src/ades/packs/refresh.py` so omitted `max_ambiguous_aliases` now resolves per-pack defaults instead of forcing one global budget: `general-en=0`, `medical-en=25`, and `finance-en=300`.
- Validated the first live general snapshot dated `2026-04-10`: the rebuilt bundle now emits `8` normalized entity records, the quality gate passes at `expected_recall=1.0` and `precision=1.0`, and the bounded real-source general dependency stays at `alias_count=96` with `ambiguous_alias_count=0`.
- Validated the first live medical snapshot dated `2026-04-10`: the rebuilt bundle now emits `57765` normalized entity records, the quality gate passes at `expected_recall=1.0` and `precision=1.0`, and the live ambiguity baseline fell from `2020` to `22` with `dropped_alias_ratio=0.0003`.
- Replaced the helper general dependency in the publication rehearsal: `refresh-generated-packs` now produces a fully real-source combined `general-en` + `medical-en` registry release under `/mnt/githubActions/ades_big_data/pack_releases/general-medical-2026-04-10`.

### 32. Three-pack generated release rehearsal and object-storage publication helper

- Ran the first fully real-source three-pack refresh rehearsal with the dated live `finance-en`, `general-en`, and `medical-en` bundles, producing a publishable reviewed release under `/mnt/githubActions/ades_big_data/pack_releases/finance-general-medical-2026-04-10`.
- The combined release passes all three quality gates with the current live baselines: `finance-en alias_count=39101 ambiguous_alias_count=256`, `general-en alias_count=96 ambiguous_alias_count=0`, and `medical-en alias_count=127335 ambiguous_alias_count=22`.
- Added a reusable object-storage publication helper in `src/ades/packs/publish.py`, then exposed it through `ades.publish_generated_registry_release(...)`, `ades registry publish-generated-release`, and `POST /v0/registry/publish-generated-release` so reviewed generated registries can be synced reproducibly without ad hoc shell commands.
- Validated the new helper against the live Hetzner object store: the reviewed three-pack registry is now published at `s3://ades/generated-pack-releases/finance-general-medical-2026-04-10/` with `22` uploaded objects and the reviewed index at `s3://ades/generated-pack-releases/finance-general-medical-2026-04-10/index.json`.
- Extended the helper response with direct HTTPS registry candidates and validated the preferred virtual-hosted URL `https://ades.fsn1.your-objectstorage.com/generated-pack-releases/finance-general-medical-2026-04-10/index.json` plus the path-style fallback `https://fsn1.your-objectstorage.com/ades/generated-pack-releases/finance-general-medical-2026-04-10/index.json`.
- Proved clean consumer behavior against the published generated release: a fresh local runtime can pull `finance-en` directly from the object-storage registry URL, and a separate clean smoke can pull `medical-en` plus its `general-en` dependency and tag the expected medical entities without relying on repo-local bundle directories.
- Added categorized unit, component, integration, and API coverage for the new publication surface while keeping the existing deploy-owned hosted-registry promotion path authoritative.

### 33. Hosted-registry promotion spec and deploy preparation

- Added deploy-preparation logic in `src/ades/packs/publish.py` so `ades` can now prepare the production registry payload in two modes: rebuild the bundled starter registry as before, or materialize an approved reviewed registry URL from `src/ades/resources/registry/promoted-release.json`.
- The promoted path runs the shipped clean-consumer smoke against the reviewed registry URL before materialization, then rewrites the reviewed manifests and checksum-verified artifacts back into the deploy-owned `dist/registry` shape so the existing CI/CD workflow remains authoritative.
- Exposed the new surface through `ades.prepare_registry_deploy_release(...)`, `ades registry prepare-deploy-release`, and `POST /v0/registry/prepare-deploy-release`.
- Updated `.github/workflows/deploy-prod.yml` so the production deployment now calls `ades registry prepare-deploy-release --output-dir dist/registry`, preserving the old bundled-registry fallback whenever no promotion spec is committed.
- Added categorized unit, component, integration, and API coverage for promoted deploy preparation and the bundled fallback path.

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

- Extended `src/ades/release.py` so release smoke verification now runs the installed CLI through `ades status`, `ades pull general-en`, and `ades tag "Issuer Alpha said TICKA traded on EXCHX after USD 12.5 guidance."` instead of stopping at `status` only.
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
- Extended the categorized tagger tests in `tests/test_tagger.py`, `tests/component/test_lookup_driven_tagger.py`, `tests/integration/test_lookup_driven_tag_api.py`, and `tests/api/test_tag_lookup_aliases.py` with alias-only assertions on `TICKA` and `EXCHX` after deleting the `finance-en` row from `storage_root/registry/ades.db`.
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

### 48. Live batch-manifest replay payload-identity smoke validation for installed artifacts

- Extended `src/ades/release.py` so the replayed live `/v0/tag/files` release smoke now requires the top-level replay payload to keep `pack=\"finance-en\"` and `item_count=2` through both the served wheel and the npm wrapper, not just the replay summary metadata.
- Kept the release verify/validate surface additive by reusing the existing `serve_tag_files_replay` result while tightening the internal smoke pass/fail checks and warning generation for missing or invalid replay payload identity fields.
- Updated the categorized release verification tests in `tests/unit/test_release_verification.py`, `tests/component/test_cli_release_verify.py`, `tests/integration/test_release_verify_api.py`, and `tests/api/test_release_verify_endpoint.py` so successful replay payload-identity validation and explicit invalid replay pack/item-count warnings are covered across unit, component, integration, and API layers.
- Verified the item through focused release verification tests with `27 passed`, `python -m compileall src/ades`, and the repo-standard `docdexd run-tests --repo /home/wodo/apps/ades` release-validation flow, including `190 passed` under `pytest -q` plus successful clean-environment wheel/npm live `/v0/tag/files` replay payload-identity smoke checks.

### 49. Live batch-manifest replay rerun-diff smoke validation for installed artifacts

- Extended `src/ades/release.py` so the replayed live `/v0/tag/files` release smoke now requires top-level `rerun_diff.manifest_input_path` to match the original saved manifest path and `rerun_diff.changed` to match the deterministic two-file finance-pack smoke corpus through both the served wheel and the npm wrapper.
- Kept the release verify/validate surface additive by reusing the existing `serve_tag_files_replay` result while tightening the internal smoke pass/fail checks and warning generation for missing or invalid replay rerun-diff metadata.
- Updated `tests/release_helpers.py` and the categorized release verification tests in `tests/unit/test_release_verification.py`, `tests/component/test_cli_release_verify.py`, `tests/integration/test_release_verify_api.py`, and `tests/api/test_release_verify_endpoint.py` so successful replay rerun-diff validation and explicit invalid replay rerun-diff warnings are covered across unit, component, integration, and API layers.
- Verified the item through focused release verification tests with `29 passed`, `python -m compileall src/ades`, and the repo-standard `docdexd run-tests --repo /home/wodo/apps/ades` release-validation flow, including `192 passed` under `pytest -q` plus successful clean-environment wheel/npm live `/v0/tag/files` replay rerun-diff smoke checks.

### 50. Live batch-manifest replay empty rerun-diff collection smoke validation for installed artifacts

- Extended `src/ades/release.py` so the replayed live `/v0/tag/files` release smoke now requires top-level `rerun_diff.newly_processed`, `rerun_diff.reused`, `rerun_diff.repaired`, and `rerun_diff.skipped` to remain deterministic empty lists through both the served wheel and the npm wrapper.
- Kept the release verify/validate surface additive by reusing the existing `serve_tag_files_replay` result while tightening the internal smoke pass/fail checks and warning generation for missing or invalid replay rerun-diff empty-collection metadata.
- Updated the categorized release verification tests in `tests/unit/test_release_verification.py`, `tests/component/test_cli_release_verify.py`, `tests/integration/test_release_verify_api.py`, and `tests/api/test_release_verify_endpoint.py` so successful replay empty-collection validation and explicit invalid replay rerun-diff collection warnings are covered across unit, component, integration, and API layers.
- Verified the item through focused release verification tests with `33 passed`, `python -m compileall src/ades`, and the repo-standard `docdexd run-tests --repo /home/wodo/apps/ades` release-validation flow, including `196 passed` under `pytest -q` plus successful clean-environment wheel/npm live `/v0/tag/files` replay rerun-diff empty-collection smoke checks.

### 51. Live batch-manifest replay lineage root/parent run-id smoke validation for installed artifacts

- Extended `src/ades/release.py` so the replayed live `/v0/tag/files` release smoke now requires the parent batch payload to keep `lineage.run_id` plus matching `lineage.root_run_id`, and requires the replay child payload to keep `lineage.root_run_id` and `lineage.parent_run_id` aligned with that parent lineage through both the served wheel and the npm wrapper.
- Kept the release verify/validate surface additive by reusing the existing `serve_tag_files` and `serve_tag_files_replay` results while tightening the internal smoke pass/fail checks and warning generation for missing or invalid replay lineage run-id metadata.
- Updated `tests/release_helpers.py` plus the categorized release verification tests in `tests/unit/test_release_verification.py`, `tests/component/test_cli_release_verify.py`, `tests/integration/test_release_verify_api.py`, and `tests/api/test_release_verify_endpoint.py` so successful replay lineage validation and explicit invalid lineage run-id warnings are covered across unit, component, integration, and API layers.
- Verified the item through focused release verification tests with `39 passed`, `python -m compileall src/ades`, and the repo-standard `docdexd run-tests --repo /home/wodo/apps/ades` release-validation flow, including `202 passed` under `pytest -q` plus successful clean-environment wheel/npm live `/v0/tag/files` replay lineage smoke checks.

### 52. Live batch-manifest replay child run-id smoke validation for installed artifacts

- Extended `src/ades/release.py` so the replayed live `/v0/tag/files` release smoke now requires the child replay payload to expose a non-empty `lineage.run_id` that stays distinct from the parent batch `lineage.run_id` through both the served wheel and the npm wrapper, instead of only checking replay root/parent lineage wiring.
- Kept the release verify/validate surface additive by reusing the existing `serve_tag_files` and `serve_tag_files_replay` results while tightening both the internal smoke pass/fail checks and warning generation for missing or invalid replay child run-id metadata.
- Updated the categorized release verification tests in `tests/unit/test_release_verification.py`, `tests/component/test_cli_release_verify.py`, `tests/integration/test_release_verify_api.py`, and `tests/api/test_release_verify_endpoint.py` so successful replay child run-id validation and explicit missing/invalid replay child run-id warnings are covered across unit, component, integration, and API layers.
- Verified the item through focused release verification tests with `41 passed`, `python -m compileall src/ades tests`, and the repo-standard `docdexd run-tests --repo /home/wodo/apps/ades` release-validation flow, including `204 passed` under `pytest -q` plus successful clean-environment wheel/npm live `/v0/tag/files` replay child run-id smoke checks.

### 53. Live batch-manifest root-lineage identity smoke validation for installed artifacts

- Extended `src/ades/release.py` so the initial live `/v0/tag/files` batch manifest from the served wheel and npm wrapper must stay a root lineage node by keeping `lineage.parent_run_id` and `lineage.source_manifest_path` unset/null, instead of only checking root `lineage.run_id` and `lineage.root_run_id`.
- Kept the release verify/validate surface additive by reusing the existing `serve_tag_files` and `serve_tag_files_replay` results while tightening both the internal smoke pass/fail checks and warning generation for invalid root-manifest lineage linkage fields.
- Updated the categorized release verification tests in `tests/unit/test_release_verification.py`, `tests/component/test_cli_release_verify.py`, `tests/integration/test_release_verify_api.py`, and `tests/api/test_release_verify_endpoint.py` so successful root-lineage validation and explicit invalid root-manifest lineage warnings are covered across unit, component, integration, and API layers.
- Verified the item through focused release verification tests with `43 passed`, `python -m compileall src/ades tests`, and the repo-standard `docdexd run-tests --repo /home/wodo/apps/ades` release-validation flow, including `206 passed` under `pytest -q` plus successful clean-environment wheel/npm live `/v0/tag/files` root-lineage smoke checks.
- Hardened the remaining release-helper operator surfaces in `src/ades/cli.py` so `ades release verify`, `ades release validate`, `ades release versions`, `ades release sync-version`, and `ades release manifest` now convert request-time `FileNotFoundError` and `ValueError` failures into deterministic stderr plus exit code `1`, while keeping the public Python API on raw exceptions.
- Hardened `src/ades/service/app.py` so `GET /v0/release/versions` now matches the existing release-endpoint `404`/`400` contract instead of leaking raw release-layout and release-version parsing failures.
- Added categorized release-helper failure coverage in `tests/unit/test_release_verification.py`, `tests/unit/test_release_validation.py`, `tests/component/test_cli_release_verify.py`, `tests/integration/test_release_verify_api.py`, and `tests/api/test_release_verify_endpoint.py` so missing release-layout files and invalid release-version metadata/update failures are proven consistently across unit, component, integration, and API layers.
- Verified the slice through focused release suites with `91 passed` plus `python -m compileall src/ades tests`, which closes the remaining CLI/service operational-hardening queue and moves the active work to end-to-end production-readiness validation.

### 54. Exact saved-path parity validation for installed-artifact batch smoke

- Tightened `src/ades/release.py` so clean-environment wheel/npm `/v0/tag/files` smoke now requires the exact deterministic root manifest path, replay manifest path, and both saved output paths under `serve-smoke-batch-outputs`, rather than accepting any path with the right filename suffix or output count.
- Reused the existing packaged serve-smoke response shape while strengthening both the internal pass/fail checks and warning generation for root/replay saved-path mismatches.
- Updated `tests/release_helpers.py`, `tests/unit/test_release_verification.py`, `tests/component/test_cli_release_verify.py`, `tests/integration/test_release_verify_api.py`, and `tests/api/test_release_verify_endpoint.py` so successful exact-path parity plus explicit invalid root/replay saved-path warnings are covered across unit, component, integration, and API layers.
- Verified the item through focused release verification tests with `90 passed` and `python -m compileall src/ades tests`, keeping the active queue on end-to-end production-readiness validation.

### 55. Root batch identity validation for installed-artifact batch smoke

- Tightened `src/ades/release.py` so clean-environment wheel/npm `/v0/tag/files` smoke now requires the initial root batch response itself to report the expected top-level `pack=finance-en` and `item_count=2`, matching the replay payload identity checks instead of trusting only replay-side identity.
- Reused the existing packaged serve-smoke response shape while strengthening both the internal pass/fail checks and warning generation for invalid root batch `pack` and `item_count`.
- Updated `tests/unit/test_release_verification.py`, `tests/component/test_cli_release_verify.py`, `tests/integration/test_release_verify_api.py`, and `tests/api/test_release_verify_endpoint.py` so successful root batch identity plus explicit invalid root pack/item-count warnings are covered across unit, component, integration, and API layers.
- Verified the item through focused release verification tests with `92 passed`, `python -m compileall src/ades tests`, and the repo-standard `docdexd run-tests --repo /home/wodo/apps/ades` release-validation flow, including `340 passed` under `pytest -q` plus successful clean-environment wheel/npm root-batch-identity smoke checks.

### 56. Exact source-path parity validation for installed-artifact batch smoke

- Tightened `src/ades/release.py` so clean-environment wheel/npm `/v0/tag/files` smoke now requires both the root batch payload and replay payload to keep the exact deterministic per-item `source_path` list for the two packaged smoke inputs, instead of only agreeing on rerun-diff changed paths.
- Reused the existing packaged serve-smoke response shape while strengthening both the internal pass/fail checks and warning generation for invalid root/replay `source_path` lists.
- Updated `tests/release_helpers.py`, `tests/unit/test_release_verification.py`, `tests/component/test_cli_release_verify.py`, `tests/integration/test_release_verify_api.py`, and `tests/api/test_release_verify_endpoint.py` so successful root/replay source-path parity plus explicit invalid source-path warnings are covered across unit, component, integration, and API layers.
- Verified the item through focused release verification tests with `94 passed`, `python -m compileall src/ades tests`, `docdexd hook pre-commit --repo /home/wodo/apps/ades`, and the repo-standard `docdexd run-tests --repo /home/wodo/apps/ades` release-validation flow, including `342 passed` under `pytest -q` plus successful clean-environment wheel/npm source-path smoke checks.
- Tightened `src/ades/release.py` so clean-environment wheel/npm `/v0/tag/files` smoke now requires both the root batch payload and replay payload to keep the exact deterministic aggregate `summary.discovered_input_bytes`, `summary.included_input_bytes`, and `summary.processed_input_bytes` totals for the two packaged smoke inputs.
- Reused the existing packaged serve-smoke response shape while strengthening both the internal pass/fail checks and warning generation for invalid root/replay batch-summary input-byte totals.
- Updated `tests/release_helpers.py`, `tests/unit/test_release_verification.py`, `tests/component/test_cli_release_verify.py`, `tests/integration/test_release_verify_api.py`, and `tests/api/test_release_verify_endpoint.py` so successful root/replay summary-input-byte parity plus explicit invalid aggregate-byte warnings are covered across unit, component, integration, and API layers.
- Verified the item through focused release verification tests with `100 passed`, `python -m compileall src/ades tests`, `docdexd hook pre-commit --repo /home/wodo/apps/ades`, and the repo-standard `docdexd run-tests --repo /home/wodo/apps/ades` release-validation flow, including `348 passed` under `pytest -q` plus successful clean-environment wheel/npm summary-input-byte smoke checks.
- Tightened `src/ades/release.py` so clean-environment wheel/npm `/v0/tag/files` smoke now requires both the root batch payload and replay payload to keep the exact deterministic aggregate `summary.discovered_count`, `summary.included_count`, and `summary.processed_count` totals for the two packaged smoke inputs, distinct from the replay manifest candidate/selected counts already validated elsewhere.
- Reused the existing packaged serve-smoke response shape while strengthening both the internal pass/fail checks and warning generation for invalid root/replay batch-summary item-count totals.
- Updated `tests/release_helpers.py`, `tests/unit/test_release_verification.py`, `tests/component/test_cli_release_verify.py`, `tests/integration/test_release_verify_api.py`, and `tests/api/test_release_verify_endpoint.py` so successful root/replay summary-item-count parity plus explicit invalid aggregate-count warnings are covered across unit, component, integration, and API layers.
- Verified the item through focused release verification tests with `102 passed`, `python -m compileall src/ades tests`, `docdexd hook pre-commit --repo /home/wodo/apps/ades`, and the repo-standard `docdexd run-tests --repo /home/wodo/apps/ades` release-validation flow, including `350 passed` under `pytest -q` plus successful clean-environment wheel/npm summary-item-count smoke checks.
- Tightened `src/ades/release.py` so clean-environment wheel/npm `/v0/tag/files` smoke now requires both the root batch payload and replay payload to keep the exact deterministic zero-state `summary.excluded_count`, `summary.skipped_count`, `summary.rejected_count`, and `summary.limit_skipped_count` values for the clean two-file smoke corpus.
- Reused the existing packaged serve-smoke response shape while strengthening both the internal pass/fail checks and warning generation for invalid root/replay zero-state batch-summary counters.
- Updated `tests/release_helpers.py`, `tests/unit/test_release_verification.py`, `tests/component/test_cli_release_verify.py`, `tests/integration/test_release_verify_api.py`, and `tests/api/test_release_verify_endpoint.py` so successful root/replay zero-state counter parity plus explicit invalid zero-state warnings are covered across unit, component, integration, and API layers.
- Verified the item through focused release verification tests with `104 passed`, `python -m compileall src/ades tests`, `docdexd hook pre-commit --repo /home/wodo/apps/ades`, and the repo-standard `docdexd run-tests --repo /home/wodo/apps/ades` release-validation flow, including `352 passed` under `pytest -q` plus successful clean-environment wheel/npm zero-state counter smoke checks.
- Tightened `src/ades/release.py` so clean-environment wheel/npm `/v0/tag/files` smoke now requires both the root batch payload and replay payload to keep the exact deterministic zero-state reuse counters `summary.unchanged_skipped_count`, `summary.unchanged_reused_count`, `summary.reused_output_missing_count`, and `summary.repaired_reused_output_count` for the clean two-file smoke corpus.
- Reused the existing packaged serve-smoke response shape while strengthening both the internal pass/fail checks and warning generation for invalid root/replay zero-state reuse counters.
- Updated `tests/release_helpers.py`, `tests/unit/test_release_verification.py`, `tests/component/test_cli_release_verify.py`, `tests/integration/test_release_verify_api.py`, and `tests/api/test_release_verify_endpoint.py` so successful root/replay zero-state reuse-counter parity plus explicit invalid reuse-counter warnings are covered across unit, component, integration, and API layers.
- Verified the item through focused release verification tests with `106 passed`, `python -m compileall src/ades tests`, `docdexd hook pre-commit --repo /home/wodo/apps/ades`, and the repo-standard `docdexd run-tests --repo /home/wodo/apps/ades` release-validation flow, including `354 passed` under `pytest -q` plus successful clean-environment wheel/npm zero-state reuse-counter smoke checks.

### 54. Offline generated-pack refresh and publication orchestration

- Added an orchestration layer in `src/ades/packs/refresh.py` that turns one or more normalized bundle directories into a quality-gated static registry release by chaining report generation, pack-specific quality validation, and final registry publication only when all requested packs pass.
- Exposed the refresh workflow consistently through the public Python API `ades.refresh_generated_packs(...)`, the CLI command `ades registry refresh-generated-packs`, and the localhost service endpoint `POST /v0/registry/refresh-generated-packs`.
- Added categorized unit, component, integration, and API coverage for successful generated-pack refresh runs plus the failure path where quality review preserves report/quality output and intentionally skips the final registry build.
- Added durable operator docs in `docs/library_pack_source_bundle_spec.md` and `docs/library_pack_publication_workflow.md`, and linked the refresh output to the existing object-storage publication seam in `docs/production_deployment.md`.

### 55. Source-governance enforcement for generated-pack publication

- Extended `src/ades/packs/generation.py` so bundle source snapshots now carry explicit `license_class` metadata, generated `sources.json` / `build.json` sidecars summarize publishable versus restricted sources, and local generate/report output surfaces warn when a bundle is not eligible for registry publication.
- Updated the bundle builders in `src/ades/packs/finance_bundle.py`, `src/ades/packs/general_bundle.py`, and `src/ades/packs/medical_bundle.py` so generated bundles emit explicit source-governance metadata instead of overloading the freeform `license` field as a policy class.
- Tightened `src/ades/packs/refresh.py` so quality-passing bundles still fail the publication step when any source is tagged `build-only`, `blocked-pending-license-review`, or missing provenance, while preserving all report and quality evidence and emitting the deterministic warning `registry_build_skipped:source_governance_failed`.
- Added categorized unit, component, integration, and API coverage for the new source-governance summary fields on generate/report surfaces plus the publication-skip path for refresh orchestration, and updated the durable bundle/publication docs so the operator contract matches the implementation.

### 56. Deterministic source-lock emission for normalized bundle builders

- Added shared deterministic lock helpers in `src/ades/packs/source_lock.py` so normalized bundle builders can emit reproducible raw-snapshot lock metadata without changing the runtime pack contract.
- Extended `src/ades/packs/finance_bundle.py`, `src/ades/packs/general_bundle.py`, and `src/ades/packs/medical_bundle.py` so every built bundle now writes `sources.lock.json` alongside `bundle.json` and returns `sources_lock_path` through the public Python API, CLI, and localhost service response models.
- Recorded exact snapshot URIs, SHA-256 digests, byte sizes, recorded snapshot timestamps, license metadata, adapter ids, and adapter versions inside the lock file so real upstream bundle rebuilds can be audited against the raw inputs instead of only the normalized JSONL handoff.
- Added categorized unit, component, integration, and API coverage for the new lock-file contract and updated `docs/library_pack_source_bundle_spec.md` so the builder/output contract explicitly documents `sources.lock.json`.

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
- `ades registry build-finance-bundle --sec-company-tickers <path> --symbol-directory <path> --curated-entities <path> --output-dir <dir>`
- `ades registry build-general-bundle --wikidata-entities <path> --geonames-places <path> --curated-entities <path> --output-dir <dir>`
- `ades registry build-medical-bundle --disease-ontology <path> --hgnc-genes <path> --uniprot-proteins <path> --clinical-trials <path> --curated-entities <path> --output-dir <dir>`
- `ades registry report-pack <bundle-dir> --output-dir <dir>`
- `ades registry generate-pack <bundle-dir> --output-dir <dir>`
- `ades registry validate-finance-quality <bundle-dir> --output-dir <dir>`
- `ades registry validate-general-quality <bundle-dir> --output-dir <dir>`
- `ades registry validate-medical-quality <bundle-dir> --general-bundle-dir <dir> --output-dir <dir>`
- `ades registry refresh-generated-packs <bundle-dir...> --output-dir <dir>`
- `ades registry prepare-deploy-release --output-dir <dir>`
- `ades registry publish-generated-release <registry-dir> --prefix <object-storage-prefix>`
- `ades registry smoke-published-release <registry-url>`
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
- `POST /v0/registry/build-finance-bundle`
- `POST /v0/registry/build-general-bundle`
- `POST /v0/registry/build-medical-bundle`
- `POST /v0/registry/report-pack`
- `POST /v0/registry/generate-pack`
- `POST /v0/registry/validate-finance-quality`
- `POST /v0/registry/validate-general-quality`
- `POST /v0/registry/validate-medical-quality`
- `POST /v0/registry/refresh-generated-packs`
- `POST /v0/registry/prepare-deploy-release`
- `POST /v0/registry/publish-generated-release`
- `POST /v0/registry/smoke-published-release`
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

- 2026-04-10: expanded the bounded real-source `general-en` GeoNames seed set by enabling London alongside Istanbul, added a generated-pack quality case for the `Londres` alias, and revalidated the bounded live general slice at `entity_record_count=9`, `alias_count=206`, `ambiguous_alias_count=0`, `expected_recall=1.0`, and `precision=1.0`.
- 2026-04-10: widened the bounded real-source `general-en` Wikidata seed set with `Satya Nadella` and `Jensen Huang`, added a generated-pack quality case for both names, and revalidated the bounded live general slice at `entity_record_count=11`, `alias_count=213`, `ambiguous_alias_count=0`, `expected_recall=1.0`, and `precision=1.0`.
- 2026-04-10: widened the bounded real-source `general-en` Wikidata seed set with `Nvidia` and `Anthropic`, added a generated-pack quality case for both organizations, and revalidated the bounded live general slice at `entity_record_count=13`, `alias_count=216`, `ambiguous_alias_count=0`, `expected_recall=1.0`, and `precision=1.0`.

## Current Next Step

- Evaluate whether the next bounded `general-en` additions should come from more Wikidata entities or a carefully curated location slice, while keeping the current zero-ambiguity quality contract intact for the dependency pack that `medical-en` relies on.
