# ades Next Work Items

Updated after completing live batch-manifest replay summary-count smoke validation for installed artifacts on 2026-04-09.

## Priority Queue

1. Pack lifecycle hardening
   - Extend the installed-artifact release smoke so live `POST /v0/tag/files` replay through the served wheel and npm wrapper also proves top-level replay payload identity such as `pack` and `item_count` after `finance-en` pull and metadata recovery, not just manifest-input, replay-mode, lineage wiring, and replay summary counts.
   - Keep the rollback-safe install path, same-version metadata-repair semantics, direct lookup-repair semantics, dependency-chain guardrails, and dependency-bearing release smoke stable while closing the remaining pack-lifecycle gaps.

2. Storage and metadata recovery
   - Keep hardening first-run bootstrap, stale-record cleanup, and repair behavior around the local SQLite registry now that both list-driven and lookup-driven recovery can repair missing installed-pack rows from on-disk manifests.
   - Keep the storage seam explicit so future PostgreSQL work can reuse the public contracts without inheriting local-only assumptions.

3. CLI and service operational hardening
   - Tighten startup validation, config error reporting, exit behavior, and operator-facing machine-readable responses on the active local-tool commands and endpoints.
   - Confirm the existing local surfaces behave correctly outside the repo checkout and under realistic storage-root conditions.

4. End-to-end production-readiness validation
   - Add more clean-environment flows that cover install, pull, serve, tag, and recovery behavior together instead of validating them only in narrower slices.
   - Keep the completed local-tool modules aligned with the repo’s production-ready completion bar.

5. Post-`v0.1.0` NLP-backed enrichment evaluation
   - The default runtime is now intentionally deterministic for `v0.1.0`, and `pyproject.toml` still carries no `spaCy`, `GLiNER`, or `FlashText` runtime dependencies.
   - If richer extraction is revisited later, evaluate it as a post-release track with explicit cost, packaging, and test-surface impact rather than as an implied baseline.

6. Production-server seam hardening
   - The local-tool release path is real, while the PostgreSQL-backed production server remains a placeholder seam.
   - Keep future local-tool changes from leaking SQLite-only assumptions into the contracts that the later production server should reuse.

## Recently Closed

- Coordinated release publish workflow:
  - implemented in `src/ades/release.py`
  - exposed through the public Python API, `ades release publish`, and `POST /v0/release/publish`
  - gated on validated manifests from `ades release validate`
  - includes dry-run support plus explicit env-var credential handling for Python and npm publication

- Deterministic tagging-scope decision and stronger response scoring:
  - `v0.1.0` is now explicitly locked to the implemented deterministic runtime instead of implying a missing `spaCy` / `GLiNER` baseline
  - entity matches now expose explicit deterministic `relevance`
  - topic matches now expose evidence counts plus contributing entity-label summaries

- Documentation alignment for the actual `v0.1.0` scope:
  - `README.md`, `docs/project_guidance.md`, `docs/implementation_plan.md`, `docs/v0.1.0_decisions.md`, and `docs/development_progress.md` now describe the implemented deterministic tagging baseline and the completed release workflow instead of pointing at already-finished or still-unshipped NLP work

- Lightweight local candidate search/index:
  - implemented in `src/ades/storage/registry_db.py`
  - exposed through the existing public API, `ades packs lookup`, and `GET /v0/lookup`
  - adds multi-term cross-field lookup through `SQLite FTS5` when available, with deterministic fallback to standard SQLite queries when it is not
  - includes categorized unit, component, integration, and API coverage for broader metadata lookup behavior

- Rollback-safe pack install recovery:
  - implemented in `src/ades/packs/installer.py`
  - stages fresh installs into hidden temporary pack directories and restores the previous installed pack when an update fails after replacement begins
  - keeps the existing `ades pull`, public Python API, and installed-pack service surfaces unchanged while preventing broken partial pack state from surviving failed installs
  - includes categorized unit, component, integration, and API coverage for fresh-install cleanup and update rollback against published registry artifacts

- Same-version pull metadata repair:
   - implemented in `src/ades/packs/installer.py`, `src/ades/packs/registry.py`, and `src/ades/storage/registry_db.py`
   - refreshes installed pack metadata from disk before repeated same-version pulls decide to skip or reactivate a pack, which repairs stale alias/rule/label state in SQLite without changing the public pull contract
   - keeps CLI, public Python API, and local service lookup behavior aligned with the on-disk pack contents after repeated pulls
   - includes categorized unit, component, integration, and API coverage for repeated same-version metadata repair against published registry artifacts

- Dependency-aware activation/deactivation guardrails:
   - implemented in `src/ades/api.py`, with CLI/service error-path updates in `src/ades/cli.py` and `src/ades/service/app.py`
   - blocks pack deactivation when active installed dependents would be left pointing at an inactive dependency, and makes pack activation recursively reactivate inactive installed dependencies before reactivating the requested pack
   - keeps the existing public activation/deactivation surfaces unchanged while preventing broken active pack chains
   - includes categorized unit, component, integration, and API coverage for blocked dependency deactivation and recursive dependency reactivation

- Clean-environment pull-and-tag smoke validation:
   - implemented in `src/ades/release.py` and `src/ades/service/models.py`, with fake-runner updates in `tests/release_helpers.py`
   - extends release smoke verification from `status` only to `status`, `pull general-en`, and deterministic `tag` execution through both the installed wheel and npm wrapper in clean environments
   - keeps the existing release verify/validate contracts intact while exposing structured pull/tag command results plus pulled-pack and tagged-label metadata for each smoke-install artifact
   - includes categorized unit, component, integration, and API coverage for successful clean-environment pull/tag smoke validation and explicit tag-failure warnings

- Installed-pack list metadata repair:
   - implemented in `src/ades/packs/registry.py`, with shared SQLite test support in `tests/pack_registry_helpers.py`
   - repairs missing installed-pack metadata rows from on-disk manifests even when other valid SQLite rows still exist, so `list_packs`, `ades packs list`, and `/v0/packs` can heal inconsistent local registry state
   - keeps the existing public list and lookup contracts unchanged while restoring alias, rule, label, and dependency metadata for repaired packs
   - includes categorized unit, component, integration, and API coverage for list-driven metadata repair against published registry artifacts

- Clean-environment serve smoke validation:
   - implemented in `src/ades/release.py` and `src/ades/service/models.py`, with fake serve-smoke support in `tests/release_helpers.py`
   - extends release smoke verification so both the installed wheel and npm wrapper launch `ades serve`, answer `/healthz` and `/v0/status`, and report served pack ids from the running local service before shutdown
   - keeps the existing release verify/validate contracts additive by exposing `serve`, `serve_healthz`, `serve_status`, and `served_pack_ids` inside each smoke-install artifact result
   - includes categorized unit, component, integration, and API coverage for successful serve startup and explicit serve-failure warnings in clean environments

- Clean-environment bootstrap recovery smoke validation:
   - implemented in `src/ades/release.py` and `src/ades/service/models.py`, with fake recovery-status support in `tests/release_helpers.py`
   - extends release smoke verification so both the installed wheel and npm wrapper delete local SQLite metadata after pull/tag, recover `general-en` through a fresh `ades status` bootstrap, then delete the metadata again before `ades serve`
   - keeps the existing release verify/validate contracts additive by exposing `recovery_status` and `recovered_pack_ids` inside each smoke-install artifact result
   - includes categorized unit, component, integration, and API coverage for successful bootstrap recovery and explicit recovery-status failure warnings in clean environments

- Lookup-driven installed-pack metadata repair:
   - implemented in `src/ades/packs/registry.py`
   - retries lookup misses after reconciling on-disk pack manifests back into SQLite, so direct `lookup_candidates`, `ades packs lookup`, and `GET /v0/lookup` calls can heal a deleted installed-pack row without requiring a preceding list operation
   - keeps the public lookup contracts unchanged while restoring alias/rule metadata for packs that still exist on disk
   - includes categorized unit, component, integration, and API coverage for direct lookup repair against published registry artifacts

- Tagger regression coverage for partial installed-pack row loss:
   - implemented in `tests/test_tagger.py`, `tests/component/test_lookup_driven_tagger.py`, `tests/integration/test_lookup_driven_tag_api.py`, and `tests/api/test_tag_lookup_aliases.py`
   - proves alias-driven `finance-en` tagging still extracts `AAPL` and `NASDAQ` after the `finance-en` SQLite row is deleted while the on-disk pack remains installed
   - keeps product behavior unchanged while locking in the existing recovery path with categorized unit, component, integration, and API coverage

- Dependency-bearing release smoke validation for installed artifacts:
   - implemented in `src/ades/release.py`, with fake-runner updates in `tests/release_helpers.py`
   - extends release smoke verification so both the installed wheel and npm wrapper pull `finance-en`, tag with `--pack finance-en`, and keep both `general-en` and `finance-en` visible through pull, recovery, and serve status checks
   - proves the packaged artifacts emit `organization`, `ticker`, `exchange`, and `currency_amount` labels from the finance-pack dependency chain instead of only validating a base-pack tagging path
   - includes categorized unit, component, integration, and API coverage for the stronger finance-pack smoke contract

- Live service-tag smoke validation for installed artifacts:
   - implemented in `src/ades/release.py` and `src/ades/service/models.py`, with fake-runner updates in `tests/release_helpers.py`
   - extends release smoke verification so both the installed wheel and npm wrapper now send a real `POST /v0/tag` request after finance-pack pull, metadata bootstrap recovery, and `ades serve` startup
   - keeps the release verify/validate contracts additive by exposing `serve_tag` and `serve_tagged_labels` inside each smoke-install artifact result
   - includes categorized unit, component, integration, and API coverage for successful live service tagging and explicit `serve_tag` failure warnings

- Live file-tag smoke validation for installed artifacts:
   - implemented in `src/ades/release.py` and `src/ades/service/models.py`, with fake-runner updates in `tests/release_helpers.py`
   - extends release smoke verification so both the installed wheel and npm wrapper now write a deterministic local HTML file and send a real `POST /v0/tag/file` request after finance-pack pull, metadata bootstrap recovery, and `ades serve` startup
   - keeps the release verify/validate contracts additive by exposing `serve_tag_file` and `serve_tag_file_labels` inside each smoke-install artifact result
   - includes categorized unit, component, integration, and API coverage for successful live file tagging and explicit `serve_tag_file` failure warnings

- Live batch file-tag smoke validation for installed artifacts:
   - implemented in `src/ades/release.py` and `src/ades/service/models.py`, with fake-runner updates in `tests/release_helpers.py`
   - extends release smoke verification so both the installed wheel and npm wrapper now write two deterministic local HTML files and send a real `POST /v0/tag/files` request after finance-pack pull, metadata bootstrap recovery, and `ades serve` startup
   - keeps the release verify/validate contracts additive by exposing `serve_tag_files` and `serve_tag_files_labels` inside each smoke-install artifact result
   - includes categorized unit, component, integration, and API coverage for successful live batch tagging and explicit `serve_tag_files` failure warnings

- Live batch-manifest write smoke validation for installed artifacts:
   - implemented in `src/ades/release.py`, with fake-runner updates in `tests/release_helpers.py`
   - extends release smoke verification so both the installed wheel and npm wrapper now request persisted batch outputs plus manifest writing through a live `POST /v0/tag/files` request after finance-pack pull, metadata bootstrap recovery, and `ades serve` startup
   - keeps the release verify/validate contracts unchanged while requiring `saved_manifest_path` and both per-item `saved_output_path` values inside the existing `serve_tag_files` result
   - includes categorized unit, component, integration, and API coverage for successful live batch-manifest writes and explicit missing-manifest warnings

- Live batch-manifest path override smoke validation for installed artifacts:
   - implemented in `src/ades/release.py`, with fake-runner updates in `tests/release_helpers.py`
   - extends release smoke verification so both the installed wheel and npm wrapper now provide an explicit `output.manifest_path` override through the live `POST /v0/tag/files` request after finance-pack pull, metadata bootstrap recovery, and `ades serve` startup
   - keeps the release verify/validate contracts unchanged while requiring the existing `serve_tag_files` result to report that exact manifest path instead of the default batch manifest filename
   - includes categorized unit, component, integration, and API coverage for successful explicit manifest-path overrides and explicit invalid-manifest-path warnings

- Live batch-manifest replay smoke validation for installed artifacts:
   - implemented in `src/ades/release.py`, `src/ades/service/models.py`, and `tests/release_helpers.py`
   - extends release smoke verification so both the installed wheel and npm wrapper now replay the manifest they just wrote through a second live `POST /v0/tag/files` request after finance-pack pull, metadata bootstrap recovery, and `ades serve` startup
   - keeps the release verify/validate contracts additive by exposing `serve_tag_files_replay` and `serve_tag_files_replay_labels`, while requiring replay summary, lineage source-manifest, and child manifest-path behavior from the packaged serve path
   - includes categorized unit, component, integration, and API coverage for successful manifest replay plus explicit invalid replay-manifest-input warnings

- Live batch-manifest replay summary-count smoke validation for installed artifacts:
   - implemented in `src/ades/release.py` and `tests/release_helpers.py`
   - extends release smoke verification so both the installed wheel and npm wrapper now require replay `summary.manifest_candidate_count` and `summary.manifest_selected_count` to match the deterministic two-file finance-pack smoke corpus
   - keeps the release verify/validate contracts unchanged while making replay-count mismatches surface as explicit warning codes in the packaged smoke path
   - includes categorized unit, component, integration, and API coverage for successful replay summary-count validation plus explicit invalid replay-count warnings

## Not Next

- The PostgreSQL-backed production server remains intentionally deferred until the local-tool `v0.1.0` scope is closed.
- Historical architecture research notes in `docs/ades_chat_output.md` and `docs/ades_architecture.html` are not the release-truth source for `v0.1.0`; use the decisions, implementation plan, development progress, and this queue instead.
