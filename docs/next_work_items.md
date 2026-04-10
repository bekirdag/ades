# ades Next Work Items

Updated after closing the Phase A local-tool module hardening track on 2026-04-09.

## Priority Queue

1. Real upstream bundle ingestion and quality tuning
   - Evaluate whether the next bounded `general-en` additions should come from another carefully chosen Wikidata entity pair or from a curated location slice, but keep the current zero-ambiguity quality contract intact while the general pack remains a dependency for `medical-en`.
   - After the second retained-alias person expansion, the next likely additive seam is a curated location slice unless an organization pair can prove clean without introducing new domain-alias pruning.
   - The next likely pruning seam is organization/domain alias noise, not person surnames; only take it if a candidate pair actually needs that headroom.

2. End-to-end production-readiness validation
   - Keep extending clean-environment release validation so installed artifacts prove the real shipped wheel/npm behavior together rather than only through narrower source-tree regressions.
   - Convert the remaining broad readiness goals into additive release assertions and deterministic warning codes.

3. Production-server seam hardening
   - Keep future local-tool changes from leaking SQLite-only assumptions into the contracts the later PostgreSQL-backed production server should reuse.
   - Delay server-only delivery work until the local-tool operational-hardening and release-readiness queues are closed.

4. Post-`v0.1.0` NLP-backed enrichment evaluation
   - The default runtime is intentionally deterministic for `v0.1.0`, and `pyproject.toml` still carries no `spaCy`, `GLiNER`, or `FlashText` runtime dependencies.
   - If richer extraction is revisited later, evaluate it as a post-release track with explicit cost, packaging, and test-surface impact rather than as an implied baseline.

## Recently Closed

- Bounded real-source `general-en` retained-alias person expansion:
  - implemented in `src/ades/packs/general_sources.py` and `src/ades/packs/general_quality.py`, with fixture and surface coverage updates in the general bundle/source/quality tests
  - expands the bounded Wikidata person seed set with `Mira Murati` and `Ilya Sutskever`, then proves the live retained-alias path through `Ermira Murati` and `Ilya Efimovich Sutskever` instead of only through raw fetch counts
  - revalidates the bounded live general slice at `wikidata_entity_count=15`, `entity_record_count=19`, `alias_count=236`, `ambiguous_alias_count=0`, `expected_recall=1.0`, and `precision=1.0`, keeping the dependency pack zero-ambiguity compatible for `medical-en`

- Bounded real-source `general-en` person-alias pruning expansion:
  - implemented in `src/ades/packs/general_bundle.py`, `src/ades/packs/general_sources.py`, and `src/ades/packs/general_quality.py`, with fixture and surface coverage updates in the general bundle/source/quality tests
  - adds a conservative general-bundle pruning rule that drops plain single-token aliases for multi-token person entities before normalized bundle generation, which safely admits `Sundar Pichai` and `Demis Hassabis` into the bounded live Wikidata seed set
  - proves the retained multi-token alias path through `Sundara Pichai` in the generated-pack quality gate while keeping the bounded live general slice clean at `wikidata_entity_count=13`, `entity_record_count=17`, `alias_count=232`, `ambiguous_alias_count=0`, `expected_recall=1.0`, and `precision=1.0`

- Bounded real-source `general-en` follow-on Wikidata expansion:
  - implemented in `src/ades/packs/general_sources.py` and `src/ades/packs/general_quality.py`, with fixture and surface coverage updates in the general bundle/source/quality tests
  - expands the bounded Wikidata seed set with `Sam Altman` and `Google DeepMind`, then proves the follow-on additions through the generated-pack quality gate using the low-noise `DeepMind Technologies` alias instead of only through raw fetch counts
  - revalidates the bounded live general slice at `wikidata_entity_count=11`, `entity_record_count=15`, `alias_count=226`, `ambiguous_alias_count=0`, `expected_recall=1.0`, and `precision=1.0`, keeping the dependency pack zero-ambiguity compatible for `medical-en`

- Bounded real-source `general-en` Wikidata organization expansion:
  - implemented in `src/ades/packs/general_sources.py` and `src/ades/packs/general_quality.py`, with fixture and surface coverage updates in the general bundle/source/quality tests
  - expands the bounded Wikidata organization seed set with `Nvidia` and `Anthropic`, then proves both additions through the generated-pack quality gate instead of only through raw fetch counts
  - revalidates the bounded live general slice at `entity_record_count=13`, `alias_count=216`, `ambiguous_alias_count=0`, `expected_recall=1.0`, and `precision=1.0`, keeping the dependency pack zero-ambiguity compatible for `medical-en`

- Bounded real-source `general-en` Wikidata person expansion:
  - implemented in `src/ades/packs/general_sources.py` and `src/ades/packs/general_quality.py`, with fixture and surface coverage updates in the general bundle/source/quality tests
  - expands the bounded Wikidata person seed set with `Satya Nadella` and `Jensen Huang`, then proves both additions through the generated-pack quality gate instead of only through raw fetch counts
  - revalidates the bounded live general slice at `entity_record_count=11`, `alias_count=213`, `ambiguous_alias_count=0`, `expected_recall=1.0`, and `precision=1.0`, keeping the dependency pack zero-ambiguity compatible for `medical-en`

- Bounded real-source `general-en` GeoNames expansion:
  - implemented in `src/ades/packs/general_sources.py` and `src/ades/packs/general_quality.py`, with fixture and surface coverage updates in the general bundle/source/quality tests
  - expands the bounded GeoNames seed set from Istanbul-only to Istanbul plus London, then proves the new location through the generated-pack quality gate with the `Londres` alias instead of only through raw count deltas
  - revalidates the bounded live general slice at `entity_record_count=9`, `alias_count=206`, `ambiguous_alias_count=0`, `expected_recall=1.0`, and `precision=1.0`, keeping the dependency pack zero-ambiguity compatible for `medical-en`

- Deploy-owned hosted-registry promotion preparation:
  - implemented in `src/ades/packs/publish.py`, with public surfaces in `src/ades/api.py`, `src/ades/cli.py`, and `src/ades/service/app.py`
  - adds `ades.prepare_registry_deploy_release(...)`, `ades registry prepare-deploy-release`, and `POST /v0/registry/prepare-deploy-release` so the existing CI/CD flow can either rebuild the bundled starter registry or materialize an approved reviewed registry URL from `src/ades/resources/registry/promoted-release.json`
  - keeps the current push-to-`main` deployment model authoritative by running the shipped clean-consumer smoke before materialization and then rewriting the reviewed registry back into the deploy-owned `dist/registry` layout instead of promoting through a side channel
  - updates `.github/workflows/deploy-prod.yml` to use the new preparation surface while preserving the old starter-pack fallback whenever no promotion spec is committed

- Published generated-registry consumer smoke automation:
  - implemented in `src/ades/packs/publish.py`, with public surfaces in `src/ades/api.py`, `src/ades/cli.py`, and `src/ades/service/app.py`
  - adds `ades.smoke_test_published_generated_registry(...)`, `ades registry smoke-published-release`, and `POST /v0/registry/smoke-published-release` so a reviewed object-storage registry URL can be validated through clean pull/install/tag smoke before promotion
  - uses clean temporary storage roots, proves dependency installation for `medical-en -> general-en`, and verifies deterministic finance and medical entity/label matches from the published registry URL itself instead of only checking object listings
  - adds categorized unit, component, integration, and API coverage around a locally served reviewed registry so the promotion gate remains durable in repo validation

- Three-pack real-source refresh rehearsal and object-storage publication helper:
  - implemented in `src/ades/packs/publish.py`, with public surfaces in `src/ades/api.py`, `src/ades/cli.py`, and `src/ades/service/app.py`
  - adds `ades.publish_generated_registry_release(...)`, `ades registry publish-generated-release`, and `POST /v0/registry/publish-generated-release` so reviewed generated registries can be synced to the configured S3-compatible bucket without an ad hoc shell-only workflow, and now returns direct HTTPS registry URL candidates alongside the underlying `s3://` prefix
  - validates the first fully real-source `finance-en` + `general-en` + `medical-en` refresh release under `/mnt/githubActions/ades_big_data/pack_releases/finance-general-medical-2026-04-10`, which currently passes with `finance ambiguous_alias_count=256`, `general ambiguous_alias_count=0`, and `medical ambiguous_alias_count=22`
  - validates the live Hetzner publication seam through the shipped CLI path, publishing `22` reviewed objects under `s3://ades/generated-pack-releases/finance-general-medical-2026-04-10/`, surfacing the working direct HTTPS registry URL `https://ades.fsn1.your-objectstorage.com/generated-pack-releases/finance-general-medical-2026-04-10/index.json`, and proving clean `finance-en` plus dependency-bearing `medical-en` consumer pulls against that published registry

- Real general-source ingestion and first combined general/medical refresh rehearsal:
  - implemented in `src/ades/packs/general_sources.py`, with public surfaces in `src/ades/api.py`, `src/ades/cli.py`, and `src/ades/service/app.py`
  - adds `ades.fetch_general_source_snapshot(...)`, `ades registry fetch-general-sources`, and `POST /v0/registry/fetch-general-sources` so bounded live Wikidata and GeoNames inputs are fetched reproducibly into `/mnt/githubActions/ades_big_data/pack_sources/raw/general-en/<snapshot>/`, accompanied by repo-owned curated aliases and `sources.fetch.json`
  - validates the bounded live general slice on `2026-04-10`, which currently yields `entity_record_count=13`, `alias_count=216`, `ambiguous_alias_count=0`, clean fixture recall/precision, and a publishable combined general/medical refresh release under `/mnt/githubActions/ades_big_data/pack_releases/general-medical-2026-04-10`
  - adds categorized unit, component, integration, and API coverage for the new fetch surface and keeps the bounded general dependency contract aligned with the real-source publication workflow

- Deterministic source-lock emission for normalized bundle builders:
  - implemented in `src/ades/packs/source_lock.py`, with builder integration in `src/ades/packs/finance_bundle.py`, `src/ades/packs/general_bundle.py`, and `src/ades/packs/medical_bundle.py`
  - every built normalized bundle now emits `sources.lock.json` and returns `sources_lock_path` through the public Python API, CLI, and localhost service build-bundle responses
  - the lock file records exact snapshot URIs, SHA-256 digests, byte sizes, recorded snapshot timestamps, license metadata, adapter ids, and adapter versions so real upstream bundle rebuilds can be audited deterministically

- Real finance-source ingestion and tuning:
  - implemented in `src/ades/packs/finance_sources.py`, with public surfaces in `src/ades/api.py`, `src/ades/cli.py`, and `src/ades/service/app.py`
  - adds `ades.fetch_finance_source_snapshot(...)`, `ades registry fetch-finance-sources`, and `POST /v0/registry/fetch-finance-sources` so dated SEC and Nasdaq snapshots are fetched reproducibly into `/mnt/githubActions/ades_big_data/pack_sources/raw/finance-en/<snapshot>/`, accompanied by repo-owned curated exchange aliases and `sources.fetch.json`
  - extends pack generation with per-record `blocked_aliases` support and tunes the finance bundle input contract so noisy live symbols like `ON` and `USD` are stoplisted while `NASDAQ` remains available as the exchange alias instead of colliding with the real issuer name
  - validates the first real live snapshot dated `2026-04-10`, which currently yields `entity_record_count=22881`, `alias_count=39101`, `ambiguous_alias_count=256`, clean fixture recall/precision, and a publishable finance-only refresh release under `/mnt/githubActions/ades_big_data/pack_releases/finance-en-2026-04-10`
  - adds categorized unit, component, integration, and API coverage for the new builder contract and updates `docs/library_pack_source_bundle_spec.md` so the durable operator docs match the emitted sidecar

- Real medical-source ingestion and tuning:
  - implemented in `src/ades/packs/medical_sources.py`, with public surfaces in `src/ades/api.py`, `src/ades/cli.py`, and `src/ades/service/app.py`
  - adds `ades.fetch_medical_source_snapshot(...)`, `ades registry fetch-medical-sources`, and `POST /v0/registry/fetch-medical-sources` so dated Disease Ontology, HGNC, UniProt, and ClinicalTrials.gov snapshots are fetched reproducibly into `/mnt/githubActions/ades_big_data/pack_sources/raw/medical-en/<snapshot>/`, accompanied by `sources.fetch.json`
  - tunes the medical bundle input contract so compact symbolic disease aliases and compact symbolic protein aliases are dropped before generation, while refresh now resolves pack-specific ambiguity budgets automatically when `--max-ambiguous-aliases` is omitted
  - validates the first real live snapshot dated `2026-04-10`, which currently yields `entity_record_count=57765`, `alias_count=127335`, `ambiguous_alias_count=22`, `dropped_alias_ratio=0.0003`, and clean fixture recall/precision with the dated live `general-en` dependency bundle in the combined refresh rehearsal
  - adds categorized unit, component, integration, and API coverage for the new fetch surface plus the bundle-filtering/quality-default contract

- Offline generated-pack refresh and publication orchestration:
  - implemented in `src/ades/packs/refresh.py`, with public surfaces in `src/ades/api.py`, `src/ades/cli.py`, and `src/ades/service/app.py`
  - turns one or more normalized bundle directories into a quality-gated static registry release by chaining report generation, pack-specific validation, and final registry publication only when every requested pack passes
  - adds categorized unit, component, integration, and API coverage for successful refresh runs plus the intentional registry-build skip path when quality fails
  - adds durable operator docs in `docs/library_pack_source_bundle_spec.md`, `docs/library_pack_publication_workflow.md`, and `docs/production_deployment.md` so generated pack publication stays aligned with the existing object-storage and hosted-registry seam

- Source-governance enforcement for generated-pack publication:
  - implemented in `src/ades/packs/generation.py`, `src/ades/packs/reporting.py`, and `src/ades/packs/refresh.py`, with additive response fields surfaced through `src/ades/api.py`, `src/ades/cli.py`, and `src/ades/service/models.py`
  - requires explicit source `license_class` metadata, summarizes publishable versus restricted sources in `sources.json` / `build.json` and the public generate/report responses, and keeps local generation/reporting usable for inspection even when a bundle is not publishable
  - blocks the final registry build during `refresh-generated-packs` whenever a bundle uses `build-only`, `blocked-pending-license-review`, or missing source provenance, while preserving report and quality evidence and emitting `registry_build_skipped:source_governance_failed`
  - adds categorized unit, component, integration, and API coverage for both the new summary fields and the refresh publication-skip path, and updates the durable source-bundle/publication docs so the operator contract is explicit

- Exact zero-state reuse-counter validation for installed-artifact `/v0/tag/files` smoke:
  - implemented in `src/ades/release.py`
  - tightens clean-environment wheel/npm release smoke so both the root batch payload and replay payload must keep the exact deterministic `summary.unchanged_skipped_count`, `summary.unchanged_reused_count`, `summary.reused_output_missing_count`, and `summary.repaired_reused_output_count` values for the clean two-file smoke corpus
  - adds categorized unit, component, integration, and API coverage for successful root/replay zero-state reuse-counter parity plus explicit invalid reuse-counter warnings on the packaged serve flow

- Exact zero-state batch-summary counter validation for installed-artifact `/v0/tag/files` smoke:
  - implemented in `src/ades/release.py`
  - tightens clean-environment wheel/npm release smoke so both the root batch payload and replay payload must keep the exact deterministic `summary.excluded_count`, `summary.skipped_count`, `summary.rejected_count`, and `summary.limit_skipped_count` values for the clean two-file smoke corpus
  - adds categorized unit, component, integration, and API coverage for successful root/replay zero-state counter parity plus explicit invalid zero-state warnings on the packaged serve flow

- Exact batch-summary item-count parity validation for installed-artifact `/v0/tag/files` smoke:
  - implemented in `src/ades/release.py`
  - tightens clean-environment wheel/npm release smoke so both the root batch payload and replay payload must keep the exact deterministic aggregate `summary.discovered_count`, `summary.included_count`, and `summary.processed_count` totals for the two packaged smoke inputs, distinct from the already-validated replay manifest candidate/selected counts
  - adds categorized unit, component, integration, and API coverage for successful root/replay summary-item-count parity plus explicit invalid aggregate-count warnings on the packaged serve flow

- Exact batch-summary input-byte parity validation for installed-artifact `/v0/tag/files` smoke:
  - implemented in `src/ades/release.py`
  - tightens clean-environment wheel/npm release smoke so both the root batch payload and replay payload must keep the exact deterministic aggregate `summary.discovered_input_bytes`, `summary.included_input_bytes`, and `summary.processed_input_bytes` totals for the two packaged smoke inputs instead of only agreeing on per-item sizes
  - adds categorized unit, component, integration, and API coverage for successful root/replay summary-input-byte parity plus explicit invalid aggregate-byte warnings on the packaged serve flow

- Exact source-fingerprint parity validation for installed-artifact `/v0/tag/files` smoke:
  - implemented in `src/ades/release.py`
  - tightens clean-environment wheel/npm release smoke so both the root batch payload and replay payload must keep the exact deterministic per-item `source_fingerprint` list for the two packaged smoke inputs instead of only agreeing on paths and input sizes
  - adds categorized unit, component, integration, and API coverage for successful root/replay source-fingerprint parity plus explicit invalid source-fingerprint warnings on the packaged serve flow

- Exact input-size parity validation for installed-artifact `/v0/tag/files` smoke:
  - implemented in `src/ades/release.py`
  - tightens clean-environment wheel/npm release smoke so both the root batch payload and replay payload must keep the exact deterministic per-item `input_size_bytes` list for the two packaged smoke inputs instead of only agreeing on the processed source files
  - adds categorized unit, component, integration, and API coverage for successful root/replay input-size parity plus explicit invalid input-size warnings on the packaged serve flow

- Exact source-path parity validation for installed-artifact `/v0/tag/files` smoke:
  - implemented in `src/ades/release.py`
  - tightens clean-environment wheel/npm release smoke so both the root batch payload and replay payload must keep the exact deterministic `source_path` list for the two packaged smoke inputs instead of only agreeing on rerun-diff changed paths
  - adds categorized unit, component, integration, and API coverage for successful root/replay source-path parity plus explicit invalid source-path warnings on the packaged serve flow

- Root batch identity validation for installed-artifact `/v0/tag/files` smoke:
  - implemented in `src/ades/release.py`
  - tightens clean-environment wheel/npm release smoke so the initial batch response itself must report `pack=finance-en` and `item_count=2`, matching the already-hardened replay payload identity checks instead of only validating replay-side identity
  - adds categorized unit, component, integration, and API coverage for successful root batch identity plus explicit invalid root pack/item-count warnings on the packaged serve flow

- Exact saved-path parity validation for installed-artifact `/v0/tag/files` smoke:
  - implemented in `src/ades/release.py`
  - tightens clean-environment wheel/npm release smoke so the root batch manifest path, replay manifest path, and both per-item saved output paths must match the exact deterministic paths under the smoke output directory rather than only matching filename suffixes or item counts
  - adds categorized unit, component, integration, and API coverage for successful exact-path parity plus explicit invalid saved-path warnings on the packaged serve flow

- Final Phase B1 release-helper operational-hardening closure:
  - implemented in `src/ades/cli.py` and `src/ades/service/app.py`
  - aligns `ades release verify`, `ades release validate`, `ades release versions`, `ades release sync-version`, `ades release manifest`, and `GET /v0/release/versions` with the existing operator-facing release failure contract so missing release-layout files now surface deterministic CLI stderr plus exit code `1` and HTTP `404`, while invalid release-version metadata or version-update failures surface deterministic CLI stderr plus exit code `1` and HTTP `400`
  - keeps the underlying public Python release helpers on raw exceptions while adding categorized unit, component, integration, and API coverage for missing-layout and invalid-version release-helper failures
  - closes step 4 CLI and service operational hardening and moves the active queue to end-to-end production-readiness validation

- Eighth Phase B1 release-publish operational-hardening slice:
  - implemented in `src/ades/cli.py`
  - aligns `ades release publish` with the existing operator-facing publish request contract so missing manifest files and invalid/unvalidated release manifests now surface deterministic CLI stderr plus exit code `1`, while the shared Python API keeps raw exceptions and `POST /v0/release/publish` keeps its existing `404`/`400` mapping
  - adds categorized unit, component, integration, and API coverage for missing-manifest and unvalidated-manifest publish failures without changing coordinated publish success or publish-command failure warnings

- Seventh Phase B1 pack-access config-parse hardening slice:
  - implemented in `src/ades/config.py` and `src/ades/cli.py`
  - introduces `InvalidConfigurationError` for invalid TOML and scalar coercion failures such as an invalid `ADES_PORT`, and aligns `ades pull` with the deterministic CLI stderr plus exit code `1` contract for those parse-driven configuration failures
  - keeps the underlying Python API `get_pack()` and `pull_pack()` helpers on raw exceptions, preserves the existing localhost-service `400` behavior through the `ValueError` subclass boundary, and does not catch generic install-time `ValueError`s so checksum or artifact failures still bubble
  - adds categorized config, unit, component, integration, and API coverage for invalid config syntax and invalid port parsing without weakening the existing checksum-mismatch regression

- Sixth Phase B1 pack-access operational-hardening slice:
  - implemented in `src/ades/service/app.py` and `src/ades/cli.py`
  - aligns `GET /v0/packs/{pack_id}` with the existing missing-config-file `404` and invalid-runtime `400` localhost-service contract, and aligns `ades pull` with the same deterministic CLI stderr plus exit code `1` handling for the non-ambiguous configuration/runtime failures
  - keeps the underlying Python API `get_pack()` and `pull_pack()` helpers on their original exception types, and intentionally leaves generic pull-side `ValueError` parsing failures for a later seam so checksum or artifact install failures are not relabeled as configuration errors
  - adds categorized unit, component, integration, and API coverage for pack-access configuration failures while the existing checksum-mismatch CLI regression continues to prove install failures still bubble

- Fifth Phase B1 registry-build operational-hardening slice:
  - implemented in `src/ades/cli.py` with the existing localhost-service contract locked in by new tests
  - aligns `ades registry build` with the existing operator-facing failure contract so missing pack directories and related request failures now surface deterministic CLI stderr plus exit code `1`, while `/v0/registry/build` keeps its existing `404`/`400` mapping
  - keeps the underlying Python API `build_registry()` helper on its original exceptions while adding categorized unit, component, integration, and API coverage for registry-build failure cases

- Fourth Phase B1 pack-mutation operational-hardening slice:
  - implemented in `src/ades/cli.py` and `src/ades/service/app.py`
  - aligns `ades packs activate`, `ades packs deactivate`, `ades packs remove`, and the matching localhost service routes with the same missing-config-file `404` and invalid-runtime `400` operator contract already present on the status, pack-listing, and lookup surfaces
  - keeps the underlying Python API activate/deactivate/remove helpers on their original exceptions while adding categorized unit, component, integration, and API coverage for pack-mutation configuration failures

- Third Phase B1 lookup operational-hardening slice:
  - implemented in `src/ades/cli.py` and `src/ades/service/app.py`
  - aligns `ades packs lookup` and `GET /v0/lookup` with the same missing-config-file `404` and invalid-runtime `400` operator contract already present on the status and pack-listing surfaces
  - keeps the underlying Python API `lookup_candidates()` helper on its original exceptions while adding categorized unit, component, integration, and API coverage for lookup configuration failures

- Second Phase B1 pack-listing operational-hardening slice:
  - implemented in `src/ades/cli.py` and `src/ades/service/app.py`
  - aligns `ades packs list`, `ades list packs`, `GET /v0/packs`, and `GET /v0/packs/available` with the same missing-config-file `404` and invalid-runtime `400` operator contract that already exists on the status surfaces
  - keeps the underlying Python API list helpers on their original exceptions while adding categorized unit, component, integration, and API coverage for installed-pack and available-pack listing failures

- First Phase B1 status/config operational-hardening slice:
  - implemented in `src/ades/cli.py` and `src/ades/service/app.py`
  - aligns direct `ades status` and localhost `GET /v0/status` behavior so a missing explicit config file yields deterministic CLI stderr plus exit code `1` and HTTP `404`, while invalid runtime/backend configuration yields deterministic CLI stderr plus exit code `1` and HTTP `400`
  - keeps the underlying Python API contract unchanged while adding categorized unit, component, integration, and API coverage for missing-config-file and invalid-runtime cases

- Phase A pack lifecycle hardening:
  - completed across rollback-safe installs, same-version metadata repair, dependency-aware activate/deactivate semantics, direct/public pack removal, and clean-environment wheel/npm smoke validation
  - installed-artifact verification now proves `status`, `pull`, `serve`, live `/v0/tag`, `/v0/tag/file`, `/v0/tag/files`, manifest persistence, replay lineage, timestamp hygiene, dependency-removal guardrails, and packaged remove behavior against the shipped artifacts
  - keeps the packaged wheel and npm wrapper aligned with the in-repo Python API, CLI, and localhost service pack lifecycle contract

- Phase A storage and metadata recovery:
  - completed across list-driven repair, direct lookup repair, bootstrap recovery after deleting the local SQLite registry, and tagger regression coverage for partial metadata loss
  - now includes explicit backend-seam `delete_pack` contract coverage for the local SQLite store and the deferred PostgreSQL seam, proving pack-row deletion, metadata cascade cleanup, and sibling-pack preservation where that contract is shared
  - keeps the later PostgreSQL-backed server path behind the same public metadata-deletion contract without introducing a Phase C server dependency into the local-tool test flow

- Live batch-manifest root-lineage identity smoke validation for installed artifacts:
  - implemented in `src/ades/release.py`
  - extends release smoke verification so both the installed wheel and npm wrapper now require the root batch manifest to keep `lineage.parent_run_id` and `lineage.source_manifest_path` unset/null in the live `/v0/tag/files` path
  - keeps the release verify/validate contracts additive while surfacing explicit warning codes for invalid root-manifest lineage linkage fields in the packaged serve path
  - includes categorized unit, component, integration, and API coverage for successful root-lineage validation plus explicit invalid root-manifest lineage warning paths

- Live batch-manifest replay child run-id smoke validation for installed artifacts:
  - implemented in `src/ades/release.py`
  - extends release smoke verification so both the installed wheel and npm wrapper now require replay `lineage.run_id` to be present and distinct from the parent batch `lineage.run_id` in the live `/v0/tag/files` replay path
  - keeps the release verify/validate contracts additive while surfacing explicit warning codes for missing or invalid replay child run-id metadata in the packaged serve path
  - includes categorized unit, component, integration, and API coverage for successful replay child run-id validation plus explicit missing/invalid replay child run-id warning paths

- Live batch-manifest replay lineage root/parent run-id smoke validation for installed artifacts:
  - implemented in `src/ades/release.py`, with fake-runner support in `tests/release_helpers.py`
  - extends release smoke verification so both the installed wheel and npm wrapper now require the parent batch payload to keep `lineage.run_id` plus matching `lineage.root_run_id`, and require the replay child payload to keep `lineage.root_run_id` plus `lineage.parent_run_id` aligned with that parent run
  - keeps the release verify/validate contracts additive while surfacing explicit warning codes for missing or invalid replay lineage root/parent run-id metadata in the packaged serve path
  - includes categorized unit, component, integration, and API coverage for successful replay lineage validation plus explicit invalid lineage warning paths

- Live batch-manifest replay empty rerun-diff collection smoke validation for installed artifacts:
  - implemented in `src/ades/release.py`
  - extends release smoke verification so both the installed wheel and npm wrapper now require replay `rerun_diff.newly_processed`, `rerun_diff.reused`, `rerun_diff.repaired`, and `rerun_diff.skipped` to stay empty lists in the live `/v0/tag/files` replay path
  - keeps the release verify/validate contracts additive while surfacing explicit warning codes for missing or invalid replay rerun-diff empty-collection metadata in the packaged serve path
  - includes categorized unit, component, integration, and API coverage for successful replay empty-collection validation plus explicit invalid rerun-diff collection warning paths

- Live batch-manifest replay rerun-diff smoke validation for installed artifacts:
  - implemented in `src/ades/release.py`, with fake-runner support in `tests/release_helpers.py`
  - extends release smoke verification so both the installed wheel and npm wrapper now require replay `rerun_diff.manifest_input_path` plus the deterministic two-file `rerun_diff.changed` set from the live `/v0/tag/files` replay path
  - keeps the release verify/validate contracts additive while surfacing explicit warning codes for missing or invalid replay rerun-diff metadata in the packaged serve path
  - includes categorized unit, component, integration, and API coverage for successful replay rerun-diff validation plus explicit invalid rerun-diff warning paths

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

- Live batch-manifest replay payload-identity smoke validation for installed artifacts:
   - implemented in `src/ades/release.py`
   - extends release smoke verification so both the installed wheel and npm wrapper now require replay top-level `pack=\"finance-en\"` and `item_count=2` in addition to the existing replay summary and lineage contract
   - keeps the release verify/validate contracts unchanged while making replay pack/item-count mismatches surface as explicit warning codes in the packaged smoke path
   - includes categorized unit, component, integration, and API coverage for successful replay payload-identity validation plus explicit invalid replay pack/item-count warnings

## Not Next

- The PostgreSQL-backed production server remains intentionally deferred until the local-tool `v0.1.0` scope is closed.
- Historical architecture research notes in `docs/ades_chat_output.md` and `docs/ades_architecture.html` are not the release-truth source for `v0.1.0`; use the decisions, implementation plan, development progress, and this queue instead.
