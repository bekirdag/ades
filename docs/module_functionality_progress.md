# Module Functionality Progress

Started: 2026-04-08
Current goal: make the shipped local-tool modules operationally complete before widening scope.

## Plan

1. Lightweight local candidate search/index
   Status: complete
   Goal: add broader ranked lookup over installed-pack metadata without changing the existing lookup API, CLI, or service contracts.

2. Pack lifecycle hardening
   Status: in progress
   Goal: validate pull/install/activate/deactivate/remove behavior against real artifacts and failure cases.

3. Storage and metadata recovery
   Status: in progress
   Goal: ensure first-run bootstrap, repair, stale-state cleanup, and idempotent metadata operations are safe.

4. CLI and service operational hardening
   Status: pending
   Goal: tighten startup validation, error paths, exit behavior, and operator-facing responses.

5. End-to-end production-readiness validation
   Status: pending
   Goal: verify clean-environment flows for install, pull, serve, tag, and recovery paths.

## Active Slice

Focus: step 2, pack lifecycle hardening.

Planned work:
- add broader clean-environment recovery combinations after the installed pull/tag, direct lookup-repair, serve-smoke, and metadata-bootstrap recovery slices
- keep validating repeated idempotent pack operations and lifecycle recovery behavior against published registry artifacts
- keep the new rollback-safe install semantics, lookup-driven metadata repair, and SQLite recovery behavior stable while finishing the remaining pack-lifecycle recovery work

## Work Log

- 2026-04-08: started the production-readiness plan and began implementing the candidate-search layer against the existing SQLite metadata store and lookup surface.
- 2026-04-08: added the first broader local search slice in `src/ades/storage/registry_db.py` with SQLite FTS-backed multi-term lookup when available and deterministic fallback when it is not.
- 2026-04-08: added categorized coverage for multi-term cross-field lookup in `tests/unit/test_lookup_search.py`, `tests/component/test_cli_lookup_search.py`, `tests/integration/test_lookup_search_api.py`, and `tests/api/test_lookup_search_endpoint.py`.
- 2026-04-08: focused validation passed for the new coverage files, targeted lookup regressions, and `python -m compileall src/ades`.
- 2026-04-08: full repo validation passed through `docdexd run-tests --repo /home/wodo/apps/ades`, including `142 passed` under `pytest -q` plus coordinated release validation and smoke-install checks.
- 2026-04-08: hardened `src/ades/packs/installer.py` so pack installs now stage extracted artifacts into hidden temporary directories, only replace the destination after extraction succeeds, and restore the previous installed pack if metadata sync fails during an update.
- 2026-04-08: added categorized rollback and cleanup coverage in `tests/unit/test_pack_install_rollback.py`, `tests/component/test_cli_pack_install_failures.py`, `tests/integration/test_pack_install_failures_api.py`, and `tests/api/test_pack_install_rollback_endpoint.py`.
- 2026-04-08: focused failure-path tests, nearby pack/service regressions, and `python -m compileall src/ades` passed for the rollback-safe install slice.
- 2026-04-09: hardened repeated same-version pulls so `src/ades/packs/installer.py` now refreshes on-disk pack metadata into SQLite before skipping or reactivating an installed pack, which repairs stale alias/rule/label state without changing the public pull contract.
- 2026-04-09: added categorized stale-metadata repair coverage in `tests/unit/test_pack_install_reactivation.py`, `tests/component/test_cli_pack_reactivation.py`, `tests/integration/test_pack_reactivation_api.py`, and `tests/api/test_pack_reactivation_endpoint.py`.
- 2026-04-09: focused stale-metadata repair tests passed with `9 passed` across the reactivation coverage files, and `python -m compileall src/ades` passed for the slice.
- 2026-04-09: full repo release validation passed through `docdexd run-tests --repo /home/wodo/apps/ades`, including `161 passed` under `pytest -q` plus successful Python/npm smoke-install checks.
- 2026-04-09: hardened pack activation/deactivation in `src/ades/api.py` so dependency deactivation is blocked when active dependents exist and pack activation recursively reactivates inactive installed dependencies before reactivating the requested pack.
- 2026-04-09: updated CLI and local service error handling in `src/ades/cli.py` and `src/ades/service/app.py` so dependency-chain guardrail failures return deterministic CLI errors and HTTP `400` responses instead of partially mutating pack state.
- 2026-04-09: added categorized dependency-guardrail coverage in `tests/unit/test_pack_install_reactivation.py`, `tests/component/test_cli_pack_reactivation.py`, `tests/integration/test_pack_reactivation_api.py`, and `tests/api/test_pack_reactivation_endpoint.py`, with focused activation/deactivation validation passing across the lifecycle and top-level API/service tests.
- 2026-04-09: full repo release validation passed again after the dependency-guardrail slice through `docdexd run-tests --repo /home/wodo/apps/ades`, including `169 passed` under `pytest -q` plus successful Python/npm smoke-install checks.
- 2026-04-09: extended `src/ades/release.py` so clean-environment release smoke verification now runs `status`, `pull general-en`, and a deterministic `tag` command through both the installed wheel and the installed npm wrapper instead of validating `status` only.
- 2026-04-09: extended the shared release smoke response shape in `src/ades/service/models.py` and updated `tests/release_helpers.py` plus the categorized release verify/validate tests so pull/tag smoke behavior and tag-failure warnings are covered across unit, component, integration, and API layers.
- 2026-04-09: focused release verification/validation tests passed with `22 passed`, and `python -m compileall src/ades` passed for the clean-environment pull/tag smoke slice.
- 2026-04-09: hardened `src/ades/packs/registry.py` so installed-pack listing now repairs missing SQLite metadata rows from on-disk manifests even when other valid installed-pack rows still remain.
- 2026-04-09: added categorized list-repair coverage in `tests/unit/test_pack_install_reactivation.py`, `tests/component/test_cli_pack_reactivation.py`, `tests/integration/test_pack_reactivation_api.py`, and `tests/api/test_pack_reactivation_endpoint.py`, with focused recovery validation passing at `21 passed`.
- 2026-04-09: full repo release validation passed again after the list-repair slice through `docdexd run-tests --repo /home/wodo/apps/ades`, including `174 passed` under `pytest -q` plus successful Python/npm smoke-install checks.
- 2026-04-09: extended `src/ades/release.py` so clean-environment release smoke verification now launches installed-artifact `ades serve` for both the wheel and npm wrapper, probes `/healthz` plus `/v0/status`, and records the running service results before shutdown.
- 2026-04-09: extended `src/ades/service/models.py` plus `tests/release_helpers.py` and the categorized release verify tests so additive serve-smoke results and serve-failure warnings are covered across unit, component, integration, and API layers, with focused validation passing at `23 passed`.
- 2026-04-09: full repo release validation passed again after the serve-smoke slice through `docdexd run-tests --repo /home/wodo/apps/ades`, including `175 passed` under `pytest -q` plus successful wheel/npm clean-environment serve probes.
- 2026-04-09: extended `src/ades/release.py` so clean-environment release smoke verification now deletes the local SQLite registry after pull/tag, validates bootstrap recovery through a fresh `status` call, deletes the registry again, and then starts `ades serve` so the packaged artifacts prove metadata-loss recovery against on-disk packs.
- 2026-04-09: extended `src/ades/service/models.py` plus `tests/release_helpers.py` and the categorized release verify tests so additive `recovery_status` plus `recovered_pack_ids` results and explicit recovery-failure warnings are covered across unit, component, integration, and API layers.
- 2026-04-09: focused bootstrap-recovery verification/validation tests passed with `24 passed`, `python -m compileall src/ades` passed, and the full repo release-validation gate passed again through `docdexd run-tests --repo /home/wodo/apps/ades`, including `176 passed` under `pytest -q`.
- 2026-04-09: hardened `src/ades/packs/registry.py` so lookup misses now repair missing installed-pack SQLite rows from on-disk manifests before retrying, which removes the previous requirement that `list_packs`, `ades packs list`, or `/v0/packs` run first to heal a deleted pack row.
- 2026-04-09: updated categorized lookup-repair coverage in `tests/unit/test_pack_install_reactivation.py`, `tests/component/test_cli_pack_reactivation.py`, `tests/integration/test_pack_reactivation_api.py`, and `tests/api/test_pack_reactivation_endpoint.py`, with focused validation passing again at `21 passed`.
- 2026-04-09: full repo release validation passed again after the direct lookup-repair slice through `docdexd run-tests --repo /home/wodo/apps/ades`, including `176 passed` under `pytest -q` plus successful Python/npm smoke-install, recovery, and serve checks.
- 2026-04-09: added explicit tagger-regression coverage in `tests/test_tagger.py`, `tests/component/test_lookup_driven_tagger.py`, `tests/integration/test_lookup_driven_tag_api.py`, and `tests/api/test_tag_lookup_aliases.py` so alias-driven finance tagging is proven to survive partial installed-pack row loss after deleting the `finance-en` SQLite row.
- 2026-04-09: focused tagger-regression validation passed with `9 passed`, `python -m compileall src/ades` passed again, and the full repo release-validation gate passed through `docdexd run-tests --repo /home/wodo/apps/ades`, now including `180 passed` under `pytest -q`.
