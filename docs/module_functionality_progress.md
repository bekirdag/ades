# Module Functionality Progress

Started: 2026-04-08
Current goal: make the shipped local-tool modules operationally complete before widening scope.

## Plan

1. Lightweight local candidate search/index
   Status: complete
   Goal: add broader ranked lookup over installed-pack metadata without changing the existing lookup API, CLI, or service contracts.

2. Pack lifecycle hardening
   Status: pending
   Goal: validate pull/install/activate/deactivate/remove behavior against real artifacts and failure cases.

3. Storage and metadata recovery
   Status: pending
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
- validate pull/install/activate/deactivate flows against production-like artifact handling
- add failure-path coverage for partial installs, checksum mismatch, and stale local state
- keep the local/public contracts stable while tightening operational behavior

## Work Log

- 2026-04-08: started the production-readiness plan and began implementing the candidate-search layer against the existing SQLite metadata store and lookup surface.
- 2026-04-08: added the first broader local search slice in `src/ades/storage/registry_db.py` with SQLite FTS-backed multi-term lookup when available and deterministic fallback when it is not.
- 2026-04-08: added categorized coverage for multi-term cross-field lookup in `tests/unit/test_lookup_search.py`, `tests/component/test_cli_lookup_search.py`, `tests/integration/test_lookup_search_api.py`, and `tests/api/test_lookup_search_endpoint.py`.
- 2026-04-08: focused validation passed for the new coverage files, targeted lookup regressions, and `python -m compileall src/ades`.
- 2026-04-08: full repo validation passed through `docdexd run-tests --repo /home/wodo/apps/ades`, including `142 passed` under `pytest -q` plus coordinated release validation and smoke-install checks.
