# Impact Path Relationship Expansion Progress

Started: 2026-05-05
Plan: docs/planning/impact-path-relationship-expansion-plan.md

## Current Status

- Status: implementation in progress.
- Codebase audit recorded in `docs/planning/impact-path-relationship-expansion-codebase-audit.md`.
- Safety posture: disabled-by-default feature work; existing extraction, hybrid, vector, and graph-context behavior must remain unchanged unless the caller explicitly requests impact expansion.
- Data storage rule: all large source downloads, raw snapshots, normalized source-lane outputs, and large graph intermediates belong under `/mnt/githubActions/ades_big_data`.

## Milestones

1. Repo/API/config/test surfaces mapped. Done.
2. Market graph schema, builder, reader, and Python expansion API. Done for Phase 1.
3. Local tests for builder, deterministic expansion, graceful degradation, and API contract. Targeted tests passing.
4. Public refs-only HTTP wiring. Implemented behind disabled-by-default runtime flag.
5. Local full validation. Done.
6. Commit, push, deploy to prod, and prod smoke tests. Pending.

## Notes

- The new lane should use a separate `market_graph_store.sqlite` artifact and should not reuse or mutate the existing QID graph store.
- Runtime expansion must be read-only and bounded by depth, seed, edge, and candidate limits.
- Root response payloads must not add bullish, bearish, or direction fields.
- Existing hybrid/vector extraction remains unchanged unless a caller explicitly requests impact expansion and the feature flag is enabled.
- Targeted validation: `python -m pytest tests/unit/test_impact_expansion.py tests/api/test_impact_expansion_endpoint.py tests/unit/test_config_file.py` passed 14 tests.
- Enriched vector regression validation: `python -m pytest tests/unit/test_vector_service.py tests/unit/test_vector_context_engine.py tests/unit/test_vector_graph_store.py tests/api/test_tag_vector_runtime_endpoint.py` passed 102 tests.
- Full local validation: `python -m pytest` passed 1414 tests in 446.31s.
- Full-repo `python -m ruff check src tests` still reports pre-existing unrelated lint issues; touched-file Ruff passed.
- Built a small reviewed starter artifact under `/mnt/githubActions/ades_big_data/artifacts/market-graph/current/market_graph_store.sqlite` from normalized TSVs under `/mnt/githubActions/ades_big_data/pack_sources/impact_relationships/phase1_starter`.
- Local starter smoke passed: `finance-en` tagging with `include_impact_paths=True` returned the Strait of Hormuz -> crude oil candidate path and Iran -> OPEC+ passive path with no impact warnings.
- Production deployment docs now include the optional impact-expansion systemd drop-in and artifact path.
