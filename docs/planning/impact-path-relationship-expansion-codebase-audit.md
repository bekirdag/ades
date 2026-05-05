# Impact Path Relationship Expansion Codebase Audit

Status: post-Phase-1 implementation audit
Date: 2026-05-05

## Completed / Aligned

- Existing extraction returns linked ADES entities through `TagResponse.entities`.
- Existing QID graph context is feature-flagged and gracefully degrades when disabled or unavailable.
- Existing vector/QID graph code already provides useful SQLite builder/reader patterns.
- `DEFAULT_STORAGE_ROOT` already points at `/mnt/githubActions/ades_big_data`.
- API/service routes already parse opt-in tagging options through `TagRequest.options`.
- Production deployment flow exists and can deploy a new wheel without changing runtime pack artifacts.
- `src/ades/impact` now contains the market graph builder, read-only store, deterministic expansion runtime, packaged starter resources, and refs-only evaluator.
- `market_graph_store.sqlite` now has `build_metadata`, `impact_nodes`, `impact_edges`, and `impact_edge_packs`.
- Build-time `seed_degree`, artifact metadata, pack-scoped edges, deterministic dedupe, and graceful artifact warnings are implemented.
- Python API, optional tag enrichment, and refs-only HTTP endpoint are implemented.
- The packaged starter lane covers the eight inner-ring relation families with reviewed source rows.
- The production workflow now builds and uploads the starter market graph artifact and writes the prod impact-expansion drop-in.

## Partially Completed

- Phase 2 evaluation mechanics exist, but the current golden set is a starter refs-only set, not the required 20 to 30 manually reviewed article set.
- Source-lane ingestion exists for reviewed TSVs, but bulk official-source download/normalization lanes are not implemented yet.
- Public HTTP exists earlier than the original plan sequence; this is acceptable because it is refs-only, bounded, and deployed behind the feature flag, but the plan should treat the endpoint as Phase 1.5 rather than fully validated Phase 3.

## Missing

- Full Phase 2 official-source build: bulk/refreshable source lanes for EIA/OPEC/JODI/WITS/BIS/central-bank/sanctions data.
- Manual 20 to 30 article golden set with text-level extraction plus impact evaluation.
- License review and source manifest records for restricted or unclear-license sources before normalized outputs are written.
- Concrete promotion thresholds for p95 latency and empty-path rate based on the full Phase 2 evaluation.

## Misaligned

- Existing `graph_context` uses QID/Wikidata coherence, while the plan needs a separate semantic market-transmission graph.
- Existing `related_entities` are neutral related nodes, not terminal tradable candidates with relationship provenance.
- Existing graph code traverses predicates, while the plan requires explicit relation families and edge-level `direction_hint`.
- Existing vector proposal logic is for QID related entity discovery; impact vector proposals must remain disabled until graph-supported admission is built.
- The plan says public HTTP should wait until Python validation, but the implemented refs-only endpoint shipped early behind feature and caller opt-in flags. Keep it, but do not market it as Phase 3 complete until the full article golden set passes.

## Issues / Risks

- Stable ADES `entity_ref` concordance is the biggest data risk; the graph must use refs emitted by extraction.
- Source licensing can block normalized outputs for Comtrade, OECD, IEA, OpenCorporates, and similar datasets.
- Company-level relationships are high-risk and should remain deferred except official sanctions-list direct evidence.
- The feature must not mutate or weaken the existing hybrid/vector extraction system.
- Large source downloads and generated graph intermediates must stay out of git and under `/mnt/githubActions/ades_big_data`.
- The full plan includes data acquisition, licensing review, evaluation, and deployment; the runtime and starter lane are complete, but Phase 2+ requires curated source data and reviewed golden articles.
