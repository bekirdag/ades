# Impact Path Relationship Expansion Codebase Audit

Status: implementation audit
Date: 2026-05-05

## Completed / Aligned

- Existing extraction returns linked ADES entities through `TagResponse.entities`.
- Existing QID graph context is feature-flagged and gracefully degrades when disabled or unavailable.
- Existing vector/QID graph code already provides useful SQLite builder/reader patterns.
- `DEFAULT_STORAGE_ROOT` already points at `/mnt/githubActions/ades_big_data`.
- API/service routes already parse opt-in tagging options through `TagRequest.options`.
- Production deployment flow exists and can deploy a new wheel without changing runtime pack artifacts.

## Partially Completed

- Graph artifact patterns exist for Wikidata/QID relationships, but not for market transmission edges.
- Related entities and graph support exist, but they are neutral QID-context enrichment, not auditable impact paths.
- Pack-scoped graph build endpoints exist for QID graph stores, but not for market graph artifacts.
- Tag responses can carry optional enrichment fields, but no `impact_paths` payload exists yet.

## Missing

- `src/ades/impact` package for market graph build/read/expansion logic.
- `market_graph_store.sqlite` schema with `build_metadata`, `impact_nodes`, `impact_edges`, and `impact_edge_packs`.
- Normalized TSV source-lane ingestion contract for nodes and edges.
- Build-time `seed_degree` generation.
- Artifact metadata in expansion results: `graph_version`, `artifact_version`, and `artifact_hash`.
- Disabled-by-default `impact_expansion` runtime settings and environment/config parsing.
- Python API for `expand_impact_paths`.
- Optional tag response enrichment behind explicit request plus feature flag.
- Pydantic response models for source entities, relationship paths, candidates, passive paths, and builder responses.
- Graceful warnings for missing, unreadable, or disabled market graph artifacts.
- Tests for direct tradables, derived candidates, zero-degree seed skipping, deterministic ordering, deduplication, and artifact failures.
- Golden evaluation set and real inner-ring source lanes.

## Misaligned

- Existing `graph_context` uses QID/Wikidata coherence, while the plan needs a separate semantic market-transmission graph.
- Existing `related_entities` are neutral related nodes, not terminal tradable candidates with relationship provenance.
- Existing graph code traverses predicates, while the plan requires explicit relation families and edge-level `direction_hint`.
- Existing vector proposal logic is for QID related entity discovery; impact vector proposals must remain disabled until graph-supported admission is built.
- The plan says public HTTP should wait until Python validation, so the first implementation should keep HTTP impact exposure private or absent.

## Issues / Risks

- Stable ADES `entity_ref` concordance is the biggest data risk; the graph must use refs emitted by extraction.
- Source licensing can block normalized outputs for Comtrade, OECD, IEA, OpenCorporates, and similar datasets.
- Company-level relationships are high-risk and should remain deferred except official sanctions-list direct evidence.
- The feature must not mutate or weaken the existing hybrid/vector extraction system.
- Large source downloads and generated graph intermediates must stay out of git and under `/mnt/githubActions/ades_big_data`.
- The full plan includes data acquisition, licensing review, evaluation, and deployment; the first code pass can complete Phase 1 infrastructure, but Phase 2+ requires curated source data and reviewed golden articles.
