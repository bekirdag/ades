# Impact Path Relationship Expansion Plan

Status: Phase 1 deployed; starter source lane and refs-only evaluator implemented; full Phase 2 data/evaluation pending
Last merged: 2026-05-05
Scope: ADES relationship expansion for finance, economy, politics, policy, sanctions, commodities, currencies, rates, and geopolitics/security news with market transmission.

## Problem

ADES currently extracts entities that are explicitly present in a document. That is necessary, but not sufficient for market-impact use cases.

Example:

- Article mentions Iran and the Strait of Hormuz.
- ADES extracts Iran and the Strait of Hormuz.
- A downstream product wants to know that the story may relate to crude oil, oil shipping, energy markets, shipping insurers, USD-denominated commodities, and possibly country or sector exposures.

The missing layer is a deterministic relationship expansion layer:

This article mentions these ADES entities. Through these ADES graph relationships, these tradable assets or tradable proxies may be impacted.

The goal is not to predict prices. The goal is to return auditable relationship paths from extracted entities to candidate market entities.

## Product Goal

Build a bounded, deterministic market-relationship graph that expands extracted ADES entities into:

- directly mentioned tradable entities,
- derived tradable candidates,
- passive but market-relevant entities,
- full relationship paths and source provenance,
- artifact metadata so downstream systems can cache and re-evaluate old stories when the graph changes.

ADES should answer:

- what entities were mentioned,
- which mentioned entities are graph seeds,
- which market candidates are reachable from those seeds,
- why each candidate was returned,
- which source artifact and source dataset produced the edge.

ADES must not answer:

- whether the candidate is bullish or bearish,
- whether a user should trade,
- whether the market has already priced the event.

## Core Rules

1. This is not a greenfield open graph search. Expansion must be bounded by configured seed limits, edge limits, and a small maximum depth.
2. Default graph traversal depth is `max_depth = 2`.
3. The engine must be deterministic for the same request and the same artifact versions.
4. Determinism means no random tie-breaking, no runtime counters, no mutable external state, and no dependency on cache hit order.
5. Relationship expansion is read-only at runtime. No request may write edges, scores, or graph-learning state.
6. Vector search can propose candidates only after a feature flag is enabled, and proposed candidates still require explicit graph support before admission.
7. Every derived candidate must have at least one relationship path back to one or more extracted ADES entity refs.
8. A directly extracted tradable entity may appear with `evidence_level: direct` and an empty `relationship_path`.
9. Root HTTP/API responses must not contain `direction`, `bullish`, or `bearish` fields. Direction-like information is allowed only as edge-level `direction_hint` provenance for downstream interpretation.
10. The graph operates on one request and one static artifact at a time. No cross-article aggregation, trending-topic state, or session memory may strengthen a candidate.

## Entity Classes

### Mentioned Entity

An entity extracted directly from document text by the existing ADES extraction pipeline.

Required fields:

- `entity_ref`
- `canonical_name`
- `entity_type`
- `library_id`
- extraction confidence
- mention spans or mention evidence when available

### Direct Tradable Candidate

A mentioned entity that is itself tradable or directly maps to a tradable proxy.

Examples:

- crude oil,
- Brent crude,
- EUR/USD,
- US Treasury yields,
- S&P 500,
- gold.

Rules:

- `evidence_level` is `direct`.
- `relationship_path` may be empty.
- It appears in the same `candidates` list as derived candidates, not in a separate response section.

### Graph Seed

A mentioned entity eligible to start relationship expansion.

Eligibility must consider:

- extraction confidence,
- library/domain scope,
- entity class allowlist,
- document relevance,
- graph artifact presence,
- `seed_degree > 0` in `impact_nodes`.

The builder must compute `seed_degree` at build time so runtime seed selection can skip zero-degree nodes without executing a graph query per candidate.

### Derived Candidate

A non-mentioned or indirectly reached market candidate returned because a graph path connects it to one or more graph seeds.

Rules:

- `evidence_level` is `shallow` or `inferred`, not `direct`.
- `relationship_path` must be populated.
- `source_entity_refs` must identify the extracted ADES entities that led to the candidate.

### Passive Entity

A non-tradable but market-relevant entity that should be shown as context or as an intermediate node.

Examples:

- country,
- central bank,
- chokepoint,
- sanctioning body,
- policy institution,
- commodity-producing region.

Rules:

- Passive entities are not bullish or bearish.
- A directly mentioned passive may have an empty passive path.
- A passive discovered through expansion must have a populated path.

## Relationship Families

### Phase 1 Inner Ring

Start with a strict inner ring of eight families. These cover the most common finance/economy/politics/geopolitics transmission stories while keeping the first build reviewable.

1. `country_exports_commodity`
2. `country_produces_commodity`
3. `chokepoint_affects_commodity`
4. `commodity_traded_in_currency`
5. `central_bank_affects_currency`
6. `central_bank_affects_rates`
7. `sanction_affects_country`
8. `sanction_affects_commodity`

These eight cover energy disruptions, shipping chokepoints, monetary policy, and sanctions. Other relationship families should follow only after the golden evaluation set confirms the inner ring is working.

### Phase 2 Macro Extensions

Candidate follow-on families:

- `country_imports_commodity`
- `country_consumes_commodity`
- `country_affects_currency`
- `country_affects_sovereign_debt`
- `commodity_input_to_sector`
- `commodity_affects_sector`
- `rate_affects_bank_sector`
- `rate_affects_equity_index`
- `currency_affects_equity_index`
- `policy_affects_sector`
- `region_contains_country`
- `country_member_of_bloc`

The exact Phase 2 set should be locked only after Phase 1 evaluation identifies the highest-value missing paths.

### Deferred Company-Level Families

Company-level exposure families are high value but easy to pollute with weak evidence. They should not be part of Phase 1.

Deferred families include:

- `company_revenue_exposure_country`
- `company_supplier_country`
- `company_customer_country`
- `company_input_commodity`
- `company_output_commodity`
- `company_parent_subsidiary`
- `company_peer_competitor`

### Company Sanctions Exception

`sanction_affects_company` can be promoted earlier than other company families only when it is keyed to official sanctions-list identifiers.

Rules:

- source must be an official sanctions list,
- `source_name` and `source_url` are required,
- the edge must use the official list entity identifier or a reviewed mapping,
- `evidence_level` may be `direct`,
- no inferred company matching is allowed for this exception.

## Graph Artifact Decision

Use a separate SQLite artifact:

`market_graph_store.sqlite`

Do not fold these edges into the existing Wikidata-QID graph store. The market graph is a semantic transmission layer, not a general knowledge graph predicate store.

Reasons:

- different source model,
- different evidence levels,
- different scoring semantics,
- different refresh cadence,
- different response provenance requirements,
- easier deploy rollback by artifact hash.

### Required Schema Shape

The schema should be locked before response models are finalized.

`build_metadata`

```sql
CREATE TABLE build_metadata (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  graph_version TEXT NOT NULL,
  artifact_version TEXT NOT NULL,
  artifact_hash TEXT NOT NULL,
  builder_version TEXT NOT NULL,
  built_at TEXT NOT NULL,
  source_manifest_hash TEXT NOT NULL,
  node_count INTEGER NOT NULL,
  edge_count INTEGER NOT NULL
);
```

`impact_nodes`

```sql
CREATE TABLE impact_nodes (
  entity_ref TEXT PRIMARY KEY,
  canonical_name TEXT NOT NULL,
  entity_type TEXT NOT NULL,
  library_id TEXT,
  is_tradable INTEGER NOT NULL DEFAULT 0,
  is_seed_eligible INTEGER NOT NULL DEFAULT 0,
  seed_degree INTEGER NOT NULL DEFAULT 0,
  identifiers_json TEXT NOT NULL DEFAULT '{}',
  packs_json TEXT NOT NULL DEFAULT '[]'
);
```

`impact_edges`

```sql
CREATE TABLE impact_edges (
  edge_id TEXT PRIMARY KEY,
  source_ref TEXT NOT NULL,
  target_ref TEXT NOT NULL,
  relation TEXT NOT NULL,
  evidence_level TEXT NOT NULL,
  confidence REAL NOT NULL,
  direction_hint TEXT NOT NULL,
  source_name TEXT NOT NULL,
  source_url TEXT NOT NULL,
  source_snapshot TEXT NOT NULL,
  source_year INTEGER,
  refresh_policy TEXT NOT NULL,
  version TEXT NOT NULL,
  UNIQUE (source_ref, target_ref, relation, source_snapshot)
);
```

`impact_edge_packs`

```sql
CREATE TABLE impact_edge_packs (
  edge_id TEXT NOT NULL,
  pack_id TEXT NOT NULL,
  PRIMARY KEY (edge_id, pack_id)
);
```

Recommended indexes:

```sql
CREATE INDEX idx_impact_edges_source ON impact_edges(source_ref);
CREATE INDEX idx_impact_edges_target ON impact_edges(target_ref);
CREATE INDEX idx_impact_edges_relation ON impact_edges(relation);
CREATE INDEX idx_impact_edge_packs_pack ON impact_edge_packs(pack_id);
CREATE INDEX idx_impact_nodes_seed_degree ON impact_nodes(seed_degree);
```

### Multi-Pack Edge Rule

The same edge may be contributed by more than one pack.

Rules:

- Store the edge once in `impact_edges`.
- Use the highest-confidence source row as the canonical edge row.
- Store every contributing pack in `impact_edge_packs`.
- Runtime pack filtering joins through `impact_edge_packs`.
- Do not duplicate traversal edges merely because multiple packs contributed the same fact.

## Library Placement

The market graph should be an optional artifact loaded beside the existing ADES entity libraries.

Proposed paths:

- runtime artifact: `~/.ades/artifacts/market_graph_store.sqlite`
- build output: `build/market_graph_store.sqlite`
- source-lane normalized outputs: `/mnt/githubActions/ades_big_data/pack_sources/impact_relationships`
- raw source downloads: `/mnt/githubActions/ades_big_data/pack_sources/raw/impact-relationships`

The repo may contain small curated fixtures and reviewed concordance files. It must not contain large raw datasets, large extracted archives, or large generated graph intermediates.

## Proposed API Capability

### Python API First

Do not add a public HTTP endpoint until the Python API produces sensible results on real finance/economy/politics/geopolitics articles.

Initial internal API:

```python
expand_impact_paths(
    entity_refs: list[str],
    *,
    language: str = "en",
    enabled_packs: list[str] | None = None,
    max_depth: int = 2,
    impact_expansion_seed_limit: int = 16,
    max_candidates: int = 25,
    include_passive_paths: bool = True,
    vector_proposals_enabled: bool = False,
) -> ImpactExpansionResult
```

### Request Shape

```json
{
  "entity_refs": ["entity_iran", "entity_strait_of_hormuz"],
  "language": "en",
  "enabled_packs": ["finance-en", "politics-vector-en"],
  "max_depth": 2,
  "impact_expansion_seed_limit": 16,
  "max_candidates": 25,
  "include_passive_paths": true,
  "vector_proposals_enabled": false
}
```

`max_candidates` applies after deduplication by terminal candidate ref. The caller should receive up to 25 distinct terminal candidates, not 25 pre-deduplication paths.

### Response Shape

The response root must not include `direction`, `bullish`, or `bearish`.

```json
{
  "graph_version": "market-graph-v1",
  "artifact_version": "2026-05-05T00:00:00Z",
  "artifact_hash": "sha256:...",
  "warnings": [],
  "source_entities": [
    {
      "entity_ref": "entity_iran",
      "name": "Iran",
      "entity_type": "country",
      "is_graph_seed": true,
      "seed_degree": 6
    }
  ],
  "candidates": [
    {
      "entity_ref": "entity_crude_oil",
      "name": "Crude oil",
      "entity_type": "commodity",
      "is_tradable": true,
      "evidence_level": "shallow",
      "confidence": 0.74,
      "source_entity_refs": ["entity_strait_of_hormuz"],
      "relationship_paths": [
        {
          "path_depth": 1,
          "edges": [
            {
              "source_ref": "entity_strait_of_hormuz",
              "target_ref": "entity_crude_oil",
              "relation": "chokepoint_affects_commodity",
              "evidence_level": "direct",
              "confidence": 0.92,
              "direction_hint": "supply_risk",
              "source_name": "EIA World Oil Transit Chokepoints",
              "source_url": "https://www.eia.gov/international/analysis/special-topics/World_Oil_Transit_Chokepoints",
              "source_snapshot": "2026-05-05",
              "source_year": 2024
            }
          ]
        }
      ]
    },
    {
      "entity_ref": "entity_gold",
      "name": "Gold",
      "entity_type": "commodity",
      "is_tradable": true,
      "evidence_level": "direct",
      "confidence": 0.88,
      "source_entity_refs": ["entity_gold"],
      "relationship_paths": []
    }
  ],
  "passive_paths": [
    {
      "entity_ref": "entity_opec",
      "name": "OPEC",
      "entity_type": "organization",
      "relationship_paths": [
        {
          "path_depth": 1,
          "edges": [
            {
              "source_ref": "entity_iran",
              "target_ref": "entity_opec",
              "relation": "country_member_of_bloc",
              "evidence_level": "direct",
              "confidence": 0.95,
              "direction_hint": "context",
              "source_name": "OPEC member countries",
              "source_url": "https://www.opec.org/",
              "source_snapshot": "2026-05-05",
              "source_year": 2026
            }
          ]
        }
      ]
    }
  ]
}
```

### Graceful Degradation

If the market graph artifact is missing, unreadable, incompatible, or fails validation:

- extraction should still succeed,
- relationship expansion should return `candidates: []`,
- a warning such as `market_graph_artifact_missing` or `market_graph_artifact_unreadable` should be included,
- the request should not fail solely because impact expansion is unavailable.

This mirrors the current context-engine graceful-degradation pattern.

## Direction Boundary

ADES is not the direction engine. It may return edge-level hints that explain what kind of market transmission an edge represents.

Examples:

- `supply_risk`
- `demand_risk`
- `sanction_risk`
- `monetary_policy`
- `currency_denomination`
- `context`

`direction_hint` must be a required column in normalized source TSV files. The builder should not infer broad direction hints from relation family names unless the source lane explicitly owns that mapping and tests it. Curated per-edge hints are safer than static family-level defaults because policy and rate relationships can differ by event type.

Downstream systems may interpret these hints, but ADES must not convert them into bullish or bearish labels.

## Scoring

Use a two-layer confidence model.

### Build-Time Edge Confidence

Each edge has a stable confidence based on source quality and mapping certainty.

Examples:

- official direct source: high confidence,
- reviewed concordance: medium-high confidence,
- inferred sector exposure: lower confidence,
- vector-only proposal: not admitted unless graph-supported.

### Request-Time Candidate Confidence

Initial formula:

```text
candidate_confidence =
  extraction_confidence(seed)
  * min(edge_confidences_on_path)
  * depth_decay_factor
```

Direct tradable candidates may use extraction confidence directly because no graph edge is required.

Depth decay should be deterministic and versioned. A simple starting point:

- depth 0 direct mention: `1.00`
- depth 1: `0.90`
- depth 2: `0.75`

### Deduplication

Deduplicate by terminal candidate `entity_ref`.

For each terminal candidate:

- preserve all contributing source seed refs up to a bounded limit,
- preserve the best relationship paths up to a bounded per-candidate limit,
- select display ordering deterministically.

Recommended candidate ordering key:

```python
(-seed_count, shortest_path_depth, -candidate_confidence, entity_ref)
```

The final `entity_ref` tie breaker prevents input-order dependency when seed count, depth, and confidence are equal.

## Runtime Algorithm

1. Load the market graph artifact at process startup.
2. Validate `build_metadata`, schema version, artifact hash, and required indexes.
3. Build a request-local set of extracted ADES entity refs.
4. Convert mentioned entities into graph seeds only if they pass eligibility checks and `seed_degree > 0`.
5. Cap the seed set with `impact_expansion_seed_limit`.
6. Run bounded BFS from seeds:
   - default `max_depth = 2`,
   - bounded max edges per seed,
   - bounded max paths per terminal candidate,
   - no writes,
   - no mutable state.
7. Add directly mentioned tradable entities as `evidence_level: direct`.
8. If vector proposals are enabled, use vectors only to propose candidates.
9. Admit vector-proposed candidates only when explicit graph support exists.
10. Deduplicate by terminal candidate ref.
11. Apply `max_candidates` after deduplication.
12. Return artifact metadata, warnings, candidates, passive paths, and provenance.

The Phase 1 feature flag must describe the bounded-graph intent directly. Example:

```toml
[impact_expansion]
enabled = false
max_depth = 2
impact_expansion_seed_limit = 16
max_candidates = 25
vector_proposals_enabled = false
```

## Data Development

### Storage Rule

All large source downloads, raw snapshots, extracted archives, normalized source-lane outputs, and large graph-build intermediates for ADES impact relationships must live under:

`/mnt/githubActions/ades_big_data`

Recommended roots:

- raw downloads: `/mnt/githubActions/ades_big_data/pack_sources/raw/impact-relationships`
- normalized outputs: `/mnt/githubActions/ades_big_data/pack_sources/impact_relationships`

The repo may contain small source manifests, reviewed concordance fixtures, and hand-curated small TSVs. The repo must not contain large downloaded datasets or large generated graph artifacts.

Do not write normalized outputs from restricted or unclear-license sources until the license/terms have been reviewed. Do not let restricted normalized outputs accumulate under `/mnt/githubActions/ades_big_data` before that decision is made.

### Raw Source Manifest

Each raw source root should include a README or manifest with:

- source name,
- source URL,
- fetch date,
- local path,
- checksum,
- license or terms URL,
- expected refresh cadence,
- access restrictions,
- whether normalized output is allowed.

### Normalized TSV Contract

Every source lane should produce normalized TSV rows before the SQLite builder runs.

Required columns:

- `source_ref`
- `target_ref`
- `relation`
- `evidence_level`
- `confidence`
- `direction_hint`
- `source_name`
- `source_url`
- `source_snapshot`
- `source_year`
- `refresh_policy`
- `pack_ids`
- `notes`

Rules:

- `source_ref` and `target_ref` must be ADES entity refs, not arbitrary external IDs.
- External IDs can be stored in sidecar identifier columns or JSON metadata, but runtime expansion keys on ADES refs.
- Bulk dataset `source_url` should point to the dataset landing page, not a rotating row URL.
- `source_snapshot` is required for every bulk source.
- `source_year` is required when the source fact is year-specific.
- `direction_hint` is required and must not be silently inferred by the generic builder.

### Entity Ref Concordance

Before building the first graph, resolve ADES entity refs for the initial commodity, geography, currency, rates, central-bank, and chokepoint nodes.

The market graph must use the same refs returned by extraction. If extraction emits `entity_crude_oil`, the graph node must use `entity_crude_oil`, not a new local ID.

Sector datasets such as OECD ICIO require a reviewed concordance from sector codes to ADES sector entities. Keep small concordance fixtures in the repo so they can be reviewed independently from large source downloads.

## Trusted Source Candidates

Use official, multilateral, exchange, or widely trusted open datasets before commercial or weakly licensed sources.

### Chokepoints and Energy

- EIA World Oil Transit Chokepoints
- EIA International Energy Statistics
- OPEC Annual Statistical Bulletin and Monthly Oil Market Report
- JODI oil and gas data
- IEA datasets only where licensing permits local normalized use

Practical note: EIA chokepoint data is often published as analysis/PDF-style material rather than a fully normalized table. For Phase 1, a small hand-curated TSV with source URL and snapshot date is more defensible than fragile PDF parsing. The raw PDF or page snapshot belongs under `/mnt/githubActions/ades_big_data`; the small reviewed TSV can live in the repo.

### Trade and Commodity Flow

- World Bank WITS
- UN Comtrade, subject to license and access review
- WTO data where redistribution permits normalized use

Practical note: UN Comtrade free API access has strict limits and bulk access can require paid or special licensing. WITS may be a better first source for trade-flow build scripts. Do not write normalized Comtrade outputs until terms are reviewed.

### Input-Output and Sector Exposure

- OECD ICIO
- ADB MRIO where licensing permits local normalized use
- national statistical agency input-output tables where redistribution permits use

Practical note: OECD ICIO is large, released infrequently, and indexed by sector codes. Build the ISIC-sector-to-ADES-sector concordance first.

### Sanctions

- US OFAC SDN and consolidated sanctions lists
- EU consolidated sanctions list
- UK OFSI/HMT consolidated list
- UN Security Council consolidated sanctions list

These are strong sources for direct sanctions relationships. They are also the only acceptable early source lane for `sanction_affects_company`.

### Rates, Central Banks, and Currencies

- central bank official rate pages and downloadable series,
- BIS datasets,
- IMF IFS where licensing permits normalized local use,
- ECB statistical data warehouse for euro-area relationships.

### Corporate Identifiers and Ownership

- GLEIF LEI Level 1 and Level 2
- SEC EDGAR/XBRL for public-company disclosures
- OpenCorporates only after licensing review

Practical note: GLEIF LEI Level 2 is valuable but large and frequently updated. Treat it as Phase 3+ data, not Phase 1.

## Development Phases

### Phase 0: Entity Ref and Source Readiness

1. Resolve ADES entity refs for the first commodities, countries, chokepoints, currencies, central banks, rates, sanctions bodies, and sectors.
2. Create small reviewed concordance fixtures where source data uses external codes.
3. Confirm license/terms for the first source lanes.
4. Create raw source manifest conventions under `/mnt/githubActions/ades_big_data`.
5. Decide the initial `direction_hint` controlled vocabulary.

### Phase 1: Local Artifact and Python API

1. Implement `market_graph_store.sqlite` schema.
2. Implement one round-trip builder/reader test before defining the final response models.
3. Add `build_metadata`, `seed_degree`, and pack membership handling.
4. Add a Python-only expansion API behind an `impact_expansion.enabled` feature flag.
5. Ensure the flag description states that the feature is bounded graph expansion, not open graph search.
6. Add direct-tradable handling in the same `candidates` list as derived candidates.
7. Add graceful degradation for missing or unreadable artifacts.
8. Add unit tests covering:
   - direct tradable candidate with empty path,
   - derived tradable candidate with populated path,
   - both branches returned in the same `candidates` list,
   - zero-degree mentioned entities skipped as seeds,
   - max candidates applied after deduplication,
   - deterministic ordering under equal scores.

No public HTTP endpoint in Phase 1.

### Phase 2: Inner-Ring Data Build and Evaluation

Implementation status as of 2026-05-05:

- starter normalized TSV rows exist in `src/ades/resources/impact/phase1_starter`,
- the starter lane contains at least one reviewed row for each inner-ring family,
- a refs-only starter golden set and evaluator exist,
- the full official-source build and the required 20 to 30 article golden set are still pending.

1. Build normalized source lanes for the eight inner-ring relationship families.
2. Build the first full `market_graph_store.sqlite` artifact.
3. Create a manually reviewed golden evaluation set of 20 to 30 articles.
4. Include at least:
   - one chokepoint story,
   - one OPEC production cut story,
   - one central bank rate decision,
   - one country sanctions announcement,
   - one no-market-content negative example.
5. Track:
   - useful-path rate,
   - empty-path rate,
   - unrelated-asset rate,
   - per-relation-family recall,
   - per-relation-family false positives,
   - p95 expansion latency.
6. Set a concrete p95 latency target from the Phase 2 benchmark before promotion.
7. Set a concrete empty-path-rate target from the Phase 2 baseline before promotion.

### Phase 3: BDYA Contract and Refs-Only HTTP

1. Confirm with BDYA whether it needs a refs-only endpoint, a text-wrapper endpoint, or both.
2. Add the public HTTP endpoint only after the Python API has been validated on real articles.
3. Prefer refs-only first so BDYA can:
   - cache extraction results,
   - re-run expansion after graph updates,
   - batch offline processing without re-extracting text.
4. Include `graph_version`, `artifact_version`, and `artifact_hash` in every response.
5. BDYA should store at minimum:
   - `artifact_hash`,
   - full `relationship_path`,
   - `source_entity_refs`.

### Phase 4: Text Wrapper

Add a convenience endpoint that runs extraction and impact expansion in one call only after the refs-only contract is stable.

The wrapper should not introduce a second expansion implementation. It should call the same Python expansion API.

### Phase 5: Broader Coverage and Vector Proposals

1. Add Phase 2 macro-extension relationship families based on golden-set gaps.
2. Add company exposure lanes only with strong evidence standards.
3. Consider `sanction_affects_company` earlier if official-list matching is complete.
4. Enable vector proposals only as a proposal source.
5. Require graph support before vector-proposed candidates are admitted.
6. Tune confidence by relation family using golden-set results.

## Acceptance Criteria

1. Directly mentioned tradable entities appear in `candidates` with `evidence_level: direct`.
2. Derived candidates have at least one populated relationship path to extracted ADES entity refs.
3. Direct candidates and derived candidates can appear together in the same response list.
4. Passive entities are surfaced without bullish/bearish labels.
5. Root responses have no `direction`, `bullish`, or `bearish` fields.
6. `max_candidates` is applied after deduplication.
7. The same request plus the same artifact versions produce the same candidate order and confidence values.
8. Missing or unreadable market graph artifacts degrade gracefully with empty candidates and warnings.
9. Phase 2 establishes a concrete p95 latency target and promotion requires meeting it.
10. Phase 2 establishes a concrete empty-path-rate target and promotion requires beating it.
11. Per-relation-family recall is tracked separately from overall recall.
12. Runtime expansion performs no graph writes.
13. Large data artifacts and large downloads stay under `/mnt/githubActions/ades_big_data`.
14. Restricted-source normalized outputs are not written until terms are reviewed.

## Non-Goals

- Price prediction.
- Trading recommendations.
- Root-level bullish/bearish labels.
- Open-ended graph search.
- Runtime graph learning.
- Cross-article aggregation.
- Session-level evidence accumulation.
- Company-level inferred exposure in Phase 1.
- Replacing the existing extraction pipeline.
- Making ADES dependent on BDYA-specific product logic.

## Decisions

1. Use a separate `market_graph_store.sqlite` artifact.
2. Default traversal depth is 2.
3. Runtime graph expansion is read-only.
4. Direct tradable mentions and derived tradable candidates share the same `candidates` list.
5. `direction_hint` lives on edges, not response roots or terminal candidates.
6. `direction_hint` is required in source TSV rows.
7. `seed_degree` is computed at build time.
8. Multi-pack edges are stored once and scoped through `impact_edge_packs`.
9. `max_candidates` applies after terminal deduplication.
10. Missing artifacts degrade gracefully instead of failing extraction.
11. Public HTTP waits until the Python API works on real articles.
12. BDYA should persist artifact hash and relationship provenance for re-expansion.
13. Big data belongs under `/mnt/githubActions/ades_big_data`.
14. `sanction_affects_company` is the only early company-level exception, and only for official-list direct evidence.

## Remaining Open Questions

1. Which exact p95 expansion latency target should Phase 2 promotion require?
2. Which empty-path-rate target is acceptable after the Phase 2 baseline is measured?
3. Which restricted sources are legally safe for normalized local outputs?
4. Does BDYA want refs-only first, text-wrapper first, or both at the same time?
5. How many relationship paths per candidate should be returned by default?
6. What is the minimum evidence standard for Phase 3 company-level exposure lanes?

## Recommended First Implementation

1. Resolve the first stable ADES entity refs for crude oil, Brent, WTI, natural gas, gold, USD, EUR, the Strait of Hormuz, Suez Canal, OPEC, OFAC, the Federal Reserve, the ECB, Iran, the United States, China, Russia, and the European Union.
2. Create the first reviewed concordance fixture for source IDs to ADES refs.
3. Lock the `direction_hint` vocabulary.
4. Implement the SQLite schema and a builder/reader round-trip test.
5. Implement `build_metadata`, artifact hash validation, and schema validation.
6. Implement `seed_degree` generation and zero-degree seed skipping.
7. Implement the Python expansion API behind a disabled-by-default feature flag.
8. Add direct tradable candidate handling.
9. Add bounded BFS for the eight inner-ring relationship families.
10. Add deterministic deduplication and sorting.
11. Add graceful degradation warnings.
12. Validate the Python API on real finance/economy/politics/geopolitics articles.
13. Only then define the public HTTP response model and endpoint.
