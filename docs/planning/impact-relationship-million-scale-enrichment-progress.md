# Impact Relationship Million-Scale Enrichment Progress

Plan: `docs/planning/impact-relationship-million-scale-enrichment-plan.md`
Started: 2026-05-05

## Current Status

- Created the million-scale enrichment plan.
- Confirmed large data storage rule: all raw downloads, normalized source-lane outputs, large graph artifacts, and intermediates go under `/mnt/githubActions/ades_big_data`.
- Confirmed existing prod impact graph currently has 15 relationships and 18 nodes after the production correction.
- Confirmed the local finance-country-ready registry has 19 country finance packs with about 168k pack-local unique entity ids and about 361k aliases.
- Implemented `src/ades/impact/finance_country_proxy.py`, an offline generator for finance-country proxy source lanes.
- Generated the first million-scale local source lane and graph artifact:
  - run id: `20260505Tmillion-finance-country-proxy-r1`
  - normalized output: `/mnt/githubActions/ades_big_data/pack_sources/impact_relationships/finance_country_proxy/20260505Tmillion-finance-country-proxy-r1`
  - SQLite artifact: `/mnt/githubActions/ades_big_data/artifacts/market-graph/20260505Tmillion-finance-country-proxy-r1/market_graph_store.sqlite`
  - nodes: 233,106
  - edges: 1,524,674
  - source entities: 225,035
  - terminal nodes: 46,773
  - uncovered source entities: 0

## Implementation Checklist

- [x] Review current impact graph plan and storage constraints.
- [x] Inspect available finance-country pack surface.
- [x] Create enrichment plan markdown.
- [x] Implement finance-country proxy source-lane generator.
- [x] Generate first normalized million-edge source lane under `/mnt/githubActions/ades_big_data`.
- [x] Build local market graph SQLite artifact from generated lane.
- [x] Validate every source entity has at least one tradable terminal path.
- [x] Run focused tests.
- [ ] Decide whether the existing builder needs streaming mode before broader GLEIF/Wikidata ingestion.

## Notes

- The first implementation slice should use existing `finance-*-en` packs before downloading larger external datasets.
- Broad proxy edges are necessary to satisfy the coverage requirement, but they must be marked as inferred proxy relationships with lower confidence.
- Direct metadata-backed edges, such as issuer-to-ticker and person-to-employer-ticker, should outrank country/currency/DXY fallback proxy edges.

## Validation Log

- `docdexd run-tests --repo /home/wodo/apps/ades --target tests/unit/test_finance_country_proxy_impact.py` still misrouted to `ades release validate` and failed with an unexpected extra argument, so direct pytest was used.
- `python -m pytest tests/unit/test_finance_country_proxy_impact.py -q`: 2 passed.
- `python -m ruff check src/ades/impact/finance_country_proxy.py tests/unit/test_finance_country_proxy_impact.py`: passed.
- Million graph count validation:
  - manifest node count equals SQLite node count: 233,106.
  - manifest edge count equals SQLite edge count: 1,524,674.
  - relation counts match between manifest and SQLite.
- 100 random source-seed local expansion sample:
  - missing/warning count: 0.
  - p50: 1.073 ms.
  - p95: 1.180 ms.
  - max: 2.716 ms.

## Relation Counts

- `entity_affects_country_index_proxy`: 225,035
- `entity_affects_country_risk_proxy`: 225,035
- `entity_affects_currency_proxy`: 225,035
- `entity_affects_dxy_proxy`: 225,035
- `entity_affects_global_equity_proxy`: 225,035
- `entity_affects_policy_rate_proxy`: 225,035
- `issuer_has_listed_ticker`: 33,276
- `person_affects_employer_ticker`: 130,338
- `ticker_trades_on_exchange`: 10,850

## 2026-05-05 Continuation

- Found a prod-safety blocker before promotion: the first million-scale artifact contained the finance-country proxy lane but did not merge the reviewed starter graph, so replacing prod with it would remove established paths such as Hormuz to crude oil.
- Updated the generator so `--build-artifact` includes the packaged starter graph by default while keeping `--no-include-starter-graph` available for source-lane-only diagnostics.
- Added artifact-level counts to the generated manifest/result so source-lane scale and actual SQLite artifact scale can be checked separately.
- Updated the production workflow to preserve an existing million-edge impact graph if a future workflow run only prepares a starter-sized graph artifact.
- Added a unit assertion that the combined generated artifact still resolves the starter Hormuz to crude-oil path.
- Added a second enrichment slice for non-country domain packs: `finance-en`, `business-vector-en`, `economics-vector-en`, and `politics-vector-en`. Entities in these packs that are not already covered by finance-country packs now receive inferred global proxy paths to USD, DXY, global equities, and global policy risk.
- Added unit coverage proving a politics pack entity such as a public official can route to USD/DXY/global-risk terminals without content-specific runtime logic.
- Fixed tag-time impact expansion so exact entity refs are not implicitly restricted to the response pack. This matters because vector packs often emit entity ids owned by finance-country or global packs.
- Added content-agnostic cross-pack resolution for `ades:heuristic_structural_*` refs, so a structural location generated under `business-vector-en` or `economics-vector-en` can reuse the reviewed `general-en` / `finance-en` graph path when the slug matches.
- Local article-style checks now resolve:
  - `politics-vector-en` Nigel Farage -> DXY, USD, global equities, global policy risk.
  - `economics-vector-en` Strait of Hormuz -> crude oil and USD through the reviewed starter path.
  - `business-vector-en` Strait of Hormuz / United States -> crude oil plus USD/DXY/global proxies.
