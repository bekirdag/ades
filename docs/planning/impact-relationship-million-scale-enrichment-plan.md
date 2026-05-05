# Impact Relationship Million-Scale Enrichment Plan

Status: planning started; first implementation slice targets finance-country pack source lanes
Created: 2026-05-05

This plan expands the ADES impact relationship graph from the current reviewed starter graph into a million-scale relationship graph for finance, economy, politics, sanctions, commodities, currencies, rates, issuers, legal entities, and market proxies.

The runtime requirement stays strict: extraction returns ADES entities, and impact expansion returns auditable relationship paths from those ADES entity refs to tradable assets or market indicators. The graph can be very large offline, but the live API must remain bounded, deterministic, explainable, and fast.

## Goal

Build a graph where every extracted ADES entity that is relevant to finance, economy, or politics has at least one path to a tradable or market-observable terminal:

- listed ticker,
- market index,
- currency,
- currency basket or DXY-like indicator,
- rates instrument or policy-rate proxy,
- commodity,
- exchange-traded fund or broad market proxy where available,
- sanctions or risk indicator where it is the only defensible terminal.

The end-state target is millions of normalized relationships and connections in the offline source graph, with a validated hot graph published for runtime impact expansion.

## Non-Negotiables

- Use ADES entity refs as graph keys. External IDs are identifiers, not runtime refs.
- Every edge must carry provenance: source name, source URL, snapshot, year, confidence, direction hint, and notes.
- Large downloads, raw source files, normalized outputs, generated graph artifacts, and large intermediates belong only under `/mnt/githubActions/ades_big_data`.
- Do not put large datasets or generated million-edge artifacts in git.
- Do not web-search per article at runtime.
- Do not infer price direction. `direction_hint` describes exposure channel, not a trade signal.
- Company-level exposure must be evidence-tiered. Broad proxy edges are allowed, but they must be labeled as proxy relationships and kept separate from direct issuer/ticker/ownership evidence.
- Political figures can link to their country currency, broad country market proxy, and DXY-like indicators when a more direct tradable terminal is not available.

## Current Base

Current production impact graph after the 2026-05-05 correction:

- 15 relationships
- 18 nodes
- artifact hash: `sha256:c135455048048cd8c466281d11934ebd60be77c9bb1d62b6a33023429bb5128a`

Existing large local finance-country pack surface under `/mnt/githubActions/ades_big_data/prod-deploy/registry-2026-04-24-finance-country-ready-r1/packs`:

- 19 `finance-<country>-en` packs
- about 168k unique pack-local entity ids across those packs
- about 361k aliases
- about 288k matcher entries

These packs are the first enrichment source because they already contain ADES-compatible issuer, ticker, exchange, market-index, organization, and person refs.

## Two-Graph Architecture

### Source Graph

The source graph is the large offline graph. It can contain millions of relationships, including broad proxy links, ownership, filings, trade flows, and identity bridges.

Location:

- raw source downloads: `/mnt/githubActions/ades_big_data/pack_sources/raw/impact-relationships`
- normalized source lanes: `/mnt/githubActions/ades_big_data/pack_sources/impact_relationships`
- large graph artifacts: `/mnt/githubActions/ades_big_data/artifacts/market-graph`

### Hot Runtime Graph

The hot graph is the bounded graph used by `/v0/impact/expand` and `/v0/tag`.

Promotion rules:

- only rows with reviewed schema and provenance,
- no unclear-license normalized rows,
- bounded edge fanout per seed,
- per-source confidence thresholds,
- golden article evaluation must pass before prod promotion.

The hot graph can still contain millions of rows if latency and quality hold, but it must be promotable in tiers instead of all-at-once.

## Relationship Families

### Direct Finance-Country Pack Families

These are generated from existing finance-country packs.

| Relation | Source entity | Target terminal | Evidence |
| --- | --- | --- | --- |
| `issuer_has_listed_ticker` | issuer organization | ticker | direct when issuer metadata includes ticker |
| `person_affects_employer_ticker` | public company person | employer ticker | direct/shallow when person metadata includes employer ticker |
| `ticker_trades_on_exchange` | ticker | exchange | shallow when ticker metadata includes exchange |
| `exchange_affects_country_index_proxy` | exchange | country market index | inferred proxy |
| `entity_affects_country_index_proxy` | any pack entity | country market index | inferred proxy |
| `entity_affects_currency_proxy` | any pack entity | country currency | inferred proxy |
| `entity_affects_dxy_proxy` | political/regulatory/person entity | DXY-like indicator | inferred proxy |
| `entity_has_tradable_proxy` | any pack entity | best available ticker/index/currency terminal | fallback coverage edge |

### Official Bulk Families

These come from larger external datasets after source-specific ingestion.

| Relation | Source | Expected scale | Notes |
| --- | --- | ---: | --- |
| `legal_entity_has_parent` | GLEIF RR-CDF | 600k+ | direct and ultimate parent relationships |
| `legal_entity_has_lei` | GLEIF LEI-CDF | millions | identity bridge, not always a tradable path by itself |
| `company_has_sec_cik` | SEC EDGAR | 500k+ | identity bridge |
| `company_has_filing_fact` | SEC EDGAR/XBRL | millions | only selected facts become market graph edges |
| `company_has_sic_sector` | SEC EDGAR | 500k+ | sector proxy link |
| `wikidata_related_entity` | Wikidata dumps | millions possible | only selected predicates should enter hot graph |
| `country_exports_commodity` | WITS/Comtrade | millions possible | country/commodity/value/year relationships |
| `country_has_banking_exposure` | BIS | large | country/sector/risk exposure |
| `central_bank_affects_policy_rate` | BIS/central bank/ECB | small but high value | rates/risk terminal |
| `sanction_targets_entity` | OFAC/EU/UK/UN/OpenSanctions | large | sanctions paths to people/company/country |

## First Million-Scale Strategy

The first run should not wait for GLEIF or Wikidata dumps. It should use current finance-country packs and generate proxy edges with clear evidence tiers.

For each finance-country pack:

1. Read normalized `entities.jsonl` from the pack bundle when available.
2. Fall back to `aliases.json` if normalized entities are unavailable.
3. Build node rows for every entity id.
4. Identify tradable terminal candidates:
   - ticker entities in the pack,
   - market index entities in the pack,
   - country currency proxy node,
   - DXY-like proxy node,
   - rates proxy node where available.
5. Generate direct edges where metadata supports them:
   - issuer to ticker,
   - person to employer ticker,
   - ticker to exchange,
   - ticker to country index,
   - ticker to currency.
6. Generate fallback proxy edges so every entity has at least one tradable terminal path:
   - entity to country market index,
   - entity to country currency,
   - political/person/regulatory entity to DXY-like indicator.
7. Validate that every source entity reaches at least one tradable terminal within depth two.

Expected first artifact:

- at least 1,000,000 normalized relationship rows,
- all rows generated under `/mnt/githubActions/ades_big_data`,
- no large output in git,
- a summary manifest with counts by pack, relation, evidence level, and target type.

## Country Proxy Map

The first generator uses deterministic country metadata from pack ids.

| Pack | Currency proxy | DXY proxy | Default market proxy |
| --- | --- | --- | --- |
| `finance-ar-en` | ARS | DXY | pack market index if present |
| `finance-au-en` | AUD | DXY | pack market index if present |
| `finance-br-en` | BRL | DXY | pack market index if present |
| `finance-ca-en` | CAD | DXY | pack market index if present |
| `finance-cn-en` | CNY | DXY | pack market index if present |
| `finance-de-en` | EUR | DXY | pack market index if present |
| `finance-fr-en` | EUR | DXY | pack market index if present |
| `finance-id-en` | IDR | DXY | pack market index if present |
| `finance-in-en` | INR | DXY | pack market index if present |
| `finance-it-en` | EUR | DXY | pack market index if present |
| `finance-jp-en` | JPY | DXY | pack market index if present |
| `finance-kr-en` | KRW | DXY | pack market index if present |
| `finance-mx-en` | MXN | DXY | pack market index if present |
| `finance-ru-en` | RUB | DXY | pack market index if present |
| `finance-sa-en` | SAR | DXY | pack market index if present |
| `finance-tr-en` | TRY | DXY | pack market index if present |
| `finance-uk-en` | GBP | DXY | pack market index if present |
| `finance-us-en` | USD | DXY | pack market index if present |
| `finance-za-en` | ZAR | DXY | pack market index if present |

## Confidence Policy

| Edge type | Evidence level | Confidence |
| --- | --- | ---: |
| issuer to ticker from metadata | direct | 0.90 |
| person to employer ticker from filing/proxy metadata | shallow | 0.84 |
| ticker to exchange from metadata | shallow | 0.82 |
| ticker to country market index | inferred | 0.72 |
| entity to country market index proxy | inferred | 0.62 |
| entity to country currency proxy | inferred | 0.60 |
| political/person/regulatory entity to DXY-like proxy | inferred | 0.56 |
| fallback entity to best tradable proxy | inferred | 0.52 |

## Source Candidates and Trust Notes

Use official or widely trusted bulk data first.

- GLEIF publishes daily LEI-CDF, RR-CDF relationship, and reporting-exception concatenated files. Recent official listings show more than 3.2M LEI records and about 645k relationship records.
- Wikidata provides weekly JSON dumps and RDF/truthy dumps; use selected predicates only and keep the raw dump under big data storage.
- SEC EDGAR provides official APIs and bulk `submissions.zip` for public filing history plus XBRL company facts.
- BIS Data Portal provides bulk CSV and SDMX downloads for policy rates, banking, exchange-rate, credit, and payments datasets.
- ECB Data Portal provides API and bulk/SDMX access for euro-area statistics.
- WITS is the preferred first trade-flow source lane before Comtrade because Comtrade licensing and bulk-access limits need review.
- OpenSanctions and official OFAC/EU/UK/UN lists are suitable for sanctions relationships, with license review for normalized redistribution.

## Implementation Plan

### Step 1: Finance-Country Source-Lane Generator

Create an offline generator that:

- accepts a registry/packs root,
- discovers `finance-*-en` packs,
- loads normalized entity JSONL from each pack `sources.json` bundle when available,
- writes `nodes.tsv`, `edges.tsv`, and `manifest.json` under `/mnt/githubActions/ades_big_data/pack_sources/impact_relationships/finance_country_proxy/<run_id>`,
- optionally builds `market_graph_store.sqlite` under `/mnt/githubActions/ades_big_data/artifacts/market-graph/<run_id>`.

### Step 2: Coverage Validator

Add validation that fails if:

- an entity has no outgoing edge to a terminal candidate,
- an edge lacks provenance,
- an edge uses a non-ADES source ref,
- relation families are missing direction hints,
- the million-edge target is not met for the full run.

### Step 3: Artifact Builder Compatibility

Use the existing market graph builder first. If the current in-memory TSV loading becomes too slow or memory-heavy at million scale, add a streaming builder in a separate implementation pass.

### Step 4: GLEIF Source Lane

Download GLEIF raw files to:

`/mnt/githubActions/ades_big_data/pack_sources/raw/impact-relationships/gleif/<snapshot>`

Normalize:

- LEI entity nodes,
- direct parent relationships,
- ultimate parent relationships,
- LEI-to-ADES issuer bridges where finance-country metadata or Wikidata/SEC concordance can resolve the issuer.

### Step 5: SEC Source Lane

Use existing SEC raw snapshots when present and add refresh support for:

- `company_tickers.json`,
- `submissions.zip`,
- `companyfacts.zip`.

Normalize:

- issuer to CIK,
- issuer to ticker,
- public person to employer ticker,
- issuer to SIC sector,
- selected XBRL facts to financial metric proxies.

### Step 6: Wikidata Source Lane

Use selected predicates only:

- country,
- currency,
- central bank,
- parent organization,
- subsidiary,
- stock exchange,
- industry,
- office held,
- member of political body,
- located in,
- owned by.

Raw dumps stay under big data storage. Only reviewed normalized outputs move into source lanes.

### Step 7: Sanctions Source Lane

Normalize official sanctions and OpenSanctions outputs into:

- sanction body to sanctioned entity,
- sanctioned entity to country,
- sanctioned company to ticker/currency/index proxy where resolvable,
- sanctioned political figure to country currency and DXY-like proxy.

### Step 8: Trade, Commodity, Rates, and Macro Source Lanes

Normalize WITS/BIS/ECB/EIA/OPEC/JODI into:

- country to commodity export/import,
- country to currency,
- central bank to policy-rate proxy,
- commodity producer/exporter to commodity,
- chokepoint to commodity,
- country to banking/rate/exchange-rate exposure.

### Step 9: Promotion

Do not promote the full source graph directly to prod. Promote in tiers:

1. finance-country proxy graph,
2. SEC direct issuer/person/ticker graph,
3. GLEIF identity and ownership graph,
4. sanctions graph,
5. commodity/trade/rates graph,
6. selected Wikidata graph.

Each promotion needs:

- local build,
- unit/schema tests,
- source manifest,
- article golden set evaluation,
- p95 expansion latency,
- live smoke plan.

## Open Risks

- Broad proxy edges can create noisy candidate paths. They must be labeled and down-ranked.
- Millions of rows may require a streaming graph builder if the current TSV loader becomes memory-heavy.
- Some external datasets allow local use but not redistribution of normalized rows. License review must precede normalized output for those sources.
- Finance-country packs include useful but uneven person extraction quality. Person proxy edges need confidence caps and can be excluded from hot graph if they pollute article results.
- The goal that every entity reaches a tradable terminal is a coverage rule, not a claim that every entity has a direct market-moving relationship.
