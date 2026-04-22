# Finance Country Library Guidelines

This document defines the generic working rules for creating or enhancing country-specific finance packs such as `finance-de-en`, `finance-jp-en`, or other `finance-<country>-en` libraries.

It is intentionally generic. It describes the target shape of a country finance pack, how to choose sources, how to normalize and validate the data, and how to keep the pack current without hardcoding country-specific one-off logic into the pack itself.

Use this together with:

- [Library Pack Source Bundle Spec](./library_pack_source_bundle_spec.md)
- [Library Pack Normalized Record Schema](./library_pack_normalized_record_schema.md)
- [Library Pack Publication Workflow](./library_pack_publication_workflow.md)

## Goal

A country finance pack should provide durable exact-match coverage for public finance entities linked to one country.

At minimum, a mature country pack should try to cover:

- listed issuers and other public-market companies tied to the country
- ticker and market identifiers linked to those issuers
- exchanges, market segments, and important local indexes
- public officers and board-related people attached to those issuers
- regulated financial institutions when official regulator data makes that durable

The pack should not aim to become a generic people directory. The target is public finance relevance, not all employees.

Research, parser work, tests, and local code changes are not enough on their own. A country-pack enhancement is only complete when it has been materialized into a real generated `finance-<country>-en` library artifact and the intended library surface has been updated to reflect that artifact.

## Scope Rules

Country packs should prefer entities that are durable, public, and official.

Good targets:

- public companies
- listed issuers
- board members
- executive management
- supervisory board members
- investor-relations contacts if they are issuer-facing and stable
- regulated institutions published by official regulators

Avoid or exclude:

- full employee rosters
- scraped biographies with no durable identifier
- transient newsroom mentions
- unofficial secondary lists when an official source exists
- records that cannot be mapped back to a stable issuer, institution, or person entity

## Source Priority

Prefer official sources in this order.

1. Official exchange issuer lists and market-segment lists.
2. Official issuer profile or company-details pages published by exchanges.
3. Official issuer governance pages on company websites.
4. Official filing or company-register systems.
5. Official regulator datasets for institutions and licenses.
6. Structured enrichment sources such as Wikidata for identifiers and aliases.
7. Curated repo-owned aliases only when official sources do not cover a necessary exact form.

If two sources disagree, prefer the one that is:

- more official
- more recent
- more specific to the entity type
- easier to rebuild deterministically

## Source Categories

Every country finance pack should think in source lanes, not in one giant scrape.

Typical lanes are:

- issuer seed lane
- market-segment lane
- issuer profile lane
- governance people lane
- filing or registry lane
- regulator lane
- identifier-enrichment lane

Each lane should produce snapshot files that can be rebuilt and audited later. Use immutable dated snapshots outside the repo and carry exact source metadata into `sources.fetch.json` and bundle lock files.

## Coverage Model

Build country packs in layers.

### 1. Issuer Universe

Start with the strongest official public-market seed for the country:

- exchange listed-company workbook
- official listed issuer table
- official segment membership pages

This layer should produce the initial issuer set and the first ticker/exchange metadata.

### 2. Market Backfill

Use additional official market pages to recover issuers or identifiers missing from the main seed:

- secondary exchange boards
- alternative markets
- market operator pages
- ISIN, WKN, security code, or local ticker lookup pages

Do not create duplicate issuers for the same company. Merge on stable identifiers first, then on strong name matches.

### 3. Related People

After the issuer set is stable, enrich it with public people from:

- issuer governance pages
- exchange company-details pages
- annual reports or management reports when published through official channels
- company-register or filing pages

Only attach people when the source clearly links them to the issuer or institution.

### 4. Regulated Institutions

If the country has a public regulator dataset, add a distinct institution lane for:

- banks
- insurers
- payment firms
- investment firms
- asset managers

This lane may create new institution entities or enrich existing issuers, depending on the data.

### 5. External Identifier Enrichment

Add structured identifiers after the core entity set exists:

- Wikidata QIDs
- LEIs
- regulator IDs
- register numbers
- court or filing references

Identifier enrichment should not be the primary source of truth for the entity universe. It should enrich an already-derived set.

## Maturity Levels

Use explicit maturity labels when describing country-pack health.

- `scaffold`: only static baseline entities or near-baseline coverage are present; the live country lanes are absent or not reflected in the generated library
- `weak partial`: at least one live lane exists, but coverage is still thin, fragile, or only present in code/tests rather than in the actual generated pack
- `credible partial`: live issuer or ticker coverage is materially useful, key enrichment lanes are tested, and the generated library artifact has been rebuilt and verified, but important coverage gaps still remain
- `release-ready`: the primary issuer and ticker lanes plus meaningful people or institution lanes are automated, the generated library artifact is current, and the release path has been exercised against the intended registry surface

Entity count alone does not define maturity. A stale target library artifact caps the effective maturity of a pack even if the local code is ahead of it.

## Code Status Vs Library Status

Track two distinct states:

- `code status`: what the repo implementation, tests, and docs support
- `library status`: what the generated `finance-<country>-en` artifact in the target library actually contains

Do not collapse those into one status.

If the code is ahead of the library artifact, report that explicitly as `code ahead of library` and do not describe the pack as healthy or complete. The effective pack health is always limited by the weaker of the two states.

## Entity Modeling Rules

Country finance packs should keep the normalized entity model simple and stable.

Common entity types:

- `issuer`
- `ticker`
- `exchange`
- `market_index`
- `person`
- `regulated_institution`

Each entity should have:

- one stable `entity_id`
- one canonical display text
- a bounded alias list
- source and identifier metadata that supports later merges and audits

Prefer stable IDs built from durable upstream identifiers:

- ISIN
- local security code
- exchange code
- regulator ID
- register number
- normalized person slug plus issuer context when needed

Avoid entity IDs built only from raw display text when a stronger identifier exists.

## Alias Rules

Exact matching quality depends on aliases more than on raw entity count. Alias work is part of the core build, not a cleanup phase.

Country packs should add exact aliases for:

- legal company name
- short company name
- former official names when still commonly used
- local-language and English forms when both are public and durable
- punctuation and hyphen variants
- legal-suffix stripped variants when they improve exact matching
- titled and untitled public-person variants when the title is commonly published
- ASCII and diacritic variants when both forms appear in real text

Country packs should avoid aliases that create broad ambiguity:

- single common words
- generic role titles without names
- uppercase short forms that are really tickers, not org names
- aliases that collide heavily across multiple entities unless explicitly allowed

Alias generation should be conservative by default and widened by evidence from real smoke tests.

## People Extraction Rules

Public people should be modeled as finance-related people, not as general people records.

Good people targets:

- board chair
- board member
- supervisory board member
- CEO, CFO, COO, CIO, CRO
- managing director
- public officer or principal listed by a regulator

Do not attach a person unless the source shows a real issuer or institution relationship.

Useful metadata to preserve:

- role title
- issuer or institution linkage
- source URL or source lane
- whether the role came from governance, registry, regulator, or exchange profile data

Normalize people carefully:

- collapse whitespace
- normalize punctuation
- keep titled variants as aliases, not as separate people
- merge ASCII and diacritic forms where they are clearly the same person
- do not merge two people just because the surname and first initial look similar

## Parser Strategy

Prefer generic parsers that can be reused across issuers within a country.

Examples:

- governance-table parser
- definition-list parser
- list-item parser
- exchange-profile table parser
- register-information parser

The parser should be country-aware where needed, especially for local role titles, but not hardcoded to one company unless a source is uniquely structured and unavoidable.

If a source needs headless rendering, treat that as an operational dependency and document it in the upkeep instructions for the specific pack.

## Build Order

Recommended change order for a new country pack or a major enhancement:

1. Add or improve the source fetch lane.
2. Normalize the raw source into the bundle snapshot format.
3. Derive or merge the core issuer set.
4. Add people and institution enrichment on top of that set.
5. Add identifier enrichment and alias expansion.
6. Add focused tests for the new lane.
7. Rebuild the bundle and create the actual generated `finance-<country>-en` runtime pack. Do not stop at research notes, local code changes, or bundle-only output.
8. Rebuild the target registry or other real library surface that should carry the pack, and verify that the refreshed artifact is the one being served or staged.
9. Run pack-level validation and clean-install smoke checks against the generated artifact, not only the source snapshot or bundle.
10. Update the pack-specific upkeep documentation.

This order reduces drift and keeps merges easier to reason about.

## Validation Standard

A country finance pack is not ready because it built successfully. It is ready when it passes structural and real-world checks.

Each enhancement should validate:

- source snapshot fetch succeeded and recorded provenance
- bundle generation succeeded deterministically
- runtime pack generation succeeded
- the actual generated `finance-<country>-en` library artifact was rebuilt after the change
- the target library artifact `build.json`, `manifest.json`, and registry metadata reflect the new pack rather than the older baseline pack
- alias ambiguity stayed inside acceptable bounds
- at least one test exists for the new source lane
- end-to-end tests prove the lane affects the derived entity set
- clean consumer install succeeds from a static registry
- random real-world spot checks match both issuers and people where expected

If the target library still contains only the old baseline entities, or otherwise does not reflect the local change, the enhancement is not validated and should be treated as incomplete.

Recommended spot checks:

- one large issuer
- one mid-cap or secondary-market issuer
- one regulated institution if the regulator lane exists
- one board member
- one former-name alias
- one identifier-driven exact form such as a local code or short issuer name

## Operational Readiness

Treat a country pack as production-ready only when:

- the main source lanes are automated
- failures degrade partially instead of silently deleting most coverage
- the pack can be rebuilt from snapshots
- the source inventory is documented
- the release path has been exercised against a real registry
- smoke tests use the generated artifact, not just mocked tests
- local changes are reflected in the actual generated library artifact, not only in code, tests, or docs

## Completion Rule

Do not mark a country-pack task done just because the research phase is complete or because local repo files changed.

Completion requires all of the following:

1. the research findings are encoded in fetch or derive logic, tests, and docs
2. the actual `finance-<country>-en` library is created or rebuilt from those changes
3. the intended local library surface reflects that rebuilt artifact
4. if the workspace maintains a prod-library snapshot or reviewed release tree, that artifact is also refreshed before claiming the library work is complete
5. the generated pack metadata and spot checks confirm that the live artifact reflects the new coverage

If the generated library still shows the old entity counts, old source counts, or old registry timestamp, the work is not complete.

If a lane is known to be fragile, note that explicitly in the pack-specific upkeep section instead of hiding it.

## Documentation Rule

Whenever you add a new live source lane to a specific country pack, update that pack's upkeep section in [Library Pack Publication Workflow](./library_pack_publication_workflow.md) in the same change.

That update should include:

- the source name and URL
- what the source contributes
- what needs to be refreshed on each rebuild
- any operational dependency such as headless rendering, retries, rate limits, or format assumptions
- any country-specific promotion checks required before release

Do not leave source knowledge only in code or tests.

## Generic Workflow

Use the country-pack pipeline rather than editing generated runtime pack files by hand.

Typical flow:

```bash
ades registry fetch-finance-country-sources \
  --output-dir /mnt/githubActions/ades_big_data/pack_sources/raw/finance-country-en \
  --snapshot YYYY-MM-DD \
  --country-code <country>

ades registry build-finance-country-bundles \
  --snapshot-dir /mnt/githubActions/ades_big_data/pack_sources/raw/finance-country-en/YYYY-MM-DD \
  --output-dir /mnt/githubActions/ades_big_data/pack_sources/bundles \
  --country-code <country> \
  --version <pack-version>

ades registry generate-pack \
  /mnt/githubActions/ades_big_data/pack_sources/bundles/finance-<country>-en-bundle \
  --output-dir /mnt/githubActions/ades_big_data/generated_runtime_packs
```

Do not stop after the fetch, research, or local code-change stages. When those are done, you must create the actual generated `finance-<country>-en` pack and then rebuild or refresh the real library surface that should contain it.

After pack generation:

1. build or update a static registry
2. validate the generated pack
3. confirm the target library now reflects the new artifact by checking the generated pack metadata
4. test a clean pull from the target registry
5. run real-text smoke checks
6. only then promote or publish

## Checklist

Use this as the minimum checklist for any new country lane or enhancement.

- official source chosen and justified
- snapshot fetch added
- source provenance recorded
- normalized records added correctly
- core entity merge logic added
- alias policy reviewed
- people and institution linkage reviewed
- tests added
- actual `finance-<country>-en` library artifact created after the change
- generated artifact validated
- target local library surface updated to carry the new artifact
- target prod or reviewed library surface updated when that surface is part of the task
- pack-specific upkeep documentation updated

## Quality Bar

A good country finance pack is:

- official-source-first
- deterministic to rebuild
- strong on exact aliases
- cautious about ambiguity
- explicit about operational fragility
- reflected in the actual generated library, not only in local source changes
- maintained as an evolving production artifact, not a one-time scrape

That is the standard for creating a new pack and for deciding whether an enhancement actually improved one.
