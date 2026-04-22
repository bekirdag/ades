# `finance-de-en` Update Guide

This guide describes how to keep `finance-de-en` current.

Current scope:

- Germany stock-market baseline entities covering Frankfurt, Xetra, Stuttgart, Berlin, Hamburg, Hannover, Munchen, Dusseldorf, and Tradegate
- Germany public-market issuer entities seeded from the official Deutsche Borse listed-companies workbook and widened with official Borse Munchen `m:access` plus Borse Dusseldorf Primarmarkt issuer tables
- Germany public-market ticker entities aligned to the issuer set, with Tradegate ISIN lookups used as bounded ticker and WKN backfill where the seed workbook is incomplete
- Germany public people derived from official Deutsche Borse company-details pages plus Unternehmensregister register-information pages
- Germany issuer and ticker metadata enriched from Deutsche Borse company-details contact and major-shareholder widgets where the rendered page exposes populated values
- Germany major-shareholder organization entities and cleaned major-shareholder people derived from official Deutsche Borse shareholder-structure widgets
- Germany regulated financial-institution entities, explicit Germany branch entities, and issuer enrichment derived from the official BaFin company-database CSV export
- Bounded Wikidata issuer, ticker, person, and selected organization enrichment for Germany entities, now driven by deterministic ISIN and LEI claim resolution plus a bounded organization-to-public-people lane for matched Germany issuers and institutions

Current limitation:

- `finance-de-en` is not a full employee directory.
- The current Germany people lane is focused on public management board members, supervisory board members, and issuer-linked public contacts exposed through official exchange and register pages.
- The current Germany shareholder lane is still bounded. It now promotes many official major-shareholder organizations into entities and keeps clearly human shareholder names in the people lane, but it still does not model a complete ownership graph for every issuer.
- The current Germany Wikidata lane is also bounded. It now gives materially broader slug coverage for issuers, aligned tickers, and LEI-backed institutions, plus a bounded public-people overlay for organizations that resolve cleanly in Wikidata, but it still does not cover every Germany issuer or institution.
- Unternehmensregister and live `deutsche-boerse.com` rendering are still operationally sensitive lanes, so the pack should still be treated as a strong partial rather than a finished Germany pack.

## Current Sources

The current Germany pack is anchored to these official source lanes:

- `BaFin`
  `https://www.bafin.de/EN/Homepage/homepage_node_en.html`
  Used as the baseline Germany regulator anchor.

- `Unternehmensregister`
  `https://www.unternehmensregister.de/ureg/?submitaction=language&language=en`
  Used as the official company-register and filing anchor for Germany issuer metadata and public people.

- `Deutsche Borse listed companies`
  `https://www.cashmarket.deutsche-boerse.com/cash-en/Data-Tech/statistics/listed-companies`
  This is the primary official issuer and ticker seed source for listed German companies.

- `Borse Munchen m:access`
  `https://www.boerse-muenchen.de/maccess/gelistete-unternehmen`
  Used to widen Germany issuer coverage beyond the main Deutsche Borse workbook.

- `Borse Dusseldorf Primarmarkt`
  `https://www.boerse-duesseldorf.de/aktien-primaermarkt/`
  Used to widen Germany issuer coverage beyond the main Deutsche Borse workbook.

- `Tradegate` order-book pages
  `https://www.tradegate.de/orderbuch.php?isin={isin}&lang=en`
  Used as a bounded official market backfill lane for missing ticker and WKN metadata.

- `Deutsche Borse` company-details pages
  `https://live.deutsche-boerse.com/equity/{isin}/company-details`
  Used for current public board and executive extraction plus issuer-profile metadata, issuer contact details, populated major-shareholder widgets, and bounded major-shareholder entity promotion.

- `Borse Stuttgart`
  `https://www.boerse-stuttgart.de/`
  Used as a current Germany exchange baseline source.

- `Borse Berlin`
  `https://www.boerse-berlin.com/`
  Used as a current Germany exchange baseline source.

- `Borse Hamburg`
  `https://www.boerse-hamburg.de/`
  Used as a current Germany exchange baseline source.

- `Borse Hannover`
  `https://www.boerse-hannover.de/`
  Used as a current Germany exchange baseline source.

- `Unternehmensregister` register-information search and detail pages
  `https://www.unternehmensregister.de/en/search/register-information`
  Used for register metadata, former names, and additional public board or governance names.

- `BaFin` company-database export
  `https://portal.mvp.bafin.de/database/InstInfo/sucheForm.do?6578706f7274=1&sucheButtonInstitut=Suche&institutName=&d-4012550-e=1`
  Used for Germany-domiciled regulated institutions, explicit Germany branch entities, and BaFin identifier metadata.

- `Wikidata` entity resolution
  `https://www.wikidata.org/w/api.php`
  Used for bounded Germany issuer, ticker, person, and selected organization QID enrichment after deterministic official extraction.

- `Wikidata` SPARQL endpoint
  `https://query.wikidata.org/`
  Used for deterministic ISIN and LEI claim resolution plus bounded organization-linked public people where the matched Wikidata organization exposes stable human relationships.

## Update Procedure

Run the pack through the country-pack pipeline instead of editing generated artifacts.

```bash
ades registry fetch-finance-country-sources \
  --output-dir /mnt/githubActions/ades_big_data/pack_sources/raw/finance-country-en \
  --snapshot YYYY-MM-DD \
  --country-code de

ades registry build-finance-country-bundles \
  --snapshot-dir /mnt/githubActions/ades_big_data/pack_sources/raw/finance-country-en/YYYY-MM-DD \
  --output-dir /mnt/githubActions/ades_big_data/pack_sources/bundles \
  --country-code de \
  --version 0.2.0

ades registry generate-pack \
  /mnt/githubActions/ades_big_data/pack_sources/bundles/finance-de-en-bundle \
  --output-dir /mnt/githubActions/ades_big_data/generated_runtime_packs
```

After pack generation, rebuild or update the target static registry, then verify a clean pull from that registry before promotion.

## Required Checks On Each Refresh

1. Confirm the Deutsche Borse listed-companies page still resolves and still links to a parseable workbook.
2. Confirm the workbook still downloads and still yields a non-empty issuer seed slice.
3. Confirm the Borse Munchen `m:access` and Borse Dusseldorf Primarmarkt tables still parse into non-empty widening slices.
4. Confirm the BaFin export still downloads as CSV and still yields a non-empty Germany institution slice plus explicit Germany branch entities where the source exposes branch names.
5. Confirm headless Chrome is available for `live.deutsche-boerse.com` rendering and representative company-details pages still expose board or executive rows.
6. Confirm at least one representative company-details page still exposes populated contact and major-shareholder widgets, not only empty shells.
7. Confirm Unternehmensregister search and detail pages still resolve for representative issuers and still expose stable register metadata.
8. Confirm Tradegate ISIN lookups still recover representative missing ticker or WKN metadata for at least one widened issuer.
9. Confirm the derived Germany issuer slice is non-empty and still includes representative issuers from the Deutsche Borse seed plus at least one widened issuer.
10. Confirm the derived Germany people slice is non-empty and still contains representative public board or executive people.
11. Confirm the derived Germany organization slice now includes representative major-shareholder organizations from official shareholder-structure widgets and that obvious pseudo-holders such as free float or treasury shares are still suppressed.
12. Confirm the Wikidata resolution snapshot is non-empty and still attaches representative issuer, ticker, and person QIDs, plus representative regulated-institution organization QIDs and bounded organization-linked public people when the current Wikidata refresh remains responsive.
13. Rebuild the bundle and runtime pack, then verify the target registry metadata reflects the refreshed Germany artifact rather than the old `5`-entity baseline pack.
14. Smoke-test clean pull plus a few issuer and person probes against the target registry before promotion.

## Operational Notes

- The Germany pack should stay official-source-first. Do not replace the Deutsche Borse, Unternehmensregister, or BaFin lanes with third-party mirrors when the official pages are still available.
- The current Germany issuer universe is layered: Deutsche Borse workbook first, then bounded widening through official regional market tables, then identifier backfill through Tradegate.
- The current Germany people lane depends on live rendered company-details pages and register-information pages. If those lanes fail, issuer and ticker coverage may still be usable while public people coverage shrinks sharply.
- The current company-details lane should preserve populated contact and major-shareholder widgets when they are present, promote organization-like major holders into explicit organization entities, and keep only clearly human holder names in the people lane.
- The BaFin lane currently gives strong institution, branch, and identifier coverage. It does not yet provide a broad durable people lane for institution principals, so do not overstate the current institution-people depth.
- The current Germany Wikidata lane should stay enrichment-only. Prefer deterministic ISIN and LEI claim resolution, then use bounded organization-linked public people only after the Germany organization QIDs are already stable.
- The current Wikidata lane is enrichment, not the source of truth for the Germany entity universe.
- The current strong partial artifact built on `2026-04-22` stands at `9466` total entities: `4773` organizations, `428` tickers, `4255` people, `9` exchanges, and `1` market index. That includes `1250` organization entities with `wikidata_slug`, `322` tickers with `wikidata_slug`, `290` people with `wikidata_slug`, and `245` net new people coming from the bounded Wikidata organization-people lane.

## Next Expansion Lane

The next Germany enhancement should deepen official public-people and institution coverage beyond the current exchange-profile and register-information lanes.

Target official sources for that next lane:

- `Unternehmensregister` broader annual-report and governance filing extraction
  `https://www.unternehmensregister.de/ureg/?submitaction=language&language=en`
- `BaFin` public institution disclosures with named principals where stable
  `https://www.bafin.de/EN/Homepage/homepage_node_en.html`

That next lane should improve board, executive, institution-principal, and ownership-disclosure coverage while keeping the pack focused on durable public finance people rather than general employee rosters.

## Validation

- Run the focused Germany and finance-country test suite after doc or code changes that affect this pack.
- Run `docdexd hook pre-commit --repo /home/wodo/apps/ades` before closing the change.
- Keep [library_pack_publication_workflow.md](./library_pack_publication_workflow.md) aligned with this guide whenever the live Germany source inventory changes.
