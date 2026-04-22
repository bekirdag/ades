# `finance-uk-en` Update Guide

This guide describes how to keep `finance-uk-en` current.

Current scope:

- United Kingdom public-market issuer entities derived from the official FCA Official List live export, filtered to commercial listed companies
- United Kingdom AIM issuer and ticker entities widened through the rendered official `FTSE AIM All-Share` constituents table on `London Stock Exchange`
- United Kingdom public people derived from official Companies House current-officers pages plus active persons-with-significant-control pages for issuers that resolve cleanly to current company numbers
- United Kingdom shareholder-organization entities promoted from official Companies House persons-with-significant-control pages where the controller is an organization rather than a person
- United Kingdom issuer and ticker metadata enriched with official listing category, market status, trading venue, ISIN, ticker, market-segment, and Companies House company number where resolved
- Bounded Wikidata enrichment for static United Kingdom market anchors plus issuer and ticker entities where official ISIN or LSE ticker claims resolve cleanly
- Static baseline regulator, official-list, disclosure, exchange, market-segment, and market-index entities for `FCA`, `Official List`, `RNS`, `LSE`, `AIM`, `FTSE 100`, and `FTSE AIM All-Share`

Current limitation:

- `finance-uk-en` is not a full employee directory.
- The current issuer lane is much broader than the old FCA-only slice, but it is still bounded to the FCA commercial-company universe plus the rendered `FTSE AIM All-Share` roster. It does not yet widen through RNS or annual-report discovery.
- The current people lane is now Companies House officers plus active PSC coverage, but it still does not cover broader board, executive, or ownership disclosures that sit outside those official Companies House lanes.
- The current Wikidata lane is issuer-and-ticker only. It is deterministic by official ISIN and London Stock Exchange ticker claims, and it does not currently attempt broad United Kingdom people resolution.
- `finance-uk-en` should therefore be treated as a credible partial rather than a finished United Kingdom pack.

## Current Sources

The current United Kingdom pack is anchored to these official source lanes:

- `FCA`
  `https://www.fca.org.uk/`
  Used as the baseline United Kingdom securities-regulator anchor.

- `FCA Official List` live export
  `https://ukla.file.force.com/sfc/dist/version/download/?asPdf=false&d=%2Fa%2FTw000004MSPy%2FhR2bKHHyeRlkTruiX.5JyyhDqwO_HxCHSWxpMQHJ5yo&ids=068Tw00000iMBlA&oid=00Dw0000000nVzW`
  This is the current official issuer seed for listed UK commercial companies.

- `RNS`
  `https://www.lseg.com/en/capital-markets/regulatory-news-service`
  Currently used as the disclosure-system anchor and documented next expansion lane.

- `London Stock Exchange`
  `https://www.londonstockexchange.com/`
  Used as the baseline UK exchange anchor.

- `FTSE AIM All-Share` constituents table
  `https://www.londonstockexchange.com/indices/ftse-aim-all-share/constituents/table`
  Used to widen the UK issuer roster and derive AIM tickers.

- `Companies House register`
  `https://www.gov.uk/guidance/search-the-companies-house-register`
  Used to resolve listed issuers to current company numbers and current-officers pages.

- Resolved `Companies House` search result pages
  `https://find-and-update.company-information.service.gov.uk/search/companies?q={issuer}`
  Used to match FCA listed issuers to current Companies House company numbers.

- Resolved `Companies House` officer pages
  `https://find-and-update.company-information.service.gov.uk/company/{company_number}/officers`
  Used for the current public board/officer lane.

- Resolved `Companies House` persons-with-significant-control pages
  `https://find-and-update.company-information.service.gov.uk/company/{company_number}/persons-with-significant-control`
  Used for the current public shareholder people and shareholder-organization lane.

- `Wikidata` entity resolution
  `https://www.wikidata.org/`
  Used for bounded static United Kingdom market-entity slugs plus deterministic issuer and ticker QID resolution through official ISIN and London Stock Exchange ticker claims.

## Update Procedure

Run the pack through the country-pack pipeline instead of editing generated artifacts.

```bash
ades registry fetch-finance-country-sources \
  --output-dir /mnt/githubActions/ades_big_data/pack_sources/raw/finance-country-en \
  --snapshot YYYY-MM-DD \
  --country-code uk

ades registry build-finance-country-bundles \
  --snapshot-dir /mnt/githubActions/ades_big_data/pack_sources/raw/finance-country-en/YYYY-MM-DD \
  --output-dir /mnt/githubActions/ades_big_data/pack_sources/bundles \
  --country-code uk \
  --version 0.2.0

ades registry generate-pack \
  /mnt/githubActions/ades_big_data/pack_sources/bundles/finance-uk-en-bundle \
  --output-dir /mnt/githubActions/ades_big_data/generated_runtime_packs
```

After pack generation, rebuild or update the target static registry, then verify a clean pull from that registry before promotion.

If the current refresh has to recover earlier vetted local UK work instead of completing a fresh live rebuild, merge that stronger snapshot into the canonical country snapshot under `/mnt/githubActions/ades_big_data/pack_sources/raw/finance-country-en/YYYY-MM-DD/uk` before rebuilding the bundle. Do not leave the stronger UK work only in an older sidecar directory.

## Required Checks On Each Refresh

1. Confirm the FCA Official List live export still downloads and still parses into a non-empty commercial-company issuer slice.
2. Confirm representative FCA rows still expose stable issuer fields such as company name, listing category, market status, trading venue, and ISIN.
3. Confirm the rendered `FTSE AIM All-Share` page still yields a non-empty issuer slice and still exposes stable page pagination.
4. Confirm the Companies House search flow still resolves representative issuers to current company numbers.
5. Confirm the Companies House officer pages still download for representative matched issuers and still expose active current officers.
6. Confirm the Companies House PSC pages still expose either active controllers, the expected regulated-market exemption, or the expected no-PSC state for representative companies.
7. Re-run bounded Wikidata enrichment and confirm the builder writes a non-empty `wikidata-united-kingdom-entity-resolution.json` snapshot with representative issuer or ticker matches.
8. Confirm the derived United Kingdom issuer, ticker, people, and shareholder-organization slices are non-empty and still include representative entities such as `ASOS plc`, `Deltic Energy PLC`, `Aaron Izzard`, and `Roger Mainwaring`.
9. Rebuild the bundle and runtime pack, then verify the target registry metadata reflects the refreshed `finance-uk-en` artifact rather than the older narrow UK pack.
10. Smoke-test clean pull plus a few issuer, ticker, and person probes against the target registry before promotion.

## Operational Notes

- The current United Kingdom lane is official-source-first: FCA Official List first, then rendered London Stock Exchange AIM constituents, then Companies House resolution plus officer/PSC extraction. Do not replace that with third-party issuer directories when the official seams are still available.
- The current Companies House lane should stay conservative. Promote only clearly person-like active officers and person controllers into the people lane, and keep organization-like officer/controller rows out of the people lane.
- The current United Kingdom Wikidata lane should stay deterministic and enrichment-only. Prefer official ISIN and London Stock Exchange ticker claims, and do not widen to broad people search unless the lane stays bounded and fast enough to rebuild reliably.
- The current United Kingdom pack should be rebuilt from the canonical country snapshot, not left only in older sidecar UK experiment directories.
- The current strong partial artifact rebuilt on `2026-04-22` stands at `6254` total entities: `797` organizations, `459` tickers, `4995` people, `1` exchange, and `2` market indexes, sourced from `1364` publishable sources. The current normalized bundle carries `wikidata_slug` on `197` entities: `191` organizations, `3` tickers, `1` exchange, and `2` market indexes.

## Next Expansion Lane

The next United Kingdom enhancement should deepen issuer-linked people and ownership coverage beyond the current FCA-plus-Companies-House lane.

Target official sources for that next lane:

- `RNS` management and governance announcements
  `https://www.lseg.com/en/capital-markets/regulatory-news-service`
- Official issuer annual reports and governance pages discovered from regulated disclosures
  `https://www.londonstockexchange.com/`
- `FCA` Financial Services Register
  `https://www.fca.org.uk/register`

That next lane should broaden board, executive, ownership-disclosure, and regulated-institution-principal coverage beyond the current Companies House lanes while keeping the pack focused on durable public finance people rather than general employee rosters.

## Validation

- Run the focused United Kingdom and finance-country test suite after doc or code changes that affect this pack.
- Run `docdexd hook pre-commit --repo /home/wodo/apps/ades` before closing the change.
- Keep [library_pack_publication_workflow.md](./library_pack_publication_workflow.md) aligned with this guide whenever the live UK source inventory or operational limitations change.
