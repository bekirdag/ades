# `finance-jp-en` Update Guide

This guide describes how to keep `finance-jp-en` current.

Current scope:

- Japan public-market issuer entities
- Japan public-market ticker entities
- Japan listed-company public people derived from official English corporate-governance reports plus clean English representative entries exposed on bounded JPX listed-company detail pages
- JPX-listed issuer metadata such as market segment, industry, fiscal year-end, stock-price page, English IR website, English-disclosure status, ISIN, listing date, representative metadata, and share-count metadata

Current limitation:

- `finance-jp-en` is not a full employee directory.
- The current automated Japan people lane is still narrow. It covers representative executives and investor-relations contacts exposed in English corporate-governance reports plus clean English representative entries exposed on the current bounded JPX detail-page subset.
- Current issuer/ticker coverage is driven first by resolved JPX Listed Company Search result pages, with JPX listed-company detail pages currently enriching the workbook/governance subset and the official JPX English-disclosure workbook overlaid where English-disclosure metadata exists.

## Current Sources

The current Japan pack is anchored to these official source lanes:

- `FSA`
  `https://www.fsa.go.jp/en/`
  Used as the baseline Japan securities-regulator anchor.

- `EDINET`
  `https://info.edinet-fsa.go.jp/`
  Used as the official filing-system anchor and the documented next filing lane for people extraction.

- `JPX`
  `https://www.jpx.co.jp/english/`
  Used as the baseline exchange-group anchor for Japan market coverage.

- `JPX English Disclosure GATE`
  `https://www.jpx.co.jp/english/equities/listed-co/disclosure-gate/index.html`
  Used as the official disclosure-portal anchor for English listed-company materials.

- `JPX Availability of English Disclosure Information by Listed Companies`
  `https://www.jpx.co.jp/english/equities/listed-co/disclosure-gate/availability/`
  Used as the public discovery page for the current versioned monthly workbook.

- `Resolved JPX monthly English-disclosure workbook`
  Resolved from the public availability page above.
  This currently enriches the broader Japan roster with JPX company code, company name, market segment, sector, market capitalization, English-disclosure status fields, and English IR website links where JPX publishes them.

- `JPX Listed Company Search`
  `https://www.jpx.co.jp/english/listing/co-search/`
  Used as the official current search anchor for broader Japan listed-company coverage.

- `Resolved JPX listed-company search result pages`
  Resolved from the official JPX listed-company search application.
  These pages are the current source of truth for broad Prime, Standard, and Growth issuer/ticker roster derivation and supply company-code keyed market-segment, industry, fiscal-year-end, and stock-price-page metadata.

- `Resolved JPX listed-company detail pages`
  Resolved from the official JPX listed-company search application.
  These pages currently enrich the workbook/governance subset with company-code keyed ISIN, trading-unit, listing-date, address, representative, scheduling, and share-count metadata, and they also provide a bounded supplemental representative-people lane when JPX exposes a clean English name.

- `JPX Corporate Governance Information Search`
  `https://www.jpx.co.jp/english/listing/cg-search/`
  Used as the official discovery lane for current English corporate-governance report PDFs.

- `Resolved JPX corporate-governance search result pages`
  Resolved from the official JPX corporate-governance search application.
  These pages provide the current company-code keyed list of English governance reports and their publication dates.

- `Resolved JPX corporate-governance report PDFs`
  Resolved from the official JPX governance search results.
  These reports are the current source of truth for automated Japan public-people extraction in this pack.

## Update Procedure

Run the pack through the country-pack pipeline instead of editing generated artifacts.

```bash
ades registry fetch-finance-country-sources \
  --output-dir /mnt/githubActions/ades_big_data/pack_sources/raw/finance-country-en \
  --snapshot YYYY-MM-DD \
  --country-code jp

ades registry build-finance-country-bundles \
  --snapshot-dir /mnt/githubActions/ades_big_data/pack_sources/raw/finance-country-en/YYYY-MM-DD \
  --output-dir /mnt/githubActions/ades_big_data/pack_sources/bundles \
  --country-code jp \
  --version 0.2.0

ades registry generate-pack \
  /mnt/githubActions/ades_big_data/pack_sources/bundles/finance-jp-en-bundle \
  --output-dir /mnt/githubActions/ades_big_data/generated_runtime_packs
```

After pack generation, rebuild or update the static registry, then verify a clean pull from that registry before promotion.

## Required Checks On Each Refresh

1. Confirm the JPX English-disclosure availability page still resolves and still exposes a versioned workbook link.
2. Confirm the resolved workbook still downloads as `.xlsx` and still contains the expected `English Disclosure` sheet or equivalent first sheet.
3. Confirm the workbook still exposes the core headers `Code`, `Company Name`, `Market Segment`, `Sector`, and `IR Website English Links`.
4. Confirm the JPX listed-company search still returns a non-empty server-rendered result set for representative Prime, Standard, and Growth companies.
5. Confirm representative JPX listed-company detail pages still resolve from search-result search codes and still expose the `JJK020040Form` detail payload.
6. Confirm the derived Japan issuer slice is non-empty and still contains representative Prime, Standard, or Growth companies, including names outside the English-disclosure workbook subset.
7. Confirm the derived Japan ticker slice is non-empty and still stays aligned with the issuer slice by company code.
8. Confirm representative detail-enriched issuers still carry expected metadata such as ISIN, listing date, and representative fields.
9. Confirm the JPX corporate-governance search still returns a non-empty server-rendered result set and still exposes downloadable report PDFs for representative issuers.
10. Confirm the resolved governance report files still download successfully and still produce readable text with `pdftotext`.
11. Rebuild the bundle and runtime pack, then smoke-test pull plus issuer/ticker tagging on a few representative Japan examples before promotion.
12. Smoke-test at least one representative executive and one investor-relations contact from the governance-report lane, plus one clean-English representative from the bounded detail-page lane if that lane still emits people.
13. Re-review EDINET annual securities reports and broader governance filings so the next Japan expansion lane stays documented and ready.

## Operational Notes

- The current Japan automation resolves the live workbook URL from the public JPX availability page on each snapshot. Do not hardcode a dated workbook URL in the pack configuration.
- The current Japan roster is widened from live JPX listed-company search result pages. The JPX English-disclosure workbook is now an overlay lane for richer metadata on the English-disclosure subset, not the only issuer seed.
- The current Japan metadata lane also resolves bounded JPX listed-company detail pages for issuers already selected by the workbook or governance-report subset. That keeps refresh cost controlled while enriching the most useful current issuer slice with official profile metadata.
- The current Japan people lane is still governance-report-first. Detail pages only contribute people when JPX exposes a clean English representative name; local-script-only representative fields should be ignored rather than forced into person entities.
- `FSA`, `EDINET`, `JPX`, `JPX English Disclosure GATE`, `JPX Listed Company Search`, and `JPX Corporate Governance Information Search` are all live official source anchors in the current pack.
- The current Japan people lane depends on `pdftotext` to extract report text from official governance PDFs. If `pdftotext` is missing, the issuer/ticker lane still works but the automated governance-report people lane will be empty.
- The next expansion lane should stay official-source-first: broader JPX listed-company detail-page coverage plus EDINET annual securities reports and broader governance filings.

## Next Expansion Lane

The next Japan enhancement should deepen official issuer-profile and people coverage beyond the current search-results, bounded detail-page, and governance-report lane, not introduce third-party sources.

Target official sources for that next lane:

- `JPX Listed Company Search` detail pages beyond the current workbook/governance subset
  `https://www.jpx.co.jp/english/listing/co-search/`
- `EDINET` annual securities reports and related governance filings
  `https://info.edinet-fsa.go.jp/`

That next lane should extract broader named directors, executive officers, representative metadata, and other issuer-linked public people from a wider detail-page population plus official filing documents, beyond the current governance-report-plus-bounded-detail representative subset.

## Validation

- Run the focused Japan and finance-country test suite after doc or code changes that affect this pack.
- Run `docdexd hook pre-commit --repo /home/wodo/apps/ades` before closing the change.
- Keep [library_pack_publication_workflow.md](./library_pack_publication_workflow.md) aligned with this guide whenever the live source inventory changes.
