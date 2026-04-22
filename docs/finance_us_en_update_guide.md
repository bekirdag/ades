# `finance-us-en` Update Guide

This guide describes how to keep `finance-us-en` current.

Current scope:

- United States public-market issuer entities derived from the official SEC `company_tickers.json` universe
- United States public-market ticker entities aligned to the issuer and CIK universe
- Bounded Wikidata issuer and ticker enrichment keyed by official SEC `CIK` claims where Wikidata publishes them
- United States public people derived from SEC bulk submissions via up to two recent DEF `14A` or `DEFA14A` proxy filings plus multiple recent official SEC insider filings on Forms `3`/`4`/`5` per issuer
- Static baseline regulator, filing-system, exchange, and market-index entities for `SEC`, `EDGAR`, `Nasdaq`, `NYSE`, `FDIC`, and `S&P 500`

Current limitation:

- `finance-us-en` is not a full employee directory.
- The current issuer and ticker lane is broad, and the people lane is materially broader than the earlier bounded proxy-only sample, but it is still not exhaustive. The current automation uses a bounded proxy lane that inspects up to two recent proxy filings per issuer plus a bounded slice of recent insider filings per issuer, not a full filing-history crawl or a general employee roster.
- The current Wikidata lane is issuer-and-ticker only. It uses official SEC `CIK` claims and does not currently attempt broad United States people resolution.
- `8-K` item `5.02`, Schedules `13D`/`13G`, `IAPD`, `FDIC`, Nasdaq issuer-profile joins, and NYSE issuer-profile joins are still follow-on expansion lanes rather than current automated coverage.

## Current Sources

The current United States pack is anchored to these official source lanes:

- `SEC`
  `https://www.sec.gov/`
  Used as the baseline United States securities-regulator anchor.

- `EDGAR`
  `https://www.sec.gov/search-filings`
  Used as the baseline filing-system anchor for the pack.

- `SEC company_tickers.json`
  `https://www.sec.gov/files/company_tickers.json`
  This is the current source of truth for broad issuer and ticker coverage, including CIK-linked issuer identity.

- `SEC bulk submissions.zip`
  `https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip`
  This is the current official source used for the derived proxy-plus-insider people lane.

- `Nasdaq`
  `https://www.nasdaq.com/`
  Currently used as a baseline exchange anchor and documented next issuer-profile expansion lane.

- `NYSE`
  `https://www.nyse.com/`
  Currently used as a baseline exchange anchor and documented next issuer-profile expansion lane.

- `Wikidata` entity resolution
  `https://query.wikidata.org/sparql`
  Used for bounded issuer and ticker QID resolution through official SEC `CIK` claims, then enriched through the Wikidata entity API payloads.

## Update Procedure

Run the pack through the country-pack pipeline instead of editing generated artifacts.

```bash
ades registry fetch-finance-country-sources \
  --output-dir /mnt/githubActions/ades_big_data/pack_sources/raw/finance-country-en \
  --snapshot YYYY-MM-DD \
  --country-code us

ades registry build-finance-country-bundles \
  --snapshot-dir /mnt/githubActions/ades_big_data/pack_sources/raw/finance-country-en/YYYY-MM-DD \
  --output-dir /mnt/githubActions/ades_big_data/pack_sources/bundles \
  --country-code us \
  --version 0.2.0

ades registry generate-pack \
  /mnt/githubActions/ades_big_data/pack_sources/bundles/finance-us-en-bundle \
  --output-dir /mnt/githubActions/ades_big_data/generated_runtime_packs
```

After pack generation, rebuild or update the target static registry, then verify a clean pull from that registry before promotion.

If the current refresh has to recover earlier local SEC work instead of completing a fresh rebuild, merge that recovered work into the canonical country snapshot under `/mnt/githubActions/ades_big_data/pack_sources/raw/finance-country-en/YYYY-MM-DD/us` before rebuilding the bundle. Do not leave the stronger US work only in an older sidecar sample directory.

## Required Checks On Each Refresh

1. Confirm `company_tickers.json` still downloads successfully and still exposes a non-empty issuer universe with stable `CIK`, issuer-name, and ticker fields.
2. Confirm `submissions.zip` still downloads successfully and still opens as a valid SEC bulk archive.
3. Confirm the derived issuer slice is non-empty and still contains representative large issuers such as `Broadcom Inc.`.
4. Confirm the derived ticker slice is non-empty and still stays aligned with the issuer slice by `CIK` and issuer name.
5. Confirm the derived people slice is non-empty and still contains representative public people such as `Harry L. You`.
6. Confirm the people slice is no longer just the earlier bounded proxy-only sample. It should reflect both the proxy lane and the bounded multi-filing insider lane.
7. Confirm the canonical snapshot now carries `wikidata-united-states-entity-resolution.json` and that representative issuers such as `Broadcom Inc.` have `wikidata_slug` in the derived issuer output.
8. Rebuild the bundle and runtime pack, then verify the target registry metadata reflects the refreshed `finance-us-en` artifact rather than the old six-entity baseline pack.
9. Smoke-test clean pull plus a few issuer and person probes against the target registry before promotion.

## Operational Notes

- The current United States artifact should be rebuilt from the canonical SEC country snapshot, not from older sidecar sample directories. The broad issuer and ticker lane comes from the full `company_tickers.json` derivation, while the current people lane comes from a bounded multi-filing proxy pass plus a bounded multi-filing insider slice over the same SEC submissions archive.
- The current Wikidata lane should also be regenerated from that same canonical snapshot. Do not leave issuer or ticker `wikidata_slug` coverage only in an experimental sidecar pack; merge it back into the canonical `sources.fetch.json` and derived SEC issuer snapshot before bundle build.
- That makes the current `finance-us-en` artifact a stronger partial pack, but it is still not the final United States lane. Do not overstate the current people coverage.
- `company_tickers.json` is the durable current identifier backbone for this pack. If the SEC changes surrounding landing pages, prefer preserving the direct official bulk endpoint instead of replacing it with third-party mirrors.
- The current United States people lane reuses the SEC proxy-people extractor with a bounded recent-filing comparison per issuer and extends it with multiple recent Forms `3`/`4`/`5` insider filings per issuer. It is still issuer-linked public-people coverage, not general employee coverage.
- The current United States Wikidata lane is deterministic by `CIK` rather than fuzzy issuer-name search. Keep that bias unless an official better identifier seam replaces it.

## Next Expansion Lane

The next United States enhancement should deepen official people and institution coverage beyond the current issuer or ticker backbone plus proxy-plus-insider people lane.

Target official sources for that next lane:

- `SEC` `8-K` item `5.02` management-change extraction
  `https://www.sec.gov/edgar/search-and-access`
- `SEC` Schedules `13D` and `13G` beneficial-owner extraction
  `https://www.sec.gov/edgar/search-and-access`
- `IAPD`
  `https://adviserinfo.sec.gov/`
- `FDIC BankFind`
  `https://api.fdic.gov/banks/docs/`

That next lane should deepen current issuer-linked people coverage with management-change and beneficial-owner filings, then add adviser or institution principals where official publication remains durable.

## Validation

- Run the focused finance-country test suite after doc or code changes that affect this pack.
- Run `docdexd hook pre-commit --repo /home/wodo/apps/ades` before closing the change.
- Keep [library_pack_publication_workflow.md](./library_pack_publication_workflow.md) aligned with this guide whenever the live source inventory or operational limitations change.
