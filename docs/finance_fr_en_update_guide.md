# finance-fr-en Update Guide

This guide describes how to keep `finance-fr-en` current.

Current scope:

- France public-market issuer entities
- France public-market ticker entities
- France listed-company public people derived from official regulated-news contacts, HTML/XHTML filing documents, and issuer governance pages
- Official Paris equity-market coverage across the regulated market, Euronext Growth Paris, and Euronext Access Paris
- Baseline France regulators, exchanges, and the CAC 40 carry explicit Wikidata metadata

Current limitation:

- `finance-fr-en` is not a full employee directory. It derives only publicly disclosed people such as directors, executive officers, investor-relations contacts, and press contacts.
- Coverage still depends on official issuer governance pages or regulated-news disclosures being available and stable.

## Current France sources used in the build

The current France pack is built from these official source lanes:

- AMF listed-companies and issuers entry point:
  `https://www.amf-france.org/en/professionals/listed-companies-issuers/my-listed-companies-issuers-space`
- Euronext Paris regulated-market directory:
  `https://live.euronext.com/en/markets/paris/equities/euronext/list`
- Euronext Growth Paris directory:
  `https://live.euronext.com/en/markets/paris/equities/growth/list`
- Euronext Access Paris directory:
  `https://live.euronext.com/en/markets/paris/equities/access/list`
- Euronext Paris company-news latest page:
  `https://live.euronext.com/en/markets/paris/company-news`
- Euronext Paris company-news archive:
  `https://live.euronext.com/en/markets/paris/company-news-archive`
- Euronext press-release modal endpoint discovered from the company-news pages:
  `https://live.euronext.com/ajax/node/company-press-release/{node_nid}`
- Official DILA regulated-information archive used when issuer filing links resolve there:
  `https://www.info-financiere.fr/`
- HTML or XHTML annual reports and universal registration documents discovered from official regulated-news and issuer-finance links
- Issuer finance and governance pages discovered from official regulated-news disclosures
- Wikidata API for issuer and public-person QID plus alias enrichment:
  `https://www.wikidata.org/w/api.php`

What each source currently contributes:

- The three Euronext list pages are the live issuer and ticker seed sources.
- The pack renders those list pages with headless Chrome before parsing because the raw HTML does not expose the full issuer table reliably.
- The Euronext company-news latest and archive pages provide issuer-linked regulated-news rows and article IDs.
- The Euronext press-release modal endpoint provides issuer names, `ISIN`s, public contact names, email addresses, phone numbers, and outbound issuer finance-site links.
- Linked issuer-hosted or DILA-hosted HTML/XHTML filing documents provide additional directors and executive officers when those disclosures expose governance sections directly.
- Discovered issuer governance pages provide directors and executive officers from public governance rosters.
- The Wikidata lane adds `wikidata_qid`, `wikidata_slug`, `wikidata_url`, and `wikidata_label` to baseline France market entities, and it adds the same metadata plus extra issuer/person aliases after the deterministic France extraction is complete.
- The AMF issuer page remains an official entry point to review when validating France filing-discovery coverage.

## What must be refreshed each time

To keep `finance-fr-en` current, each France refresh should do all of the following:

1. Pull a fresh France snapshot with `ades registry fetch-finance-country-sources --country-code fr`.
2. Re-render the three Euronext Paris list pages and confirm each one still produces non-empty issuer rows.
3. Re-fetch the Euronext Paris latest and archive company-news pages and confirm they still expose issuer rows with `data-node-nid`.
4. Verify the press-release modal endpoint still returns issuer `ISIN`, issuer name, contact sections, and outbound finance-site links.
5. Verify discovered filing-document links still resolve to HTML or XHTML annual reports / universal registration documents for a representative issuer sample.
6. Verify discovered issuer finance sites still lead to public governance pages for a representative issuer sample.
7. Re-run the Wikidata resolution pass and confirm the builder still writes a non-empty `wikidata-france-entity-resolution.json` snapshot.
8. Rebuild the normalized France bundle and generate the runtime pack.
9. Rebuild or update the target static registry and perform a clean install smoke test before promotion.
10. Re-review the AMF listed-company issuer page and linked disclosure surfaces so the France filing lane stays documented.

## Recommended refresh loop

Use the country-pack pipeline rather than editing generated pack files by hand.

```bash
ades registry fetch-finance-country-sources \
  --output-dir /mnt/githubActions/ades_big_data/pack_sources/raw/finance-country-en \
  --snapshot YYYY-MM-DD \
  --country-code fr

ades registry build-finance-country-bundles \
  --snapshot-dir /mnt/githubActions/ades_big_data/pack_sources/raw/finance-country-en/YYYY-MM-DD \
  --output-dir /mnt/githubActions/ades_big_data/pack_sources/bundles \
  --country-code fr \
  --version 0.2.0

ades registry generate-pack \
  /mnt/githubActions/ades_big_data/pack_sources/bundles/finance-fr-en-bundle \
  --output-dir /mnt/githubActions/ades_big_data/generated_runtime_packs
```

After pack generation, update the target registry and verify a clean consumer install before promotion.

## France-specific checks before promotion

Because France currently depends on rendered Euronext directories, each refresh should explicitly confirm:

- headless Chrome is available on the build machine
- the regulated, Growth, and Access pages still render through `live.euronext.com`
- each rendered page yields a non-empty issuer slice
- parsed rows still include stable `ISIN`, ticker, and market-code fields
- cross-segment dedupe by `ISIN` still behaves correctly
- the latest and archive company-news pages still expose non-empty `data-node-nid` rows
- the press-release modal endpoint still returns issuer identity plus contact blocks
- filing-document discovery still resolves at least one HTML or XHTML annual-report / URD document from an official disclosure flow
- governance discovery still resolves at least one public governance page from a representative issuer finance site
- the Wikidata resolution snapshot is non-empty and representative issuer/person QIDs still attach cleanly
- representative exact matches still work for one regulated, one Growth, and one Access issuer
- representative people matches still work for one board member, one public contact, and one filing-derived executive or director

Recommended spot checks:

- one regulated-market issuer such as `74SOFTWARE`
- one regulated-market issuer with public governance content such as `ROCHE BOBOIS`
- one Growth issuer such as `2CRSI`
- one Access issuer such as `ACTIVIUM GROUP`
- one investor-relations or press-contact person returned from an Euronext press-release modal
- one director or executive officer returned from a filing document
- one director or executive officer returned from an issuer governance page

## Next expansion lane

The next France enhancement should deepen official filing coverage, not switch to third-party sites.

Target sources for the next lane:

- AMF listed-company issuer disclosures
- DILA regulated-information archive coverage beyond currently discovered HTML/XHTML documents
- issuer universal registration documents, annual reports, and corporate-governance reports that are still PDF-only or ESEF-specific

That next lane should harden and widen related public people coverage beyond what company-news contacts, HTML/XHTML filing documents, and issuer governance pages expose today.
