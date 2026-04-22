# `finance-ar-en` Update Guide

This guide describes how to keep `finance-ar-en` current.

Current scope:

- Argentina public-market issuer entities
- issuer-linked public people such as directors, administration members, and BYMA market representatives
- regulated financial institutions plus public directors, managers, and customer-service officers from BCRA

This pack is not a full employee directory.

## Current Sources

- `CNV` landing and `AIF` disclosure surface
  `https://www.argentina.gob.ar/cnv`
  `https://www.cnv.gov.ar/`
  Used as the official regulator/disclosure lane and as the documented next expansion surface for filing-driven enrichment.

- `BYMA` exchange landing
  `https://www.byma.com.ar/`
  Used as the primary exchange anchor for Argentina market coverage.

- `BYMA listed companies`
  `https://www.byma.com.ar/financiarse/para-emisoras/empresas-listadas`
  Used as the primary live issuer roster and as fallback issuer metadata when some BYMADATA company-profile endpoints return empty payloads.

- `CNV issuer-regime workbook`
  `https://www.cnv.gov.ar/SitioWeb/Reportes/ListadoSociedadesEmisorasPorRegimen`
  Used to enrich matched public issuers with official `CUIT`, CNV category, and category-status metadata.

- `BYMADATA all-instrument-select`
  `https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/instrumentproduct/instrument/all-instrument-select`
  Used to seed the broad live symbol universe before filtering to Argentina equities.

- `BYMADATA especies/general`
  `https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/bnown/fichatecnica/especies/general`
  Used for issuer names, ticker metadata, and ISIN-level enrichment.

- `BYMADATA sociedades/general`
  `https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/bnown/fichatecnica/sociedades/general`
  Used for issuer website, address, activity, and societary profile details.

- `BYMADATA sociedades/administracion`
  `https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/bnown/fichatecnica/sociedades/administracion`
  Used for public board and administration people linked to listed issuers.

- `BYMADATA sociedades/responsables`
  `https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/bnown/fichatecnica/sociedades/responsables`
  Used for named BYMA market representatives and related public contacts.

- `BCRA financial institutions`
  `https://www.bcra.gob.ar/informacion-sobre-entidades-financieras/`
  Used for regulated institutions plus public directors, managers, and customer-service officers from the current monthly `.7z` publication archive.

- `Wikidata API`
  `https://www.wikidata.org/w/api.php`
  Used only for QID, slug, URL, and alias enrichment after deterministic Argentina extraction. It is not the source of truth for issuer inclusion.

## Update Procedure

Run the pack through the country-pack pipeline instead of editing generated artifacts.

```bash
ades registry fetch-finance-country-sources \
  --output-dir /mnt/githubActions/ades_big_data/pack_sources/raw/finance-country-en \
  --snapshot YYYY-MM-DD \
  --country-code ar

ades registry build-finance-country-bundles \
  --snapshot-dir /mnt/githubActions/ades_big_data/pack_sources/raw/finance-country-en/YYYY-MM-DD \
  --output-dir /mnt/githubActions/ades_big_data/pack_sources/bundles \
  --country-code ar \
  --version 0.2.0

ades registry generate-pack \
  /mnt/githubActions/ades_big_data/pack_sources/bundles/finance-ar-en-bundle \
  --output-dir /mnt/githubActions/ades_big_data/generated_runtime_packs
```

After pack generation, rebuild or update the static registry, then verify a clean pull from that registry before promotion.

## Required Checks On Each Refresh

1. Confirm the BYMA listed-companies page still exposes a non-empty issuer card set with symbols and company names.
2. Confirm the CNV issuer-regime endpoint still downloads as `.xlsx` and still exposes header columns `CUIT`, `Sociedad`, `Categoria`, and `Estado Categoria`.
3. Confirm `all-instrument-select` still returns a non-empty Argentina equity slice.
4. Re-run `especies/general`, `sociedades/general`, `sociedades/administracion`, and `sociedades/responsables` for the current listed-company symbols.
5. Verify the listed-company fallback still works when `especies/general` or `sociedades/general` returns empty data for a listed issuer.
6. Confirm the BCRA publication still resolves to the latest monthly archive, including the fallback path through the latest `?fecha=YYYYMM` page when the landing page does not expose the archive directly.
7. Re-run the Wikidata resolution pass and confirm the builder still writes a non-empty `wikidata-argentina-entity-resolution.json` snapshot.
8. Rebuild the bundle and runtime pack, then smoke-test pull plus issuer/person tagging on a few representative Argentina examples before promotion.

## Operational Notes

- The current fetcher relaxes TLS verification for `open.bymadata.com.ar`. If BYMADATA changes its TLS configuration, re-check whether that exception is still necessary.
- The BYMA listed-companies page is the roster of record for listed issuers. Do not treat empty BYMADATA profile payloads as proof that a listed issuer should be dropped.
- The CNV issuer-regime workbook is enrichment, not the roster of record. Use it to add issuer `CUIT` and regime metadata to already matched Argentina issuers rather than to replace the BYMA roster.
- The BCRA lane is month-driven. Archive discovery can drift if the latest monthly page format changes.
- Wikidata is enrichment only. Keep issuer inclusion and person extraction anchored to official Argentina sources first.
- CNV/AIF filing-driven extraction is still the next expansion lane, not the current primary issuer/people lane.

## Validation

- Run the focused Argentina and finance-country test suite after doc or code changes that affect this pack.
- Run `docdexd hook pre-commit --repo /home/wodo/apps/ades` before closing the change.
- Keep [library_pack_publication_workflow.md](./library_pack_publication_workflow.md) aligned with this guide whenever the live source inventory changes.
