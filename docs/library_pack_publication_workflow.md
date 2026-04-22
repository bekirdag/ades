# Library Pack Publication Workflow

This document defines the first durable publication workflow for real-content library packs in `ades`.

## Goal

Take one or more normalized bundle directories, run the deterministic report and quality gates, and emit a publishable static registry release without changing the runtime pack format or bypassing the existing deployment/object-storage path.

## Refresh Surface

The publication orchestration surface is:

- Python: `ades.refresh_generated_packs(...)`
- Python: `ades.prepare_registry_deploy_release(...)`
- Python: `ades.publish_generated_registry_release(...)`
- CLI: `ades registry refresh-generated-packs <bundle-dir...> --output-dir <dir>`
- CLI: `ades registry prepare-deploy-release --output-dir <dir>`
- CLI: `ades registry publish-generated-release <registry-dir> --prefix <object-storage-prefix>`
- HTTP: `POST /v0/registry/refresh-generated-packs`
- HTTP: `POST /v0/registry/prepare-deploy-release`
- HTTP: `POST /v0/registry/publish-generated-release`

This surface is intentionally downstream of the bundle builders:

- `ades registry build-finance-bundle`
- `ades registry build-general-bundle`
- `ades registry build-medical-bundle`

## What The Refresh Run Does

For each bundle directory:

1. Generate a stable report with alias, label, canonical, and rule statistics.
2. Run the pack-specific quality gate:
   - `finance-en` uses the finance fixture profile
   - `general-en` uses the general fixture profile
   - `medical-en` uses the medical fixture profile and requires `general-en`
3. Review source governance from each bundle report:
   - every source snapshot must declare `license_class`
   - only `ship-now` sources are eligible for publication
   - bundles with `build-only`, `blocked-pending-license-review`, or missing source metadata remain local-review only
4. Only if every requested pack passes quality and every bundle is publication-eligible, build a static registry release from the generated pack directories.

If any pack fails quality:

- the refresh response returns `passed=false`
- the report and quality output are still preserved
- the final `registry/` build is skipped

If any pack fails source governance:

- the refresh response still returns the report and quality evidence
- the final `registry/` build is skipped with `registry_build_skipped:source_governance_failed`
- the per-pack report warnings identify which source snapshots blocked publication

## Output Layout

Given `--output-dir /path/to/release`, the refresh run writes:

```text
/path/to/release/
  reports/
    <pack-id>/
  quality/
    finance-pack-quality/
    general-pack-quality/
    medical-pack-quality/
  registry/
    index.json
    packs/
    artifacts/
```

The `reports/` directory is for human review and stable metrics. The `quality/` directory is the gating evidence. The `registry/` directory is the only publication payload.

## Example Local Flow

```bash
ades registry fetch-finance-sources \
  --output-dir /mnt/githubActions/ades_big_data/pack_sources/raw/finance-en \
  --snapshot 2026-04-10

ades registry build-finance-bundle \
  --sec-company-tickers /mnt/githubActions/ades_big_data/pack_sources/raw/finance-en/2026-04-10/sec_company_tickers.json \
  --symbol-directory /mnt/githubActions/ades_big_data/pack_sources/raw/finance-en/2026-04-10/nasdaqtraded.txt \
  --curated-entities /mnt/githubActions/ades_big_data/pack_sources/raw/finance-en/2026-04-10/curated_finance_entities.json \
  --output-dir /mnt/githubActions/ades_big_data/pack_sources/bundles

ades registry fetch-general-sources \
  --output-dir /mnt/githubActions/ades_big_data/pack_sources/raw/general-en \
  --snapshot 2026-04-10

ades registry build-general-bundle \
  --wikidata-entities /mnt/githubActions/ades_big_data/pack_sources/raw/general-en/2026-04-10/wikidata_general_entities.json \
  --geonames-places /mnt/githubActions/ades_big_data/pack_sources/raw/general-en/2026-04-10/geonames_places.txt \
  --curated-entities /mnt/githubActions/ades_big_data/pack_sources/raw/general-en/2026-04-10/curated_general_entities.json \
  --output-dir /mnt/githubActions/ades_big_data/pack_sources/bundles

ades registry build-medical-bundle \
  --disease-ontology /mnt/githubActions/ades_big_data/pack_sources/raw/medical-en/2026-04-10/disease_ontology_terms.json \
  --hgnc-genes /mnt/githubActions/ades_big_data/pack_sources/raw/medical-en/2026-04-10/hgnc_genes.json \
  --uniprot-proteins /mnt/githubActions/ades_big_data/pack_sources/raw/medical-en/2026-04-10/uniprot_proteins.json \
  --clinical-trials /mnt/githubActions/ades_big_data/pack_sources/raw/medical-en/2026-04-10/clinical_trials.json \
  --curated-entities /mnt/githubActions/ades_big_data/pack_sources/raw/medical-en/2026-04-10/curated_medical_entities.json \
  --output-dir /mnt/githubActions/ades_big_data/pack_sources/bundles

ades registry refresh-generated-packs \
  /mnt/githubActions/ades_big_data/pack_sources/bundles/finance-en-bundle \
  /mnt/githubActions/ades_big_data/pack_sources/bundles/general-en-bundle \
  /mnt/githubActions/ades_big_data/pack_sources/bundles/medical-en-bundle \
  --output-dir /mnt/githubActions/ades_big_data/pack_releases/2026-04-10-generated-packs
```

Finance-only refreshes can pass a single bundle directory. When `--max-ambiguous-aliases` is omitted, refresh now applies pack-specific defaults automatically: `finance-en=300`, `medical-en=25`, and `general-en=1000` for the full official general pack lane. The first real-data finance snapshot on `2026-04-10` validated cleanly on fixture recall/precision with `ambiguous_alias_count=256`, the bounded real-data general slice on `2026-04-10` now validates cleanly with `wikidata_entity_count=15`, `curated_entity_count=3`, `entity_record_count=20`, `alias_count=238`, and `ambiguous_alias_count=0`, and the first real-data medical snapshot on `2026-04-10` now validates cleanly with `ambiguous_alias_count=22` after pruning compact symbolic disease/protein aliases. Medical-only refreshes must still pass the matching `general-en` bundle or `--general-bundle-dir`, the first fully real-source combined general/medical release now lives at `/mnt/githubActions/ades_big_data/pack_releases/general-medical-2026-04-10`, and the first fully real-source three-pack release now lives at `/mnt/githubActions/ades_big_data/pack_releases/finance-general-medical-2026-04-10`.

## Object Storage Publication

When the refresh run returns `passed=true`, the generated `registry/` directory is ready for publication.

Preferred pattern:

1. Sync the approved release to an immutable object-storage prefix.
2. Review the uploaded `index.json`, manifests, and artifacts.
3. Write an approved promotion spec at `src/ades/resources/registry/promoted-release.json`.
4. Let the existing push-to-`main` deployment workflow materialize that reviewed release into `dist/registry` and publish it through the normal hosted-registry path.

Promotion spec shape:

```json
{
  "schema_version": 1,
  "registry_url": "https://ades.fsn1.your-objectstorage.com/generated-pack-releases/finance-general-medical-2026-04-10/index.json",
  "smoke_pack_ids": ["finance-en", "medical-en"]
}
```

The deploy-preparation surface is:

- Python: `ades.prepare_registry_deploy_release(...)`
- CLI: `ades registry prepare-deploy-release --output-dir <dir>`
- HTTP: `POST /v0/registry/prepare-deploy-release`

When `src/ades/resources/registry/promoted-release.json` is absent, deploy preparation falls back to rebuilding the bundled starter registry exactly as before. When the file is present, deploy preparation runs the shipped clean-consumer smoke against the reviewed registry URL, then materializes the reviewed `index.json`, manifests, and checksum-verified artifacts back into the deploy-owned local registry layout for the existing CI/CD workflow.

Preferred shipped helper:

```bash
ades registry publish-generated-release \
  /mnt/githubActions/ades_big_data/pack_releases/finance-general-medical-2026-04-10/registry \
  --prefix generated-pack-releases/finance-general-medical-2026-04-10
```

Equivalent raw sync:

```bash
aws s3 sync \
  /mnt/githubActions/ades_big_data/pack_releases/finance-general-medical-2026-04-10/registry \
  s3://$ADES_PACK_OBJECT_STORAGE_BUCKET/generated-pack-releases/finance-general-medical-2026-04-10/ \
  --delete \
  --endpoint-url "https://$ADES_PACK_OBJECT_STORAGE_ENDPOINT"
```

The first live publication rehearsal used the shipped helper and wrote `22` reviewed objects under `s3://ades/generated-pack-releases/finance-general-medical-2026-04-10/`.
The shipped helper also returns direct HTTPS registry candidates, and the preferred live consumer URL is already working: `https://ades.fsn1.your-objectstorage.com/generated-pack-releases/finance-general-medical-2026-04-10/index.json`.

## Keeping `finance-ar-en` Up To Date

`finance-ar-en` is currently a partially automated Argentina pack. Its intended scope is public-market issuer entities plus public people linked to those issuers, especially directors, named administration members, and BYMA market representatives. It is not a full employee directory.

The pack-specific refresh guide is also tracked in [finance_ar_en_update_guide.md](./finance_ar_en_update_guide.md).

### Current Argentina sources used in the build

The Argentina pack is currently derived from these official source lanes:

- Comisión Nacional de Valores landing and AIF disclosure surface:
  `https://www.argentina.gob.ar/cnv`
  `https://www.cnv.gov.ar/`
- BYMA exchange landing:
  `https://www.byma.com.ar/`
- BYMA listed-companies page used as the primary live issuer roster and as fallback company metadata when some BYMADATA profile endpoints return empty payloads:
  `https://www.byma.com.ar/financiarse/para-emisoras/empresas-listadas`
- CNV issuer-regime workbook used to enrich matched public issuers with official CUIT and current regime metadata:
  `https://www.cnv.gov.ar/SitioWeb/Reportes/ListadoSociedadesEmisorasPorRegimen`
- BYMADATA public instrument-universe seed:
  `https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/instrumentproduct/instrument/all-instrument-select`
- BYMADATA species technical-sheet endpoint for issuer and ISIN metadata:
  `https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/bnown/fichatecnica/especies/general`
- BYMADATA societary profile endpoint for issuer website, address, and activity metadata:
  `https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/bnown/fichatecnica/sociedades/general`
- BYMADATA societary administration endpoint for board and named administration people:
  `https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/bnown/fichatecnica/sociedades/administracion`
- BYMADATA societary responsables endpoint for named market representatives:
  `https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/bnown/fichatecnica/sociedades/responsables`
- BCRA financial-institutions publication page and its current monthly `.7z` archive for regulated institutions plus public directors, managers, and customer-service officers:
  `https://www.bcra.gob.ar/informacion-sobre-entidades-financieras/`
- Wikidata API for issuer and public-person QID resolution plus alias enrichment:
  `https://www.wikidata.org/w/api.php`

### What must be refreshed each time

To keep `finance-ar-en` current, each Argentina refresh should do all of the following:

1. Pull a fresh Argentina snapshot with `ades registry fetch-finance-country-sources --country-code ar`.
2. Re-fetch the BYMA listed-companies page and confirm the live listed-company cards still expose non-empty symbol, company-name, and ISIN data for the current equity roster.
3. Re-fetch the CNV issuer-regime workbook and confirm it still downloads as `.xlsx` with the expected issuer metadata columns.
4. Re-seed the supplemental symbol universe from the latest BYMADATA `all-instrument-select` response and keep the equity-symbol filter stable.
5. Re-run BYMADATA species and societary endpoint fetches for the listed-company symbols so issuer names, ISINs, board members, and market representatives stay current even when some profile endpoints return empty payloads.
6. Re-fetch the BCRA financial-institutions publication, confirm the latest monthly archive link still resolves, and re-derive regulated institutions plus public directors, managers, and customer-service officers from the archive TXT members.
7. Re-run the Wikidata issuer/person resolution pass so Argentina issuers, regulated institutions, and matched public people keep current QIDs and alias expansions after the deterministic BYMADATA and BCRA extraction.
8. Rebuild the normalized Argentina bundle, generate the runtime pack, compose a static registry, and smoke-test clean pull plus tagging before promotion.
9. Re-review CNV/AIF availability and format drift so the next expansion lane toward issuer filings stays documented and ready.

### Recommended refresh loop

Use the country-pack pipeline rather than hand-editing generated pack files:

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

After pack generation, build or update a static registry, then verify a clean consumer install from that registry before promotion.

### Argentina-specific checks before promotion

Because Argentina is still `partial` automation, each refresh should explicitly review these conditions:

- the BYMADATA instrument-universe endpoint still downloads and includes a non-empty equity slice
- the BYMA listed-companies page still renders a non-empty card list with symbol links into `technical-detail-equity`
- the CNV issuer-regime workbook still downloads and still exposes `CUIT`, `Sociedad`, `Categoria`, and `Estado Categoria` headers
- the BYMADATA societary endpoints still accept symbol-keyed requests and return non-empty data for a representative issuer sample
- issuer fallback still works when `sociedades/general` or `especies/general` returns empty data for a listed-company symbol but `administracion` / `responsables` still return people
- non-issuer BYMA symbols still fail cleanly or return empty societary payloads instead of polluting issuer coverage
- the BCRA financial-institutions publication still exposes a current `.7z` archive and the archive still contains the expected `Entfin/Tec_Cont/entidad`, `Entfin/Direct`, `Entfin/Gerent`, and `Entfin/Respclie` TXT members
- if the BCRA landing page no longer includes the `.7z` directly, the latest `?fecha=YYYYMM` selector page still resolves and still exposes the current archive
- the BCRA lane still yields a non-empty regulated-institution slice and still produces representative public directors, managers, and customer-service officers
- the Wikidata lane still writes a non-empty `wikidata-argentina-entity-resolution.json` snapshot for representative issuer/person samples
- issuer and person Wikidata matches still look sane for large Argentina names and do not collapse onto unrelated homonyms
- spot checks still work for listed issuers, regulated institutions, and people across large Argentina names
- CNV/AIF landing surfaces are still reachable and ready for the next filing-expansion lane

Recommended spot checks:

- one large energy issuer such as YPF
- one major financial issuer such as Grupo Financiero Galicia
- one regulated institution from the BCRA lane such as Banco BBVA Argentina or Banco de Galicia y Buenos Aires
- one market-representative person returned by the `sociedades/responsables` lane
- one board or presidency person returned by the `sociedades/administracion` lane
- one BCRA public director, manager, or customer-service officer

## Keeping `finance-jp-en` Up To Date

`finance-jp-en` is currently a partially automated Japan pack. Its current automated scope is broader Japan public-market issuers and tickers derived from official JPX listed-company search result pages with bounded JPX listed-company detail-page metadata and JPX English-disclosure workbook metadata overlaid where available, plus publicly disclosed representative executives and investor-relations contacts extracted from official English JPX corporate-governance reports and clean English representative entries exposed on the bounded JPX detail-page subset, alongside baseline static Japan regulator, exchange, and market-index entities. It is not a full employee directory.

The pack-specific refresh guide is also tracked in [finance_jp_en_update_guide.md](./finance_jp_en_update_guide.md).

### Current Japan sources used in the build

The Japan pack is currently anchored to these official source lanes:

- FSA regulator landing:
  `https://www.fsa.go.jp/en/`
- EDINET filings landing:
  `https://info.edinet-fsa.go.jp/`
- JPX English landing:
  `https://www.jpx.co.jp/english/`
- JPX English Disclosure GATE:
  `https://www.jpx.co.jp/english/equities/listed-co/disclosure-gate/index.html`
- JPX Availability of English Disclosure Information by Listed Companies:
  `https://www.jpx.co.jp/english/equities/listed-co/disclosure-gate/availability/`
- JPX Listed Company Search:
  `https://www.jpx.co.jp/english/listing/co-search/`
- JPX Corporate Governance Information Search:
  `https://www.jpx.co.jp/english/listing/cg-search/`
- The resolved JPX listed-company search result pages discovered from the official JPX listed-company search application
- The resolved JPX listed-company detail pages discovered from the official JPX listed-company search application
- The resolved versioned monthly workbook discovered from the public JPX availability page
- The resolved JPX corporate-governance search result pages and linked governance-report PDFs discovered from the official JPX governance search application

Current derivation is driven first by JPX listed-company search result pages for broad Prime, Standard, and Growth roster coverage and search metadata such as industry, fiscal year-end, and stock-price links. JPX listed-company detail pages then enrich the current workbook/governance subset with official profile metadata such as ISIN, listing date, representative, scheduling, and share-count fields. The JPX monthly workbook overlays market-cap, sector, English-disclosure status fields, and English IR website links where JPX publishes them. The current Japan people lane is governance-report-first and is supplemented only by bounded JPX detail-page representative entries when JPX exposes a clean English name.

### What must be refreshed each time

To keep `finance-jp-en` current:

1. Pull a fresh Japan snapshot with `ades registry fetch-finance-country-sources --country-code jp`.
2. Re-fetch the JPX English-disclosure availability page and confirm it still exposes a current workbook URL.
3. Re-download the resolved workbook and confirm it is a valid `.xlsx` file.
4. Confirm the workbook still exposes the expected issuer headers such as `Code`, `Company Name`, `Market Segment`, `Sector`, and `IR Website English Links`.
5. Re-run the JPX listed-company search and confirm it still yields a non-empty server-rendered result set for representative Prime, Standard, and Growth companies.
6. Re-resolve representative JPX listed-company detail pages from search-result search codes and confirm they still expose the current detail payload.
7. Re-run the JPX corporate-governance search and confirm it still yields a non-empty server-rendered results set for English reports.
8. Re-download representative governance-report PDFs and confirm `pdftotext` still extracts readable text.
9. Confirm the bounded JPX detail-page lane still yields clean-English representative people only when JPX exposes them, and still ignores local-script-only representative values cleanly.
10. Rebuild the normalized Japan bundle and generate the runtime pack.
11. Rebuild or update the target registry and perform a clean install smoke test before promotion.
12. Re-review EDINET annual securities reports and broader governance filings so the next Japan expansion lane stays documented and ready.

### Japan-specific checks before promotion

Each Japan refresh should explicitly confirm:

- the JPX availability page still resolves
- the page still exposes a versioned workbook link
- the listed-company search still yields a non-empty current roster
- representative listed-company detail pages still resolve from JPX search-result search codes
- the workbook still includes a non-empty issuer slice
- representative Prime and Growth issuers still parse cleanly from search results, bounded detail-page enrichment, and workbook overlay
- issuer and ticker outputs stay aligned by JPX company code
- representative detail-enriched issuers still carry expected ISIN, listing-date, and representative metadata
- representative governance search results still parse cleanly by JPX company code
- at least one representative executive and one investor-relations contact still extract from governance-report text
- if JPX still exposes clean English representative names on the bounded detail-page subset, those representatives still derive as people and remain issuer-linked
- representative exact matches still work for at least two or three Japan issuer names and codes
- the static Japan entities for `FSA`, `EDINET`, `JPX`, `TSE`, and `Nikkei 225` are still present

## Keeping `finance-uk-en` Up To Date

`finance-uk-en` is currently a partially automated United Kingdom pack. Its current automated scope is listed-company issuers derived from the official FCA Official List live export plus the rendered `FTSE AIM All-Share` constituents table, matched current Companies House company numbers for those issuers, current active Companies House officers, active PSC people, bounded PSC shareholder organizations, deterministic issuer and ticker Wikidata enrichment keyed by official ISIN and London Stock Exchange ticker claims, and baseline static FCA, Official List, RNS, LSE, AIM, FTSE 100, and FTSE AIM All-Share entities. It is not a full employee directory.

The pack-specific refresh guide is also tracked in [finance_uk_en_update_guide.md](./finance_uk_en_update_guide.md).

### Current United Kingdom sources used in the build

The United Kingdom pack is currently anchored to these official source lanes:

- FCA landing:
  `https://www.fca.org.uk/`
- FCA Official List live export:
  `https://ukla.file.force.com/sfc/dist/version/download/?asPdf=false&d=%2Fa%2FTw000004MSPy%2FhR2bKHHyeRlkTruiX.5JyyhDqwO_HxCHSWxpMQHJ5yo&ids=068Tw00000iMBlA&oid=00Dw0000000nVzW`
- RNS landing:
  `https://www.lseg.com/en/capital-markets/regulatory-news-service`
- London Stock Exchange landing:
  `https://www.londonstockexchange.com/`
- FTSE AIM All-Share constituents table:
  `https://www.londonstockexchange.com/indices/ftse-aim-all-share/constituents/table`
- Companies House register guidance:
  `https://www.gov.uk/guidance/search-the-companies-house-register`
- Resolved Companies House search result pages discovered from listed-issuer names
- Resolved Companies House officer pages discovered from matched company numbers
- Resolved Companies House persons-with-significant-control pages discovered from matched company numbers
- Wikidata entity resolution for static market entities plus deterministic issuer and ticker QIDs:
  `https://www.wikidata.org/`

Current derivation uses the FCA Official List commercial-company slice plus the rendered FTSE AIM All-Share roster as the issuer source of truth, then resolves each matched issuer against official Companies House search results and extracts active current officers, active PSC people, and bounded PSC shareholder organizations from official Companies House pages where the match is clean. After that deterministic extraction, the current UK lane adds bounded Wikidata slugs only where official ISIN or London Stock Exchange ticker claims resolve cleanly. That makes the current pack a strong credible partial rather than a finished United Kingdom people pack.

### What must be refreshed each time

To keep `finance-uk-en` current:

1. Pull a fresh United Kingdom snapshot with `ades registry fetch-finance-country-sources --country-code uk`.
2. Confirm the FCA Official List live export still downloads and still parses into a non-empty commercial-company issuer slice.
3. Confirm the rendered FTSE AIM All-Share page still yields a non-empty AIM issuer and ticker slice.
4. Re-run Companies House resolution and confirm representative issuers still map cleanly to current company numbers.
5. Re-run the officer and PSC extraction and confirm the derived United Kingdom people plus shareholder-organization slices are non-empty.
6. Re-run bounded Wikidata enrichment and confirm the builder still writes a non-empty `wikidata-united-kingdom-entity-resolution.json` snapshot.
7. Rebuild the normalized United Kingdom bundle, generate the runtime pack, and update the target registry.
8. Perform a clean install smoke test before promotion.
9. Keep the next RNS, annual-report, and FCA register expansion lanes documented rather than silently assumed.

### United Kingdom-specific checks before promotion

Each United Kingdom refresh should explicitly confirm:

- the FCA Official List live export still resolves
- the export still yields non-empty commercial-company rows
- the rendered FTSE AIM All-Share page still exposes a non-empty issuer table and stable pagination
- representative issuers still carry expected listing category, market status, trading venue, and ISIN metadata
- Companies House search resolution still returns current company numbers for representative listed issuers
- active Companies House officers still parse from representative issuer officer pages
- active Companies House PSC pages still yield expected controller rows, regulated-market exemptions, or no-PSC states for representative companies
- representative exact matches still work for issuers such as `ASOS plc`, `Aston Martin Lagonda Global Holdings PLC`, and `Deltic Energy PLC`
- representative people matches still work for officers and controllers such as `Aaron Izzard`, `Natalie Dame Massenet`, and `Roger Mainwaring`
- representative deterministic Wikidata matches still work for issuers or tickers such as `ASOS plc`, `Aston Martin Lagonda Global Holdings PLC`, or `Deltic Energy PLC`
- the static United Kingdom entities for `FCA`, `Official List`, `RNS`, `LSE`, `AIM`, `FTSE 100`, and `FTSE AIM All-Share` are still present

## Keeping `finance-fr-en` Up To Date

`finance-fr-en` is currently a partially automated France pack. It covers official Paris public-market issuers and tickers across the regulated market, Euronext Growth Paris, and Euronext Access Paris, derives public people such as directors, executive officers, investor-relations contacts, and press contacts from official Euronext regulated news, linked HTML/XHTML filing documents, plus issuer governance pages, and now carries explicit Wikidata metadata on the baseline France regulators, exchanges, and CAC 40. It is not a full employee directory.

The pack-specific refresh guide is also tracked in [finance_fr_en_update_guide.md](./finance_fr_en_update_guide.md).

### Current France sources used in the build

The France pack currently uses these official source lanes:

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
- Euronext press-release modal endpoint discovered from company-news rows:
  `https://live.euronext.com/ajax/node/company-press-release/{node_nid}`
- Official DILA regulated-information archive used when issuer filing links resolve there:
  `https://www.info-financiere.fr/`
- HTML or XHTML annual reports and universal registration documents discovered from official regulated-news and issuer-finance links
- Issuer finance and governance pages discovered from official regulated-news disclosures
- Wikidata API for issuer and public-person QID plus alias enrichment:
  `https://www.wikidata.org/w/api.php`

### What must be refreshed each time

To keep `finance-fr-en` current:

1. Pull a fresh France snapshot with `ades registry fetch-finance-country-sources --country-code fr`.
2. Re-render the three Euronext Paris list pages and confirm each one still yields non-empty issuer rows.
3. Re-fetch the latest and archive company-news pages and confirm they still expose non-empty `data-node-nid` issuer rows.
4. Verify the Euronext press-release modal endpoint still returns issuer identity, public contacts, and outbound issuer finance-site links.
5. Verify discovered filing-document links still resolve to HTML or XHTML annual reports / universal registration documents for a representative issuer sample.
6. Verify discovered issuer finance sites still resolve public governance pages for a representative issuer sample.
7. Re-run the France Wikidata resolution pass and confirm the builder still writes a non-empty `wikidata-france-entity-resolution.json` snapshot.
8. Rebuild the normalized France bundle, generate the runtime pack, and update the target registry.
9. Perform a clean install smoke test before promotion.
10. Re-review the AMF issuer/disclosure surface so the France filing lane stays documented.

### France-specific checks before promotion

Each France refresh should explicitly confirm:

- headless Chrome is available for `live.euronext.com` rendering
- the regulated, Growth, and Access pages still render usable table rows
- each segment yields a non-empty issuer/ticker slice
- dedupe by `ISIN` still keeps the France issuer universe stable
- the latest and archive company-news pages still expose non-empty company-news rows
- the press-release modal endpoint still returns issuer `ISIN` plus public contact blocks
- filing-document discovery still resolves at least one HTML or XHTML annual-report / URD document from an official disclosure flow
- governance discovery still resolves at least one public board/executive page from a representative issuer finance site
- the France Wikidata resolution snapshot is non-empty and representative issuer/person QIDs still attach cleanly
- representative exact matches still work for one regulated, one Growth, and one Access issuer
- representative people matches still work for one contact person, one filing-derived director or executive, and one governance-derived director or executive

## Keeping `finance-de-en` Up To Date

`finance-de-en` is currently a partially automated Germany pack. Its intended scope is public-market issuer entities plus public people linked to those issuers, especially management board members, supervisory board members, issuer-facing contacts, bounded major-shareholder disclosures, and BaFin-backed regulated institution or Germany branch entities. It is not a full employee directory.

The pack-specific refresh guide is also tracked in [finance_de_en_update_guide.md](./finance_de_en_update_guide.md).

### Current Germany sources used in the build

The Germany pack is currently derived from these official source lanes:

- Börse Stuttgart exchange baseline:
  `https://www.boerse-stuttgart.de/`
- Börse Berlin exchange baseline:
  `https://www.boerse-berlin.com/`
- Börse Hamburg exchange baseline:
  `https://www.boerse-hamburg.de/`
- Börse Hannover exchange baseline:
  `https://www.boerse-hannover.de/`
- Deutsche Börse listed companies workbook seed:
  `https://www.cashmarket.deutsche-boerse.com/cash-en/Data-Tech/statistics/listed-companies`
- Börse München `m:access` issuer table:
  `https://www.boerse-muenchen.de/maccess/gelistete-unternehmen`
- Börse Düsseldorf Primärmarkt issuer table:
  `https://www.boerse-duesseldorf.de/aktien-primaermarkt/`
- Tradegate order-book pages for ISIN-based ticker and WKN backfill:
  `https://www.tradegate.de/orderbuch.php?isin={isin}&lang=en`
- Deutsche Börse issuer company-details pages for board, executive, contact, major-shareholder extraction, and bounded shareholder-entity promotion:
  `https://live.deutsche-boerse.com/equity/{isin}/company-details`
- Unternehmensregister register-information search and detail pages for register metadata and board/governance names:
  `https://www.unternehmensregister.de/en/search/register-information`
- BaFin company-database CSV export for Germany-domiciled regulated institutions and explicit Germany branch entities:
  `https://portal.mvp.bafin.de/database/InstInfo/sucheForm.do?6578706f7274=1&sucheButtonInstitut=Suche&institutName=&d-4012550-e=1`
- Wikidata entity resolution for issuer, ticker, person, and bounded organization QIDs:
  `https://www.wikidata.org/w/api.php`
- Wikidata SPARQL for deterministic ISIN/LEI claim matching and bounded organization-linked public people:
  `https://query.wikidata.org/`

### What must be refreshed each time

To keep `finance-de-en` current, each Germany refresh should do all of the following:

1. Pull a fresh Germany snapshot with `ades registry fetch-finance-country-sources --country-code de`.
2. Re-seed the issuer universe from the latest Deutsche Börse workbook, then extend it with the latest Börse München `m:access` and Börse Düsseldorf Primärmarkt issuer lists.
3. Re-fetch the BaFin company-database export and merge Germany-domiciled institutions plus explicit Germany branch entities into the organization set.
4. Re-run Tradegate ISIN lookups to fill missing ticker and WKN identifiers for issuers not fully covered by the seed workbook.
5. Re-fetch Deutsche Börse company-details pages and Unternehmensregister register-information pages so board, executive, contact, major-shareholder, former-name, and register metadata stay current.
6. Re-run the shareholder organization/person split so official holder widgets continue to produce explicit organization entities while pseudo-holders such as free float or treasury shares stay suppressed.
7. Re-run bounded Germany Wikidata enrichment so deterministic ISIN and LEI claim resolution remain attached to the latest entity set, then rebuild the bounded organization-linked public-people overlay for the organizations that resolved cleanly.
8. Rebuild the normalized Germany bundle, generate the runtime pack, compose a static registry, and smoke-test clean pull plus tagging before promotion.

### Recommended refresh loop

Use the country-pack pipeline rather than hand-editing generated pack files:

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

After pack generation, build or update a static registry, then verify a clean consumer install from that registry before promotion.

### Germany-specific checks before promotion

Because Germany is still `partial` automation, each refresh should explicitly review these conditions:

- the Deutsche Börse workbook still downloads and parses into the expected issuer seed set
- the BaFin export still downloads as CSV and produces a non-empty Germany institution slice plus explicit Germany branch entities where branch names are present
- headless Chrome is available for `live.deutsche-boerse.com` company-details rendering
- representative company-details pages still expose populated contact and major-shareholder widgets where the issuer publishes them
- representative company-details pages still promote explicit major-shareholder organizations, and generic pseudo-holders such as free float or treasury shares do not leak in as entities
- Unternehmensregister requests are not timing out broadly enough to collapse the people lane
- Wikidata resolution produced a real snapshot instead of an empty or warning-only result, representative issuer or regulated-institution organization QIDs still survive the current bounded refresh, and the bounded organization-linked public-people lane still adds real people where matched QIDs exist
- spot checks still work for both issuers and people across the main German listing lanes

Recommended spot checks:

- one Frankfurt/Xetra issuer
- one Börse München `m:access` issuer
- one Börse Düsseldorf Primärmarkt issuer
- one BaFin-only regulated institution
- one known board member from a large issuer

### Current quality bar

`finance-de-en` is good enough to ship, but it should still be treated as an actively maintained pack rather than a finished one. The refreshed `2026-04-22` artifact now stands at `9466` total entities with `4773` organizations, `428` tickers, `4255` people, `9` exchanges, and `1` market index. That includes materially broader Wikidata coverage: `1250` organization entities, `322` tickers, and `290` people now carry `wikidata_slug`, and the bounded organization-linked Wikidata lane added `245` net new Germany people.

The highest-value next improvements are:

- improve exact issuer alias coverage for some large German companies
- harden the Unternehmensregister lane against slow or unstable responses
- broaden the bounded organization-linked Wikidata public-people lane carefully, only where additional properties keep precision high
- extend BaFin coverage beyond institution metadata into public principal / officer relationships where official sources make that durable
- keep the Germany people lane focused on public officers, cleaned shareholder people, and board members rather than trying to model all employees

## Keeping `finance-us-en` Up To Date

`finance-us-en` is currently a partially automated United States pack. Its current automated scope is the broad SEC issuer and ticker universe derived from `company_tickers.json`, bounded issuer and ticker Wikidata enrichment keyed by official SEC `CIK` claims, plus a broader SEC bulk-submissions people lane built from up to two recent DEF `14A` or `DEFA14A` proxy filings and multiple recent Forms `3`/`4`/`5` insider filings per issuer, alongside baseline regulator, filing-system, exchange, and market-index entities. It is not a full employee directory.

The pack-specific refresh guide is also tracked in [finance_us_en_update_guide.md](./finance_us_en_update_guide.md).

### Current United States sources used in the build

The United States pack is currently anchored to these official source lanes:

- SEC landing:
  `https://www.sec.gov/`
- EDGAR filings landing:
  `https://www.sec.gov/search-filings`
- SEC `company_tickers.json` bulk issuer universe:
  `https://www.sec.gov/files/company_tickers.json`
- SEC bulk `submissions.zip` archive:
  `https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip`
- Wikidata SPARQL / entity APIs:
  `https://query.wikidata.org/sparql`
  `https://www.wikidata.org/w/api.php`
- Nasdaq exchange landing:
  `https://www.nasdaq.com/`
- NYSE exchange landing:
  `https://www.nyse.com/`

Current derivation uses the full SEC company-ticker universe for issuer and ticker coverage, then adds bounded Wikidata QIDs where that SEC `CIK` is published in Wikidata claims. The current people lane uses the same SEC bulk submissions archive for a bounded recent-proxy comparison plus bounded multi-filing insider coverage. That makes the pack materially better than the old six-entity baseline, but it is still `partial` and should not be described as full United States people coverage.

### What must be refreshed each time

To keep `finance-us-en` current:

1. Pull a fresh United States snapshot with `ades registry fetch-finance-country-sources --country-code us`.
2. Confirm `company_tickers.json` still downloads and still exposes a non-empty current issuer universe.
3. Confirm `submissions.zip` still downloads and still opens as a valid SEC bulk archive.
4. Re-run issuer and ticker derivation from the full SEC company universe.
5. Re-run bounded United States Wikidata enrichment so the canonical snapshot writes the current `wikidata-united-states-entity-resolution.json` sidecar and carries fresh `wikidata_slug` metadata onto matched issuers and tickers.
6. Re-run SEC people derivation from the submissions archive so the canonical snapshot reflects both the bounded recent-proxy lane and the bounded multi-filing insider lane. If the refresh has to recover earlier stronger local work, merge that result into the canonical country snapshot before bundle build.
7. Rebuild the normalized United States bundle, generate the runtime pack, and update the target reviewed or prod-local registry.
8. Perform a clean install smoke test before promotion.
9. Keep the next expansion lanes for `8-K` item `5.02`, Schedules `13D`/`13G`, `IAPD`, and `FDIC` documented rather than silently assumed.

### United States-specific checks before promotion

Each United States refresh should explicitly confirm:

- the generated `finance-us-en` pack is no longer at the old six-entity baseline
- the reviewed and prod-local `build.json` now reflect the refreshed `generated_at`, entity count, and source count
- the issuer slice still contains representative large issuers such as `Broadcom Inc.`
- representative issuers and aligned tickers now carry `wikidata_slug` when the SEC `CIK` resolves cleanly in the current bounded Wikidata lane
- the people slice still contains representative public people such as `Harry L. You`
- issuer and ticker outputs still stay aligned by `CIK` and issuer name
- the current Wikidata lane is still described honestly as issuer-and-ticker-only rather than broad United States people resolution
- the current people lane is described honestly as proxy-plus-bounded-insider coverage rather than a general employee directory
- the next official-source expansion lanes remain documented and ready

## Consumer Smoke

The published generated release is not review-only anymore. A clean local runtime can already consume it directly:

- `ades pull finance-en --registry-url https://ades.fsn1.your-objectstorage.com/generated-pack-releases/finance-general-medical-2026-04-10/index.json`
- `ades pull medical-en --registry-url https://ades.fsn1.your-objectstorage.com/generated-pack-releases/finance-general-medical-2026-04-10/index.json`

That proof is now shipped as a durable validation surface:

- Python: `ades.smoke_test_published_generated_registry(...)`
- CLI: `ades registry smoke-published-release <registry-url>`
- HTTP: `POST /v0/registry/smoke-published-release`

The automated smoke uses clean temporary storage roots, pulls from the published registry URL itself, verifies dependency installation, and tags deterministic finance and medical smoke texts against the consumed packs. The `medical-en` smoke also installs the generated `general-en` dependency and successfully tags the expected medical entities from the reviewed published release.

Important:

- keep generated-pack publication additive to the existing deployment model
- do not bypass the current server-owned registry release path without an explicit promotion decision
- do not store raw source dumps in the repo

## Promotion Rule

The current production deployment flow remains authoritative for the live root registry. Generated pack refreshes produce a reviewed static-registry payload; promotion to the live hosted registry now happens by committing the small promotion spec and letting the existing deploy workflow materialize that reviewed release into `dist/registry`. Before promotion, the published registry URL must pass the shipped clean-consumer smoke so the reviewed object-storage release proves real pull/install/tag behavior instead of only looking structurally correct.
