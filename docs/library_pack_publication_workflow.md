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

## Keeping `finance-de-en` Up To Date

`finance-de-en` is currently a partially automated Germany pack. Its intended scope is public-market issuer entities plus public people linked to those issuers, especially management board members, supervisory board members, and issuer-facing contacts. It is not a full employee directory.

### Current Germany sources used in the build

The Germany pack is currently derived from these official source lanes:

- Deutsche Börse listed companies workbook seed:
  `https://www.cashmarket.deutsche-boerse.com/cash-en/Data-Tech/statistics/listed-companies`
- Börse München `m:access` issuer table:
  `https://www.boerse-muenchen.de/maccess/gelistete-unternehmen`
- Börse Düsseldorf Primärmarkt issuer table:
  `https://www.boerse-duesseldorf.de/aktien-primaermarkt/`
- Tradegate order-book pages for ISIN-based ticker and WKN backfill:
  `https://www.tradegate.de/orderbuch.php?isin={isin}&lang=en`
- Deutsche Börse issuer company-details pages for board and executive extraction:
  `https://live.deutsche-boerse.com/equity/{isin}/company-details`
- Unternehmensregister register-information search and detail pages for register metadata and board/governance names:
  `https://www.unternehmensregister.de/en/search/register-information`
- BaFin company-database CSV export for Germany-domiciled regulated institutions and Germany branches:
  `https://portal.mvp.bafin.de/database/InstInfo/sucheForm.do?6578706f7274=1&sucheButtonInstitut=Suche&institutName=&d-4012550-e=1`
- Wikidata entity resolution for issuer and person QIDs:
  `https://www.wikidata.org/w/api.php`

### What must be refreshed each time

To keep `finance-de-en` current, each Germany refresh should do all of the following:

1. Pull a fresh Germany snapshot with `ades registry fetch-finance-country-sources --country-code de`.
2. Re-seed the issuer universe from the latest Deutsche Börse workbook, then extend it with the latest Börse München `m:access` and Börse Düsseldorf Primärmarkt issuer lists.
3. Re-fetch the BaFin company-database export and merge Germany-domiciled institutions plus Germany branches into the issuer set.
4. Re-run Tradegate ISIN lookups to fill missing ticker and WKN identifiers for issuers not fully covered by the seed workbook.
5. Re-fetch Deutsche Börse company-details pages and Unternehmensregister register-information pages so board, executive, former-name, and register metadata stay current.
6. Re-run Wikidata resolution so issuer and person QIDs remain attached to the latest entity set.
7. Rebuild the normalized Germany bundle, generate the runtime pack, compose a static registry, and smoke-test clean pull plus tagging before promotion.

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
- the BaFin export still downloads as CSV and produces a non-empty Germany institution slice
- headless Chrome is available for `live.deutsche-boerse.com` company-details rendering
- Unternehmensregister requests are not timing out broadly enough to collapse the people lane
- Wikidata resolution produced a real snapshot instead of an empty or warning-only result
- spot checks still work for both issuers and people across the main German listing lanes

Recommended spot checks:

- one Frankfurt/Xetra issuer
- one Börse München `m:access` issuer
- one Börse Düsseldorf Primärmarkt issuer
- one BaFin-only regulated institution
- one known board member from a large issuer

### Current quality bar

`finance-de-en` is good enough to ship, but it should still be treated as an actively maintained pack rather than a finished one. The highest-value next improvements are:

- improve exact issuer alias coverage for some large German companies
- harden the Unternehmensregister lane against slow or unstable responses
- extend BaFin coverage beyond institution metadata into public principal / officer relationships where official sources make that durable
- keep the Germany people lane focused on public officers and board members rather than trying to model all employees

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
