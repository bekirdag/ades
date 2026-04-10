# Library Pack Publication Workflow

This document defines the first durable publication workflow for real-content library packs in `ades`.

## Goal

Take one or more normalized bundle directories, run the deterministic report and quality gates, and emit a publishable static registry release without changing the runtime pack format or bypassing the existing deployment/object-storage path.

## Refresh Surface

The publication orchestration surface is:

- Python: `ades.refresh_generated_packs(...)`
- CLI: `ades registry refresh-generated-packs <bundle-dir...> --output-dir <dir>`
- HTTP: `POST /v0/registry/refresh-generated-packs`

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

Finance-only refreshes can pass a single bundle directory. When `--max-ambiguous-aliases` is omitted, refresh now applies pack-specific defaults automatically: `finance-en=300`, `medical-en=25`, and `general-en=0`. The first real-data finance snapshot on `2026-04-10` validated cleanly on fixture recall/precision with `ambiguous_alias_count=256`, and the first real-data medical snapshot on `2026-04-10` now validates cleanly with `ambiguous_alias_count=22` after pruning compact symbolic disease/protein aliases. Medical-only refreshes must still pass the matching `general-en` bundle or `--general-bundle-dir`.

## Object Storage Publication

When the refresh run returns `passed=true`, the generated `registry/` directory is ready for publication.

Preferred pattern:

1. Sync the approved release to an immutable object-storage prefix.
2. Review the uploaded `index.json`, manifests, and artifacts.
3. Promote the approved registry through the existing hosted path.

Example sync to the configured S3-compatible bucket:

```bash
aws s3 sync \
  /mnt/githubActions/ades_big_data/pack_releases/2026-04-10-general-medical/registry \
  s3://$ADES_PACK_OBJECT_STORAGE_BUCKET/generated-pack-releases/2026-04-10-general-medical/ \
  --delete \
  --endpoint-url "https://$ADES_PACK_OBJECT_STORAGE_ENDPOINT"
```

Important:

- keep generated-pack publication additive to the existing deployment model
- do not bypass the current server-owned registry release path without an explicit promotion decision
- do not store raw source dumps in the repo

## Promotion Rule

The current production deployment flow remains authoritative for the live root registry. Generated pack refreshes produce a reviewed static-registry payload; promotion to the live hosted registry should happen through the existing deploy/object-storage path, not through an ad hoc side channel.
