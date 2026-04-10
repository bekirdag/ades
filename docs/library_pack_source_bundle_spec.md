# Library Pack Source Bundle Spec

This document defines the first executable offline input contract for turning downloaded library data into `ades` pack directories without changing the current runtime or registry format.

## Goal

One normalized source bundle should deterministically produce one runtime-compatible pack directory containing:

- `manifest.json`
- `labels.json`
- `aliases.json`
- `rules.json`
- optional sidecar provenance files such as `sources.json` and `build.json`

The generated pack must work with the existing `ades` registry builder, installer, CLI, API, and localhost service with no runtime schema changes.

## Bundle Layout

```text
bundle-root/
  bundle.json
  sources.lock.json
  normalized/
    entities.jsonl
    rules.jsonl
```

Rules:

- `bundle.json` is required.
- `sources.lock.json` is emitted by the bundle builders and is required for deterministic rebuild/audit output once a bundle has been built.
- `normalized/entities.jsonl` is required.
- `normalized/rules.jsonl` is optional.
- Large raw downloads stay outside the repo, normally under `/mnt/githubActions/ades_big_data/`.
- The bundle is the normalized handoff boundary between raw source adapters and the pack generator.

## `bundle.json`

Required fields:

- `schema_version`: integer, currently `1`
- `pack_id`: target pack id such as `finance-en`
- `language`: target language code such as `en`
- `domain`: target domain such as `finance`
- `tier`: pack tier such as `base` or `domain`

Optional fields:

- `version`: default generated pack version when the caller does not override it
- `description`: manifest description
- `tags`: manifest tags
- `dependencies`: pack dependencies
- `min_ades_version`: minimum supported runtime version, default `0.1.0`
- `entities_path`: relative path to the normalized entity JSONL file, default `normalized/entities.jsonl`
- `rules_path`: relative path to the normalized rule JSONL file, default `normalized/rules.jsonl`
- `label_mappings`: map raw source types to runtime labels
- `stoplisted_aliases`: aliases to drop after normalization
- `allowed_ambiguous_aliases`: aliases allowed to survive across multiple labels
- `sources`: upstream snapshot descriptors used to populate `sources.json`

Builder-emitted sidecar after bundle creation:

- `sources.lock.json`: deterministic raw-snapshot lock metadata with exact snapshot URIs, SHA-256 digests, byte sizes, adapter identifiers, adapter versions, and recorded snapshot timestamps

Example:

```json
{
  "schema_version": 1,
  "pack_id": "finance-en",
  "version": "0.2.0",
  "language": "en",
  "domain": "finance",
  "tier": "domain",
  "description": "Generated English finance entities and deterministic rules.",
  "tags": ["finance", "generated", "english"],
  "dependencies": [],
  "min_ades_version": "0.1.0",
  "label_mappings": {
    "issuer": "organization",
    "ticker": "ticker",
    "exchange": "exchange"
  },
  "stoplisted_aliases": ["N/A"],
  "allowed_ambiguous_aliases": [],
  "sources": [
    {
      "name": "sec-companyfacts",
      "snapshot_uri": "s3://ades-bundles/finance/sec/companyfacts-2026-04-10.jsonl",
      "license_class": "ship-now",
      "license": "SEC public company_tickers.json",
      "retrieved_at": "2026-04-10T09:00:00Z",
      "record_count": 123456
    }
  ]
}
```

## Generator Behavior

The generator reads the bundle and writes one pack directory under the requested output root:

```text
generated-root/
  finance-en/
    manifest.json
    labels.json
    aliases.json
    rules.json
    sources.json
    build.json
```

Normalization performed during generation:

- Unicode NFKC normalization
- whitespace collapse and trimming
- quote and dash normalization
- alias deduplication by case-insensitive key
- optional organization-suffix stripping for organization-like labels
- stoplist filtering
- ambiguous-alias dropping unless explicitly allowed
- optional exclusion of records marked `build_only`

Source-governance rules:

- Every `sources[]` entry must declare `license_class`.
- Supported classes are `ship-now`, `build-only`, and `blocked-pending-license-review`.
- `generate-pack` and `report-pack` remain usable for local inspection even when restricted sources are present.
- `refresh-generated-packs` treats the refresh as publication-oriented and will skip the final `registry/` build unless every source is `ship-now`.
- Bundles with no declared `sources[]` entries are treated as non-publishable.

## CLI / API / Service

Public surfaces for the first generator slice:

- Python: `ades.generate_pack_source(...)`
- Python: `ades.report_generated_pack(...)`
- Python: `ades.refresh_generated_packs(...)`
- Python: `ades.prepare_registry_deploy_release(...)`
- Python: `ades.publish_generated_registry_release(...)`
- CLI: `ades registry generate-pack <bundle-dir> --output-dir <dir>`
- CLI: `ades registry report-pack <bundle-dir> --output-dir <dir>`
- CLI: `ades registry refresh-generated-packs <bundle-dir...> --output-dir <dir>`
- CLI: `ades registry prepare-deploy-release --output-dir <dir>`
- CLI: `ades registry publish-generated-release <registry-dir> --prefix <object-storage-prefix>`
- HTTP: `POST /v0/registry/generate-pack`
- HTTP: `POST /v0/registry/report-pack`
- HTTP: `POST /v0/registry/refresh-generated-packs`
- HTTP: `POST /v0/registry/prepare-deploy-release`
- HTTP: `POST /v0/registry/publish-generated-release`

Finance raw-source fetch surface:

- Python: `ades.fetch_finance_source_snapshot(...)`
- CLI: `ades registry fetch-finance-sources --output-dir <dir> [--snapshot YYYY-MM-DD]`
- HTTP: `POST /v0/registry/fetch-finance-sources`

The finance fetch surface downloads the real SEC and Nasdaq source snapshots into an immutable dated directory under `/mnt/githubActions/ades_big_data/pack_sources/raw/finance-en/<snapshot>/`, writes repo-owned curated exchange aliases alongside them, and records exact fetch metadata in `sources.fetch.json`.

General raw-source fetch surface:

- Python: `ades.fetch_general_source_snapshot(...)`
- CLI: `ades registry fetch-general-sources --output-dir <dir> [--snapshot YYYY-MM-DD]`
- HTTP: `POST /v0/registry/fetch-general-sources`

The general fetch surface downloads a bounded live Wikidata entity seed set plus filtered GeoNames place data into an immutable dated directory under `/mnt/githubActions/ades_big_data/pack_sources/raw/general-en/<snapshot>/`, writes repo-owned curated aliases alongside them, and records exact fetch metadata in `sources.fetch.json`.

Generated registry publication surface:

- Python: `ades.publish_generated_registry_release(...)`
- CLI: `ades registry publish-generated-release <registry-dir> --prefix <object-storage-prefix>`
- HTTP: `POST /v0/registry/publish-generated-release`

The publication surface syncs a reviewed `registry/` directory to the configured S3-compatible bucket using the existing `ADES_PACK_OBJECT_STORAGE_*` environment variables, then returns the published `s3://...` prefix plus a stable object listing for auditability. The first fully real-source three-pack rehearsal is now published at `s3://ades/generated-pack-releases/finance-general-medical-2026-04-10/` with `22` reviewed objects.
The publication response now also returns direct HTTPS registry candidates for immediate consumers; the preferred live URL is `https://ades.fsn1.your-objectstorage.com/generated-pack-releases/finance-general-medical-2026-04-10/index.json`.

Finance-first raw snapshot builder surfaces:

- Python: `ades.build_finance_source_bundle(...)`
- CLI: `ades registry build-finance-bundle --sec-company-tickers <path> --symbol-directory <path> --curated-entities <path> --output-dir <dir>`
- HTTP: `POST /v0/registry/build-finance-bundle`

General-first raw snapshot builder surfaces:

- Python: `ades.build_general_source_bundle(...)`
- CLI: `ades registry build-general-bundle --wikidata-entities <path> --geonames-places <path> --curated-entities <path> --output-dir <dir>`
- HTTP: `POST /v0/registry/build-general-bundle`

Medical-first raw snapshot builder surfaces:

- Python: `ades.build_medical_source_bundle(...)`
- CLI: `ades registry build-medical-bundle --disease-ontology <path> --hgnc-genes <path> --uniprot-proteins <path> --clinical-trials <path> --curated-entities <path> --output-dir <dir>`
- HTTP: `POST /v0/registry/build-medical-bundle`

Builder response contract additions:

- every `build-*-bundle` surface now returns `sources_lock_path`
- the emitted `sources.lock.json` always includes:
  - `pack_id`
  - `version`
  - `bundle_manifest_path`
  - `entities_path`
  - `rules_path`
  - `source_count`
  - per-source `snapshot_uri`, `snapshot_sha256`, `snapshot_size_bytes`, `license_class`, `license`, `retrieved_at`, `record_count`, `adapter`, `adapter_version`, and original `notes`

Finance quality-gate surfaces:

- Python: `ades.validate_finance_pack_quality(...)`
- CLI: `ades registry validate-finance-quality <bundle-dir> --output-dir <dir>`
- HTTP: `POST /v0/registry/validate-finance-quality`

General quality-gate surfaces:

- Python: `ades.validate_general_pack_quality(...)`
- CLI: `ades registry validate-general-quality <bundle-dir> --output-dir <dir>`
- HTTP: `POST /v0/registry/validate-general-quality`

General live-data note:

- The first dated live `general-en` snapshot on `2026-04-10` validated cleanly at `expected_recall=1.0` and `precision=1.0` with `alias_count=96` and `ambiguous_alias_count=0`.
- The initial live general slice intentionally uses a bounded real-source Wikidata seed set plus filtered GeoNames seed rows so the dependency pack stays zero-ambiguity compatible while the broader general-source expansion remains future work.

Medical quality-gate surfaces:

- Python: `ades.validate_medical_pack_quality(...)`
- CLI: `ades registry validate-medical-quality <bundle-dir> --general-bundle-dir <dir> --output-dir <dir>`
- HTTP: `POST /v0/registry/validate-medical-quality`

Medical live-data note:

- The first dated live `medical-en` snapshot on `2026-04-10` validated cleanly at `expected_recall=1.0` and `precision=1.0` with `ambiguous_alias_count=22`.
- `medical-en` now prunes compact symbolic disease/protein aliases such as terse uppercase or digit-bearing abbreviations before generation, and the default medical ambiguity budget is `25`.

Release-oriented refresh flow:

1. Build normalized bundles from raw snapshots with the `build-*-bundle` commands.
   - For `finance-en`, the real-source path can start with `ades registry fetch-finance-sources`.
   - For `general-en`, the real-source path can start with `ades registry fetch-general-sources`.
2. Run `ades registry refresh-generated-packs <bundle-dir...> --output-dir <dir>`.
3. Review the emitted `reports/` and `quality/` output under the chosen refresh directory.
4. If `passed=true`, publish the generated `registry/` directory through the existing static-registry host path or an approved object-storage sync.
5. Run clean consumer smoke against the published registry URL before promotion.
6. If the reviewed release is approved, write `src/ades/resources/registry/promoted-release.json` with the reviewed registry URL and desired smoke pack ids.
7. Let `ades registry prepare-deploy-release --output-dir <dir>` materialize the reviewed release into the deploy-owned local registry layout through the existing CI/CD flow.
8. Pull and tag with the resulting hosted registry as usual.

`refresh-generated-packs` is the orchestration surface for the publish/refresh phase. It reuses the existing report and quality gates, then only builds `registry/` when every requested pack passes validation. When `max_ambiguous_aliases` is omitted, refresh uses pack-specific defaults instead of one global override: `general-en=0`, `medical-en=25`, and `finance-en=300`. Medical refreshes auto-resolve `general-en` when that bundle is included in the same request, or they accept `--general-bundle-dir` / `general_bundle_dir` when it is external to the bundle list. The first fully real-source combined `general-en` + `medical-en` refresh release now lives under `/mnt/githubActions/ades_big_data/pack_releases/general-medical-2026-04-10`.

Published-registry consumer-smoke surfaces:

- Python: `ades.smoke_test_published_generated_registry(...)`
- CLI: `ades registry smoke-published-release <registry-url>`
- HTTP: `POST /v0/registry/smoke-published-release`

Deploy-preparation promotion surfaces:

- Python: `ades.prepare_registry_deploy_release(...)`
- CLI: `ades registry prepare-deploy-release --output-dir <dir>`
- HTTP: `POST /v0/registry/prepare-deploy-release`
