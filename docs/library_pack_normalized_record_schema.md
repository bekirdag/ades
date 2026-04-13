# Library Pack Normalized Record Schema

This document defines the canonical intermediate records that the first offline pack generator consumes.

## Purpose

Raw upstream downloads come in mixed formats such as CSV, TSV, XML, RDF, JSON, or API dumps. Source adapters must convert them into deterministic JSONL records before pack generation.

The JSONL boundary keeps three concerns separate:

- raw download and license handling
- source-specific normalization
- runtime pack emission

## `normalized/entities.jsonl`

One JSON object per line.

Required logical fields:

- `canonical_text`: canonical display text for the entity
- one of `label`, `entity_type`, or `type`: runtime label or a raw type that can be mapped through `bundle.json.label_mappings`

Supported optional fields:

- `entity_id`: stable source-side identifier
- `aliases`: list of alias strings
- `build_only`: boolean flag for records that should stay out of default runtime packs
- `source_name`: upstream dataset name
- `source_id`: upstream record identifier
- `metadata`: free-form adapter metadata retained only in the normalized stage

Example:

```json
{"entity_id":"issuer:issuer-alpha","entity_type":"issuer","canonical_text":"Issuer Alpha Holdings","aliases":["Issuer Alpha","Issuer Alpha Common"],"source_name":"finance-source-a"}
{"entity_id":"ticker:ticka","entity_type":"ticker","canonical_text":"TICKA","aliases":[],"source_name":"finance-source-b"}
{"entity_id":"exchange:exchx","entity_type":"exchange","canonical_text":"EXCHX","aliases":["Exchange Alpha"],"source_name":"exchange-source"}
{"entity_id":"private:internal","entity_type":"issuer","canonical_text":"Internal Holdings LLC","build_only":true}
```

Generator output mapping:

- `canonical_text` and `aliases` become `aliases.json`
- resolved labels become `labels.json`
- non-runtime fields stay out of the runtime pack and may only appear in sidecar metadata

## `normalized/rules.jsonl`

One JSON object per line.

Required fields:

- `name`: stable rule identifier
- `pattern`: regex pattern text
- one of `label`, `rule_type`, `entity_type`, or `type`

Supported optional fields:

- `kind`: must currently be `regex` when present
- `build_only`: boolean flag for non-runtime rules
- `source_name`: upstream dataset or adapter name
- `source_id`: upstream rule identifier

Example:

```json
{"name":"currency_amount","label":"currency_amount","kind":"regex","pattern":"(USD|EUR|GBP|TRY)\\s?[0-9]+(?:\\.[0-9]+)?"}
{"name":"internal_reference","label":"organization","kind":"regex","pattern":"PRIVATE-[0-9]+","build_only":true}
```

Generator output mapping:

- `name`, `label`, `kind`, and `pattern` become `rules.json`
- only regex rules are allowed in the first runtime-compatible slice

## Formatting Requirements

Adapters that convert raw downloads into normalized JSONL must:

- emit UTF-8 text
- write one JSON object per line
- preserve deterministic field values
- normalize obvious formatting noise before emission where possible
- avoid embedding secrets or raw credential material

Recommended adapter-side cleanup:

- trim and collapse whitespace
- normalize Unicode punctuation
- expand source-specific type names into stable `entity_type` or `label` values
- explode multi-value alias cells into `aliases`
- mark non-runtime or license-restricted records with `build_only`

## Runtime Boundary

The normalized schema is richer than the current runtime pack format. That is intentional.

The first generator slice only emits the fields the current runtime understands:

- labels
- aliases
- regex rules
- manifest metadata

Everything else remains bundle-time metadata until the runtime contract widens.
