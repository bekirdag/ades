# scripts

This directory is reserved for pack-building, release, and local development helper scripts.

Planned early scripts:

- pack manifest validation
- static registry generation
- pack artifact checksum generation
- local release helpers for `v0.1.0`

Current review helpers:

- `review_general_retained_aliases.py`
  Batch `general-en` retained aliases through either a raw local reviewer command or a local mcoda agent via `--mcoda-agent <slug>`, persist JSONL/SQLite decisions, and render a markdown exclusion report.
- `apply_general_alias_exclusions.py`
  Export reviewed drop decisions into `general-retained-alias-exclusions.jsonl` so `generate_pack_source()` can apply the curated exclusions deterministically.
- `audit_stale_relationships.py`
  Emit a local JSON audit for monthly ADES source-lane review. The report reuses
  market-graph source-lane validation, surfaces stale ownership/security/issuer
  relationship warnings, flags source rows whose dated snapshots exceed their
  refresh policy, and writes action items for Ops/ADES follow-up.
- `audit_source_licenses.py`
  Emit a local JSON audit for quarterly ADES source-license review. The report
  confirms canonical source rows have production-eligible `license_status`
  values and `license_notes`, confirms source manifests still allow normalized
  output, requires a source manifest when legacy TSVs have no license columns,
  and writes action items for Ops/ADES follow-up.
- `public_ades_news_smoke.py`
  Run read-only ADES smoke checks for `/healthz`, `/v0/status`, and
  `/v0/news/analyze` against either public ADES or a local `ades serve`
  instance, including expected artifact hash/version, stale endpoint detection,
  golden news examples, and changed relationship-path support.
