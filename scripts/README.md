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
