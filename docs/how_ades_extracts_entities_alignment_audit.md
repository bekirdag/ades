# How ades Extracts Entities: Alignment Audit

Date: 2026-04-11

This document compares [how_ades_extracts_entities.md](/home/wodo/apps/ades/docs/how_ades_extracts_entities.md) against the current codebase.

The audit uses five buckets:

- aligned
- misaligned
- completed
- partially completed
- missing

## Scope Reviewed

The comparison was checked against the current extraction, runtime, generation, storage, and service path in:

- [tagger.py](/home/wodo/apps/ades/src/ades/pipeline/tagger.py)
- [hybrid.py](/home/wodo/apps/ades/src/ades/pipeline/hybrid.py)
- [text_processing.py](/home/wodo/apps/ades/src/ades/text_processing.py)
- [rule_validation.py](/home/wodo/apps/ades/src/ades/packs/rule_validation.py)
- [generation.py](/home/wodo/apps/ades/src/ades/packs/generation.py)
- [runtime.py](/home/wodo/apps/ades/src/ades/packs/runtime.py)
- [registry_db.py](/home/wodo/apps/ades/src/ades/storage/registry_db.py)
- [postgresql.py](/home/wodo/apps/ades/src/ades/storage/postgresql.py)
- [extraction_quality.py](/home/wodo/apps/ades/src/ades/extraction_quality.py)
- [versioning.py](/home/wodo/apps/ades/src/ades/packs/versioning.py)
- [models.py](/home/wodo/apps/ades/src/ades/service/models.py)

## Aligned

- The doc is correct that `ades` is local-first, pack-grounded, and deterministic at its core. That matches [tagger.py](/home/wodo/apps/ades/src/ades/pipeline/tagger.py) and [runtime.py](/home/wodo/apps/ades/src/ades/packs/runtime.py).
- The normalization and segmentation story is aligned. Shared normalization, sentence-aware segmentation, token counting, and normalized-to-raw offset mapping are implemented in [text_processing.py](/home/wodo/apps/ades/src/ades/text_processing.py).
- The doc is correct that deterministic extraction still matters. Regex rules and exact alias lookup remain the precision core in [tagger.py](/home/wodo/apps/ades/src/ades/pipeline/tagger.py).
- The dependency-aware runtime description is aligned. [runtime.py](/home/wodo/apps/ades/src/ades/packs/runtime.py) merges the requested pack with its dependency chain and exposes one runtime view.
- The shared normalization-contract claim is aligned. Pack generation, runtime cache keys, SQLite lookup, and PostgreSQL lookup all use `normalize_lookup_text(...)` from [text_processing.py](/home/wodo/apps/ades/src/ades/text_processing.py).
- The doc is correct that variant expansion belongs in pack generation, not runtime. Generated alias variants are emitted in [generation.py](/home/wodo/apps/ades/src/ades/packs/generation.py).
- The doc is correct that `ades` now has a learned proposal lane that is still pack-grounded. The optional hybrid span proposer is implemented in [hybrid.py](/home/wodo/apps/ades/src/ades/pipeline/hybrid.py), and hybrid candidate linking is still resolved through pack-backed candidates in [tagger.py](/home/wodo/apps/ades/src/ades/pipeline/tagger.py).
- The overlap-resolution section is aligned. [tagger.py](/home/wodo/apps/ades/src/ades/pipeline/tagger.py) now performs weighted interval scheduling across candidates and records span decisions in debug mode.
- The narrow repeated-surface consistency section is aligned. [tagger.py](/home/wodo/apps/ades/src/ades/pipeline/tagger.py) applies a same-surface same-label anchor choice before final overlap resolution.
- The structured output and provenance sections are aligned. [models.py](/home/wodo/apps/ades/src/ades/service/models.py) now includes `schema_version`, `pack_version`, `metrics`, `debug`, provenance, and lightweight link metadata.
- The extraction-quality loop section is aligned. Golden-set evaluation, report comparison, density metrics, and warning thresholds are implemented in [extraction_quality.py](/home/wodo/apps/ades/src/ades/extraction_quality.py).
- The pack lifecycle section is aligned. Pack diffing, re-tag signaling, and release-threshold decisions are implemented in [versioning.py](/home/wodo/apps/ades/src/ades/packs/versioning.py) and exposed through the public API, CLI, and service surfaces.

## Misaligned

- No major design-vs-code contradictions remain after the latest doc refresh.
- The one important caveat is scope: the doc describes the right long-term architecture, but some of the richer linking ideas are still future extensions rather than current baseline behavior.

## Completed

- Shared normalization and sentence-aware segmentation are completed in [text_processing.py](/home/wodo/apps/ades/src/ades/text_processing.py).
- Input-size limiting and deterministic chunking are completed in [tagger.py](/home/wodo/apps/ades/src/ades/pipeline/tagger.py).
- Regex validation and reviewed compilation are completed in [rule_validation.py](/home/wodo/apps/ades/src/ades/packs/rule_validation.py) and [runtime.py](/home/wodo/apps/ades/src/ades/packs/runtime.py).
- Deterministic rule extraction and exact alias lookup are completed in [tagger.py](/home/wodo/apps/ades/src/ades/pipeline/tagger.py) and the metadata stores in [registry_db.py](/home/wodo/apps/ades/src/ades/storage/registry_db.py) and [postgresql.py](/home/wodo/apps/ades/src/ades/storage/postgresql.py).
- Global overlap resolution is completed in [tagger.py](/home/wodo/apps/ades/src/ades/pipeline/tagger.py).
- Narrow repeated-surface consistency is completed in [tagger.py](/home/wodo/apps/ades/src/ades/pipeline/tagger.py).
- Build-time alias variant generation is completed in [generation.py](/home/wodo/apps/ades/src/ades/packs/generation.py).
- Popularity and pack-prior plumbing for ambiguity scoring are completed through [alias_analysis.py](/home/wodo/apps/ades/src/ades/packs/alias_analysis.py), [generation.py](/home/wodo/apps/ades/src/ades/packs/generation.py), and the lookup stores.
- The first hybrid recall lane is completed as an optional `spaCy` baseline in [hybrid.py](/home/wodo/apps/ades/src/ades/pipeline/hybrid.py).
- Pack-aware candidate generation and lightweight contextual linking for proposed spans are completed in [tagger.py](/home/wodo/apps/ades/src/ades/pipeline/tagger.py).
- Quality reporting, golden-set comparisons, and low-density warnings are completed in [extraction_quality.py](/home/wodo/apps/ades/src/ades/extraction_quality.py).
- Machine-readable pack diffs and release-threshold decisions are completed in [versioning.py](/home/wodo/apps/ades/src/ades/packs/versioning.py).
- Public operational surfaces for the quality loop and release gates are completed in [api.py](/home/wodo/apps/ades/src/ades/api.py), [cli.py](/home/wodo/apps/ades/src/ades/cli.py), and [app.py](/home/wodo/apps/ades/src/ades/service/app.py).

## Partially Completed

- The contextual linker is partially completed. The current hybrid scorer uses span confidence, label compatibility, surface similarity, alias prior, and same-document anchor consistency, but it does not yet use richer semantic document-context similarity or co-mentioned-entity graph features.
- Proposal-provider diversity is partially completed. The architecture supports an optional proposal provider lane, but the shipped baseline is currently `spaCy` only.
- Variant generation is partially completed relative to the broadest long-term vision. Canonicalization, punctuation cleanup, and organization-suffix stripping are implemented, but broader derivable acronym-generation strategies are still limited by what source data provides today.
- Release-threshold enforcement is partially completed relative to the most automated long-term vision. The decision engine can evaluate model artifact size and peak-memory limits when those values are supplied, but it does not yet collect peak-memory telemetry by itself.

## Missing

- A stable long-term canonical entity ID contract is still missing. Current IDs are deterministic runtime link IDs, but they should not yet be treated as durable public IDs across pack-version changes.
- Embedding-assisted linker context is missing.
- Broader nearby co-mentioned entity graph features in the linker are missing.
- Full coreference resolution is missing.
- Relation extraction remains missing as a core runtime feature.

## Bottom Line

The extraction design docs are now mostly aligned with the codebase.

The current product is no longer just a deterministic exact-alias matcher. It is now:

- a safe deterministic extractor
- with shared normalization and segmentation
- with build-time variant generation
- with weighted overlap resolution
- with narrow repeated-surface consistency
- with an optional local `spaCy` proposal lane
- with pack-grounded lightweight contextual linking
- with a real extraction-quality loop
- with pack-versioning and release-gate primitives

The remaining gaps are no longer foundational architecture gaps. They are the next precision-and-recall improvements on top of the hybrid local baseline.
