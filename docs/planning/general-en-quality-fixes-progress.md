# General-En Quality Fixes Progress

Date: 2026-04-15

This file records implementation progress for
[general-en-quality-fixes.md](/home/wodo/apps/ades/docs/planning/general-en-quality-fixes.md).

## Goal

Finish the remaining structural `general-en` quality fixes without introducing
entity-specific logic.

## Scope

- build-time alias-class filtering
- stricter runtime tiers
- label-family corrections
- span completion and fragment suppression
- person-name assembly improvements where feasible in the deterministic path
- short geopolitical and acronym recovery
- real-prose quality gates
- install/storage cleanup tracking

## Current Baseline

- Runtime latency is healthy on the daemon-first path.
- This batch focused on structural precision, family selection, span hygiene,
  and benchmark-quality release gates.
- The remaining live-pack issues are now source-content and deployment-state
  issues rather than core deterministic runtime behavior.

## Work Log

### 2026-04-15

- Confirmed the implementation targets in:
  - [src/ades/packs/alias_analysis.py](/home/wodo/apps/ades/src/ades/packs/alias_analysis.py)
  - [src/ades/pipeline/tagger.py](/home/wodo/apps/ades/src/ades/pipeline/tagger.py)
  - [src/ades/packs/general_quality.py](/home/wodo/apps/ades/src/ades/packs/general_quality.py)
- Confirmed the existing deterministic test surfaces in:
  - [tests/unit/test_alias_analysis.py](/home/wodo/apps/ades/tests/unit/test_alias_analysis.py)
  - [tests/unit/test_tagger_lookup_filters.py](/home/wodo/apps/ades/tests/unit/test_tagger_lookup_filters.py)
  - [tests/component/test_lookup_driven_tagger.py](/home/wodo/apps/ades/tests/component/test_lookup_driven_tagger.py)
  - [tests/api/test_tag_lookup_aliases.py](/home/wodo/apps/ades/tests/api/test_tag_lookup_aliases.py)
  - [tests/integration/test_pack_generation_api.py](/home/wodo/apps/ades/tests/integration/test_pack_generation_api.py)
- Confirmed the remaining generic failure classes from recent live probes:
  - `This` as `location`
  - `March` as `location`
  - `Stock` as `location`
  - `James` as `organization`
  - `Europe` drifting to `organization`
  - shorter spans winning where fuller institution/location spans are preferable
- Implemented build-time alias-class tightening in
  [src/ades/packs/alias_analysis.py](/home/wodo/apps/ades/src/ades/packs/alias_analysis.py):
  - blocklisted demonstrative, calendar, and generic-head single-token aliases
  - added family-preferred location selection for strong geographic aliases
  - kept short geopolitical acronyms intact while blocking common-word
    short-form locations
  - allowed safe explicit multi-token organization aliases back into the
    exact runtime tier
- Fixed explicit-alias generation in
  [src/ades/packs/generation.py](/home/wodo/apps/ades/src/ades/packs/generation.py):
  - explicit source aliases are no longer incorrectly marked as generated
  - long organization variants like `Org Alpha Variant` remain eligible for
    the hot exact lane
- Tightened runtime fragment suppression in
  [src/ades/pipeline/tagger.py](/home/wodo/apps/ades/src/ades/pipeline/tagger.py):
  - skips demonstratives, months, and generic heads as single-token entities
  - suppresses titleish single-token fragments inside larger names
- Added deterministic span-hygiene fixtures in
  [tests/pack_generation_helpers.py](/home/wodo/apps/ades/tests/pack_generation_helpers.py):
  - overlapping location spans such as `Talora` vs `Strait of Talora`
  - overlapping organization spans such as `Bank of Talora` vs
    `National Bank of Talora`
- Added pipeline coverage for the new structural classes in:
  - [tests/unit/test_alias_analysis.py](/home/wodo/apps/ades/tests/unit/test_alias_analysis.py)
  - [tests/unit/test_tagger_lookup_filters.py](/home/wodo/apps/ades/tests/unit/test_tagger_lookup_filters.py)
  - [tests/unit/test_pack_generation.py](/home/wodo/apps/ades/tests/unit/test_pack_generation.py)
  - [tests/component/test_lookup_driven_tagger.py](/home/wodo/apps/ades/tests/component/test_lookup_driven_tagger.py)
  - [tests/api/test_tag_lookup_aliases.py](/home/wodo/apps/ades/tests/api/test_tag_lookup_aliases.py)
  - [tests/integration/test_pack_generation_api.py](/home/wodo/apps/ades/tests/integration/test_pack_generation_api.py)
- Extended benchmark-quality release gates in
  [src/ades/packs/general_quality.py](/home/wodo/apps/ades/src/ades/packs/general_quality.py)
  and
  [src/ades/packs/quality_common.py](/home/wodo/apps/ades/src/ades/packs/quality_common.py):
  - added generic-prose noise regression coverage
  - added acronym backfill coverage
  - added explicit longest-valid-span coverage without subsumed-hit
    normalization
- Extended benchmark fixtures in
  [tests/general_bundle_helpers.py](/home/wodo/apps/ades/tests/general_bundle_helpers.py)
  with structural noise and overlap cases, plus strong-support geopolitical
  examples
- Updated benchmark expectation tests in:
  - [tests/unit/test_general_pack_quality.py](/home/wodo/apps/ades/tests/unit/test_general_pack_quality.py)
  - [tests/integration/test_general_pack_quality_api.py](/home/wodo/apps/ades/tests/integration/test_general_pack_quality_api.py)
  - [tests/api/test_general_pack_quality_endpoint.py](/home/wodo/apps/ades/tests/api/test_general_pack_quality_endpoint.py)
- Validated the combined structural slice:
  - `83 passed`
- Reviewed install/storage path handling in
  [src/ades/packs/installer.py](/home/wodo/apps/ades/src/ades/packs/installer.py)
  and related registry code:
  - current code installs into `registry.layout.packs_dir / pack_id`
  - no current repo code path hardcodes a duplicate `/packs/packs` install path
  - historical duplicate live paths appear to be environment-state/stale-root
    issues, not a current installer bug

## Checklist

- [x] Create a separate progress log for this work
- [x] Map impacted code and tests
- [x] Tighten build-time alias-class filtering for remaining generic classes
- [x] Tighten runtime fragment suppression and family corrections
- [x] Add or tighten longest-span and partial-fragment preference tests
- [x] Add real-prose quality gates for the remaining failure classes
- [x] Validate the affected unit/component/api/integration slices
- [x] Review install/storage cleanup status and record remaining work

## Notes

- The fixes must remain generic and class-based.
- No entity-name allowlists or blocklists should be introduced.
- Remaining work after this batch is primarily outside these structural code
  fixes:
  - live-pack rebuild/install alignment
  - source-content improvements for real-world recall gaps not represented by
    the deterministic fixtures
