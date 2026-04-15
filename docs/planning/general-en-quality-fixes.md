# General-En Quality Fixes

Date: 2026-04-15

This document lists the remaining structural fixes needed to make `general-en`
reliable on real prose without introducing content-specific logic.

The goal is to stop recurring classes of failures:
- generic words matched as entities
- wrong label family selection
- partial spans instead of full spans
- missing high-value acronyms and short geopolitical forms
- weak contextual linking for ambiguous mentions

## Principles

- Keep fixes generic and class-based, not entity-name based.
- Prefer build-time alias quality controls over runtime cleanup where possible.
- Keep the hot exact lane small, high-precision, and fast.
- Move risky aliases into delayed or contextual lanes instead of letting them
  into `runtime_exact_high_precision`.

## Required Fixes

### 1. Add alias-class filtering at build time

Reject or demote low-information alias classes before pack emission:
- demonstratives and pronouns
- months and weekdays in general prose contexts
- generic head nouns like `stock`, `report`, `bank`, `company`
- bare first names and bare surnames unless support is exceptionally strong

Use structural features:
- token count
- stopword ratio
- generic-head detection
- ambiguity count
- support score
- popularity
- label-family conflict

### 2. Tighten runtime alias tiers

Keep only the safest aliases in `runtime_exact_high_precision`.

Use separate lanes for:
- `runtime_exact_high_precision`
- `runtime_exact_acronym_high_precision`
- `runtime_exact_geopolitical_high_precision`
- `runtime_ambiguous_contextual`
- `search_only`

This prevents noisy aliases from entering the hot exact path.

### 3. Strengthen label-family priors

Correct family drift like geographic entities being retained as organizations.

Add stronger priors from:
- source entity type
- alias shape
- canonical text shape
- family-consistency across aliases

Expected behavior:
- countries, regions, cities strongly prefer `location`
- institutions and companies strongly prefer `organization`
- human-name patterns strongly prefer `person`

### 4. Prefer longest valid spans

Suppress shorter head fragments when a longer compatible span exists.

Examples of the class of issue:
- full geopolitical/location phrases over trailing location heads
- full institution names over generic institution heads

Generic rule:
- if a shorter span is mostly a head noun and a longer nearby span is valid,
  keep the longer span and drop the fragment

### 5. Add head-fragment suppression

Drop weak standalone fragments that are usually parts of longer names:
- bare generic institution heads
- bare surnames or first names
- bare commodity or financial head nouns

Base this on:
- span shape
- neighboring capitalized tokens
- whether the fragment is commonly a name component rather than a safe
  standalone entity

### 6. Improve person-name assembly

Recover full person spans rather than fragments or misses.

Prefer:
- multi-token capitalized name shapes
- hyphenated surnames and given names
- surname continuity
- person particles where appropriate

This should improve person recall without hardcoding known people.

### 7. Add bounded contextual disambiguation

Only rerank ambiguous survivors after exact extraction.

Use lightweight context signals:
- nearby cue words
- sentence context
- document topic
- label prior
- source prior

Do not run heavy contextual logic on every match.

### 8. Fix short geopolitical alias recovery

Recover structurally important short geopolitical aliases like country/state
abbreviations when support is high enough.

Guard them with:
- ambiguity limits
- family priors
- popularity/support thresholds
- contextual sanity checks

This is the missing class behind examples like `US`.

### 9. Make acronym handling a unified subsystem

Acronym support should consistently cover:
- direct exact acronym aliases
- document-defined acronym backfill
- acronym-to-expansion consistency across the document

This should work for any strong organization or geopolitical acronym, not just
today's observed examples.

### 10. Score aliases before runtime emission

Do not treat all normalized aliases as runtime candidates.

Each alias should be scored at build time by:
- token count
- ambiguity
- support
- popularity
- generation origin
- lexical genericness
- cross-family conflict

Runtime should consume pre-ranked aliases, not act as the primary cleanup
stage.

### 11. Add real-prose release gates

Fail pack promotion when the output shows known bad classes:
- demonstratives as entities
- months as locations or organizations
- bare name fragments without strong support
- generic heads as standalone entities
- repeated partial-span wins over better full spans

Also require:
- acronym recall
- geopolitical short-form recall
- person-span completeness
- ambiguous-link sanity on news prose

### 12. Clean install and storage layout

There must be one authoritative installed `general-en`.

Current validation is less trustworthy if stale and live pack paths coexist.
Pack install, status, and diagnostics should all resolve to the same on-disk
location.

## Recommended Execution Order

1. Alias-class filtering
2. Tier tightening
3. Longest-span preference and fragment suppression
4. Label-family priors
5. Short geopolitical alias recovery
6. Bounded contextual disambiguation
7. Real-prose release gates
8. Storage and install cleanup

## Expected Outcome

If the above is implemented correctly, the same categories of failures should
stop recurring:
- generic words as entities
- wrong entity family labels
- partial spans beating better spans
- missing strong acronyms
- weak short-form geopolitical recall
- ambiguous links chosen without enough context
