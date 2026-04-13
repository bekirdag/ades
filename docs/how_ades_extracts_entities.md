# How ades Extracts Entities

`ades` is designed as a local-first, pack-driven entity extraction system.

The important point is that the current `v0.1.0` runtime is not doing open-ended LLM inference for tagging. It extracts entities by combining:

- installed pack metadata
- deterministic alias lookup
- deterministic regex rules
- pack-aware scoring and topic assembly

That design keeps tagging local, explainable, fast, and reproducible.

## The Main Idea

`ades` does not try to understand all text with one giant global model.

Instead, it expects you to install one or more packs such as:

- `general-en`
- `finance-en`
- `medical-en`

Each pack contributes structured extraction knowledge:

- aliases
- labels
- rules
- dependencies on other packs

For example, a domain pack like `finance-en` can depend on `general-en`, so finance tagging can still reuse shared general entities.

## What A Pack Contains

At the pack level, `ades` works from versioned artifacts described by a manifest. The manifest model is defined in [manifest.py](/home/wodo/apps/ades/src/ades/packs/manifest.py), and starter registry examples live under [src/ades/resources/registry/packs](/home/wodo/apps/ades/src/ades/resources/registry/packs).

In practice, the extraction-relevant parts are:

- `aliases.json`: surface forms and labels
- `rules.json`: regex-style deterministic extraction rules
- `labels.json`: the label inventory
- `manifest.json`: metadata, pack id, domain, language, dependencies, artifact pointers

Example:

- [general-en/aliases.json](/home/wodo/apps/ades/src/ades/resources/registry/packs/general-en/aliases.json)
- [finance-en/rules.json](/home/wodo/apps/ades/src/ades/resources/registry/packs/finance-en/rules.json)

When a pack is installed, that pack metadata is synchronized into the local metadata store so runtime lookup can query it efficiently.

## Runtime Flow

The live tagging path is implemented in [tagger.py](/home/wodo/apps/ades/src/ades/pipeline/tagger.py).

### 1. Normalize The Input

`tag_text()` first normalizes the input text.

- If the input is HTML, tags are stripped.
- HTML entities are unescaped.
- whitespace is collapsed.

This produces one normalized string for downstream matching.

### 2. Load The Active Runtime For The Requested Pack

`tag_text()` loads the runtime view for the selected pack through [runtime.py](/home/wodo/apps/ades/src/ades/packs/runtime.py).

That step:

- resolves the selected installed pack
- walks its dependency chain recursively
- collects all active rules from the selected pack and its dependencies
- builds a `pack_chain` that limits which aliases are allowed at tag time

This is how a tag request for a domain pack can still see dependency-pack entities without treating every installed pack as globally active for every request.

### 3. Extract Rule-Based Entities

The first extraction lane is deterministic rules.

In `tagger.py`, `_extract_rule_entities(...)` iterates over rule definitions and applies their regex patterns to the normalized text.

This is used for patterns that are better expressed structurally than by static aliases, for example:

- currency amounts
- ticker formats like `$TICK`
- other compact symbolic patterns

Each rule match becomes an `EntityMatch` with:

- text span
- label
- confidence
- provenance
- a canonical link id

Rule provenance is explicit, for example:

- `match_kind = "rule"`
- `match_path = "rule.regex"`

### 4. Extract Alias-Based Entities

The second extraction lane is alias lookup.

This is the core entity extraction path for named entities today.

In `tagger.py`, `_extract_lookup_alias_entities(...)` does the following:

1. Tokenize the normalized text with `TOKEN_RE`.
2. Generate candidate windows from longest to shortest.
3. Use windows up to `MAX_ALIAS_WINDOW_TOKENS = 8`.
4. Query the local registry with `lookup_candidates(..., exact_alias=True)`.
5. Filter candidates so only aliases from the active `pack_chain` are allowed.

This matters because `ades` is not doing approximate semantic matching in the current baseline. It is doing deterministic exact-alias lookup over a locally indexed pack registry.

That is why pack quality matters so much: if the alias inventory is sparse, extraction will be sparse.

## Why The Lookup Path Uses Candidate Windows

The tagger does not only look up single tokens.

It generates n-gram style text windows so multi-token aliases can be matched, for example:

- person names
- organization names
- location names
- multi-word biomedical entities

The code currently generates windows from longest to shortest, which gives the runtime a chance to match longer, more specific aliases before short ones.

## Where Alias Lookup Comes From

The runtime lookup surface comes from [registry.py](/home/wodo/apps/ades/src/ades/packs/registry.py).

`PackRegistry.lookup_candidates(...)` reads from the local metadata store and returns deterministic candidate rows for:

- aliases
- rules

At runtime, the tagger uses the alias portion of that lookup result to create entity matches.

This is a deliberate architecture choice:

- `PackRuntime` carries the active rule set and pack dependency chain
- `PackRegistry` is the indexed lookup engine for aliases and metadata

So entity extraction is split between:

- runtime pack context
- local metadata lookup

## Dedupe And Conflict Handling

After rule extraction, alias extraction, and the optional hybrid proposal lane are combined, the tagger resolves candidates in three explicit steps:

- `_dedupe_exact_candidates(...)` removes exact duplicates for the same span and label
- `_apply_repeated_surface_consistency(...)` keeps same-surface same-label mentions coherent across the document
- `_resolve_overlaps(...)` runs weighted interval scheduling so the final entity set is non-overlapping and stable

That means the runtime no longer relies on accidental duplicate filtering alone. It has an explicit final conflict-resolution stage with stable lane priority and relevance-based tie breaking.

## Relevance Scoring

After extraction, `ades` scores entity relevance.

That logic is in `_score_entity_relevance(...)` in [tagger.py](/home/wodo/apps/ades/src/ades/pipeline/tagger.py).

The score uses a weighted combination of:

- extraction confidence
- position in the document
- span length
- repeated occurrence count
- whether the match came from a rule or an alias
- whether the entity domain matches the requested pack domain

This is not a probabilistic entity-linking model. It is a deterministic relevance heuristic designed to rank the extracted entities in a useful, local-first way.

## Provenance And Linking

Every extracted entity can carry provenance and link metadata.

`ades` explicitly records:

- how the entity was found
- which pack supplied it
- which domain supplied it
- which canonical text the runtime associated with it

That metadata is built through:

- `_build_provenance(...)`
- `_build_entity_link(...)`

This is important because the goal is not just to find strings. The goal is to return explainable structured enrichment.

## Topic Assembly

After entities are extracted, `ades` derives topics from the domains of the extracted entities.

That logic lives in `_build_topics(...)` in [tagger.py](/home/wodo/apps/ades/src/ades/pipeline/tagger.py).

Current behavior:

- if non-general domain entities are present, topics are built from those domains
- otherwise the runtime falls back to the requested pack domain

So topics are currently downstream of entity evidence, not a separate large-model classification pass.

## What This Means In Practice

Today, `ades` extracts entities from text by doing:

1. input normalization
2. sentence-aware segmentation and offset preservation
3. pack runtime resolution
4. regex rule extraction
5. exact alias lookup over installed pack metadata
6. optional local learned span proposal plus pack-grounded linking
7. exact duplicate removal, repeated-surface consistency, and global overlap resolution
8. relevance scoring, topic derivation, metrics, and warnings
9. structured JSON output with provenance and link metadata

That means `ades` is currently best described as:

- deterministic
- pack-driven
- local-first
- explainable
- content-agnostic in code, but data-dependent in behavior

## Why Pack Quality Is So Important

Because the baseline runtime is deterministic, entity extraction quality depends heavily on the pack contents.

If a pack does not include:

- enough aliases
- enough canonical coverage
- enough rule patterns

then `ades tag "..."` will under-extract from real prose even if the runtime itself is working correctly.

So there are two separate concerns:

- runtime design: the extraction engine
- pack quality: the actual entity inventory and rule coverage

The runtime can be correct while extraction quality is still weak if the installed pack data is too thin.

This means pack curation is the product's primary ongoing cost. For domains with standardized entity inventories — financial tickers, ICD codes, drug names — deterministic alias lookup is a good fit and actually outperforms neural NER on precision for known entities. For open-ended prose where entities are not pre-enumerable, recall will be structurally limited regardless of how correct the runtime is.

## Known Gaps And Issues

### Deterministic Recall Is Still Structurally Limited

The deterministic precision core is much stronger than it was originally because it now includes shared normalization, generated alias variants, and conflict resolution.

It is still structurally limited on recall when the active packs do not contain enough candidate surfaces. It cannot reliably recover:

- unseen aliases
- inflected or reformatted variants
- abbreviations not already present in the pack
- context-dependent mentions that require more than pack-grounded surface retrieval

This is why `ades` now has an optional hybrid proposal lane and why pack quality is still a first-class product concern.

### Lightweight Contextual Linking Is Still Lightweight

`ades` now has a lightweight contextual linker in the optional hybrid lane. It scores multiple pack-backed candidates for one proposed span using:

- span confidence
- label compatibility
- surface similarity
- pack/domain prior
- alias prior or popularity-derived weight from pack generation
- same-document anchor consistency

That is enough to improve recall without abandoning the product's local-first design.

What it still does not have is a richer semantic context layer based on embedding similarity, broader discourse features, or heavier cross-sentence reasoning. That remains future work.

### Proposal Provider Diversity Is Still Narrow

The shipped hybrid proposal baseline is local `spaCy`.

That is a reasonable first local recall layer, but it is not the final answer. `ades` still needs a controlled bakeoff before adopting any additional proposal provider, and any future provider has to beat the same precision, latency, and memory thresholds instead of being added speculatively.

### Stable Canonical IDs Are Still Not A Public Contract

The runtime now emits deterministic link ids and includes `pack_version` in the response.

That is good enough for internal linking and diff-based release workflows, but it is still not a stable long-term public identity contract. Downstream consumers should treat those ids as pack-version-scoped until `ades` defines a stronger canonical-id policy.

## What ades Is Not Doing Yet

The current baseline still does not yet rely on:

- embedding-assisted candidate reranking
- richer document-context similarity in the linker
- nearby co-mentioned entity graph features in the linker
- full coreference resolution
- relation extraction as a core runtime feature
- a stable long-term canonical entity ID contract

Those are still the next architecture layer, not optional polish.

## What The Best Long-Term Design Looks Like

The best long-term design for `ades` is not to throw away the deterministic pack-driven core.

The right design is a hybrid pipeline:

1. deterministic extraction lanes for precision and provenance
2. learned span proposal for recall on real prose
3. pack-aware candidate generation and linking for grounded canonical IDs
4. document-level conflict resolution for stable final output

That keeps `ades` local-first and explainable while fixing the main weakness of exact alias lookup.

## The Recommended Long-Term Pipeline

### Stage 1. Normalize And Segment

Keep the existing normalization step, but extend it into a richer document-prep stage that also produces:

- sentence boundaries
- token offsets
- paragraph boundaries
- normalized lookup forms for spans

This stage should stay deterministic and content agnostic.

### Stage 2. High-Precision Deterministic Extraction

Keep the current deterministic lanes because they are the highest-precision part of the system:

- regex rules from packs
- exact alias lookup from packs
- generated normalized-alias variants from packs

The important change is that pack generation should produce more surface variants automatically instead of relying on hardcoded runtime logic. Examples include:

- punctuation-normalized variants
- whitespace-normalized variants
- case-normalized variants
- possessive-safe variants where appropriate
- acronym or abbreviation variants when they can be generated from pack data

Those variants should come from pack-build algorithms, not from content-specific runtime exceptions.

### Stage 3. Learned Span Proposal

This is now present as an optional local recall layer.

`ades` now has a local learned proposal path whose job is only to propose candidate spans and coarse labels from open text. The current shipped baseline uses a local `spaCy` model. That model is not the final linker and does not invent canonical IDs by itself.

The important constraint is that the learned model must remain pack-aware:

- only labels supported by the active pack chain should be considered valid
- pack domains should bias which label families are enabled
- model output should be treated as proposals, not final truth

A good long-term fit remains a local span-oriented NER layer such as `GLiNER`-style zero-shot span labeling or a stronger `spaCy`-based trained span recognizer, because both can operate as mention proposal systems instead of replacing the whole architecture.

### Stage 4. Pack-Aware Candidate Generation

Once a deterministic or learned lane proposes a span, `ades` generates canonical candidates from pack data.

Candidate generation should combine:

- exact alias matches
- normalized alias matches
- abbreviation or acronym matches generated from pack data
- label-compatible candidates from the active pack chain

The critical rule is this: model output proposes spans and coarse types, but pack data remains the source of canonical entities.

That preserves the product's local grounding model and avoids turning `ades` into an ungrounded NER-only system.

### Stage 5. Contextual Linking

After candidate generation, `ades` now scores candidates with a lightweight linker instead of relying only on direct surface equality.

The current linker score is a weighted combination of:

- span boundary confidence
- label compatibility
- surface similarity
- pack/domain prior
- entity prior or popularity from the pack
- same-document mention consistency

The next version of this linker should also add:

- richer local document context similarity
- nearby co-mentioned entity signals

This is where real-text reliability improves. A mention does not need to be perfectly enumerated in the alias table if the candidate generator can retrieve plausible entities and the linker can choose the best one from context.

### Stage 6. Global Span Conflict Resolution

After all extraction lanes and linking scores are available, `ades` now runs a dedicated span resolver.

The current resolver:

- discard low-confidence candidates
- resolve overlapping spans
- prefer longer or better-linked spans when they conflict
- preserve narrow repeated-surface consistency across repeated mentions

The current algorithm is weighted interval scheduling over span candidates, with repeated-surface consistency applied before the final overlap pass. A more advanced version can later add richer document-level coherence penalties and rewards, but the important part is that this is already a formal stage rather than an accidental side effect of dedupe.

### Stage 7. Structured Output And Provenance

Keep the existing structured output model and extend it.

Each final entity should still report:

- surface span
- canonical label
- canonical entity id
- confidence
- relevance
- pack provenance
- extraction lane provenance

For the long-term design, provenance should also include the proposal lane and the linker stage so callers can tell whether an entity came from:

- deterministic rule extraction
- deterministic alias extraction
- learned span proposal plus pack linking

That keeps the system debuggable.

## The Right Division Of Responsibility

Long-term `ades` should divide work like this:

- packs define the ontology, aliases, priors, and rules
- deterministic runtime handles normalization, exact matching, and provenance
- learned models propose spans and coarse labels
- linker logic maps spans to canonical pack entities
- span resolver chooses the final non-overlapping entity set

That division is logical because it keeps the product content agnostic in code while allowing behavior to improve through better data, better pack generation, and better learned proposal models.

## Production Readiness Status

Most of the original production-readiness gaps are now closed in the shipped runtime.

The current extractor already has:

- install-time regex validation and reviewed runtime rule compilation
- bounded input handling with size limits, segmentation, and deterministic chunking
- shared normalization across pack generation, runtime lookup, SQLite, and PostgreSQL
- explicit output `schema_version` and `pack_version`
- structured metrics, low-density warnings, and debug span-decision output
- golden-set evaluation and report comparison tooling
- pack diffing, re-tag signaling, and release-threshold evaluation

That means the remaining work is no longer about basic runtime safety. It is about making recall and linking quality stronger without violating the local-first product constraints.

## What Should Change Next

The next practical rollout order is:

1. expand golden sets from synthetic coverage toward harder real-prose coverage in `general-en`, `finance-en`, and `medical-en`
2. improve pack quality and generated variants so the deterministic and hybrid lanes have stronger candidate inventories
3. strengthen the hybrid linker with richer local document-context features and better ambiguity handling
4. add a second local proposal provider only if it beats the shipped `spaCy` baseline on the existing quality gates
5. define a real canonical-entity-id stability contract for downstream consumers
6. add broader document-coherence features only after the linker and id contract are stable
7. keep relation extraction and full coreference as later layers, not prerequisites for core entity extraction readiness

That order still matters. `ades` should keep improving recall and linking from the pack-grounded core outward, not jump to heavier model behavior before the current local hybrid path is clearly better on measured quality gates.

## What Should Stay The Same

The following parts of the current design are still correct and should remain central:

- local-first execution
- pack-driven ontology and aliases
- deterministic rule support
- stable provenance
- structured JSON output
- content-agnostic code

The mistake would be treating the current exact alias lane as the whole long-term solution. It is the precision foundation, not the finished extraction architecture.

## Short Version

`ades` is currently a safe, pack-driven local extractor with a deterministic precision core and an optional hybrid recall lane.

That is now a logical production baseline, but it still needs stronger pack coverage and richer contextual linking to keep improving on real text.

The best long-term design remains a hybrid local pipeline:

- deterministic rules and alias lookup for precision
- learned span proposal for recall
- pack-aware candidate generation and contextual linking for grounding
- explicit overlap resolution for stable final output

That is the architecture that best matches the actual product goal.
