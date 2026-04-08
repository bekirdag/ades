# ades Next Work Items

Derived from a codebase review against `docs/implementation_plan.md` and
`docs/development_progress.md` on 2026-04-08.

## Priority Queue

1. Coordinated release publish workflow
   - The codebase has `verify`, `sync-version`, `manifest`, and `validate` release flows in `src/ades/release.py`, but it has no publish step for Python or npm.
   - Add one manifest-driven publish workflow that consumes a validated release manifest and performs Python and npm publication from one explicit surface.
   - Suggested deliverables:
     - library helper in `src/ades/release.py`
     - CLI command such as `ades release publish`
     - optional local admin/service endpoint if release automation should stay available over localhost
     - dry-run mode, explicit environment-variable credential handling, and categorized tests

2. Documentation alignment for the actual `v0.1.0` tagging scope
   - `README.md` still says the next step is smoke-install validation even though that work is already complete.
   - `docs/implementation_plan.md` still reads as if `spaCy`, `GLiNER`, `FlashText`, richer topic tagging, and relevance scoring are the active near-term build path, while the implemented tagger is still deterministic regex plus alias lookup.
   - Align the docs to the real current state before more feature work lands, or explicitly restate which of those planned NLP features are still required for `v0.1.0`.

3. Richer tagging pipeline decision and execution
   - Current tagging in `src/ades/pipeline/tagger.py` is intentionally lightweight and deterministic.
   - Missing versus the plan:
     - `spaCy` integration
     - `GLiNER` integration
     - `FlashText` usage
     - any relation-extraction surface
   - Decide whether `v0.1.0` should stay deterministic-only or whether an initial NLP-backed enrichment step is still required. Then either implement the minimum acceptable stack or explicitly defer it in the plan and decision docs.

4. Relevance scoring and richer topic output
   - The current response surface has entity confidence and simple topic labels, but no explicit relevance field and no richer topic-classification logic.
   - The implementation plan still calls for confidence and relevance scoring plus stronger topic tagging.
   - Add a stable relevance field and a better topic derivation/scoring layer, or revise the plan to defer that work past `v0.1.0`.

5. Lightweight local search/index for candidate lookup
   - The implementation plan calls out `SQLite FTS5` or another lightweight local index for candidate search.
   - The current lookup path appears to rely on installed pack metadata and exact alias lookup, without a dedicated FTS-style search layer.
   - Add a lightweight candidate-search path if broader lookup is still part of `v0.1.0`; otherwise mark it deferred.

## Not Next

- The PostgreSQL-backed production server remains intentionally deferred and should stay out of the immediate queue until the local-tool `v0.1.0` scope is closed.
