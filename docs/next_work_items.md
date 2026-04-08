# ades Next Work Items

Updated after completing coordinated release publication and the related doc alignment on 2026-04-08.

## Priority Queue

1. Richer tagging pipeline decision and execution
   - The implemented tagger in `src/ades/pipeline/tagger.py` is still intentionally deterministic: regex and rule matching plus pack-provided alias lookup.
   - `pyproject.toml` does not yet ship `spaCy`, `GLiNER`, or `FlashText`, and there is still no relation-extraction layer.
   - Decide whether `v0.1.0` stays deterministic-only or whether it still requires a minimum NLP-backed enrichment step. Then either implement that stack with categorized tests or defer it explicitly in the release decisions.

2. Relevance scoring and richer topic output
   - The current response surface has entity confidence and lightweight topic labels, but no explicit relevance field and no stronger topic-classification layer.
   - `docs/implementation_plan.md` now reflects that this work is still pending rather than already present.
   - Add a stable relevance field plus richer topic derivation and scoring if they remain inside the local-tool `v0.1.0` scope.

3. Lightweight local search/index for candidate lookup
   - The current lookup path is still installed-pack metadata, aliases, and rules rather than a broader local candidate-search layer.
   - `SQLite FTS5` or a similar lightweight index remains the planned next option if broader lookup needs to be part of the first local release.
   - Implement that search layer only after the tagging-scope decision is settled so the lookup design matches the real extraction pipeline.

## Recently Closed

- Coordinated release publish workflow:
  - implemented in `src/ades/release.py`
  - exposed through the public Python API, `ades release publish`, and `POST /v0/release/publish`
  - gated on validated manifests from `ades release validate`
  - includes dry-run support plus explicit env-var credential handling for Python and npm publication

- Documentation alignment for the actual `v0.1.0` scope:
  - `README.md`, `docs/implementation_plan.md`, and `docs/development_progress.md` now describe the implemented deterministic tagging baseline and the completed release workflow instead of pointing at already-finished smoke-install work

## Not Next

- The PostgreSQL-backed production server remains intentionally deferred until the local-tool `v0.1.0` scope is closed.
