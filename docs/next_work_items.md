# ades Next Work Items

Updated after completing deterministic tagging-scope alignment plus relevance/topic scoring on 2026-04-08.

## Priority Queue

1. Lightweight local search/index for candidate lookup
   - The current lookup path is still installed-pack metadata, aliases, and rules rather than a broader local candidate-search layer.
   - `SQLite FTS5` or a similar lightweight index remains the planned next option if broader lookup still needs to be part of the first local release.
   - Implement that search layer against the current deterministic tagger instead of re-opening the NLP stack decision.

2. Post-`v0.1.0` NLP-backed enrichment evaluation
   - The default runtime is now intentionally deterministic for `v0.1.0`, and `pyproject.toml` still carries no `spaCy`, `GLiNER`, or `FlashText` runtime dependencies.
   - If richer extraction is revisited later, evaluate it as a post-release track with explicit cost, packaging, and test-surface impact rather than as an implied baseline.

3. Production-server seam hardening
   - The local-tool release path is real, while the PostgreSQL-backed production server remains a placeholder seam.
   - Keep future local-tool changes from leaking SQLite-only assumptions into the contracts that the later production server should reuse.

## Recently Closed

- Coordinated release publish workflow:
  - implemented in `src/ades/release.py`
  - exposed through the public Python API, `ades release publish`, and `POST /v0/release/publish`
  - gated on validated manifests from `ades release validate`
  - includes dry-run support plus explicit env-var credential handling for Python and npm publication

- Deterministic tagging-scope decision and stronger response scoring:
  - `v0.1.0` is now explicitly locked to the implemented deterministic runtime instead of implying a missing `spaCy` / `GLiNER` baseline
  - entity matches now expose explicit deterministic `relevance`
  - topic matches now expose evidence counts plus contributing entity-label summaries

- Documentation alignment for the actual `v0.1.0` scope:
  - `README.md`, `docs/project_guidance.md`, `docs/implementation_plan.md`, `docs/v0.1.0_decisions.md`, and `docs/development_progress.md` now describe the implemented deterministic tagging baseline and the completed release workflow instead of pointing at already-finished or still-unshipped NLP work

## Not Next

- The PostgreSQL-backed production server remains intentionally deferred until the local-tool `v0.1.0` scope is closed.
- Historical architecture research notes in `docs/ades_chat_output.md` and `docs/ades_architecture.html` are not the release-truth source for `v0.1.0`; use the decisions, implementation plan, development progress, and this queue instead.
