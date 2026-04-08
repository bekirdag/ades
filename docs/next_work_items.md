# ades Next Work Items

Updated after completing lightweight local candidate search/index on 2026-04-08.

## Priority Queue

1. Pack lifecycle hardening
   - Exercise pull, install, activation, deactivation, and artifact verification against production-like registry inputs rather than only the bundled happy path.
   - Add explicit recovery behavior for partial installs, checksum mismatch, stale metadata, and repeated idempotent operations.

2. Storage and metadata recovery
   - Harden first-run bootstrap, stale-record cleanup, and repair behavior around the local SQLite registry so the shipped local tool can recover cleanly from interrupted or inconsistent state.
   - Keep the storage seam explicit so future PostgreSQL work can reuse the public contracts without inheriting local-only assumptions.

3. CLI and service operational hardening
   - Tighten startup validation, config error reporting, exit behavior, and operator-facing machine-readable responses on the active local-tool commands and endpoints.
   - Confirm the existing local surfaces behave correctly outside the repo checkout and under realistic storage-root conditions.

4. End-to-end production-readiness validation
   - Add more clean-environment flows that cover install, pull, serve, tag, and recovery behavior together instead of validating them only in narrower slices.
   - Keep the completed local-tool modules aligned with the repo’s production-ready completion bar.

5. Post-`v0.1.0` NLP-backed enrichment evaluation
   - The default runtime is now intentionally deterministic for `v0.1.0`, and `pyproject.toml` still carries no `spaCy`, `GLiNER`, or `FlashText` runtime dependencies.
   - If richer extraction is revisited later, evaluate it as a post-release track with explicit cost, packaging, and test-surface impact rather than as an implied baseline.

6. Production-server seam hardening
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

- Lightweight local candidate search/index:
  - implemented in `src/ades/storage/registry_db.py`
  - exposed through the existing public API, `ades packs lookup`, and `GET /v0/lookup`
  - adds multi-term cross-field lookup through `SQLite FTS5` when available, with deterministic fallback to standard SQLite queries when it is not
  - includes categorized unit, component, integration, and API coverage for broader metadata lookup behavior

## Not Next

- The PostgreSQL-backed production server remains intentionally deferred until the local-tool `v0.1.0` scope is closed.
- Historical architecture research notes in `docs/ades_chat_output.md` and `docs/ades_architecture.html` are not the release-truth source for `v0.1.0`; use the decisions, implementation plan, development progress, and this queue instead.
