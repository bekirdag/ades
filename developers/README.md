# ADES developer documentation

Buildless static site. Serve this directory as the document root for developers.adestool.com. No external scripts, fonts, analytics, runtime dependencies, API keys or cloud hosting state. index.html, styles.css and app.js are deployment assets.

## Verification (2026-10-14)

Source reviewed through Docdex: project README, npm README and service model symbols. Live OpenAPI captured from https://api.adestool.com/openapi.json. Layout inspiration: https://developers.theneuralledger.com/ (documentation navigation, search, API orientation and appearance control).

- `/v0/tag`, `NVIDIA reported quarterly earnings.`, `finance-us-en`: returns ticker NVIDIA linked to finance-us-ticker:NVDA and finance topic.
- `/v0/tag`, Apple in same pack: empty. Do not replace verified example with Apple until regression is fixed.
- `/v0/news/analyze`, NVIDIA, explicit finance-us-en, automatic planning false: latest production 0.4.1 verification: one source entity with mention_texts=["NVIDIA"], zero terminal candidates, event_incompatible/macro_gated reasons and missing event-compatibility / macro-FX-rates event-signal warnings.
- `/v0/impact/expand`, NVDA ref: three candidates (NVDA, US broad equity market proxy, USD), one passive path. This differs semantically from article-grounded news filtering.
- Python default User-Agent Python-urllib/3.12 returned HTTP 403 for news analysis. Identical request with ades-docs-example/1.0 returned 200; curl also returned 200. Documentation Python example supplies User-Agent.
- node --check app.js passed. HTML parsed and all local anchor targets and assets checked. No automated browser QA available in this task; visual and interaction review in a browser remains useful.

## Content gaps / cautions

OpenAPI does not enumerate the free-form /v0/tag options keys; docs avoid claiming unsupported knobs. No promises about SDK signatures, SLA, quotas or unsupported languages. Local installation commands are source-backed but were not executed into a new local environment. Relationship edges may point to local file paths; such provenance is not publicly fetchable evidence. Documentation calls out artifact/source review rather than treating graph confidence as market prediction.

Search is client-side, case-insensitive, with Ctrl/Cmd+K and Escape. Theme preference persists in localStorage when available; defaults to OS appearance. Mobile navigation, copy buttons, reduced motion and keyboard focus are included. Main document content remains available without JavaScript.

## Verified 0.4.1 contract

Source model `src/ades/service/models.py:ImpactSourceEntity` adds optional `mention_texts: list[str]` with an empty-list default. It retains matched article surfaces separately from the canonical name, for impact grounding (for example NVIDIA versus NVDA). Production response now reports version 0.4.1 and preserves mention_texts=["NVIDIA"]. Older clients should tolerate the additive field; readers of old payloads can default it to an empty list. Refs-only expansion may have no surfaces. No claim that Apple extraction is fixed is published here.

The quality-review section distinguishes operations, precision, coverage, grounding, impact evidence and correct abstention. Green status is operational evidence; empty output can be correct; broad refs-only proxies differ from text-grounded news candidates.

## Entity-only display guidance

Docs distinguish canonical refs and display labels from causal prose. Both entities and impact paths must come from the exact created/published article text, without unions from research or source-story metadata. Entity/asset chips must use canonical refs extracted from the exact created/published article text; never split arrow narratives into pseudo-entities. Structured relationship edges remain evidence rather than chip identifiers. This follows the user's entity-only impact presentation requirement.

## Final asset validation

Rechecked the news example against production 0.4.1. Removed malformed paragraph wrapping around the response section heading. JavaScript syntax, unique IDs, all local anchors/assets, paragraph nesting, every embedded shell/Python/JSON example and balanced HTML non-void elements passed static checks. Browser QA remains unperformed. Finalized article text is the sole boundary for displayed entities and impact paths; research/source-story unions remain explicitly prohibited.
