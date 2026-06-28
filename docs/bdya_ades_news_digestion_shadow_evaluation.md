# BDYA ADES News Digestion Shadow Evaluation

Date: 2026-06-27

## Summary

BDYA is already getting meaningful value from ADES, but the current production settings should be treated as a good default rather than a proven maximum. The next optimization step should be measurement-driven shadow evaluation, not blindly enabling every ADES pack for every story.

The current production pattern is sensible:

- Always use the base news coverage packs:
  - `general-en`
  - `business-vector-en`
  - `economics-vector-en`
  - `politics-vector-en`
  - `finance-en`
- Dynamically add country finance coverage up to `max_country_finance_packs = 6`.
- Keep `max_packs = 24`, but let ADES choose relevant packs instead of running all 24 by default.

This controls latency and reduces irrelevant matches. Most stories do not need every country finance pack, and running all installed packs by default would probably add noise before it adds useful recall.

## Why BDYA Is Not Yet Proven To Be At The Ceiling

The current cap of six country finance packs may be right for most stories, but some story classes are naturally multi-country:

- sanctions
- trade wars
- supply chains
- central bank spillovers
- shipping and logistics
- commodities and energy
- tariffs
- defense
- elections with market impact
- regional market contagion

For those stories, ADES may stop at six country packs even when the seventh, eighth, or ninth pack could add a useful entity, terminal candidate, or impact path.

The right question is not whether BDYA should enable all packs by default. The right question is whether a wider pack budget improves story quality enough to justify extra latency and possible noise.

## Recommended Shadow Evaluation

Add a BDYA shadow mode that does not affect live story output.

For a small sample of stories, for example 5-10%, run two ADES analyses:

1. Control
   - Current production settings.
   - `max_country_finance_packs = 6`.

2. Shadow
   - Wider settings.
   - Example: `max_country_finance_packs = 10`.
   - Alternative: allow all country finance packs that ADES thinks match, still bounded by `max_packs = 24`.

The live article should continue using the control result. The shadow result should be stored only for comparison.

## Metrics To Compare

Compare the control and shadow outputs using these fields:

- `packs_used`
  - Which extra packs were selected?
  - Were those packs clearly relevant to the story?

- Entity lift
  - Did shadow find new meaningful companies, countries, institutions, commodities, tickers, people, or policy bodies?

- Passive entity lift
  - Did shadow add useful passive entities that improve context without polluting the primary entity list?

- Terminal candidates
  - Did shadow find additional tradable terminals, tickers, ETFs, indices, commodities, currencies, or affected instruments?

- Impact paths
  - Did shadow produce better or more complete impact paths?
  - This is probably the most important metric for BDYA.

- Relationship proposals
  - Did shadow add valid relationships, or mostly noisy ones?

- Warning and error rate
  - Did wider pack use cause more ambiguous matches, pack warnings, timeouts, or low-confidence results?

- Latency
  - Compare p50, p95, and max ADES call duration.

- Downstream usefulness
  - Did BDYA actually use the additional ADES output in categorization, impact path generation, story QA, or final story enrichment?

## Decision Rule

Only widen production settings if shadow mode shows one or more clear benefits:

- More valid impact paths on market-relevant stories.
- Better terminal or tradable references.
- Better country and entity coverage for multi-country stories.
- Acceptable latency increase.
- No meaningful increase in false positives or irrelevant entities.

Suggested interpretation:

- Keep `max_country_finance_packs = 6` if shadow mostly adds unused or noisy entities.
- Raise to `8` or `10` if shadow consistently improves multi-country finance, trade, sanctions, commodities, or macro stories without hurting latency.
- Do not use all 24 packs by default unless evaluation data shows clear quality lift.

## Implementation Shape

Recommended BDYA implementation:

- Add deterministic sampling by story ID or content hash.
- Run shadow ADES asynchronously where possible so it does not slow live publishing.
- Store comparison records with:
  - `story_id`
  - `source`
  - `country_hint`
  - `packs_used_control`
  - `packs_used_shadow`
  - `entities_added`
  - `terminal_candidates_added`
  - `impact_paths_added`
  - `warnings`
  - `latency_ms`
- Build a report after a few hundred or thousand stories.
- Review examples where shadow found useful output that control missed.

## Recommendation

Keep the current ADES settings live. Add shadow evaluation, collect enough examples, then tune from evidence.

Current production behavior is conservative and healthy. The optimization opportunity is measurement-driven widening, not enabling every ADES library by default.
