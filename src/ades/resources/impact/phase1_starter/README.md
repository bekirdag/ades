# Impact Relationship Phase 1 Starter

This directory contains a small reviewed source lane used to build a starter
`market_graph_store.sqlite` artifact. It is not the full Phase 2 market graph.

The rows are intentionally narrow:

- one or more examples for each of the eight inner-ring relationship families,
- refs aligned with ADES extraction where those refs are currently emitted,
- local `ades:impact:*` refs for terminal market variables that ADES does not
  yet extract as mentioned entities,
- official or multilateral source URLs only.

Large raw downloads and normalized bulk outputs belong under
`/mnt/githubActions/ades_big_data`, not in this package.
