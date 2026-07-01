# ADES News Story Creation Local Verification Runbook

Status: Local-only runbook
Work item: NSC-I002
Scope: ADES artifact build and verification for BDYA news story creation

This runbook documents the local build and verification sequence for an ADES
market graph artifact before any deployment decision. The commands below build
an artifact, validate source lanes, run golden checks, and smoke-test the local
`/v0/news/analyze` service. They do not deploy, publish, upload, restart a
service, promote an artifact, or mutate production.

## Local Safety Rules

- Run from the ADES repository root.
- Keep all generated output under `/mnt/githubActions/ades_big_data`.
- Do not run `git push`, `gh workflow run`, `ssh`, `scp`, `rsync`, `systemctl`,
  `docker compose up` against a production host, or the production deploy
  workflow from this runbook.
- Treat `/mnt/githubActions/ades_big_data/artifacts/market-graph/current` as a
  production-facing pointer and do not rewrite it during local verification.
- Record the final artifact version, artifact hash, manifest path, validation
  output, golden output, and local smoke output in the release note template
  before any separate release approval process.

## Shared Shell Setup

```bash
set -euo pipefail

cd /home/wodo/apps/ades

python -m pip install -e ".[dev,server]"

export ADES_RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-news-story-local"
export ADES_ARTIFACT_DIR="/mnt/githubActions/ades_big_data/artifacts/market-graph/${ADES_RUN_ID}"
export ADES_STORAGE_ROOT="/mnt/githubActions/ades_big_data/local_service_storage/${ADES_RUN_ID}"
export ADES_LOCAL_HOST="127.0.0.1"
export ADES_LOCAL_PORT="8765"
export ADES_LOCAL_BASE_URL="http://${ADES_LOCAL_HOST}:${ADES_LOCAL_PORT}"
export ADES_NODE_TSV="src/ades/resources/impact/phase1_starter/impact_nodes.tsv"
export ADES_EDGE_TSV="src/ades/resources/impact/phase1_starter/impact_edges.tsv"
export ADES_GOLDEN_JSONL="/mnt/githubActions/ades_big_data/news_story_creation/goldens/ades_impact_news_analysis_golden.jsonl"

mkdir -p "${ADES_ARTIFACT_DIR}" "${ADES_STORAGE_ROOT}" "$(dirname "${ADES_GOLDEN_JSONL}")"
```

The packaged starter TSVs are the minimum local artifact surface. For a country
pack or source-lane release, replace `ADES_NODE_TSV` and `ADES_EDGE_TSV` with
the reviewed lane outputs from that build while keeping the same verification
order.

## 1. Build The Local Artifact

Build the market graph store into a run-specific directory.

```bash
ades registry build-market-graph-store \
  --node-tsv-path "${ADES_NODE_TSV}" \
  --edge-tsv-path "${ADES_EDGE_TSV}" \
  --output-dir "${ADES_ARTIFACT_DIR}" \
  --artifact-version "${ADES_RUN_ID}" \
  | tee "${ADES_ARTIFACT_DIR}/build-market-graph-store.json"
```

Capture the artifact identity for later checks.

```bash
export ADES_ARTIFACT_PATH="${ADES_ARTIFACT_DIR}/market_graph_store.sqlite"
export ADES_ARTIFACT_MANIFEST="${ADES_ARTIFACT_DIR}/market_graph_store_manifest.json"
export ADES_ARTIFACT_HASH="$(
  python -c 'import json, os; print(json.load(open(os.environ["ADES_ARTIFACT_MANIFEST"], encoding="utf-8"))["artifact_hash"])'
)"

test -f "${ADES_ARTIFACT_PATH}"
test -f "${ADES_ARTIFACT_MANIFEST}"
test -n "${ADES_ARTIFACT_HASH}"
```

Expected result: the build command exits `0`, writes `market_graph_store.sqlite`,
writes `market_graph_store_manifest.json`, and the manifest contains
`artifact_version`, `artifact_hash`, `source_lanes`, `lane_hashes`, `row_count`,
`edge_count`, `node_count`, `validation_summary`, `golden_summary`, `warnings`,
and `promotion_status`.

## 2. Validate Source Lanes And Manifest Shape

Run the explicit source-lane validator against the exact TSVs used in the build.

```bash
ades registry validate-market-graph-source-lanes \
  --node-tsv-path "${ADES_NODE_TSV}" \
  --edge-tsv-path "${ADES_EDGE_TSV}" \
  --sample-limit 100 \
  | tee "${ADES_ARTIFACT_DIR}/validate-market-graph-source-lanes.json"
```

Run the focused deterministic regression tests that cover source-lane validation,
market graph build behavior, artifact hash stability, and news-analysis artifact
visibility.

```bash
python -m pytest \
  tests/unit/test_source_lane_validation.py \
  tests/unit/test_impact_expansion.py \
  tests/unit/test_impact_starter_evaluation.py \
  tests/unit/test_impact_news_analysis_evaluation.py \
  tests/component/test_cli_market_graph_store.py \
  -q \
  | tee "${ADES_ARTIFACT_DIR}/pytest-artifact-validation.txt"
```

Expected result: the validator exits `0`; pytest exits `0`. Any source warning
must be copied into the release note template with owner and mitigation before a
separate release decision.

## 2.1 Monthly Stale Relationship Audit

Run this local-only audit once per month for the exact source-lane TSVs used by
the current artifact candidate. It flags ownership, issuer/security, and source
rows needing refresh before a separate release decision.

```bash
python scripts/audit_stale_relationships.py \
  --node-tsv-path "${ADES_NODE_TSV}" \
  --edge-tsv-path "${ADES_EDGE_TSV}" \
  --sample-limit 100 \
  | tee "${ADES_ARTIFACT_DIR}/monthly-stale-relationship-audit.json"
```

Expected result: the command exits `0` and emits JSON with
`stale_relationship_warning_counts`, `source_refresh_warning_counts`, samples,
and `action_items`. Any non-empty warning count must be assigned to Ops/ADES for
source refresh or relationship re-verification before promotion.

## 3. Run Golden News-Analysis Checks

Run repo-embedded golden coverage first. These tests cover the evaluator, the
CLI contract, and representative `/v0/news/analyze` path behavior.

```bash
python -m pytest \
  tests/unit/test_impact_news_analysis_evaluation.py \
  tests/component/test_cli_market_graph_store.py::test_cli_registry_evaluate_impact_news_analysis_golden_calls_public_api \
  tests/api/test_news_analyze_endpoint.py \
  -q \
  | tee "${ADES_ARTIFACT_DIR}/pytest-news-analysis-goldens.txt"
```

When BDYA exports a reviewed news-story golden JSONL, run it against the local
artifact before smoke-testing the service. The canonical exchange path for this
pipeline is `${ADES_GOLDEN_JSONL}`.

```bash
test -f "${ADES_GOLDEN_JSONL}"

ades registry evaluate-impact-news-analysis-golden \
  --golden-set-path "${ADES_GOLDEN_JSONL}" \
  --artifact-path "${ADES_ARTIFACT_PATH}" \
  --storage-root "${ADES_STORAGE_ROOT}" \
  --reviewed-only \
  --max-depth 3 \
  --max-terminal-candidates 25 \
  --min-terminal-candidate-recall 1.0 \
  --min-terminal-candidate-precision 0.95 \
  --min-source-entity-recall 0.8 \
  --min-event-type-recall 0.8 \
  | tee "${ADES_ARTIFACT_DIR}/evaluate-impact-news-analysis-golden.json"
```

Expected result: embedded pytest exits `0`; the BDYA JSONL gate exits `0` when
the reviewed golden file is present. If the JSONL file is absent, stop the
release-preparation flow and record that the BDYA golden export dependency is
missing; do not deploy based only on the local smoke.

## 4. Run A Local `/v0/news/analyze` Smoke

Start a local ADES service with the built artifact. Keep this process attached
to the terminal so it can be stopped with `Ctrl-C` after smoke testing.

```bash
ADES_NEWS_ANALYZE_ENABLED=1 \
ADES_IMPACT_EXPANSION_ARTIFACT_PATH="${ADES_ARTIFACT_PATH}" \
ADES_STORAGE_ROOT="${ADES_STORAGE_ROOT}" \
ades serve --host "${ADES_LOCAL_HOST}" --port "${ADES_LOCAL_PORT}"
```

In another terminal, run the read-only smoke script against the local service.

```bash
python scripts/public_ades_news_smoke.py \
  --base-url "${ADES_LOCAL_BASE_URL}" \
  --expected-artifact-hash "${ADES_ARTIFACT_HASH}" \
  --expected-artifact-version "${ADES_RUN_ID}" \
  --timeout-seconds 30 \
  | tee "${ADES_ARTIFACT_DIR}/local-news-analyze-smoke.json"
```

For relation-specific releases, add one `--require-changed-relation` flag per
changed relation that must appear in at least one local golden case.

```bash
python scripts/public_ades_news_smoke.py \
  --base-url "${ADES_LOCAL_BASE_URL}" \
  --expected-artifact-hash "${ADES_ARTIFACT_HASH}" \
  --expected-artifact-version "${ADES_RUN_ID}" \
  --goldens "${ADES_ARTIFACT_DIR}/local-smoke-goldens.json" \
  --require-changed-relation issuer_has_listed_ticker \
  --timeout-seconds 30 \
  | tee "${ADES_ARTIFACT_DIR}/local-news-analyze-changed-relation-smoke.json"
```

Expected result: smoke output has `"ok": true`, `failure_codes` is empty, the
status artifact hash matches the news-analysis artifact hash, and every required
changed relation is observed.

## 5. Local Completion Checklist

- `build-market-graph-store.json` exists and points to the local artifact path.
- `market_graph_store_manifest.json` records artifact identity, lane hashes,
  validation summary, warning counts, and promotion status.
- `validate-market-graph-source-lanes.json` exists and has no blocking source or
  relation failures.
- `pytest-artifact-validation.txt` exits `0`.
- `pytest-news-analysis-goldens.txt` exits `0`.
- `evaluate-impact-news-analysis-golden.json` exits `0` when the BDYA reviewed
  JSONL exists.
- `local-news-analyze-smoke.json` exits `0` and has `"ok": true`.
- No production deploy, production apply, object-storage publish, remote restart,
  or production pointer rewrite command was run.
