# ADES News Story Creation Approved Deploy Runbook

Status: Blocked until explicit release/deploy approval
Work item: NSC-I003
Scope: Future approved ADES production deploys that affect BDYA news story
creation

This runbook records the exact production deployment entry point, the workflow
steps, and the artifact paths to use after a separate approval process completes.
Do not execute any command in this runbook during local verification,
autonomous work-item execution, or review-only tasks.

## Approval Gate

Deployment execution is blocked until all of the following are true:

- The NSC-I002 local verification runbook has completed for the exact artifact
  or source-lane change being released.
- The release note template records artifact version, artifact hash, manifest
  path, validation output, golden output, and local `/v0/news/analyze` smoke
  output.
- A release owner explicitly approves production deployment for the exact git
  SHA and artifact identity.

Until those conditions are met, the only allowed action is to read this runbook.

## Production Deploy Entry Point

Production deploys are GitHub Actions-owned. The authoritative workflow is:

- Workflow file: `.github/workflows/deploy-prod.yml`
- Workflow name: `Deploy Production`
- Repository: `bekirdag/ades`
- Branch/ref for approved deploys: `main`
- Trigger: push to `main` or manual `workflow_dispatch`

Approved manual dispatch command:

```bash
gh workflow run deploy-prod.yml --repo bekirdag/ades --ref main
```

This command is recorded for the future approved deploy path only. It must not
be run from this work item.

## Workflow Build Steps

The approved workflow performs these local CI steps before any remote mutation:

```bash
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
python -m pytest -q
python -m build --wheel
```

It then builds the deploy market graph artifact:

```bash
mkdir -p dist/impact-market-graph
python - <<'PY'
from ades.impact.starter import build_starter_market_graph_store

response = build_starter_market_graph_store(output_dir="dist/impact-market-graph")
print(response.model_dump_json(indent=2))
PY
```

It prepares the deploy-owned static registry payload:

```bash
mkdir -p dist/registry
ades registry prepare-deploy-release --output-dir dist/registry
```

The workflow release identifier is generated as:

```bash
release_id="$(date -u +%Y%m%dT%H%M%SZ)-${GITHUB_SHA::12}"
```

## Production Artifact Paths

The deploy workflow uploads build outputs to these production host paths:

- Wheel release: `/mnt/ades/app/releases/<release_id>/<wheel_name>`
- Registry release: `/mnt/ades/repo/releases/<release_id>/`
- Impact graph release: `/mnt/ades/impact/releases/<release_id>/`
- Shared virtualenv install target: `/mnt/ades/app/shared/.venv`
- Active registry symlink: `/mnt/ades/repo/current`

The impact artifact uploaded by the workflow is:

```text
/mnt/ades/impact/releases/<release_id>/market_graph_store.sqlite
```

During install, the workflow resolves the active impact root from
`GET http://127.0.0.1:8734/v0/status` on the production host. If the service
already reports an `impact_artifact.configured_path`, the active target is that
path's parent directory. Otherwise the fallback active target is:

```text
/home/deploy/.local/share/ades-artifacts/market-graph/current/market_graph_store.sqlite
```

The public baseline captured for this pipeline currently reports this active
production impact artifact path:

```text
/home/deploy/.local/share/ades-artifacts/market-graph/20260513Tgeneric-market-relationship-r3/market_graph_store.sqlite
```

After an approved deployment, verify the post-deploy active path from public
status instead of assuming the fallback path:

```bash
curl -fsS --connect-timeout 10 --max-time 30 https://api.adestool.com/v0/status
```

The expected response field is
`impact_artifact.configured_path`, with `impact_artifact.exists=true` and
`impact_artifact.readable=true`.

## Remote Install And Activation Steps

The workflow performs these production mutations after artifact upload:

- Installs the wheel into `/mnt/ades/app/shared/.venv`.
- Ensures `psycopg[binary]>=3.2,<4.0` is installed in that shared virtualenv.
- Writes pack health settings under
  `/home/deploy/.config/systemd/user/ades.service.d/pack-health.conf`.
- Copies or merges the release impact graph into the active impact artifact path.
- Writes
  `/home/deploy/.config/systemd/user/ades.service.d/zz-impact-expansion.conf`
  with the active `ADES_IMPACT_EXPANSION_ARTIFACT_PATH`.
- Writes
  `/home/deploy/.config/systemd/user/ades.service.d/zz-news-analyze.conf`
  with `ADES_NEWS_ANALYZE_ENABLED=true`.
- Switches `/mnt/ades/repo/current` to the new registry release unless the
  workflow preserves a larger existing registry and merges only release packs.
- Runs `systemctl --user daemon-reload`.
- Restarts `ades.service`.

These steps are not local commands. They are executed only by the approved
GitHub Actions production workflow.

## Post-Deploy Verification

The workflow verifies production health with:

```bash
curl --retry 10 --retry-all-errors --retry-delay 10 \
  --connect-timeout 5 --max-time 15 -fsS \
  https://api.adestool.com/healthz >/dev/null
curl --retry 10 --retry-all-errors --retry-delay 10 \
  --connect-timeout 5 --max-time 15 -fsS \
  https://api.adestool.com/v0/status >/dev/null
curl --retry 10 --retry-all-errors --retry-delay 10 \
  --connect-timeout 5 --max-time 15 -fsS \
  https://api.adestool.com/v0/packs/available >/dev/null
curl --retry 10 --retry-all-errors --retry-delay 10 \
  --connect-timeout 5 --max-time 15 -fsS \
  https://adestool.com/healthz >/dev/null
curl --retry 10 --retry-all-errors --retry-delay 10 \
  --connect-timeout 5 --max-time 15 -fsS \
  https://adestool.com/v0/status >/dev/null
curl --retry 10 --retry-all-errors --retry-delay 10 \
  --connect-timeout 5 --max-time 15 -fsS \
  https://adestool.com/v0/packs/available >/dev/null
curl --retry 10 --retry-all-errors --retry-delay 10 \
  --connect-timeout 5 --max-time 15 -fsS \
  https://repo.adestool.com/index.json >/dev/null
```

For BDYA news-story releases, approval also requires a post-deploy public
`/v0/news/analyze` smoke that confirms terminal candidates for the release's
changed path families and confirms the public `artifact_hash` matches the
approved release note.

## Rollback Reference

The baseline artifact identity captured before this runbook is recorded in
`docs/public_ades_artifact_identity_baseline_2026-06-30.md`. Use that document
as the rollback comparison reference before approving any new production
artifact.
