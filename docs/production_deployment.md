# Production Deployment

This repository now has a live production deployment with:

- API base URL: `https://api.adestool.com`
- Registry index: `https://repo.adestool.com/index.json`
- Production runtime: PostgreSQL-backed `ades` service behind `nginx`
- Registry/static pack host: `repo.adestool.com`

## Server Layout

- API config: `/etc/ades/config.toml`
- Runtime storage root: `/mnt/ades/storage`
- Shared virtualenv: `/mnt/ades/app/shared/.venv`
- Service unit: `deploy` user `systemd --user` service `ades.service`
- Vector service root: `/home/deploy/.local/share/qdrant`
- Vector service unit: `deploy` user `systemd --user` service `qdrant.service`
- Wheel releases: `/mnt/ades/app/releases/<release_id>/`
- Registry releases: `/mnt/ades/repo/releases/<release_id>/`
- Active registry symlink: `/mnt/ades/repo/current`

## Production Vector Runtime

Hosted vector enrichment is part of the live production runtime and is not enabled by
default merely by switching `runtime_target` to `production_server`.

The production host must provide all of the following:

- a running local Qdrant HTTP endpoint on `127.0.0.1:6333`
- a published collection alias named `ades-qids-current`
- the following `ades` runtime settings, provided either through `/etc/ades/config.toml`
  or the active `systemd --user` drop-ins:
  - `vector_search_enabled = true`
  - `vector_search_url = "http://127.0.0.1:6333"`
  - `vector_search_collection_alias = "ades-qids-current"`
  - optional tuning such as `vector_search_related_limit`

Current live host note:

- the deployed `ades.service` currently gets the vector flags from
  `/home/deploy/.config/systemd/user/ades.service.d/vector.conf`
  rather than from `/etc/ades/config.toml`
- verify the active values with:

```bash
systemctl --user cat ades.service
systemctl --user show ades.service --property=Environment --property=EnvironmentFiles
```

Recommended verification commands on the production host:

```bash
curl -fsS http://127.0.0.1:6333/collections/ades-qids-current
python3 - <<'PY'
import json
import urllib.request

request = urllib.request.Request(
    "http://127.0.0.1:8734/v0/tag",
    data=json.dumps(
        {
            "text": "Anthropic announced a partnership with Amazon in San Francisco.",
            "pack": "general-en",
            "options": {
                "include_related_entities": True,
                "include_graph_support": True,
                "refine_links": True,
                "refinement_depth": "deep",
            },
        }
    ).encode("utf-8"),
    headers={"content-type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(request, timeout=60) as response:
    payload = json.load(response)
print(json.dumps(payload.get("graph_support"), indent=2, sort_keys=True))
PY
```

The `/v0/tag` HTTP surface reads vector switches from the request `options` object.
Sending `include_related_entities` or `include_graph_support` as top-level JSON fields
does not activate the hosted vector lane on the service endpoint.

## Production Graph Context Runtime

The graph-context lane is now the first hosted path for graph-backed suppression and
metadata-backed implied-entity discovery. It is still feature-flagged and must be
enabled explicitly in the live service environment.

Required runtime values:

- `graph_context_enabled = true`
- `graph_context_artifact_path = "/home/deploy/.local/share/ades-artifacts/graph-store/current/qid_graph_store.sqlite"`
- optional tuning:
  - `graph_context_max_depth = 2`
  - `graph_context_related_limit = 5`
  - `graph_context_seed_neighbor_limit = 24`
  - `graph_context_candidate_limit = 128`
  - `graph_context_min_supporting_seeds = 2`
  - `graph_context_genericity_penalty_enabled = true`

Operational notes:

- the current staged artifact location is `/home/deploy/.local/share/ades-artifacts/graph-store/current`
- the runtime expects a graph store built with the richer `node_stats` table introduced by the entity-context work
- the runtime now tolerates migrated/legacy graph artifacts whose `nodes` table is missing optional metadata columns such as `popularity`; missing fields are treated as `null` rather than crashing the request path
- the preferred rollout path on the current host is a new `systemd --user` drop-in such as
  `/home/deploy/.config/systemd/user/ades.service.d/graph-context.conf` with:

```ini
[Service]
Environment=ADES_GRAPH_CONTEXT_ENABLED=true
Environment=ADES_GRAPH_CONTEXT_ARTIFACT_PATH=/home/deploy/.local/share/ades-artifacts/graph-store/current/qid_graph_store.sqlite
Environment=ADES_GRAPH_CONTEXT_MAX_DEPTH=2
Environment=ADES_GRAPH_CONTEXT_RELATED_LIMIT=5
Environment=ADES_GRAPH_CONTEXT_SEED_NEIGHBOR_LIMIT=24
Environment=ADES_GRAPH_CONTEXT_CANDIDATE_LIMIT=128
Environment=ADES_GRAPH_CONTEXT_MIN_SUPPORTING_SEEDS=2
Environment=ADES_GRAPH_CONTEXT_GENERICITY_PENALTY_ENABLED=true
```

- after adding or changing the drop-in, run:

```bash
systemctl --user daemon-reload
systemctl --user restart ades.service
systemctl --user show ades.service --property=Environment
```

- graph-backed implied entities are intentionally limited to metadata-backed candidates; anonymous adjacent graph hubs are not expected to surface in `related_entities`
- `include_related_entities = true` is now enough to activate the graph-context related-entity path when the feature flag is enabled; callers do not need to force `refine_links = true`

Recommended host-local verification:

```bash
python3 - <<'PY'
import json
import urllib.request

request = urllib.request.Request(
    "http://127.0.0.1:8734/v0/tag",
    data=json.dumps(
        {
            "text": "OpenAI and Microsoft announced a joint AI initiative in Washington.",
            "pack": "general-en",
            "options": {
                "include_related_entities": True,
                "include_graph_support": True,
            },
        }
    ).encode("utf-8"),
    headers={"content-type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(request, timeout=60) as response:
    payload = json.load(response)

graph_support = payload.get("graph_support") or {}
warnings = list(graph_support.get("warnings") or [])
if not graph_support.get("applied"):
    raise SystemExit(f"graph context did not apply: {warnings}")
disallowed = {
    "graph_context_disabled",
    "graph_context_artifact_missing",
    "no_wikidata_seed_entities",
}
if disallowed.intersection(warnings):
    raise SystemExit(f"graph context smoke check failed: {warnings}")
print(json.dumps(graph_support, indent=2, sort_keys=True))
print(json.dumps(payload.get("related_entities"), indent=2, sort_keys=True))
PY
```

## Production Impact Expansion Runtime

The market impact expansion lane is a separate optional artifact from the QID
graph-context store. It is disabled by default and only runs when callers request
impact paths.

Required runtime values when enabled:

- `impact_expansion_enabled = true`
- `impact_expansion_artifact_path = "/home/deploy/.local/share/ades-artifacts/market-graph/current/market_graph_store.sqlite"`
- optional tuning:
  - `impact_expansion_max_depth = 2`
  - `impact_expansion_seed_limit = 16`
  - `impact_expansion_max_candidates = 25`
  - `impact_expansion_max_edges_per_seed = 64`
  - `impact_expansion_max_paths_per_candidate = 3`
  - `impact_expansion_vector_proposals_enabled = false`

Suggested `systemd --user` drop-in:

```ini
[Service]
Environment=ADES_IMPACT_EXPANSION_ENABLED=true
Environment=ADES_IMPACT_EXPANSION_ARTIFACT_PATH=/home/deploy/.local/share/ades-artifacts/market-graph/current/market_graph_store.sqlite
Environment=ADES_IMPACT_EXPANSION_MAX_DEPTH=2
Environment=ADES_IMPACT_EXPANSION_SEED_LIMIT=16
Environment=ADES_IMPACT_EXPANSION_MAX_CANDIDATES=25
Environment=ADES_IMPACT_EXPANSION_MAX_EDGES_PER_SEED=64
Environment=ADES_IMPACT_EXPANSION_MAX_PATHS_PER_CANDIDATE=3
Environment=ADES_IMPACT_EXPANSION_VECTOR_PROPOSALS_ENABLED=false
```

The artifact is read-only at request time. Missing or unreadable artifacts should
return empty impact candidates with a warning instead of breaking extraction.

The deploy workflow now builds the packaged starter market graph from
`src/ades/resources/impact/phase1_starter`, uploads it under
`/mnt/ades/impact/releases/<release_id>/`, copies it to
`/home/deploy/.local/share/ades-artifacts/market-graph/current/`, and writes the
`impact-expansion.conf` drop-in before restarting `ades.service`.

Latest verified live smoke on `2026-04-21` using `/home/deploy/deleteme.txt`:

- host-local plain `POST /v0/tag/file`: `29` entities, `0` related entities, no graph metadata, about `14.485s`
- host-local flagged `POST /v0/tag/file`: `24` entities, `1` related entity, `graph_support.applied = true`, `suppressed_entity_count = 5`, about `13.698s`
- public plain `POST https://api.adestool.com/v0/tag`: `29` entities, `0` related entities, no graph metadata, about `12.491s`
- public flagged `POST https://api.adestool.com/v0/tag`: `24` entities, `1` related entity, `graph_support.applied = true`, `suppressed_entity_count = 5`, about `12.765s`
- latest verified related entity for that sample:
  - `Americas` -> `wikidata:Q828`
  - supporting seeds: `wikidata:Q30`, `wikidata:Q49`

## CI/CD Model

Deployments are pipeline-owned.

- Workflow: `.github/workflows/deploy-prod.yml`
- Trigger: push to `main`
- Build outputs:
  - Python wheel
  - deploy-owned static registry prepared by `ades registry prepare-deploy-release --output-dir dist/registry`
- Remote deploy flow:
  - upload wheel and registry release
  - install wheel into `/mnt/ades/app/shared/.venv`
  - move `/mnt/ades/repo/current` to the new registry release
  - restart `ades.service`
  - verify local API health plus the live vector alias/tag smoke
  - verify public health endpoints

The production API service is expected to be always running under the deploy-owned
`systemd --user` unit `ades.service`, with the public API exposed on
`https://api.adestool.com`. `https://adestool.com` may continue to proxy the same
origin for backward compatibility, but `api.adestool.com` is the canonical API
hostname going forward.

Required GitHub secrets:

- `ADES_PROD_SSH_KEY`
- `ADES_PROD_KNOWN_HOSTS`

Optional GitHub secrets for S3-compatible static registry publication:

- `ADES_PACK_OBJECT_STORAGE_BUCKET`
- `ADES_PACK_OBJECT_STORAGE_ENDPOINT`
- `ADES_PACK_OBJECT_STORAGE_ACCESS_KEY_ID`
- `ADES_PACK_OBJECT_STORAGE_SECRET_ACCESS_KEY`

When those optional secrets are present, the production workflow keeps the existing SSH-based deploy flow and also syncs `dist/registry` to the configured object-storage bucket with `aws s3 sync --endpoint-url ...`. This is additive and does not replace the current server-owned registry release path.

Generated real-content pack refreshes use the same object-storage seam. The offline bundle/report/quality/refresh workflow is documented in `docs/library_pack_publication_workflow.md`; its `registry/` output can be published through `ades registry publish-generated-release` or the equivalent raw `aws s3 sync`, then promoted through the existing hosted registry path by committing `src/ades/resources/registry/promoted-release.json`. The deploy workflow now uses `ades registry prepare-deploy-release --output-dir dist/registry`, which falls back to the bundled starter packs when no promotion spec exists and otherwise smoke-checks plus materializes the approved reviewed registry URL into the normal deploy-owned registry layout. The first reviewed three-pack release is already directly consumer-readable at `https://ades.fsn1.your-objectstorage.com/generated-pack-releases/finance-general-medical-2026-04-10/index.json`.

## Local Client Config

To make the local CLI use the public registry by default, create `~/.config/ades/config.toml`:

```toml
[ades]
registry_url = "https://repo.adestool.com/index.json"
```

That keeps the local runtime in SQLite/local mode while pointing pack discovery and pulls at the hosted registry.

## Expected Local Commands

With the local config above:

```bash
ades status
ades list packs
ades pull finance-en
ades tag "Org Alpha issued updated guidance."
```

`ades packs list --available` remains available for the explicit legacy path, while `ades list packs` is the shorter default for registry pack discovery.
