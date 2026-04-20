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
- the following `ades` config values in `/etc/ades/config.toml`:
  - `vector_search_enabled = true`
  - `vector_search_url = "http://127.0.0.1:6333"`
  - `vector_search_collection_alias = "ades-qids-current"`
  - optional tuning such as `vector_search_related_limit`

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
