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
- Wheel releases: `/mnt/ades/app/releases/<release_id>/`
- Registry releases: `/mnt/ades/repo/releases/<release_id>/`
- Active registry symlink: `/mnt/ades/repo/current`

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
  - verify local and public health endpoints

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
