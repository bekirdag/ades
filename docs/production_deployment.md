# Production Deployment

This repository now has a live production deployment with:

- API base URL: `https://adestool.com`
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
  - static registry built from `src/ades/resources/registry/packs/*`
- Remote deploy flow:
  - upload wheel and registry release
  - install wheel into `/mnt/ades/app/shared/.venv`
  - move `/mnt/ades/repo/current` to the new registry release
  - restart `ades.service`
  - verify local and public health endpoints

Required GitHub secrets:

- `ADES_PROD_SSH_KEY`
- `ADES_PROD_KNOWN_HOSTS`

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
ades tag "Apple shares rose after strong quarterly guidance."
```

`ades packs list --available` remains available for the explicit legacy path, while `ades list packs` is the shorter default for registry pack discovery.

