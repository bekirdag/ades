# Raijin website and runtime repairs

The homepage source is `website/index.html`. Production serves it from
`/mnt/ades/website/current/index.html` using an exact `/` location in
`/etc/nginx-raijin-webapps/nginx.conf`. Existing `/v0/*`, `/docs`, and `/healthz`
requests continue through the API on loopback port 8734.

The API and Qdrant run under the deploy user as `ades.service` and
`qdrant.service`. Both are enabled, with user lingering enabled.

## Repairs

- `src/ades/vector/service.py` tolerates a graph seed without a matching extracted
  entity. This prevents the missing-label exception in news analysis.
- `src/ades/service/app.py` runs status checks in a dedicated executor, keeping
  readiness requests independent of the AnyIO extraction worker limit.

The production installation uses an older wheel. Only these two scoped changes
were applied to its installed Python files; unrelated repository changes were
not deployed. Build future releases from source containing these fixes so a
wheel upgrade does not remove them. `deploy-raijin.py` records the one-time
installation and intentionally refuses to reapply the patches to modified files.

## Backup and rollback

Original runtime files and origin Nginx configuration are saved in
`/mnt/ades/app/hotfix-backups/20261014T100140Z/`.
Restore `service/app.py` and `vector/service.py` from that directory into
`/mnt/ades/app/shared/.venv/lib/python3.14/site-packages/ades/`, restore `nginx.conf`
into `/etc/nginx-raijin-webapps/nginx.conf`, validate Nginx, restart the deploy
user's `ades.service`, and reload `raijin-webapps-nginx.service`.

## Verification (2026-10-14)

73 targeted source regression tests passed. The two new regression tests also
passed against the installed production package. The isolation test occupies all
AnyIO extraction worker slots and still receives a successful status response.
Public homepage and www alias return HTTP 200 with the expected HTML; API docs
and repository remain accessible. Public status returned HTTP 200 in about
0.8 seconds, with overall readiness true and no warnings. A public news analysis
request returned HTTP 200 in about 8.5 seconds. Both services are active with no
automatic restarts. No browser automation was available for visual QA.
