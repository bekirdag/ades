# Public ADES Artifact Identity Baseline

Work item: NSC-0002
Captured at: 2026-06-30T13:04:43Z
Public endpoint checked: `https://api.adestool.com`
Alias endpoint checked: `https://adestool.com`

## Result

The public ADES endpoint exposes the currently served impact artifact identity through
`GET /v0/status` under `impact_artifact.metadata`, and the graph client endpoint
`POST /v0/impact/expand` returns the same impact artifact version/hash.

There is no separate top-level `artifact_id` field in either response today. The
served artifact identity is therefore recorded as the exposed `artifact_version`,
`artifact_hash`, configured artifact path, and build/modified timestamps.

## Served Impact Artifact

- Artifact version: `20260513Tgeneric-market-relationship-r3+starter-20260629T054955Z-3711da7723f7`
- Artifact hash: `sha256:044236818a45eba47bd9a46d2147ad0529438ea5b05db537c78ac77dd1433c32`
- Artifact generated at: `2026-05-13T19:14:48+00:00`
- Artifact modified at: `2026-06-29T05:52:02.777185Z`
- Configured path: `/home/deploy/.local/share/ades-artifacts/market-graph/20260513Tgeneric-market-relationship-r3/market_graph_store.sqlite`
- Size: `2518016000` bytes
- Schema version: `1`
- Item count: `247010`
- Node count: `247010`
- Edge count: `1764906`
- Pack count: `19`
- Status flags: `enabled=true`, `exists=true`, `readable=true`

## Service Context

- Service: `ades`
- Service version: `0.3.1`
- Runtime target: `production_server`
- Metadata backend: `postgresql`
- Metadata persistence backend: `postgresql`
- Exact extraction backend: `compiled_matcher`
- Operator lookup backend: `postgresql_search`
- Storage root: `/mnt/ades/repo/current`
- Registry URL: `https://repo.adestool.com/index.json`
- BDYA impact readiness: `ready=true`, `required_artifacts_ready=true`, `vector_ready=true`, `metadata_ready=true`

## Corroborating Endpoint Evidence

### `GET /healthz`

Command:

```bash
curl -fsS --connect-timeout 10 --max-time 20 https://api.adestool.com/healthz
```

Observed response:

```json
{"status":"ok","version":"0.3.1"}
```

### `GET /v0/status`

Command:

```bash
curl -fsS --connect-timeout 10 --max-time 30 https://api.adestool.com/v0/status
```

Relevant observed fields:

```json
{
  "service": "ades",
  "version": "0.3.1",
  "runtime_target": "production_server",
  "metadata_backend": "postgresql",
  "metadata_persistence_backend": "postgresql",
  "storage_root": "/mnt/ades/repo/current",
  "registry_url": "https://repo.adestool.com/index.json",
  "impact_artifact": {
    "enabled": true,
    "configured_path": "/home/deploy/.local/share/ades-artifacts/market-graph/20260513Tgeneric-market-relationship-r3/market_graph_store.sqlite",
    "exists": true,
    "readable": true,
    "size_bytes": 2518016000,
    "modified_at": "2026-06-29T05:52:02.777185Z",
    "generated_at": "2026-05-13T19:14:48+00:00",
    "schema_version": 1,
    "item_count": 247010,
    "metadata": {
      "artifact_version": "20260513Tgeneric-market-relationship-r3+starter-20260629T054955Z-3711da7723f7",
      "artifact_hash": "sha256:044236818a45eba47bd9a46d2147ad0529438ea5b05db537c78ac77dd1433c32",
      "node_count": 247010,
      "edge_count": 1764906,
      "pack_count": 19
    },
    "warnings": []
  },
  "bdya_impact_readiness": {
    "ready": true,
    "required_artifacts_ready": true,
    "vector_ready": true,
    "metadata_ready": true,
    "warnings": []
  }
}
```

The alias endpoint `https://adestool.com/v0/status` returned the same artifact
version, hash, generated timestamp, modified timestamp, and service context.

### `POST /v0/impact/expand`

Command:

```bash
curl -fsS --connect-timeout 10 --max-time 30 \
  -H 'content-type: application/json' \
  -d '{"entity_refs":["wikidata:Q63132"],"enabled_packs":["finance-en"],"max_depth":2,"max_candidates":5}' \
  https://api.adestool.com/v0/impact/expand
```

Relevant observed fields:

```json
{
  "graph_version": "market-graph-v1",
  "artifact_version": "20260513Tgeneric-market-relationship-r3+starter-20260629T054955Z-3711da7723f7",
  "artifact_hash": "sha256:044236818a45eba47bd9a46d2147ad0529438ea5b05db537c78ac77dd1433c32",
  "warnings": [],
  "candidates": [],
  "passive_paths": []
}
```
