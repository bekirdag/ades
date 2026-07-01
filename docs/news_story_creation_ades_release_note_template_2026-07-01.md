# ADES News Story Creation Release Note Template

Status: Template, local release-preparation artifact

Use this template for ADES market graph, source-lane, or `/v0/news/analyze`
artifact releases that affect BDYA news story creation. Replace every
`[REQUIRED: ...]` value before release approval. Use `none` only when the
section truly has no entries.

## Release Identity

- Release ID: `[REQUIRED: release identifier]`
- Release owner: `[REQUIRED: owner name or team]`
- Release date: `[REQUIRED: YYYY-MM-DD]`
- Target environment: `[REQUIRED: local, staging, public ADES, production]`
- Deployment status: `[REQUIRED: not deployed, staged, deployed, rolled back]`
- Rollback artifact/reference: `[REQUIRED: previous artifact id/hash or runbook link]`

## Artifact Provenance

- Artifact ID: `[REQUIRED: artifact_id]`
- Artifact version: `[REQUIRED: artifact_version]`
- Artifact hash: `[REQUIRED: sha256:...]`
- Artifact built at: `[REQUIRED: ISO-8601 timestamp]`
- Artifact deployed at: `[REQUIRED: ISO-8601 timestamp or not_deployed]`
- Source git SHA: `[REQUIRED: git commit SHA]`
- Build host: `[REQUIRED: host or CI runner]`
- Artifact path or registry key: `[REQUIRED: path/key]`
- Manifest path: `[REQUIRED: manifest path]`
- Promotion status: `[REQUIRED: proposal, validated, promoted, blocked]`

## Source Lane Versions

Record this section from the manifest `source_lane_versions` map plus per-lane
hashes/counts.

| Lane | Version | Lane hash | Source rows | Production rows | Notes |
| --- | --- | --- | ---: | ---: | --- |
| `[REQUIRED: lane name]` | `[REQUIRED: version]` | `[REQUIRED: sha256 or manifest hash]` | `[REQUIRED]` | `[REQUIRED]` | `[REQUIRED]` |

## Changed Countries

| Country/region | Change type | Lanes changed | Expected BDYA impact | Replay/golden coverage |
| --- | --- | --- | --- | --- |
| `[REQUIRED: country or region]` | `[REQUIRED: added, updated, removed, unchanged]` | `[REQUIRED: lane names]` | `[REQUIRED: terminal paths or no-terminal coverage]` | `[REQUIRED: case names or none]` |

## Graph and Artifact Counts

- Node count: `[REQUIRED]`
- Edge count: `[REQUIRED]`
- Source row count: `[REQUIRED]`
- Terminal candidate path families affected: `[REQUIRED: list or none]`
- Removed/expired relationships: `[REQUIRED: list or none]`

## Validation Summary

| Check | Command or evidence | Result | Blocking failures |
| --- | --- | --- | --- |
| Schema tests | `[REQUIRED]` | `[REQUIRED: pass/fail/skipped]` | `[REQUIRED: failures or none]` |
| Source-lane validation | `[REQUIRED]` | `[REQUIRED: pass/fail/skipped]` | `[REQUIRED: failures or none]` |
| Graph build | `[REQUIRED]` | `[REQUIRED: pass/fail/skipped]` | `[REQUIRED: failures or none]` |
| Local `/v0/news/analyze` smoke | `[REQUIRED]` | `[REQUIRED: pass/fail/skipped]` | `[REQUIRED: failures or none]` |
| Public ADES smoke | `[REQUIRED]` | `[REQUIRED: pass/fail/skipped/not_run]` | `[REQUIRED: failures or none]` |

## Golden Results

| Golden suite/case | Scope | Expected result | Observed result | Status |
| --- | --- | --- | --- | --- |
| `[REQUIRED: suite or case]` | `[REQUIRED: country/lane/path family]` | `[REQUIRED]` | `[REQUIRED]` | `[REQUIRED: pass/fail/skipped]` |

Golden summary:

- Passed: `[REQUIRED: count]`
- Failed: `[REQUIRED: count]`
- Skipped: `[REQUIRED: count]`
- Known red gates accepted for this release: `[REQUIRED: list or none]`

## Known Warnings

| Warning code | Severity | Scope | Owner | Mitigation/follow-up |
| --- | --- | --- | --- | --- |
| `[REQUIRED: warning code or none]` | `[REQUIRED: info/warn/blocking]` | `[REQUIRED]` | `[REQUIRED]` | `[REQUIRED]` |

## BDYA Compatibility Notes

- Expected ADES artifact hash for BDYA config/smoke: `[REQUIRED: sha256:...]`
- Response fields verified for BDYA: `[REQUIRED: artifact metadata, terminal candidates, paths, diagnostics, warnings]`
- Changed no-terminal behavior: `[REQUIRED: summary or none]`
- Replay targets after release: `[REQUIRED: country/lane/news ids or none]`

## Release Decision

- Release recommendation: `[REQUIRED: approve, block, rollback, observe only]`
- Decision rationale: `[REQUIRED]`
- Approver: `[REQUIRED: name or process]`
- Post-release verification window: `[REQUIRED: duration and checks]`
