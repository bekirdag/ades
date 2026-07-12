set -euo pipefail
release_id="$1"
wheel_name="$2"
release_root="/mnt/ades/repo/releases/$release_id"
pack_entry_count() {
  local manifest_path="$1"
  python3 - "$manifest_path" <<'PY'
import json
import sys

try:
    with open(sys.argv[1], encoding="utf-8") as handle:
        payload = json.load(handle)
    print(int((payload.get("matcher") or {}).get("entry_count") or 0))
except Exception:
    print(0)
PY
}
update_current_registry_index() {
  local current_index="$1"
  local release_index="$2"
  shift 2
  if [ "$#" -eq 0 ] || [ ! -f "$release_index" ]; then
    return 0
  fi
  python3 - "$current_index" "$release_index" "$@" <<'PY'
import json
import sys
from pathlib import Path

current_path = Path(sys.argv[1])
release_path = Path(sys.argv[2])
pack_ids = sys.argv[3:]

if current_path.exists():
    with current_path.open(encoding="utf-8") as handle:
        current = json.load(handle)
else:
    current = {"packs": {}}
with release_path.open(encoding="utf-8") as handle:
    release = json.load(handle)

current_packs = current.setdefault("packs", {})
release_packs = release.get("packs") or {}
for pack_id in pack_ids:
    if pack_id in release_packs:
        current_packs[pack_id] = release_packs[pack_id]

current_path.write_text(json.dumps(current, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}
copy_release_pack_artifacts() {
  local current_root="$1"
  local release_root="$2"
  local release_manifest="$3"
  python3 - "$current_root" "$release_root" "$release_manifest" <<'PY'
import json
import shutil
import sys
from pathlib import Path

current_root = Path(sys.argv[1]).resolve()
release_root = Path(sys.argv[2]).resolve()
manifest_path = Path(sys.argv[3]).resolve()

with manifest_path.open(encoding="utf-8") as handle:
    manifest = json.load(handle)

for artifact in manifest.get("artifacts") or []:
    url = str((artifact or {}).get("url") or "")
    if not url or "://" in url or url.startswith("/"):
        continue
    source = (manifest_path.parent / url).resolve()
    relative_source = source.relative_to(release_root)
    destination = current_root / relative_source
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
PY
}
merge_release_packs_into_current() {
  local current_root="$1"
  local release_root="$2"
  local release_id="$3"
  local release_packs_root="$release_root/packs"
  local current_packs_root="$current_root/packs"
  local backup_root="$current_root/.pack-backups/$release_id"
  local merged_count=0
  local merged_pack_ids=()

  if [ ! -d "$release_packs_root" ]; then
    return 0
  fi

  mkdir -p "$current_packs_root" "$backup_root"
  for release_pack_dir in "$release_packs_root"/*; do
    [ -d "$release_pack_dir" ] || continue
    local pack_id
    pack_id="$(basename "$release_pack_dir")"
    local release_manifest="$release_pack_dir/manifest.json"
    local current_pack_dir="$current_packs_root/$pack_id"
    local current_manifest="$current_pack_dir/manifest.json"
    local release_entries
    local current_entries
    local allow_entry_decrease=0
    release_entries="$(pack_entry_count "$release_manifest")"
    current_entries="$(pack_entry_count "$current_manifest")"

    if [ "$release_entries" -le 0 ]; then
      echo "Skipping release pack $pack_id because it has no matcher entries."
      continue
    fi
    case "$pack_id" in
      business-vector-en|economics-vector-en|politics-vector-en)
        allow_entry_decrease=1
        ;;
    esac
    if [ -f "$current_manifest" ] \
      && [ "$current_entries" -gt "$release_entries" ] \
      && [ "$allow_entry_decrease" -ne 1 ]; then
      echo "Skipping release pack $pack_id because current has $current_entries entries and release has $release_entries."
      continue
    fi
    if [ "$allow_entry_decrease" -eq 1 ] && [ "$current_entries" -gt "$release_entries" ]; then
      echo "Promoting release pack $pack_id despite entry-count decrease ($current_entries -> $release_entries)."
    fi

    local tmp_pack_dir="$current_packs_root/.${pack_id}.tmp-$release_id"
    rm -rf "$tmp_pack_dir"
    mkdir -p "$tmp_pack_dir"
    cp -a "$release_pack_dir/." "$tmp_pack_dir/"
    copy_release_pack_artifacts "$current_root" "$release_root" "$release_manifest"
    if [ -d "$current_pack_dir" ]; then
      rm -rf "$backup_root/$pack_id"
      mv "$current_pack_dir" "$backup_root/$pack_id"
    fi
    mv "$tmp_pack_dir" "$current_pack_dir"
    merged_count=$((merged_count + 1))
    merged_pack_ids+=("$pack_id")
    echo "Merged release pack $pack_id into preserved current registry ($release_entries entries)."
  done

  update_current_registry_index \
    "$current_root/index.json" \
    "$release_root/index.json" \
    "${merged_pack_ids[@]}"
  echo "Merged $merged_count release pack(s) into preserved current registry."
}
impact_edge_count() {
  local impact_root="$1"
  python3 - "$impact_root" <<'PY'
import json
import sqlite3
import sys
from pathlib import Path

root = Path(sys.argv[1])
manifest_path = root / "market_graph_store_manifest.json"
if manifest_path.exists():
    try:
        with manifest_path.open(encoding="utf-8") as handle:
            payload = json.load(handle)
        print(int(payload.get("edge_count") or 0))
        raise SystemExit(0)
    except Exception:
        pass
artifact_path = root / "market_graph_store.sqlite"
if artifact_path.exists():
    try:
        connection = sqlite3.connect(f"file:{artifact_path}?mode=ro", uri=True)
        try:
            print(int(connection.execute("SELECT COUNT(*) FROM impact_edges").fetchone()[0]))
            raise SystemExit(0)
        finally:
            connection.close()
    except Exception:
        pass
print(0)
PY
}
active_service_impact_root() {
  python3 - <<'PY'
import json
import urllib.request
from pathlib import Path

try:
    with urllib.request.urlopen("http://127.0.0.1:8734/v0/status", timeout=10) as response:
        payload = json.load(response)
    configured_path = str(
        ((payload.get("impact_artifact") or {}).get("configured_path") or "")
    ).strip()
except Exception:
    configured_path = ""

if configured_path:
    print(Path(configured_path).expanduser().parent)
PY
}
merge_starter_impact_graph() {
  local current_root="$1"
  local starter_root="$2"
  local release_id="$3"
  python3 - "$current_root" "$starter_root" "$release_id" <<'PY'
import hashlib
import json
import os
import shutil
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

current_root = Path(sys.argv[1])
starter_root = Path(sys.argv[2])
release_id = sys.argv[3]
current_db = current_root / "market_graph_store.sqlite"
starter_db = starter_root / "market_graph_store.sqlite"
if not current_db.exists() or not starter_db.exists():
    raise SystemExit("current and starter impact graph artifacts are both required")

tmp_db = current_root / f".market_graph_store.sqlite.{release_id}.tmp"
backup_db = current_root / f"market_graph_store.sqlite.before-{release_id}"
if tmp_db.exists():
    tmp_db.unlink()
shutil.copy2(current_db, tmp_db)

connection = sqlite3.connect(str(tmp_db))
try:
    connection.execute("ATTACH DATABASE ? AS starter", (str(starter_db),))
    before_nodes = int(connection.execute("SELECT COUNT(*) FROM impact_nodes").fetchone()[0])
    before_edges = int(connection.execute("SELECT COUNT(*) FROM impact_edges").fetchone()[0])
    current_edge_columns = {
        str(row[1])
        for row in connection.execute("PRAGMA table_info(impact_edges)").fetchall()
    }
    starter_edge_columns = {
        str(row[1])
        for row in connection.execute("PRAGMA starter.table_info(impact_edges)").fetchall()
    }
    for column_name in (
        "compatible_event_types_json",
        "direction_preconditions_json",
    ):
        if column_name not in current_edge_columns:
            connection.execute(
                f"ALTER TABLE impact_edges ADD COLUMN {column_name} TEXT NOT NULL DEFAULT '[]'"
            )
            current_edge_columns.add(column_name)
    connection.execute(
        """
        INSERT OR REPLACE INTO impact_nodes (
          entity_ref,
          canonical_name,
          entity_type,
          library_id,
          is_tradable,
          is_seed_eligible,
          seed_degree,
          identifiers_json,
          packs_json
        )
        SELECT
          entity_ref,
          canonical_name,
          entity_type,
          library_id,
          is_tradable,
          is_seed_eligible,
          seed_degree,
          identifiers_json,
          packs_json
        FROM starter.impact_nodes
        """
    )
    compatible_event_sql = (
        "compatible_event_types_json"
        if "compatible_event_types_json" in starter_edge_columns
        else "'[]'"
    )
    direction_preconditions_sql = (
        "direction_preconditions_json"
        if "direction_preconditions_json" in starter_edge_columns
        else "'[]'"
    )
    connection.execute(
        f"""
        INSERT OR REPLACE INTO impact_edges (
          edge_id,
          source_ref,
          target_ref,
          relation,
          evidence_level,
          confidence,
          direction_hint,
          source_name,
          source_url,
          source_snapshot,
          source_year,
          refresh_policy,
          version,
          compatible_event_types_json,
          direction_preconditions_json
        )
        SELECT
          edge_id,
          source_ref,
          target_ref,
          relation,
          evidence_level,
          confidence,
          direction_hint,
          source_name,
          source_url,
          source_snapshot,
          source_year,
          refresh_policy,
          version,
          {compatible_event_sql},
          {direction_preconditions_sql}
        FROM starter.impact_edges
        """
    )
    connection.execute(
        """
        INSERT OR IGNORE INTO impact_edge_packs (edge_id, pack_id)
        SELECT edge_id, pack_id
        FROM starter.impact_edge_packs
        """
    )
    connection.execute(
        """
        UPDATE impact_nodes
        SET seed_degree = (
          SELECT COUNT(*)
          FROM impact_edges
          WHERE impact_edges.source_ref = impact_nodes.entity_ref
        )
        """
    )
    node_count = int(connection.execute("SELECT COUNT(*) FROM impact_nodes").fetchone()[0])
    edge_count = int(connection.execute("SELECT COUNT(*) FROM impact_edges").fetchone()[0])
    metadata = connection.execute(
        """
        SELECT graph_version, artifact_version, artifact_hash, builder_version, source_manifest_hash
        FROM build_metadata
        WHERE id = 1
        """
    ).fetchone()
    built_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    if metadata:
        graph_version, artifact_version, artifact_hash, builder_version, source_hash = metadata
        next_version = f"{artifact_version}+starter-{release_id}"
        next_hash = "sha256:" + hashlib.sha256(
            f"{artifact_hash}:{release_id}:{node_count}:{edge_count}".encode("utf-8")
        ).hexdigest()
        connection.execute(
            """
            UPDATE build_metadata
            SET artifact_version = ?,
                artifact_hash = ?,
                built_at = ?,
                node_count = ?,
                edge_count = ?
            WHERE id = 1
            """,
            (next_version, next_hash, built_at, node_count, edge_count),
        )
    else:
        next_version = f"starter-merged-{release_id}"
        next_hash = "sha256:" + hashlib.sha256(
            f"{release_id}:{node_count}:{edge_count}".encode("utf-8")
        ).hexdigest()
        connection.execute(
            """
            INSERT INTO build_metadata (
              id,
              graph_version,
              artifact_version,
              artifact_hash,
              builder_version,
              built_at,
              source_manifest_hash,
              node_count,
              edge_count
            ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "market-graph-v1",
                next_version,
                next_hash,
                "market-graph-builder-v1",
                built_at,
                next_hash,
                node_count,
                edge_count,
            ),
        )
    connection.commit()
finally:
    connection.close()

if not backup_db.exists():
    shutil.copy2(current_db, backup_db)
os.replace(tmp_db, current_db)

manifest_path = current_root / "market_graph_store_manifest.json"
if manifest_path.exists():
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        payload = {}
else:
    payload = {}
payload.update(
    {
        "artifact_path": str(current_db),
        "artifact_version": next_version,
        "artifact_hash": next_hash,
        "built_at": built_at,
        "node_count": node_count,
        "edge_count": edge_count,
        "starter_merge_release_id": release_id,
    }
)
manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
print(
    json.dumps(
        {
            "before_nodes": before_nodes,
            "before_edges": before_edges,
            "after_nodes": node_count,
            "after_edges": edge_count,
            "inserted_nodes": node_count - before_nodes,
            "inserted_edges": edge_count - before_edges,
        },
        sort_keys=True,
    )
)
PY
}
/mnt/ades/app/shared/.venv/bin/python -m pip install --disable-pip-version-check --force-reinstall --no-deps "/mnt/ades/app/releases/$release_id/$wheel_name"
/mnt/ades/app/shared/.venv/bin/python -m pip install --disable-pip-version-check --upgrade "psycopg[binary]>=3.2,<4.0"
service_dropin_dir="$HOME/.config/systemd/user/ades.service.d"
mkdir -p "$service_dropin_dir" /mnt/ades/storage/registry
cat > "$service_dropin_dir/pack-health.conf" <<EOF
[Service]
Environment=ADES_PACK_HEALTH_DB_PATH=/mnt/ades/storage/registry/pack_health.sqlite3
EOF
impact_release_root="/mnt/ades/impact/releases/$release_id"
legacy_impact_root="$(active_service_impact_root)"
impact_current_root="$HOME/.local/share/ades-artifacts/market-graph/current"
if [ -f "$impact_release_root/market_graph_store.sqlite" ]; then
  mkdir -p "$impact_current_root" "$service_dropin_dir"
  switch_impact_current=1
  current_root_resolved="$(readlink -f "$impact_current_root" 2>/dev/null || true)"
  legacy_root_resolved=""
  if [ -n "$legacy_impact_root" ]; then
    legacy_root_resolved="$(readlink -f "$legacy_impact_root" 2>/dev/null || true)"
  fi
  if [ -n "$legacy_impact_root" ] \
    && [ -f "$legacy_impact_root/market_graph_store.sqlite" ] \
    && { [ -z "$legacy_root_resolved" ] || [ "$legacy_root_resolved" != "$current_root_resolved" ]; }; then
    existing_current_edges="$(impact_edge_count "$impact_current_root")"
    legacy_impact_edges="$(impact_edge_count "$legacy_impact_root")"
    if [ "$legacy_impact_edges" -gt "$existing_current_edges" ]; then
      echo "Seeding stable current impact graph from previously active root $legacy_impact_root ($legacy_impact_edges edges)."
      cp "$legacy_impact_root/market_graph_store.sqlite" "$impact_current_root/market_graph_store.sqlite"
      if [ -f "$legacy_impact_root/market_graph_store_manifest.json" ]; then
        cp "$legacy_impact_root/market_graph_store_manifest.json" "$impact_current_root/market_graph_store_manifest.json"
      fi
    fi
  fi
  existing_impact_edges="$(impact_edge_count "$impact_current_root")"
  new_impact_edges="$(impact_edge_count "$impact_release_root")"
  if [ "$existing_impact_edges" -ge 1000000 ] && [ "$new_impact_edges" -lt 1000000 ]; then
    echo "Prepared deploy impact graph is starter-sized ($new_impact_edges edges); merging starter rows into existing current graph with $existing_impact_edges edges."
    merge_starter_impact_graph "$impact_current_root" "$impact_release_root" "$release_id"
    switch_impact_current=0
  fi
  if [ "$switch_impact_current" -eq 1 ]; then
    cp "$impact_release_root/market_graph_store.sqlite" "$impact_current_root/market_graph_store.sqlite"
    if [ -f "$impact_release_root/market_graph_store_manifest.json" ]; then
      cp "$impact_release_root/market_graph_store_manifest.json" "$impact_current_root/market_graph_store_manifest.json"
    fi
  fi
  touch "$impact_current_root/market_graph_store.sqlite"
  if [ -f "$impact_current_root/market_graph_store_manifest.json" ]; then
    touch "$impact_current_root/market_graph_store_manifest.json"
  fi
  rm -f "$service_dropin_dir/impact-expansion.conf" "$service_dropin_dir/news-analyze.conf"
  cat > "$service_dropin_dir/zz-impact-expansion.conf" <<EOF
[Service]
Environment=ADES_IMPACT_EXPANSION_ENABLED=true
Environment=ADES_IMPACT_EXPANSION_ARTIFACT_PATH=$impact_current_root/market_graph_store.sqlite
Environment=ADES_IMPACT_EXPANSION_MAX_DEPTH=2
Environment=ADES_IMPACT_EXPANSION_SEED_LIMIT=16
Environment=ADES_IMPACT_EXPANSION_MAX_CANDIDATES=25
Environment=ADES_IMPACT_EXPANSION_MAX_EDGES_PER_SEED=64
Environment=ADES_IMPACT_EXPANSION_MAX_PATHS_PER_CANDIDATE=3
Environment=ADES_IMPACT_EXPANSION_VECTOR_PROPOSALS_ENABLED=false
EOF
  cat > "$service_dropin_dir/zz-news-analyze.conf" <<EOF
[Service]
Environment=ADES_NEWS_ANALYZE_ENABLED=true
EOF
  XDG_RUNTIME_DIR=/run/user/$(id -u) systemctl --user daemon-reload
fi
previous_repo_current="$(readlink -f /mnt/ades/repo/current 2>/dev/null || true)"
switch_repo_current=1
if [ -n "$previous_repo_current" ] \
  && [ -f "$previous_repo_current/packs/general-en/manifest.json" ] \
  && [ -f "$release_root/packs/general-en/manifest.json" ]; then
  existing_general_entries="$(pack_entry_count "$previous_repo_current/packs/general-en/manifest.json")"
  new_general_entries="$(pack_entry_count "$release_root/packs/general-en/manifest.json")"
  if [ "$existing_general_entries" -ge 1000000 ] && [ "$new_general_entries" -lt 1000000 ]; then
    echo "Prepared deploy registry has starter general-en ($new_general_entries entries); preserving existing repo current with $existing_general_entries entries."
    merge_release_packs_into_current "$previous_repo_current" "$release_root" "$release_id"
    switch_repo_current=0
  fi
fi
if [ "$switch_repo_current" -eq 1 ]; then
  ln -sfn "$release_root" /mnt/ades/repo/current
fi
/mnt/ades/app/shared/.venv/bin/python - <<'PY'
from ades.config import get_settings
from ades.packs.registry import PackRegistry

expected_versions = {
    "business-vector-en": "0.2.0",
    "economics-vector-en": "0.2.0",
    "politics-vector-en": "0.2.1",
}

settings = get_settings()
registry = PackRegistry(
    settings.storage_root,
    runtime_target=settings.runtime_target,
    metadata_backend=settings.metadata_backend,
    database_url=settings.database_url,
)
for pack_id, expected_version in expected_versions.items():
    manifest_path = settings.storage_root / "packs" / pack_id / "manifest.json"
    if not manifest_path.is_file():
        raise SystemExit(f"missing promoted pack manifest after deploy: {manifest_path}")
    if not registry.sync_pack_from_disk(pack_id, active=True):
        raise SystemExit(f"failed to sync promoted pack metadata: {pack_id}")
    refreshed = registry.get_pack(pack_id, active_only=False)
    if refreshed is None or refreshed.version != expected_version:
        raise SystemExit(
            "promoted pack metadata mismatch after sync: "
            f"{pack_id} expected={expected_version!r} got={getattr(refreshed, 'version', None)!r}"
        )
    print(f"Synced promoted pack metadata: {pack_id} {expected_version}")
PY
XDG_RUNTIME_DIR=/run/user/$(id -u) systemctl --user daemon-reload
XDG_RUNTIME_DIR=/run/user/$(id -u) systemctl --user restart ades.service
timeout 90s bash -c 'until curl --connect-timeout 5 --max-time 10 -fsS http://127.0.0.1:8734/healthz >/dev/null; do sleep 2; done'
python3 - <<'PY'
import json
import urllib.request

expected_versions = {
    "business-vector-en": "0.2.0",
    "economics-vector-en": "0.2.0",
    "politics-vector-en": "0.2.1",
}
with urllib.request.urlopen("http://127.0.0.1:8734/v0/packs/available", timeout=15) as response:
    packs = json.load(response)
pack_by_id = {pack.get("pack_id"): pack for pack in packs}
for pack_id, expected_version in expected_versions.items():
    pack = pack_by_id.get(pack_id) or {}
    if pack.get("version") != expected_version or pack.get("tier") != "domain":
        raise SystemExit(f"local service did not promote {pack_id}: {pack}")
PY
python3 - "$impact_current_root/market_graph_store.sqlite" <<'PY'
import json
import sys
import urllib.request
from pathlib import Path

expected_path = Path(sys.argv[1]).expanduser()
with urllib.request.urlopen("http://127.0.0.1:8734/v0/status", timeout=15) as response:
    payload = json.load(response)
artifact = payload.get("impact_artifact") or {}
configured_path = Path(str(artifact.get("configured_path") or "")).expanduser()
if configured_path != expected_path:
    raise SystemExit(
        f"active impact artifact mismatch: expected {expected_path}, got {configured_path}"
    )
if not artifact.get("exists") or not artifact.get("readable"):
    raise SystemExit(f"active impact artifact is not readable: {artifact}")
age_seconds = float(artifact.get("age_seconds") or 1_000_000_000)
if age_seconds > 7200:
    raise SystemExit(f"active impact artifact is stale after deploy: {artifact}")
metadata = artifact.get("metadata") or {}
edge_count = int(metadata.get("edge_count") or 0)
if edge_count <= 0:
    raise SystemExit(f"active impact artifact has no edges: {artifact}")
print(
    json.dumps(
        {
            "configured_path": str(configured_path),
            "age_seconds": age_seconds,
            "edge_count": edge_count,
            "version": artifact.get("generated_at"),
        },
        sort_keys=True,
    )
)
PY
curl --connect-timeout 5 --max-time 10 -fsS http://127.0.0.1:8734/v0/packs/available >/dev/null
curl --connect-timeout 5 --max-time 10 -fsS http://127.0.0.1:6333/collections/ades-qids-current >/dev/null
if command -v sudo >/dev/null 2>&1; then
  sudo -n systemctl reload nginx >/dev/null 2>&1 || sudo -n systemctl restart nginx >/dev/null 2>&1 || true
fi
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

graph_support = payload.get("graph_support") or {}
warnings = list(graph_support.get("warnings") or [])
if not graph_support.get("requested"):
    raise SystemExit("vector smoke check did not request graph support")
disallowed = {
    "vector_search_disabled",
    "vector_search_url_missing",
    "vector_search_production_only",
}
if disallowed.intersection(warnings):
    raise SystemExit(
        f"vector smoke check reported disabled production vector search: {warnings}"
    )
if any(str(item).startswith("vector_search_failed:") for item in warnings):
    raise SystemExit(
        f"vector smoke check reported qdrant failure: {warnings}"
    )
PY
