set -euo pipefail

current_root="$(readlink -f "${ADES_REPO_CURRENT_ROOT:-/mnt/ades/repo/current}")"
required_paths_file="$(mktemp)"
roots_file="$(mktemp)"
archive_file="$(mktemp)"
nginx_dump_file="$(mktemp)"
trap 'rm -f "$required_paths_file" "$roots_file" "$archive_file" "$nginx_dump_file"' EXIT

python3 - "$current_root" > "$required_paths_file" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1]).resolve()
index_path = root / "index.json"
target_versions = {
    "business-vector-en": "0.2.0",
    "economics-vector-en": "0.2.0",
    "politics-vector-en": "0.2.1",
}
required = ["index.json"]

with index_path.open(encoding="utf-8") as handle:
    index = json.load(handle)
packs = index.get("packs") or {}
for pack_id, expected_version in target_versions.items():
    entry = packs.get(pack_id) or {}
    version = entry.get("version")
    tier = entry.get("tier")
    if version != expected_version or tier != "domain":
        raise SystemExit(f"{pack_id} is not promoted in {index_path}: version={version!r}, tier={tier!r}")
    manifest_path = root / "packs" / pack_id / "manifest.json"
    if not manifest_path.is_file():
        raise SystemExit(f"missing promoted pack manifest: {manifest_path}")
    required.append(f"packs/{pack_id}")
    with manifest_path.open(encoding="utf-8") as handle:
        manifest = json.load(handle)
    artifacts = manifest.get("artifacts") or []
    if not artifacts:
        raise SystemExit(f"promoted pack has no artifacts in manifest: {pack_id}")
    for artifact in artifacts:
        url = str((artifact or {}).get("url") or "")
        if not url or "://" in url or url.startswith("/"):
            continue
        artifact_path = (manifest_path.parent / url).resolve()
        relative_artifact = artifact_path.relative_to(root).as_posix()
        if not artifact_path.is_file():
            raise SystemExit(f"missing promoted pack artifact: {artifact_path}")
        required.append(relative_artifact)

for path in dict.fromkeys(required):
    print(path)
PY

detect_nginx_roots() {
  if ! command -v sudo >/dev/null 2>&1 || ! sudo -n true >/dev/null 2>&1; then
    return 0
  fi
  if ! sudo -n nginx -T > "$nginx_dump_file" 2>/dev/null; then
    return 0
  fi
  python3 - "$nginx_dump_file" <<'PY'
import re
import sys
from pathlib import Path

text = Path(sys.argv[1]).read_text(encoding="utf-8", errors="ignore")
for match in re.finditer(r"\bserver\s*\{", text):
    start = match.start()
    depth = 0
    end = None
    for index in range(match.end() - 1, len(text)):
        char = text[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                end = index + 1
                break
    if end is None:
        continue
    block = text[start:end]
    if not re.search(r"\bserver_name\s+[^;]*\brepo\.adestool\.com\b", block):
        continue
    for directive in ("root", "alias"):
        for root in re.findall(rf"(?m)^\s*{directive}\s+([^;]+);", block):
            root = root.strip().strip('"').strip("'")
            if root and "$" not in root:
                print(root)
PY
}

verify_public_registry_url() {
  python3 - <<'PY'
import json
import time
import urllib.request

expected_versions = {
    "business-vector-en": "0.2.0",
    "economics-vector-en": "0.2.0",
    "politics-vector-en": "0.2.1",
}
url = f"https://repo.adestool.com/index.json?sync_verify={int(time.time())}"
request = urllib.request.Request(url, headers={"user-agent": "ades-public-registry-sync/1.0"})
with urllib.request.urlopen(request, timeout=30) as response:
    index = json.load(response)
packs = index.get("packs") or {}
for pack_id, expected_version in expected_versions.items():
    entry = packs.get(pack_id) or {}
    if entry.get("version") != expected_version or entry.get("tier") != "domain":
        raise SystemExit(
            f"public registry URL did not promote {pack_id}: "
            f"expected={expected_version!r} entry={entry!r}"
        )
PY
}

if [ -n "${ADES_PUBLIC_REGISTRY_ROOTS:-}" ]; then
  printf '%s\n' $ADES_PUBLIC_REGISTRY_ROOTS >> "$roots_file"
fi
detect_nginx_roots >> "$roots_file"
printf '%s\n' \
  /var/www/repo.adestool.com \
  /var/www/html/repo.adestool.com \
  /var/www/html \
  /var/www/adestool-repo \
  /srv/repo.adestool.com \
  /srv/adestool-repo \
  /usr/share/nginx/html \
  /home/deploy/repo.adestool.com \
  /home/deploy/repo.adestool.com/public \
  /home/deploy/www/repo.adestool.com \
  /home/deploy/www/repo.adestool.com/public \
  /home/deploy/.local/share/ades-artifacts/repo/current \
  /home/deploy/.local/share/ades-artifacts/repo/public \
  /mnt/ades/repo/current \
  /mnt/ades/storage \
  /mnt/ades/storage/registry \
  /mnt/ades/repo/public \
  >> "$roots_file"

tar -C "$current_root" -cf "$archive_file" --files-from "$required_paths_file"

synced=0
while IFS= read -r public_root; do
  [ -n "$public_root" ] || continue
  public_resolved="$(readlink -f "$public_root" 2>/dev/null || true)"
  if [ "$public_resolved" = "$current_root" ]; then
    echo "Public registry root already resolves to $current_root."
    synced=1
    continue
  fi
  if [ ! -d "$public_root" ]; then
    echo "Skipping missing public registry candidate: $public_root"
    continue
  fi
  if [ ! -f "$public_root/index.json" ]; then
    echo "Skipping public registry candidate without index.json: $public_root"
    continue
  fi

  echo "Syncing promoted registry files from $current_root to $public_root."
  if [ -w "$public_root" ]; then
    tar -C "$public_root" -xf "$archive_file"
  elif command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
    sudo -n tar -C "$public_root" -xf "$archive_file"
  else
    echo "Cannot write public registry candidate and sudo is unavailable: $public_root" >&2
    continue
  fi

  python3 - "$public_root" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1]).resolve()
target_versions = {
    "business-vector-en": "0.2.0",
    "economics-vector-en": "0.2.0",
    "politics-vector-en": "0.2.1",
}
with (root / "index.json").open(encoding="utf-8") as handle:
    index = json.load(handle)
packs = index.get("packs") or {}
for pack_id, expected_version in target_versions.items():
    entry = packs.get(pack_id) or {}
    if entry.get("version") != expected_version or entry.get("tier") != "domain":
        raise SystemExit(f"{pack_id} was not promoted in public root {root}")
    manifest_path = root / "packs" / pack_id / "manifest.json"
    if not manifest_path.is_file():
        raise SystemExit(f"missing public manifest for {pack_id}: {manifest_path}")
    with manifest_path.open(encoding="utf-8") as handle:
        manifest = json.load(handle)
    for artifact in manifest.get("artifacts") or []:
        url = str((artifact or {}).get("url") or "")
        if not url or "://" in url or url.startswith("/"):
            continue
        artifact_path = (manifest_path.parent / url).resolve()
        artifact_path.relative_to(root)
        if not artifact_path.is_file():
            raise SystemExit(f"missing public artifact for {pack_id}: {artifact_path}")
PY
  synced=1
done < <(awk 'NF && !seen[$0]++' "$roots_file")

if [ "$synced" -ne 1 ]; then
  echo "No writable public registry root was found for repo.adestool.com." >&2
  exit 1
fi
verify_public_registry_url

echo "Public registry static root sync completed."
