"""Install the static homepage and two scoped runtime fixes on Raijin.
Run as root after regression tests pass. Source package upgrades remain separate.
"""
from pathlib import Path
import ast
import shutil
import subprocess
import time

root = Path("/home/wodo/apps/adestool")
runtime = Path("/mnt/ades/app/shared/.venv/lib/python3.14/site-packages/ades")
backup = Path("/mnt/ades/app/hotfix-backups") / time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
backup.mkdir(parents=True)
replacements = {}
p = runtime / "vector/service.py"
s = p.read_text()
old = 'return str(entity.label or "").strip().casefold()'
assert s.count(old) == 1, "Unexpected seed-label helper"
replacements[p] = s.replace(old, 'return "" if entity is None else str(entity.label or "").strip().casefold()', 1)
p = runtime / "service/app.py"
s = p.read_text()
assert s.count("    def runtime_status() -> StatusResponse:") == 1
s = s.replace("from contextlib import asynccontextmanager", "import asyncio\nfrom concurrent.futures import ThreadPoolExecutor\nfrom functools import partial\nfrom contextlib import asynccontextmanager", 1)
s = s.replace("    runtime_state = _ServiceRuntimeState(storage_root)", "    runtime_state = _ServiceRuntimeState(storage_root)\n    # Isolate readiness from long-running extraction worker slots.\n    status_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=\"ades-status\")", 1)
assert "        yield\n\n    app = FastAPI" in s
s = s.replace("        yield\n\n    app = FastAPI", "        try:\n            yield\n        finally:\n            status_executor.shutdown(wait=False, cancel_futures=True)\n\n    app = FastAPI", 1)
s = s.replace("    def runtime_status() -> StatusResponse:", "    async def runtime_status() -> StatusResponse:", 1)
s = s.replace("            response = status(storage_root=storage_root)", "            response = await asyncio.get_running_loop().run_in_executor(\n                status_executor, partial(status, storage_root=storage_root)\n            )", 1)
replacements[p] = s
for p,s in replacements.items():
    ast.parse(s)
    saved = backup / p.relative_to(runtime)
    saved.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(p, saved)
nginx = Path("/etc/nginx-raijin-webapps/nginx.conf")
config = nginx.read_text()
start = config.index("    server_name adestool.com api.adestool.com www.adestool.com;")
end = config.index("server_name repo.adestool.com", start)
block = config[start:end]
assert "location = /" not in block
block = block.replace("    location / {", "    location = / {\n        root /mnt/ades/website/current;\n        try_files /index.html =404;\n        add_header Cache-Control \"public, max-age=300\";\n    }\n\n    location / {", 1)
config = config[:start]+block+config[end:]
shutil.copy2(nginx, backup / "nginx.conf")
website = Path("/mnt/ades/website/current")
website.mkdir(parents=True, exist_ok=True)
shutil.copy2(root / "website/index.html", website / "index.html")
nginx.write_text(config)
try:
    subprocess.run(["nginx", "-t", "-c", str(nginx)], check=True)
except Exception:
    shutil.copy2(backup / "nginx.conf", nginx)
    raise
for p,s in replacements.items():
    p.write_text(s)
print("Backup:",backup)
print("Installed homepage and scoped runtime fixes; restart ades and reload origin Nginx.")
