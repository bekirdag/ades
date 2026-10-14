"""Promote validated versioned packs, metadata, graph settings and Python wheel.

All replaced files are retained for rollback. Published TNL content is untouched.
Run on Raijin as root after candidate/API/corpus validation.
"""
from pathlib import Path
import json
import os
import shutil
import sqlite3
import subprocess

base=Path('/home/wodo/adestool-audits/2026-10-14')
root=Path('/mnt/ades/repo/current')
registry=base/'pack-candidate/registry'
backup=Path('/mnt/ades/app/hotfix-backups/20261014-v041');backup.mkdir(parents=True,exist_ok=True)
changes=json.loads((base/'pack-candidate/changes.json').read_text())
wheel=base/'release/python/ades_tool-0.4.1-py3-none-any.whl';assert wheel.exists()
# Use the consistent candidate metadata already exercised by all text replays.
a=sqlite3.connect('file:'+str(base/'runtime-candidate/registry/ades.db')+'?mode=ro',uri=True)
b=sqlite3.connect(base/'promoted-registry.db');a.backup(b);a.close()
for pack_id, in b.execute('SELECT pack_id FROM installed_packs').fetchall():
    p=root/'packs'/pack_id
    b.execute('UPDATE installed_packs SET install_path=?,manifest_path=? WHERE pack_id=?',(str(p),str(p/'manifest.json'),pack_id))
b.commit();assert b.execute('PRAGMA integrity_check').fetchone()[0]=='ok';b.close()
shutil.copy2(root/'index.json',backup/'index.json')
# Existing wheel is the known rollback release, including a separate saved hotfix.
for p in [Path('/mnt/ades/app/shared/.venv/lib/python3.14/site-packages/ades')]:
    shutil.copytree(p,backup/'python-ades',dirs_exist_ok=True)
for change in changes:
    pid=change['pack_id'];shutil.copytree(root/'packs'/pid,backup/'packs'/pid,dirs_exist_ok=True)
old_db=sqlite3.connect('file:'+str(root/'registry/ades.db')+'?mode=ro',uri=True);saved=sqlite3.connect(backup/'ades.db');old_db.backup(saved);old_db.close();saved.close()
shutil.copytree(registry/'artifacts',root/'artifacts',dirs_exist_ok=True)
# Prepare complete replacement directories and registry index before the short cutover.
for change in changes:
    pid=change['pack_id'];shutil.copytree(registry/'packs'/pid,root/'packs'/(pid+'.audit-new'),dirs_exist_ok=True)
index=json.loads((root/'index.json').read_text());new=json.loads((registry/'index.json').read_text());index['packs'].update(new['packs']);index['generated_at']=new['generated_at'];index['pack_count']=len(index['packs']);(root/'index.json.audit-new').write_text(json.dumps(index,indent=2)+'\n')
service_env={'XDG_RUNTIME_DIR':'/run/user/1001','DBUS_SESSION_BUS_ADDRESS':'unix:path=/run/user/1001/bus'}
def service(*args):
    return subprocess.run(['runuser','-u','deploy','--','env',*[f'{k}={v}' for k,v in service_env.items()],'systemctl','--user',*args],check=True)
(backup/'original-pack-directories').mkdir(exist_ok=True)
service('stop','ades')
try:
    for change in changes:
        pid=change['pack_id'];old=root/'packs'/pid;old.rename(backup/'original-pack-directories'/pid);(root/'packs'/(pid+'.audit-new')).rename(old)
    # No other service uses this local registry; move SQLite sidecars together.
    for suffix in ['', '-wal', '-shm']:
        p=root/'registry'/('ades.db'+suffix)
        if p.exists():p.rename(backup/('active-ades.db'+suffix))
    shutil.copy2(base/'promoted-registry.db',root/'registry/ades.db')
    os.chown(root/'registry/ades.db',1001,1001);os.chmod(root/'registry/ades.db',0o644)
    (root/'index.json.audit-new').replace(root/'index.json')
    subprocess.run(['/mnt/ades/app/shared/.venv/bin/pip','install','--no-deps','--force-reinstall',str(wheel)],check=True)
    dropin=Path('/home/deploy/.config/systemd/user/ades.service.d/zzzz-audit-v041.conf')
    dropin.write_text('[Service]\nEnvironment=ADES_IMPACT_EXPANSION_ARTIFACT_PATH=/home/deploy/.local/share/ades-artifacts/market-graph/audit-20261014-v041/market_graph_store.sqlite\n')
    service('daemon-reload');service('start','ades')
except Exception:
    # Restore the known source files, registry and packs if installation fails.
    for change in changes:
        pid=change['pack_id'];shutil.rmtree(root/'packs'/pid);shutil.copytree(backup/'packs'/pid,root/'packs'/pid)
    shutil.copy2(backup/'ades.db',root/'registry/ades.db');shutil.copy2(backup/'index.json',root/'index.json')
    shutil.copytree(backup/'python-ades',Path('/mnt/ades/app/shared/.venv/lib/python3.14/site-packages/ades'),dirs_exist_ok=True)
    service('start','ades');raise
print(json.dumps({'backup':str(backup),'python_version':'0.4.1','packs':changes}),flush=True)
