"""Start an isolated candidate with existing runtime settings held only in memory."""
from pathlib import Path
import os
import subprocess
import json

pid=int(subprocess.check_output(['runuser','-u','deploy','--','env','XDG_RUNTIME_DIR=/run/user/1001','DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1001/bus','systemctl','--user','show','ades','-p','MainPID','--value'],text=True).strip())
env=os.environ.copy()
for entry in Path(f'/proc/{pid}/environ').read_bytes().split(b'\0'):
    if b'=' in entry:
        k,v=entry.split(b'=',1)
        if k.startswith(b'ADES_'):
            env[k.decode()]=v.decode()
env['PYTHONPATH']='/home/wodo/apps/adestool/src'
env['ADES_STORAGE_ROOT']='/home/wodo/adestool-audits/2026-10-14/runtime-candidate'
env['ADES_SERVICE_PREWARM_ENABLED']='false'
env['ADES_IMPACT_EXPANSION_ARTIFACT_PATH']='/home/deploy/.local/share/ades-artifacts/market-graph/audit-20261014-v041/market_graph_store.sqlite'
log=Path('/home/wodo/adestool-audits/2026-10-14/candidate-api.log').open('w')
p=subprocess.Popen(['/mnt/ades/app/shared/.venv/bin/python','-m','uvicorn','ades.api:create_service_app','--factory','--host','127.0.0.1','--port','8744','--workers','8'],env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
Path('/home/wodo/adestool-audits/2026-10-14/candidate-api.pid').write_text(str(p.pid))
print(json.dumps({'candidate_pid':p.pid,'port':8744}),flush=True)
