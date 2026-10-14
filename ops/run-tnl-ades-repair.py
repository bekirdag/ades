"""Run the scoped repair with the existing service environment; never print credentials."""
import os, pathlib, subprocess
pid = subprocess.check_output(['systemctl', 'show', 'theneuralledger-api.service', '-p', 'MainPID', '--value'], text=True).strip()
env = dict(os.environ)
for item in pathlib.Path(f'/proc/{pid}/environ').read_bytes().split(b'\0'):
    if item:
        key, value = item.split(b'=', 1)
        if key.startswith(b'MYSQL_') or key == b'DATABASE_URL': env[key.decode()] = value.decode()
env['TNL_ADES_API_BASE_URL'] = 'http://127.0.0.1:8744'
root = '/srv/theneuralledger/releases/20261014-ades-output-boundary'
raise SystemExit(subprocess.call([root+'/node_modules/.bin/tsx', root+'/scripts/repair-editorial-ades.ts'], cwd=root, env=env))
