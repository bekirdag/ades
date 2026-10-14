"""Build versioned, reversible pack repairs from existing source-backed releases."""
from pathlib import Path
from dataclasses import asdict
import hashlib
import json
import shutil

from ades.packs.entity_taxonomy import is_invalid_finance_person_name
from ades.runtime_matcher import build_matcher_artifact_from_aliases_json
from ades.packs.publish import build_static_registry

source=Path('/mnt/ades/repo/current')
stage=Path('/home/wodo/adestool-audits/2026-10-14/pack-candidate')
stage.mkdir(parents=True,exist_ok=True)
changes=[];dirs=[]
for aliases_path in sorted((source/'packs').glob('finance-*/aliases.json')):
    payload=json.loads(aliases_path.read_text());old=payload['aliases']
    kept=[a for a in old if not (a.get('label')=='person' and is_invalid_finance_person_name(a.get('text','')))]
    added=[]
    if aliases_path.parent.name=='finance-en':
        apple=next(a for a in kept if a.get('text')=='Apple Inc.' and a.get('entity_id')=='sec-cik:0000320193')
        if not any(a.get('text')=='Apple' and a.get('entity_id')==apple['entity_id'] for a in kept):
            added=[dict(apple,text='Apple',runtime_tier='runtime_exact_high_precision')];kept+=added
    if len(kept)==len(old) and not added:continue
    dest=stage/'source-packs'/aliases_path.parent.name
    shutil.copytree(aliases_path.parent,dest,dirs_exist_ok=True)
    payload['aliases']=kept;(dest/'aliases.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n')
    manifest=json.loads((dest/'manifest.json').read_text());previous=manifest['version'];major,minor,patch=map(int,previous.split('.'));manifest['version']=f'{major}.{minor}.{patch+1}'
    result=build_matcher_artifact_from_aliases_json(dest/'aliases.json',output_dir=dest/'matcher',include_runtime_tiers={'runtime_exact_acronym_high_precision','runtime_exact_geopolitical_high_precision','runtime_exact_high_precision'})
    manifest['matcher'].update({k:getattr(result,k) for k in ['algorithm','normalization','entry_count','state_count','max_alias_length','artifact_sha256','entries_sha256']})
    (dest/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    changes.append({'pack_id':manifest['pack_id'],'old_version':previous,'new_version':manifest['version'],'removed_invalid_person_aliases':len(old)+len(added)-len(kept),'added_verified_issuer_aliases':len(added),'matcher_entry_count':result.entry_count})
    dirs.append(dest)
    print(changes[-1],flush=True)
release=build_static_registry(dirs,output_dir=stage/'registry',enforce_deploy_matcher_size_guard=True)
(stage/'changes.json').write_text(json.dumps(changes,indent=2)+'\n')
(stage/'release.json').write_text(release.model_dump_json(indent=2)+'\n' if hasattr(release,'model_dump_json') else json.dumps(asdict(release),indent=2)+'\n')
print('Built registry:',stage/'registry',flush=True)
