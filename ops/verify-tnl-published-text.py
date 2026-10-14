"""Read-only verification against the retained baseline; private inputs stay on Raijin."""
import json, hashlib, subprocess, os, gzip
from pathlib import Path
root=Path('/home/wodo/adestool-audits/2026-10-14')
fields=['title','dek','thesis','sections','slug','status','publishedAt','contentHash','researchSources','sourceStories']
base={}
for line in (root/'corpus.jsonl').open():
 d=json.loads(line)
 if d['table']=='tnl_editorial_posts':base[d['row']['id']]=d['row']['post_json']
projection=','.join("'"+k+"',JSON_EXTRACT(post_json,'$."+k+"')" for k in fields+['entities','impactPaths','adesAnalysis.contract','adesAnalysis.inputSha256','metadataCorrection'])
sql="SELECT JSON_OBJECT('id',id,"+projection+") FROM tnl_editorial_posts;"
command=['mysql','--batch','--raw','-N','theneuralledger'] if os.geteuid()==0 else ['ssh','deploy@192.168.12.165','sudo -n mysql --batch --raw -N theneuralledger']
process=subprocess.Popen(command,stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True)
process.stdin.write(sql);process.stdin.close()
counts={'total':0,'corrected':0,'unchanged_published_content':0,'violations':0,'new_posts':0}
failures=[]
for line in process.stdout:
 d=json.loads(line);rid=d['id'];counts['total']+=1
 if rid not in base:counts['new_posts']+=1;continue
 b=base[rid]
 changed=[k for k in fields if d.get(k)!=b.get(k)]
 if changed:failures.append({'id':rid,'changed_fields':changed});counts['violations']+=1
 else:counts['unchanged_published_content']+=1
 if d.get('adesAnalysis.contract')=='tnl.published-text-ades.v2':
  counts['corrected']+=1
  text='\n\n'.join(x for x in [d['title'],d['dek'],d['thesis'],*[x for s in d['sections'] for x in [s['heading'],*s['paragraphs']]]] if x)
  reasons=[]
  if hashlib.sha256(text.encode()).hexdigest()!=d.get('adesAnalysis.inputSha256'):reasons.append('input_hash_mismatch')
  if len(d.get('entities') or [])>32 or len(d.get('impactPaths') or [])>16:reasons.append('display_limit_violation')
  if any('→' in x or ':' in x for x in (d.get('impactPaths') or [])):reasons.append('non_entity_path')
  revision=d.get('metadataCorrection',{}).get('revisionId')
  revision_path=Path('/srv/theneuralledger/shared/published-content-revisions/ades-published-text-20261014',(revision or '')+'.json.gz')
  if not revision or not revision_path.exists():reasons.append('missing_retained_revision')
  else:
   with gzip.open(revision_path,'rt') as retained:original=json.load(retained)['original']
   if original.get('id')!=rid or any(original.get(k)!=b.get(k) for k in fields):reasons.append('retained_original_content_mismatch')
  if reasons:failures.append({'id':rid,'reasons':reasons});counts['violations']+=1
if process.wait():raise RuntimeError('MySQL projection export failed')
(root/'tnl-correction-verification.json').write_text(json.dumps({'counts':counts,'failures':failures},indent=2))
print(json.dumps(counts))
if failures:raise SystemExit(1)
