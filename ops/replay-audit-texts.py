"""Replay every primary public TNL text through a fixed, explicit tagging profile.

This is a reproducible extraction comparison, not a claim that historical ADES
request options or graph/market predictions have been reconstructed.
"""
import argparse
from concurrent.futures import ProcessPoolExecutor
import hashlib
import json
from pathlib import Path
import time
from ades import tag


def texts(v):
    if isinstance(v,str):return [v]
    if isinstance(v,list):return [t for x in v for t in texts(x)]
    if isinstance(v,dict):return [t for k,x in v.items() if k in {'text','heading','paragraphs','title','thesis','dek','summary','introduction'} for t in texts(x)]
    return []


def replay(task):
    rid,kind,text,storage=task
    start=time.monotonic()
    try:
        result=tag(text,pack='finance-us-en',storage_root=storage,debug=True)
        return {'id':rid,'kind':kind,'input_sha256':hashlib.sha256(text.encode()).hexdigest(),'characters':len(text),'profile':'finance-us-en + installed dependencies; no vector/impact runtime config','elapsed_seconds':time.monotonic()-start,'response':result.model_dump(mode='json')}
    except Exception as e:
        return {'id':rid,'kind':kind,'input_sha256':hashlib.sha256(text.encode()).hexdigest(),'characters':len(text),'error':type(e).__name__+': '+str(e),'elapsed_seconds':time.monotonic()-start}


def main():
    a=argparse.ArgumentParser();a.add_argument('--corpus',type=Path,required=True);a.add_argument('--storage',required=True);a.add_argument('--output',type=Path,required=True);a.add_argument('--limit',type=int,default=0);a.add_argument('--workers',type=int,default=2);args=a.parse_args();tasks=[]
    for l in args.corpus.open():
        d=json.loads(l);r=d['row'];table=d['table']
        if table=='news_stories':text='\n'.join(str(r.get(k) or '') for k in ['title','excerpt','body']);rid=r['story_id']
        elif table in {'tnl_editorial_posts','tnl_story_enrichments','tnl_editorial_editions'}:
            payload=r.get('post_json') or r.get('overlay_json') or r.get('edition_json');text='\n'.join(texts(payload));rid=r['id']
        else:continue
        tasks.append((rid,table,text,args.storage))
    if args.limit:tasks=tasks[:args.limit]
    start=time.monotonic();errors=0
    with args.output.open('w') as f, ProcessPoolExecutor(max_workers=args.workers) as executor:
        for i,result in enumerate(executor.map(replay,tasks,chunksize=10),1):
            errors+=bool(result.get('error'));f.write(json.dumps(result,ensure_ascii=False)+'\n')
            if i%100==0:print(json.dumps({'reviewed':i,'total':len(tasks),'errors':errors,'seconds':time.monotonic()-start}),flush=True);f.flush()
    print(json.dumps({'reviewed':len(tasks),'total':len(tasks),'errors':errors,'seconds':time.monotonic()-start}),flush=True)

if __name__=='__main__':main()
