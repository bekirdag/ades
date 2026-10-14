"""Trace every replayed text's graph candidates; keep refs-only semantics explicit."""
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
import json
import time
from ades.config import Settings
from ades.impact.expansion import expand_impact_paths, _mentions_from_entities
from ades.service.models import EntityMatch

ROOT=Path('/home/wodo/adestool-audits/2026-10-14')
GRAPH=Path('/home/deploy/.local/share/ades-artifacts/market-graph/audit-20261014-v041/market_graph_store.sqlite')


def trace(d):
    if d.get('error'):return {'id':d['id'],'kind':d['kind'],'error':d['error']}
    entities=[EntityMatch.model_validate(x) for x in d['response']['entities']]
    refs=[e.link.entity_id for e in entities if e.link is not None]
    start=time.monotonic()
    try:
        result=expand_impact_paths(refs,settings=Settings(impact_expansion_enabled=True,impact_expansion_artifact_path=GRAPH),artifact_path=GRAPH,max_depth=2,impact_expansion_seed_limit=16,max_candidates=25,vector_proposals_enabled=False,source_mentions=_mentions_from_entities(entities))
        findings=[];paths=0;edges=0
        for candidate in result.candidates:
            for path in candidate.relationship_paths:
                paths+=1;edges+=len(path.edges)
                if path.path_depth!=len(path.edges):findings.append('path_depth_mismatch')
                if any(a.target_ref!=b.source_ref for a,b in zip(path.edges,path.edges[1:])):findings.append('disconnected_path')
                if any(not e.source_name or not e.source_url for e in path.edges):findings.append('incomplete_edge_provenance')
        return {'id':d['id'],'kind':d['kind'],'input_sha256':d['input_sha256'],'profile':'text-extracted refs-only depth-2 graph trace; not a news event-direction forecast','elapsed_seconds':time.monotonic()-start,'paths':paths,'edges':edges,'findings':sorted(set(findings)),'response':result.model_dump(mode='json')}
    except Exception as e:return {'id':d['id'],'kind':d['kind'],'error':type(e).__name__+': '+str(e),'elapsed_seconds':time.monotonic()-start}


def main():
    start=time.monotonic();errors=0;total=0;findings=0
    with (ROOT/'replay-all-final.jsonl').open() as f, (ROOT/'replay-all-final-paths.jsonl').open('w') as out,ProcessPoolExecutor(max_workers=8) as ex:
        for i,d in enumerate(ex.map(trace,(json.loads(l) for l in f),chunksize=10),1):
            total=i;errors+=bool(d.get('error'));findings+=bool(d.get('findings'));out.write(json.dumps(d,ensure_ascii=False)+'\n')
            if i%500==0:out.flush();print(json.dumps({'traced':i,'errors':errors,'flagged':findings,'seconds':time.monotonic()-start}),flush=True)
    print(json.dumps({'traced':total,'errors':errors,'flagged':findings,'seconds':time.monotonic()-start}),flush=True)

if __name__=='__main__':main()
