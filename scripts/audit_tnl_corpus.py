"""Audit every retained TNL text and saved ADES projection without changing publications.

Consumes consistent JSONL database exports; emits a per-record ledger, per-issue
findings and reproducible coverage totals. Semantic heuristics are review flags,
never silently promoted to confirmed factual errors or a human-quality score.
"""
from __future__ import annotations
import argparse
import collections
import csv
import hashlib
import json
from pathlib import Path
import re
from typing import Any

GEOGRAPHIC_NAMES = {
    'middle east', 'west bank', 'latin america', 'north america', 'south america',
    'central america', 'eastern europe', 'western europe', 'south east asia',
    'southeast asia', 'south asia', 'east asia', 'north africa', 'south africa',
    'europe', 'asia', 'africa', 'united states', 'united kingdom', 'hong kong',
    'new york', 'new zealand', 'saudi arabia', 'south korea', 'north korea',
    'european union', 'gulf of oman', 'strait of hormuz', 'south china sea',
}
ISSUES = {
    'ENT-001': ('Entity coverage', 'review', 'No retained extracted entities', 'Replay with appropriate packs; an empty list alone does not establish an extraction defect.'),
    'ENT-002': ('Entity taxonomy', 'confirmed', 'Geographic name classified as a person', 'Reject geographical phrases during finance people generation and guard legacy contaminated aliases at runtime; republish cleaned packs.'),
    'ENT-005': ('Entity identity', 'confirmed', 'Geographic phrase classified as a security ticker', 'Guard geographic short company aliases; require an actual issuer/security mention rather than a city or region.'),
    'ENT-004': ('Entity taxonomy', 'confirmed', 'Institution/date phrase classified as a person', 'Reject invalid SEC person phrases in source generation and legacy runtime aliases; republish cleaned people lanes.'),
    'ENT-003': ('Entity grounding', 'review', 'Entity label not found in retained input text', 'Check canonical aliases and source evidence before classifying as spurious extraction.'),
    'IMP-001': ('Impact coverage', 'review', 'No retained impact candidates', 'Check endpoint mode, domain and actual event transmission; do not manufacture an impact to fill an empty list.'),
    'IMP-002': ('Impact explainability', 'confirmed', 'Displayed impacts retain identifiers but not inspectable relationship paths', 'Persist ADES path edges, seed refs, provenance and candidate decisions alongside displayed identifiers.'),
    'IMP-003': ('Impact grounding', 'review', 'Candidate has no retained source/seed relationship evidence', 'Inspect upstream diagnostics; keep broad proxies separate from article-grounded impact claims.'),
    'INT-001': ('Generated-content integration', 'confirmed', 'No per-text ADES response retained for generated content', 'Analyze the generated text independently and retain request hash/version/result; do not treat inherited story paths as independent analysis.'),
    'INT-002': ('Output integrity', 'confirmed', 'Malformed or unexpected saved output shape', 'Validate persisted response contracts and preserve rejected payload diagnostics.'),
    'INT-003': ('Processing failure', 'confirmed', 'Retained ADES processing error', 'Use exact saved error and request for regression testing; retry only after fixing the failure.'),
    'SRC-001': ('Content evidence', 'review', 'No retained source or citation evidence', 'Require source-linked generated claims and distinguish missing retention from absence of sources.'),
    'SRC-002': ('Content evidence', 'confirmed', 'Generated section cites an unknown retained source id', 'Repair the citation mapping through a visible correction; preserve the original publication.'),
    'SRC-003': ('Provenance accessibility', 'review', 'Path evidence uses a local file source', 'Publish source manifest or stable source URL; local paths cannot be checked by API clients.'),
    'TXT-001': ('Text quality', 'review', 'Replacement characters or placeholder text', 'Inspect source encoding and generation input; use append-only corrections for published text.'),
    'OBS-001': ('Auditability', 'confirmed', 'Full ADES response not retained in this output projection', 'Retain the complete response with text hash, endpoint, pack/runtime/artifact versions and request options.'),
}
PUBLIC_TABLES = {'news_stories','tnl_editorial_posts','tnl_story_enrichments','tnl_editorial_editions'}
TEXT_KEYS = {
    'title','text','body','body_text','summary','summary_text','description','excerpt',
    'dek','thesis','paragraphs','heading','introduction','rationale','claim_text',
    'content','cause_summary','method_summary','primary_cause_summary','primary_method_summary',
    'observation','interpretation','finding','question','whyConsequential','whatToWatch',
    'transmission','mechanism','evidence','explanation','reason','caption','statement',
}

def strings(value: Any, path: str = '', selected: bool = False):
    """Yield complete textual content leaves, excluding credential/identity metadata."""
    if isinstance(value, str):
        if selected and value.strip():
            yield path, value
    elif isinstance(value, list):
        for i,item in enumerate(value):
            yield from strings(item, f'{path}[{i}]', selected)
    elif isinstance(value, dict):
        for key,item in value.items():
            if any(x in key.casefold() for x in ['password','secret','token','api_key','email','owner_sub']):
                continue
            yield from strings(item, f'{path}.{key}' if path else key, selected or key in TEXT_KEYS)


def is_full_response(value: Any) -> bool:
    return isinstance(value, dict) and 'schema_version' in value and any(k in value for k in ['timing_breakdown_ms','diagnostics','metrics'])


def main():
    a=argparse.ArgumentParser();a.add_argument('--corpus',type=Path,required=True);a.add_argument('--upstream',type=Path);a.add_argument('--assistant-texts',type=Path);a.add_argument('--extra',type=Path,action='append',default=[]);a.add_argument('--output',type=Path,required=True);args=a.parse_args()
    args.output.mkdir(parents=True,exist_ok=True)
    from ades.packs.entity_taxonomy import GEOGRAPHIC_PERSON_EXCLUSIONS
    GEOGRAPHIC_NAMES.update(GEOGRAPHIC_PERSON_EXCLUSIONS)
    stats=collections.Counter();issues=collections.Counter();affected=collections.defaultdict(set);inputs={};known_entities=set();known_paths=set();story_ids=set();upstream_story_ids=set();record_hashes={};examples=collections.defaultdict(list)
    source_files=[args.corpus]+([args.upstream] if args.upstream else [])+([args.assistant_texts] if args.assistant_texts else [])+args.extra
    with (args.output/'records.jsonl').open('w') as records, (args.output/'findings.jsonl').open('w') as findings, (args.output/'texts.jsonl').open('w') as texts:
        for source in source_files:
            for number,line in enumerate(source.open(),1):
                d=json.loads(line);table=d['table'];row=d['row'];stats['rows:'+table]+=1
                if table=='news_entities':known_entities.add(row['id']);continue
                if table=='news_impact_paths':known_paths.add(row['id']);continue
                if table not in PUBLIC_TABLES|{'bdya_news_events','tnl_assistant_texts','news_story_revisions','tnl_research_sources','tnl_editorial_post_sources','retained_published_revision'}:continue
                rid=str(row.get('story_id') if table=='news_stories' else row.get('news_id') or row.get('id') or f'{table}:{number}')
                private=table=='tnl_assistant_texts'
                if private:rid=hashlib.sha256(rid.encode()).hexdigest()[:24]
                title='' if private else row.get('title','')
                text_leaves=list(strings(row));dedup={hashlib.sha256(text.encode()).hexdigest():(path,text) for path,text in text_leaves}
                stats['text_units']+=len(text_leaves);stats['distinct_text_units_per_record']+=len(dedup);stats['text_characters']+=sum(len(text) for _,text in text_leaves)
                for digest,(path,text) in dedup.items():
                    texts.write(json.dumps({'table':table,'id':rid,'path':path,'sha256':digest,'characters':len(text),'bytes':len(text.encode())})+'\n')
                flags=[];evidence=[]
                def flag(code,detail):
                    if (code,detail) not in evidence:
                        flags.append(code);evidence.append((code,detail));issues[code]+=1;affected[code].add((table,rid))
                        item={'code':code,'category':ISSUES[code][0],'assessment':ISSUES[code][1],'table':table,'id':rid,'title':title,'detail':detail,'fix':ISSUES[code][3]}
                        if len(examples[code])<12:examples[code].append(item)
                        findings.write(json.dumps(item,ensure_ascii=False)+'\n')
                all_text='\n'.join(t for _,t in dedup.values())
                if '\ufffd' in all_text or re.search(r'\b(?:TODO|PLACEHOLDER|INSERT TITLE HERE)\b',all_text):flag('TXT-001','Text requires encoding/placeholder review')
                if table=='news_stories':
                    story_ids.add(rid);raw=row.get('raw_json') or {};input_text='\n'.join(str(row.get(k) or '') for k in ['title','excerpt','body']);inputs[rid]=input_text
                    entities=raw.get('entities') if isinstance(raw,dict) else None
                    impacts=raw.get('impacts') if isinstance(raw,dict) else None
                    if not isinstance(raw,dict):flag('INT-002','raw_json is not an object');raw={}
                    if entities is not None and not isinstance(entities,list):flag('INT-002','entities is not an array');entities=[]
                    if not entities:flag('ENT-001','No entities in retained story projection')
                    for entity in entities or []:
                        if not isinstance(entity,dict):flag('INT-002','Entity is not an object');continue
                        name=str(entity.get('entity_name') or entity.get('text') or '').strip();label=str(entity.get('entity_type') or entity.get('label') or '')
                        if label=='ticker' and name.strip(' •§*-,.').casefold().removeprefix('the ') in GEOGRAPHIC_NAMES:flag('ENT-005',f'{name!r} classified as ticker')
                        if label=='person':
                            from ades.packs.entity_taxonomy import is_invalid_finance_person_name
                            if name.strip(' •§*-,.').casefold().removeprefix('the ') in GEOGRAPHIC_NAMES:flag('ENT-002',f'{name!r} classified as person')
                            elif is_invalid_finance_person_name(name):flag('ENT-004',f'{name!r} classified as person')
                        if name and len(name)>3 and name.casefold() not in input_text.casefold() and label not in {'country','country_scope'}:flag('ENT-003',f'{name!r} ({label}) absent from display input; alias/source review required')
                    if not impacts:flag('IMP-001','No impacts in retained story projection')
                    if impacts and isinstance(impacts,list):
                        inspectable=[i for i in impacts if isinstance(i,dict) and any(i.get(k) for k in ['edges','paths','path_edges','reasoning_path','source_refs'])]
                        if not inspectable:flag('IMP-002',f'{len(impacts)} impacts have refs/scores but no inspectable edges in TNL projection')
                    if not row.get('sources_json'):flag('SRC-001','No source list retained')
                    if not any(is_full_response(v) for v in raw.values()):flag('OBS-001','Story stores ADES entity/impact projections, not complete response')
                elif table in {'tnl_editorial_posts','tnl_story_enrichments','tnl_editorial_editions','tnl_assistant_texts'}:
                    payload=row.get('post_json') or row.get('overlay_json') or row.get('edition_json') or row
                    if not isinstance(payload,dict):flag('INT-002','Generated content JSON is not an object');payload={}
                    if not any('ades' in k.casefold() and is_full_response(v) for k,v in payload.items()):flag('INT-001','No independently retained ADES analysis for this generated text')
                    sources=payload.get('researchSources') or payload.get('sources') or row.get('citations_json') or []
                    if table not in {'tnl_editorial_editions'} and not sources:flag('SRC-001','Generated text has no retained citation/source list')
                    source_ids={str(s.get('id') or s.get('sourceId') or '') for s in sources if isinstance(s,dict)} if isinstance(sources,list) else set()
                    for section in payload.get('sections',[]):
                        if isinstance(section,dict):
                            for ref in section.get('citationIds',section.get('sourceIds',[])):
                                if source_ids and str(ref) not in source_ids:flag('SRC-002',f'Section cites unretained source {ref}')
                elif table=='bdya_news_events':
                    stats['upstream_events']+=1;sid=str(row.get('story_id') or row.get('origin_story_id') or '');upstream_story_ids.add(sid)
                    output=row.get('categorization_output_json') or {};tool=output.get('ades_tool') or {} if isinstance(output,dict) else {}
                    if not isinstance(tool,dict):flag('INT-002','ades_tool is not an object');tool={}
                    stats['upstream_with_ades_tool']+=bool(tool)
                    for key,value in tool.items():
                        if ('error' in key or key.endswith('last_error')) and value:flag('INT-003',f'{key}: {str(value)[:200]}')
                    passive=tool.get('news_analyze_passive_entities') or []
                    for entity in passive:
                        if not isinstance(entity,dict):continue
                        name=str(entity.get('name') or entity.get('entity_name') or entity.get('text') or '');label=str(entity.get('entity_type') or entity.get('label') or '')
                        if label=='ticker' and name.strip(' •§*-,.').casefold().removeprefix('the ') in GEOGRAPHIC_NAMES:flag('ENT-005',f'{name!r} classified as ticker')
                        if label=='person':
                            from ades.packs.entity_taxonomy import is_invalid_finance_person_name
                            if name.strip(' •§*-,.').casefold().removeprefix('the ') in GEOGRAPHIC_NAMES:flag('ENT-002',f'Upstream ADES {name!r} classified as person')
                            elif is_invalid_finance_person_name(name):flag('ENT-004',f'Upstream ADES {name!r} classified as person')
                    # Retention quality: source-local relationship provenance cannot be fetched by API clients.
                    def walk(value):
                        if isinstance(value,dict):
                            for k,v in value.items():
                                if k in {'source_url','source_ref','source_path','source_refs'}:
                                    vs=v if isinstance(v,list) else [v]
                                    if any(isinstance(x,str) and x.startswith(('/home/','/mnt/','file://')) for x in vs):return True
                                if walk(v):return True
                        elif isinstance(value,list):return any(walk(x) for x in value)
                        return False
                    if walk(tool):flag('SRC-003','Relationship evidence includes local artifact source paths')
                record_hash=hashlib.sha256(line.encode()).hexdigest();record_hashes[f'{table}:{rid}']=record_hash
                records.write(json.dumps({'table':table,'id':rid,'title':title,'text_units':len(text_leaves),'characters':sum(len(t) for _,t in text_leaves),'record_sha256':record_hash,'issue_codes':sorted(set(flags)),'assessment':'flagged_for_review' if flags else 'no_rule_findings','semantic_review':'deterministic_full_text_and_saved_output_checks'},ensure_ascii=False)+'\n')
                if stats['rows:'+table]%5000==0:print(table,stats['rows:'+table],flush=True)
    summary={'scope':'All rows in exported TNL content tables, saved upstream outputs for matching story IDs, retained source/revision texts and assistant outputs; no sampling in deterministic checks. Heuristic flags require semantic review. No claim of a human factual read or live replay of every text.', 'inventory':dict(stats),'issue_occurrences':dict(issues),'affected_records':{k:len(v) for k,v in affected.items()},'story_count':len(story_ids),'upstream_story_count':len(story_ids&upstream_story_ids),'stories_without_upstream_event':sorted(story_ids-upstream_story_ids),'issues':{k:{'category':v[0],'assessment':v[1],'title':v[2],'fix':v[3]} for k,v in ISSUES.items()},'examples':dict(examples)}
    (args.output/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2))
    with (args.output/'categorized-issues.csv').open('w') as f:
        w=csv.writer(f);w.writerow(['id','category','assessment','affected_records','occurrences','issue','fix'])
        for k,v in ISSUES.items():w.writerow([k,v[0],v[1],len(affected[k]),issues[k],v[2],v[3]])
    print(json.dumps({'story_count':len(story_ids),'inventory':dict(stats),'affected_records':summary['affected_records']}),flush=True)

if __name__=='__main__':main()
