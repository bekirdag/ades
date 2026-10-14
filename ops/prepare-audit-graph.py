"""Create a filtered graph release; preserve the original graph for rollback."""
from pathlib import Path
from datetime import datetime,timezone
import hashlib
import json
import sqlite3
from ades.packs.entity_taxonomy import is_invalid_finance_person_name

source=Path('/home/deploy/.local/share/ades-artifacts/market-graph/current')
release=source.parent/'audit-20261014-v041';release.mkdir(exist_ok=True)
src=sqlite3.connect('file:'+str(source/'market_graph_store.sqlite')+'?mode=ro',uri=True)
dst=sqlite3.connect(release/'market_graph_store.sqlite');src.backup(dst);src.close()
bad=[r[0] for r in dst.execute('SELECT entity_ref,canonical_name FROM impact_nodes WHERE entity_type=?',('person',)) if str(r[0]).startswith('finance-person:') and is_invalid_finance_person_name(r[1])]
dst.execute('CREATE TEMP TABLE excluded_person_refs (ref TEXT PRIMARY KEY)');dst.executemany('INSERT INTO excluded_person_refs VALUES (?)',[(x,) for x in bad])
removed_edges=dst.execute('SELECT count(*) FROM impact_edges WHERE source_ref IN (SELECT ref FROM excluded_person_refs) OR target_ref IN (SELECT ref FROM excluded_person_refs)').fetchone()[0]
dst.execute('DELETE FROM impact_edge_packs WHERE edge_id IN (SELECT edge_id FROM impact_edges WHERE source_ref IN (SELECT ref FROM excluded_person_refs) OR target_ref IN (SELECT ref FROM excluded_person_refs))')
dst.execute('DELETE FROM impact_edges WHERE source_ref IN (SELECT ref FROM excluded_person_refs) OR target_ref IN (SELECT ref FROM excluded_person_refs)')
dst.execute('DELETE FROM impact_identity_refs WHERE entity_ref IN (SELECT ref FROM excluded_person_refs) OR identity_ref IN (SELECT ref FROM excluded_person_refs)')
dst.execute('DELETE FROM impact_nodes WHERE entity_ref IN (SELECT ref FROM excluded_person_refs)')
old_hash=dst.execute('SELECT artifact_hash FROM build_metadata WHERE id=1').fetchone()[0]
new_hash='sha256:'+hashlib.sha256(json.dumps({'base':old_hash,'policy':'invalid-finance-person-v1','removed':sorted(bad)},sort_keys=True).encode()).hexdigest()
version='20261014Ttnl-audit-person-taxonomy-v041';now=datetime.now(timezone.utc).isoformat();nodes=dst.execute('SELECT count(*) FROM impact_nodes').fetchone()[0];edges=dst.execute('SELECT count(*) FROM impact_edges').fetchone()[0]
dst.execute('UPDATE build_metadata SET artifact_version=?, artifact_hash=?, built_at=?, node_count=?, edge_count=? WHERE id=1',(version,new_hash,now,nodes,edges));dst.commit();assert dst.execute('PRAGMA integrity_check').fetchone()[0]=='ok';dst.close()
manifest=json.loads((source/'market_graph_store_manifest.json').read_text());manifest.update(artifact_version=version,artifact_hash=new_hash,built_at=now,node_count=nodes,edge_count=edges);manifest['audit_correction']={'base_artifact_hash':old_hash,'policy':'invalid-finance-person-v1','removed_nodes':len(bad),'removed_edges':removed_edges,'removed_entity_refs':bad};(release/'market_graph_store_manifest.json').write_text(json.dumps(manifest,indent=2)+'\n');print(json.dumps({'release':str(release),'artifact_hash':new_hash,'removed_nodes':len(bad),'removed_edges':removed_edges,'remaining_nodes':nodes,'remaining_edges':edges}),flush=True)
