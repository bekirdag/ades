"""Apply the bounded ADES integration to an exact TNL release, without replacing unrelated code."""
from pathlib import Path
import re, sys
root = Path(sys.argv[1])
p = root / 'server/editorial/composition.ts'
s = p.read_text()
s = re.sub(r'  const aggregateEntities = uniqueStrings\(\[.*?  const aggregateImpactPaths = uniqueStrings\(\[.*?\n  \]\);\n', '', s, count=1, flags=re.S)
s = re.sub(r'entities: contentType === .analysis. \? aggregateEntities : aggregateEntities.slice\(0, 40\),', 'entities: [],', s)
s = re.sub(r'impactPaths: contentType === .analysis. \? aggregateImpactPaths : aggregateImpactPaths.slice\(0, 40\),', 'impactPaths: [],', s)
s = re.sub(r"  if \(contentType === 'analysis' && \(\n    draft\.impactPaths.length < 1.*?\n  }\n", '', s, count=1, flags=re.S)
s = s.replace('Country-analysis impact paths must be source-supported causal chains containing arrows.', 'Return entities and impactPaths as empty arrays; ADES computes them from the final published prose.')
s = s.replace('Extract entities from the complete retained evidence, not only the primary story. Express each impact path as a concise causal chain using arrows, for example "policy decision → financing cost → exposed sector → time horizon", and include only source-supported links.', 'Return entities and impactPaths as empty arrays. ADES computes those fields independently using only the finished article text.')
s = s.replace('Produce multiple source-supported causal impact paths that connect policy, institutions, markets, sectors, assets, populations, and plausible time horizons.', 'Explain source-supported consequences in the prose. Do not generate impact-path metadata; ADES computes it from the finished article.')
p.write_text(s)
p = root / 'server/index.ts'; s = p.read_text()
s = "import { analyzePublishedEditorial } from './editorial/adesAnalysis.ts';\n" + s
s = s.replace('const post = await synthesizeEditorialPost(', 'const draftPost = await synthesizeEditorialPost(')
marker = '    recordEditorialOutcome(true, freshestEntryAt(coveredEntries));'
if marker in s: s = s.replace(marker, '    const post = await analyzePublishedEditorial(draftPost);\n' + marker, 1)
else:
    marker = '    await saveEditorialPost(post);'
    s = s.replace(marker, '    const post = await analyzePublishedEditorial(draftPost);\n' + marker, 1)
s = s.replace('backfillEditorialEvidenceData, backfillEditorialImpactPaths', 'backfillEditorialEvidenceData')
s = s.replace("const entities=unique([...(post.entities||[]),...sources.flatMap((story)=>story.passiveEntities||[])]);", "const entities=post.entities||[];")
s = s.replace("const impactPaths=unique([...(post.impactPaths||[]),...sources.flatMap((story)=>story.impactPaths||[]),...backfillEditorialImpactPaths(post)]);", "const impactPaths=post.impactPaths||[];")
p.write_text(s)
p = root / 'server/editorial/legacy.ts'; s = p.read_text()
start = s.index('export function backfillEditorialImpactPaths(')
s = s[:start] + "export function backfillEditorialImpactPaths(_post: EditorialPost): string[] {\n  // Headings and source metadata are not graph evidence. Await published-text ADES analysis.\n  return [];\n}\n"
p.write_text(s)
