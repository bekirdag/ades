# ADES Country Pack Coverage Inventory

Date: 2026-06-30

Work item: NSC-0004, "Inventory existing country packs and coverage."

Scope: current ADES G20 country finance packs, reviewed program/org
relationship rows, local golden-style tests, known gaps, and BDYA replay
coverage relevant to the news story creation pipeline. This is a local
inventory only; no source fetch, artifact build, deployment, or production
apply command was run.

Source basis:

- Country profiles and source plans in `src/ades/packs/finance_country.py`.
- Current source-lane inventory in
  `docs/news_story_creation_ades_source_lane_inventory_2026-06-30.md`.
- Runtime pack artifacts under `/mnt/githubActions/ades_big_data`.
- Reviewed relationship rows under
  `/mnt/githubActions/ades_big_data/pack_sources/impact_relationships/program_org_relationship/reviewed/2026-06-29`.
- Local tests under `tests/unit/test_finance_country.py`,
  `tests/unit/test_program_org_relationship_impact.py`,
  `tests/unit/test_impact_starter_evaluation.py`, and
  `tests/api/test_news_analyze_endpoint.py`.

## Status Labels

- `mature-partial`: generated pack has materially broader country entities than
  the five-entity scaffold, but it is still not a complete country market
  graph.
- `scaffold`: generated pack exists mostly as static baseline entities and is
  not enough for broad issuer/product/program coverage.
- `prod-deploy pack`: pack is present in
  `/mnt/githubActions/ades_big_data/prod-deploy/registry-2026-04-19-finance-country-g20-r5/packs`.
- `local-only pack`: a stronger generated pack or release candidate exists
  locally, but the prod-deploy registry snapshot checked here does not contain
  that pack.
- `not present`: no generated pack or overlay artifact was found in the checked
  local artifact roots.

## Country Matrix

`Pack source rows` is the sum of `record_count` values from the best local
pack artifact's `sources.json`. `Reviewed relationship rows` is the row count
from reviewed program/org TSV files dated `2026-06-29`; `Built relationship
edges` is the edge count in the corresponding built lane manifest when present.

| Country / overlay | G20/EU role | Pack / overlay status | Pack source rows | Best local pack entities | Reviewed relationship rows | Built relationship edges | Golden / regression coverage | Known gaps | Replay coverage |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| Argentina | G20 country | `finance-ar-en`; mature-partial; prod-deploy pack | 4,520 | 2,094 | 10 | 10 | Finance-country parser/profile tests; reviewed program/org fixture test for BPAT/Rio Negro paths; starter graph promoted Argentina relationship test covers BYMA/BCRA/Rio Negro/Banco Patagonia/BPAT paths. | Relationship coverage is narrow around Banco Patagonia/Rio Negro paths, but the promoted starter slice now includes official exchange, regulator, government, issuer, security, ticker, and product mappings. | No persisted BDYA no-terminal replay output found. |
| Australia | G20 country | `finance-au-en`; mature-partial; prod-deploy pack | 8,137 | 4,045 | 30 | 30 | Finance-country parser/profile tests; reviewed program/org fixture; starter graph promoted Australia relationship test. | Relationship lane is reviewed but only partial; country pack still lacks broad product/program/sector terminal coverage. | No persisted BDYA no-terminal replay output found. |
| Brazil | G20 country | `finance-br-en`; mature-partial; local-only pack | 25,897 | 12,948 | 47 | 47 | Finance-country parser/profile tests; reviewed program/org fixture; starter graph promoted Brazil relationship test. | Strong generated pack exists locally, but the checked prod-deploy registry snapshot does not include `finance-br-en`; coverage should be reconciled before release claims. | No persisted BDYA no-terminal replay output found. |
| Canada | G20 country | `finance-ca-en`; mature-partial; prod-deploy pack | 23,595 | 11,395 | 79 | 79 | Finance-country parser/profile tests; reviewed program/org fixture; starter graph promoted Canada relationship test. | Reviewed relationship rows cover a narrow TMX/BoC/infrastructure slice, not complete Canada terminal coverage. | No persisted BDYA no-terminal replay output found. |
| China | G20 country | `finance-cn-en`; scaffold; local-only pack | 8 | 5 | 81 | 81 | Finance-country parser/profile tests; reviewed program/org fixture; starter graph promoted China relationship test. | Pack artifact is scaffold-only even though reviewed program/org graph rows exist; terminal coverage depends on graph lanes rather than a mature country pack. | No persisted BDYA no-terminal replay output found. |
| France | G20 country and EU member | `finance-fr-en`; mature-partial; prod-deploy pack | 1,569 | 650 | 0 | 0 | Finance-country parser/profile tests. | No reviewed 2026-06-29 program/org TSV, reviewed fixture test, or starter promoted-relationship test was found for France. | No persisted BDYA no-terminal replay output found. |
| Germany | G20 country and EU member | `finance-de-en`; mature-partial; prod-deploy pack | 19,363 | 9,466 | 89 | 89 | Finance-country parser/profile tests; reviewed program/org fixture; starter graph promoted Germany relationship test. | Good pack maturity, but relationship rows still cover a narrow reviewed policy/program subset rather than a complete Germany terminal graph. | No persisted BDYA no-terminal replay output found. |
| India | G20 country | `finance-in-en`; scaffold; local-only pack | 6 | 5 | 136 | 136 | Finance-country parser/profile tests; reviewed program/org fixture; starter graph promoted India relationship test. | Pack artifact is scaffold-only while reviewed relationships are broad; issuer/security terminal reliability needs a rebuilt country pack. | No persisted BDYA no-terminal replay output found. |
| Indonesia | G20 country | `finance-id-en`; mature-partial; local-only pack | 3,922 | 1,962 | 13 | 13 | Finance-country parser/profile tests; reviewed PNM Mekaar program/org fixture expands to BBRI only; starter graph promoted Indonesia relationship test covers PNM Mekaar, Ultra Micro, BRI/IDX:BBRI, Bank Indonesia, and BPS rows. | Relationship coverage is bounded to PNM/BRI, Indonesia financial-services and microfinance sectors, and macro rows with explicit macro/FX/rates preconditions; broader Indonesia issuer/product/program coverage remains follow-up. | No persisted BDYA no-terminal replay output found. |
| Italy | G20 country and EU member | `finance-it-en`; scaffold; local-only pack | 8 | 5 | 187 | 187 | Finance-country parser/profile tests; reviewed program/org fixture; starter graph promoted Italy relationship test. | Pack artifact is scaffold-only; reviewed relationship rows are ahead of generated pack maturity. | No persisted BDYA no-terminal replay output found. |
| Japan | G20 country | `finance-jp-en`; mature-partial; prod-deploy pack | 16,558 | 7,947 | 0 | 0 | Finance-country parser/profile tests. | No reviewed 2026-06-29 program/org TSV, reviewed fixture test, or starter promoted-relationship test was found for Japan. | No persisted BDYA no-terminal replay output found. |
| Mexico | G20 country | `finance-mx-en`; mature-partial; local-only pack | 6,021 | 2,945 | 199 | 199 | Finance-country parser/profile tests; reviewed program/org fixture; starter graph promoted Mexico relationship test. | Stronger generated pack exists locally, but the checked prod-deploy registry snapshot does not include `finance-mx-en`. | No persisted BDYA no-terminal replay output found. |
| Russia | G20 country | `finance-ru-en`; mature-partial; local-only pack | 6,533 | 3,267 | 165 | 165 | Finance-country parser/profile tests; reviewed program/org fixture; starter graph promoted Russia relationship test. | Coverage has source-availability and sanctions caveats; broad country-to-commodity rows were previously removed from production to avoid raw country expansion. | No persisted BDYA no-terminal replay output found. |
| Saudi Arabia | G20 country | `finance-sa-en`; scaffold; local-only pack | 5 | 5 | 146 | 146 | Finance-country parser/profile tests; reviewed program/org fixture; starter graph promoted Saudi relationship test. | Pack artifact is scaffold-only; reviewed relationship lanes are ahead of generated pack maturity. | No persisted BDYA no-terminal replay output found. |
| South Africa | G20 country | `finance-za-en`; scaffold; local-only pack | 7 | 5 | 212 | 212 | Finance-country parser/profile tests; reviewed program/org fixture; starter graph promoted South Africa relationship test. | Pack artifact is scaffold-only; graph relationship rows exist but country pack issuer/person depth is missing. | No persisted BDYA no-terminal replay output found. |
| South Korea | G20 country | `finance-kr-en`; mature-partial; prod-deploy pack | 11,097 | 5,547 | 172 | 172 | Finance-country parser/profile tests; reviewed program/org fixture; starter graph promoted South Korea relationship test. | Relationship coverage is narrow around reviewed KRX/BOK/industrial paths; broader chaebol/product/security paths remain follow-up work. | No persisted BDYA no-terminal replay output found. |
| Turkiye | G20 country | `finance-tr-en`; mature-partial; prod-deploy pack | 38,952 | 18,716 | 244 | 244 | Finance-country parser/profile tests; reviewed program/org fixture; starter graph promoted Turkiye relationship test. | Strong pack depth, but macro-gate and sector/product coverage remain incomplete. | No persisted BDYA no-terminal replay output found. |
| United Kingdom | G20 country | `finance-uk-en`; mature-partial; prod-deploy pack | 13,860 | 6,254 | 264 | 264 | Finance-country parser/profile tests; reviewed program/org fixture; starter graph promoted United Kingdom relationship test. | Housebuilder/legal-sector story coverage is not yet represented as a dedicated golden case in ADES; current relationship lane focuses on LSE/BoE/FCA/PRA/gilts. | No persisted BDYA no-terminal replay output found. |
| United States | G20 country | `finance-us-en`; mature-partial; prod-deploy pack | 202,179 | 101,091 | 200 | 200 | Finance-country parser/profile tests; reviewed program/org fixture; starter graph promoted United States relationship test. | Strong issuer/ticker backbone, but product/program/legal/regulatory terminal paths are still only partial. | No persisted BDYA no-terminal replay output found. |
| European Union overlay | EU policy overlay | No `finance-eu-en`, EU pack, or EU policy overlay artifact found | 0 | 0 | 0 | 0 | Generic text/country hint tests mention EU as an entity alias, but no EU policy overlay golden test was found. | NSC-B020 remains unimplemented locally; EU bodies, laws, directives, regulations, and cross-country sector paths are not represented as a country-pack overlay. | No persisted BDYA no-terminal replay output found. |

## Cross-Cutting Findings

- Source profiles exist for all 19 G20 countries in
  `FINANCE_COUNTRY_PROFILES`; no equivalent EU overlay profile or artifact was
  found.
- The strongest local pack artifacts are uneven. The United States, Turkiye,
  Brazil, Canada, Germany, Japan, United Kingdom, South Korea, Australia,
  Russia, Mexico, Argentina, Indonesia, and France have mature-partial pack
  artifacts. China, India, Italy, Saudi Arabia, and South Africa still appear
  scaffold-only in the checked G20 r5 candidate release.
- The checked prod-deploy registry snapshot contains only Argentina,
  Australia, Canada, Germany, France, Japan, South Korea, Turkiye, United
  Kingdom, and United States country packs. Brazil, China, India, Indonesia,
  Italy, Mexico, Russia, Saudi Arabia, and South Africa were local-only in this
  inventory.
- Reviewed program/org relationship rows and matching built graph edges exist for
  17 countries. France and Japan do not have clear matching 2026-06-29 reviewed
  program/org fixture coverage.
- Golden/regression coverage is strongest for reviewed program/org fixture
  paths and starter graph promoted relationship checks. It is not the same as
  BDYA worthy-but-no-terminal replay coverage.
- No ADES-side persisted BDYA replay output was found for this inventory. Replay
  should be treated as a known gap until NSC-0008 and later replay work items
  produce concrete item IDs, before/after decisions, artifact hashes, and
  country-pack replay summaries.

## Deterministic Inventory Commands

These local read-only commands can reproduce the inventory inputs without
deploying or rebuilding artifacts:

```bash
python - <<'PY'
from pathlib import Path
import json
import sys

sys.path.insert(0, "src")
from ades.packs.finance_country import (
    FINANCE_COUNTRY_PEOPLE_SOURCE_PLANS,
    FINANCE_COUNTRY_PROFILES,
    _COUNTRY_ENTITY_DERIVERS,
)

countries = [
    ("ar", "Argentina"),
    ("au", "Australia"),
    ("br", "Brazil"),
    ("ca", "Canada"),
    ("cn", "China"),
    ("fr", "France"),
    ("de", "Germany"),
    ("in", "India"),
    ("id", "Indonesia"),
    ("it", "Italy"),
    ("jp", "Japan"),
    ("mx", "Mexico"),
    ("ru", "Russia"),
    ("sa", "Saudi Arabia"),
    ("za", "South Africa"),
    ("kr", "South Korea"),
    ("tr", "Turkiye"),
    ("uk", "United Kingdom"),
    ("us", "United States"),
]
base = Path("/mnt/githubActions/ades_big_data")
roots = [
    base / "generated_runtime_packs",
    base / "pack_releases/finance-country-g20-2026-04-19-r5/candidates",
    base / "prod-deploy/registry-2026-04-19-finance-country-g20-r5/packs",
]
for code, country in countries:
    pack_id = FINANCE_COUNTRY_PROFILES[code]["pack_id"]
    candidates = []
    for root in roots:
        for build_path in root.rglob(f"{pack_id}/build.json") if root.exists() else []:
            build = json.loads(build_path.read_text())
            entities = build.get("included_entity_count") or build.get("input_entity_count") or 0
            aliases = build.get("alias_count") or build.get("retained_alias_count") or 0
            sources_path = build_path.parent / "sources.json"
            source_rows = 0
            if sources_path.exists():
                sources = json.loads(sources_path.read_text())
                source_rows = sum(int(row.get("record_count") or 0) for row in sources.get("sources", []))
            candidates.append((entities, aliases, source_rows, build_path.parent))
    best = sorted(candidates, reverse=True)[0]
    print(
        code,
        country,
        pack_id,
        "profile_sources=" + str(len(FINANCE_COUNTRY_PROFILES[code].get("sources", []))),
        "deriver=" + str(code in _COUNTRY_ENTITY_DERIVERS),
        "people_plan=" + str(code in FINANCE_COUNTRY_PEOPLE_SOURCE_PLANS),
        "entities=" + str(best[0]),
        "aliases=" + str(best[1]),
        "source_rows=" + str(best[2]),
        "path=" + str(best[3]),
    )
PY

rg -n "def test_reviewed_.*fixture|def test_starter_graph_includes_promoted" \
  tests/unit/test_program_org_relationship_impact.py \
  tests/unit/test_impact_starter_evaluation.py
```
