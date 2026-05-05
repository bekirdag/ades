import csv
import json
import sqlite3
from pathlib import Path

from ades.config import Settings
from ades.impact.expansion import expand_impact_paths
from ades.impact.finance_country_proxy import (
    build_finance_country_proxy_source_lane,
    discover_finance_country_pack_dirs,
    load_finance_country_pack_entities,
)


def _write_pack(root: Path, pack_id: str, rows: list[dict[str, object]]) -> Path:
    pack_dir = root / pack_id
    bundle_dir = root / "bundles" / f"{pack_id}-bundle"
    normalized_dir = bundle_dir / "normalized"
    normalized_dir.mkdir(parents=True)
    pack_dir.mkdir()
    entities_path = normalized_dir / "entities.jsonl"
    entities_path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )
    (pack_dir / "aliases.json").write_text('{"aliases":[]}\n', encoding="utf-8")
    (pack_dir / "sources.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "pack_id": pack_id,
                "entities_path": str(entities_path),
                "sources": [
                    {
                        "name": "unit-test-source",
                        "retrieved_at": "2026-05-05T00:00:00Z",
                    }
                ],
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    (pack_dir / "build.json").write_text('{"generated_at":"2026-05-05T12:00:00Z"}\n', encoding="utf-8")
    return pack_dir


def _read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def _write_alias_pack(root: Path, pack_id: str, rows: list[dict[str, object]]) -> Path:
    pack_dir = root / pack_id
    pack_dir.mkdir(parents=True)
    (pack_dir / "aliases.json").write_text(
        json.dumps({"aliases": rows}, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (pack_dir / "sources.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "pack_id": pack_id,
                "sources": [
                    {
                        "name": "unit-test-alias-source",
                        "url": "https://example.test/politics",
                        "retrieved_at": "2026-05-05T00:00:00Z",
                    }
                ],
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return pack_dir


def test_finance_country_proxy_generator_writes_direct_and_proxy_edges(tmp_path: Path) -> None:
    packs_root = tmp_path / "packs"
    packs_root.mkdir()
    _write_pack(
        packs_root,
        "finance-us-en",
        [
            {
                "entity_id": "finance-us-issuer:0001",
                "entity_type": "organization",
                "canonical_text": "Example Issuer Inc.",
                "metadata": {"country_code": "us", "category": "issuer", "ticker": "ABC"},
            },
            {
                "entity_id": "finance-us-ticker:ABC",
                "entity_type": "ticker",
                "canonical_text": "ABC",
                "metadata": {"country_code": "us", "category": "ticker", "ticker": "ABC", "exchanges": ["NYSE"]},
            },
            {
                "entity_id": "finance-person:0001:chief-executive",
                "entity_type": "person",
                "canonical_text": "Example CEO",
                "metadata": {
                    "country_code": "us",
                    "employer_ticker": "ABC",
                    "source_url": "https://www.sec.gov/example",
                },
            },
            {
                "entity_id": "finance-us:sec",
                "entity_type": "organization",
                "canonical_text": "Securities and Exchange Commission",
                "metadata": {"country_code": "us", "category": "securities_regulator"},
            },
        ],
    )

    result = build_finance_country_proxy_source_lane(
        packs_root=packs_root,
        output_root=tmp_path / "impact_relationships",
        run_id="unit-test",
        build_artifact=True,
        artifact_output_root=tmp_path / "artifacts",
        extra_proxy_pack_ids=(),
    )

    edges = _read_tsv(result.edge_tsv_path)
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    edge_keys = {(row["source_ref"], row["relation"], row["target_ref"]) for row in edges}
    relation_counts = {}
    for row in edges:
        relation_counts[row["relation"]] = relation_counts.get(row["relation"], 0) + 1
    assert result.pack_count == 1
    assert result.entity_count == 4
    assert result.uncovered_entity_count == 0
    assert manifest["relation_counts"] == relation_counts
    assert ("finance-us-issuer:0001", "issuer_has_listed_ticker", "finance-us-ticker:ABC") in edge_keys
    assert (
        "finance-person:0001:chief-executive",
        "person_affects_employer_ticker",
        "finance-us-ticker:ABC",
    ) in edge_keys
    assert ("finance-us:sec", "entity_affects_dxy_proxy", "ades:impact:indicator:dxy") in edge_keys
    assert result.edge_count >= 27

    assert result.artifact_path is not None
    assert result.artifact_edge_count is not None
    assert result.artifact_edge_count > result.edge_count
    assert manifest["include_starter_graph"] is True
    assert manifest["artifact_edge_count"] == result.artifact_edge_count
    connection = sqlite3.connect(str(result.artifact_path))
    try:
        sqlite_edge_count = connection.execute("SELECT COUNT(*) FROM impact_edges").fetchone()[0]
        assert sqlite_edge_count == result.artifact_edge_count
    finally:
        connection.close()

    starter_result = expand_impact_paths(
        ["ades:heuristic_structural_location:finance-en:location:strait-of-hormuz"],
        settings=Settings(
            impact_expansion_enabled=True,
            impact_expansion_artifact_path=result.artifact_path,
        ),
        max_depth=2,
        max_candidates=5,
    )
    assert {
        candidate.entity_ref for candidate in starter_result.candidates
    } >= {"ades:impact:commodity:crude-oil"}


def test_finance_country_proxy_generator_can_fallback_to_aliases(tmp_path: Path) -> None:
    packs_root = tmp_path / "packs"
    pack_dir = packs_root / "finance-uk-en"
    pack_dir.mkdir(parents=True)
    (pack_dir / "aliases.json").write_text(
        json.dumps(
            {
                "aliases": [
                    {
                        "entity_id": "finance-uk:ftse-100",
                        "canonical_text": "FTSE 100",
                        "label": "market_index",
                        "text": "FTSE 100",
                    },
                    {
                        "entity_id": "finance-uk:regulator",
                        "canonical_text": "UK regulator",
                        "label": "organization",
                        "text": "UK regulator",
                    },
                ]
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    assert discover_finance_country_pack_dirs(packs_root) == [pack_dir.resolve()]
    entities = load_finance_country_pack_entities(pack_dir)
    assert {entity.entity_ref for entity in entities} == {"finance-uk:ftse-100", "finance-uk:regulator"}

    result = build_finance_country_proxy_source_lane(
        packs_root=packs_root,
        output_root=tmp_path / "impact_relationships",
        run_id="alias-fallback",
        extra_proxy_pack_ids=(),
    )

    edges = _read_tsv(result.edge_tsv_path)
    assert result.entity_count == 2
    assert result.uncovered_entity_count == 0
    assert any(
        row["source_ref"] == "finance-uk:regulator"
        and row["target_ref"] == "ades:impact:currency:gbp"
        and row["relation"] == "entity_affects_currency_proxy"
        for row in edges
    )


def test_domain_pack_proxy_generator_links_political_entities_to_global_assets(
    tmp_path: Path,
) -> None:
    packs_root = tmp_path / "packs"
    packs_root.mkdir()
    _write_pack(
        packs_root,
        "finance-us-en",
        [
            {
                "entity_id": "finance-us-issuer:0001",
                "entity_type": "organization",
                "canonical_text": "Example Issuer Inc.",
                "metadata": {"country_code": "us", "category": "issuer", "ticker": "ABC"},
            }
        ],
    )
    _write_alias_pack(
        packs_root,
        "politics-vector-en",
        [
            {
                "entity_id": "wikidata:QTEST",
                "label": "person",
                "canonical_text": "Example Minister",
                "text": "Example Minister",
            }
        ],
    )

    result = build_finance_country_proxy_source_lane(
        packs_root=packs_root,
        output_root=tmp_path / "impact_relationships",
        run_id="domain-proxy",
        extra_proxy_pack_ids=("politics-vector-en",),
    )

    edges = _read_tsv(result.edge_tsv_path)
    assert result.extra_proxy_pack_count == 1
    assert result.extra_proxy_entity_count == 1
    assert {
        (row["source_ref"], row["relation"], row["target_ref"])
        for row in edges
        if row["source_ref"] == "wikidata:QTEST"
    } >= {
        ("wikidata:QTEST", "entity_affects_usd_proxy", "ades:impact:currency:usd"),
        ("wikidata:QTEST", "entity_affects_dxy_proxy", "ades:impact:indicator:dxy"),
        (
            "wikidata:QTEST",
            "entity_affects_global_policy_risk_proxy",
            "ades:impact:risk:global-policy-risk",
        ),
    }
