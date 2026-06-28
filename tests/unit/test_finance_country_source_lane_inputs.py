import csv
import json
from pathlib import Path

from ades.impact.issuer_exposure import build_issuer_exposure_source_lane
from ades.impact.policy_sector_proxy import build_policy_sector_source_lane
from ades.impact.source_lane_inputs import derive_finance_country_source_lane_inputs


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
                        "name": "unit-test-finance-country-source",
                        "retrieved_at": "2026-06-28T00:00:00Z",
                    }
                ],
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    (pack_dir / "build.json").write_text(
        '{"generated_at":"2026-06-28T12:00:00Z"}\n',
        encoding="utf-8",
    )
    return pack_dir


def _read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def test_derives_finance_country_source_lane_inputs_for_existing_builders(
    tmp_path: Path,
) -> None:
    packs_root = tmp_path / "packs"
    packs_root.mkdir()
    _write_pack(
        packs_root,
        "finance-uk-en",
        [
            {
                "entity_id": "finance-uk-issuer:alpha-mining",
                "entity_type": "organization",
                "canonical_text": "Alpha Mining plc",
                "metadata": {
                    "country_code": "uk",
                    "category": "issuer",
                    "ticker": "ALP",
                    "sector": ["Mining", "Precious Metals"],
                    "source_url": "https://example.test/alpha/profile",
                },
            },
            {
                "entity_id": "finance-uk-ticker:alp",
                "entity_type": "ticker",
                "canonical_text": "ALP",
                "metadata": {
                    "country_code": "uk",
                    "category": "ticker",
                    "ticker": "ALP",
                    "issuer_name": "Alpha Mining plc",
                },
            },
        ],
    )

    result = derive_finance_country_source_lane_inputs(
        packs_root=packs_root,
        output_root=tmp_path / "source_lane_inputs",
        run_id="derive-inputs",
    )

    assert result.pack_count == 1
    assert result.issuer_count == 1
    assert result.ticker_resolved_count == 1
    assert result.issuer_sector_row_count == 2
    assert result.issuer_geography_row_count == 1

    sector_rows = _read_tsv(result.issuer_sector_tsv_path)
    geography_rows = _read_tsv(result.issuer_geography_tsv_path)
    sector_refs = {row["sector_ref"] for row in sector_rows}
    assert sector_refs == {
        "finance-uk-sector:mining",
        "finance-uk-sector:precious-metals",
    }
    assert {row["ticker_ref"] for row in sector_rows} == {"finance-uk-ticker:alp"}
    assert {row["source_url"] for row in sector_rows} == {"https://example.test/alpha/profile"}
    assert geography_rows == [
        {
            "issuer_ref": "finance-uk-issuer:alpha-mining",
            "issuer_name": "Alpha Mining plc",
            "country_ref": "country:uk",
            "country_name": "United Kingdom",
            "geography_type": "country",
            "ticker_ref": "finance-uk-ticker:alp",
            "ticker_symbol": "ALP",
            "jurisdiction": "uk",
            "relation": "issuer_exposed_to_country",
            "evidence_level": "shallow",
            "confidence": "0.66",
            "direction_hint": "issuer_country_exposure",
            "source_name": "ades-finance-country-pack-country-metadata",
            "source_url": "https://example.test/alpha/profile",
            "source_snapshot": "2026-06-28",
            "source_year": "2026",
            "refresh_policy": "monthly",
            "pack_ids": "finance-uk-en",
            "notes": "Primary issuer jurisdiction from finance-country pack metadata.",
        }
    ]

    policy_lane = build_policy_sector_source_lane(
        policy_sector_tsv_paths=[],
        issuer_sector_tsv_paths=[result.issuer_sector_tsv_path],
        output_root=tmp_path / "policy_lane",
        run_id="policy-lane",
    )
    assert policy_lane.relation_counts == {
        "issuer_has_listed_ticker": 1,
        "sector_affects_issuer": 2,
    }

    exposure_lane = build_issuer_exposure_source_lane(
        issuer_geography_tsv_paths=[result.issuer_geography_tsv_path],
        output_root=tmp_path / "issuer_exposure_lane",
        run_id="issuer-exposure-lane",
    )
    assert exposure_lane.relation_counts == {
        "issuer_exposed_to_country": 1,
        "issuer_has_listed_ticker": 1,
    }
