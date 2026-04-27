import json
from hashlib import sha256
from pathlib import Path
import zipfile

from ades.packs.general_bundle import (
    _iter_wikidata_general_entities,
    build_general_source_bundle,
)
from tests.general_bundle_helpers import create_general_raw_snapshots


def test_build_general_source_bundle_writes_normalized_bundle(tmp_path: Path) -> None:
    snapshots = create_general_raw_snapshots(tmp_path)

    result = build_general_source_bundle(
        wikidata_entities_path=snapshots["wikidata_entities"],
        geonames_places_path=snapshots["geonames_places"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    assert result.pack_id == "general-en"
    assert result.version == "0.2.0"
    assert Path(result.sources_lock_path).exists()
    assert result.source_count == 3
    assert result.entity_record_count == 33
    assert result.rule_record_count == 2
    assert result.wikidata_entity_count == 29
    assert result.geonames_location_count == 2
    assert result.curated_entity_count == 2
    assert result.warnings == []

    bundle_manifest = json.loads(Path(result.bundle_manifest_path).read_text(encoding="utf-8"))
    assert bundle_manifest["pack_id"] == "general-en"
    assert bundle_manifest["entities_path"] == "normalized/entities.jsonl"
    assert bundle_manifest["alias_resolution"] == "analyze"
    assert len(bundle_manifest["sources"]) == 3
    sources_lock = json.loads(Path(result.sources_lock_path).read_text(encoding="utf-8"))
    assert sources_lock["pack_id"] == "general-en"
    assert sources_lock["version"] == "0.2.0"
    assert sources_lock["bundle_manifest_path"] == "bundle.json"
    assert sources_lock["entities_path"] == "normalized/entities.jsonl"
    assert sources_lock["rules_path"] == "normalized/rules.jsonl"
    assert sources_lock["source_count"] == 3
    lock_sources = {item["name"]: item for item in sources_lock["sources"]}
    assert lock_sources["wikidata-general-entities"]["snapshot_sha256"] == _sha256(
        snapshots["wikidata_entities"]
    )
    assert lock_sources["geonames-places"]["snapshot_sha256"] == _sha256(
        snapshots["geonames_places"]
    )
    assert lock_sources["curated-general-entities"]["snapshot_sha256"] == _sha256(
        snapshots["curated_entities"]
    )
    assert lock_sources["wikidata-general-entities"]["adapter"] == "wikidata_general_entities"
    assert lock_sources["geonames-places"]["adapter"] == "geonames_places_delimited"
    assert lock_sources["curated-general-entities"]["adapter"] == "curated_general_entities"
    assert all(item["adapter_version"] == "1" for item in lock_sources.values())

    entity_records = [
        json.loads(line)
        for line in Path(result.entities_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert any(
        item["entity_type"] == "person" and item["canonical_text"] == "Person Alpha"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "organization" and item["canonical_text"] == "Org Alpha"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "location" and item["canonical_text"] == "Metro Alpha"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "location" and item["canonical_text"] == "Metro Beta"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "person" and item["canonical_text"] == "Person Beta"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "person" and item["canonical_text"] == "Person Gamma"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "organization" and item["canonical_text"] == "Org Delta"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "organization" and item["canonical_text"] == "Org Epsilon"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "person" and item["canonical_text"] == "Person Delta"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "organization"
        and item["canonical_text"] == "Org Zeta"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "person" and item["canonical_text"] == "Person Epsilon"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "person" and item["canonical_text"] == "Person Zeta"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "person" and item["canonical_text"] == "Person Eta"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "person" and item["canonical_text"] == "Person Theta"
        for item in entity_records
    )
    assert any(
        item["entity_type"] == "location" and item["canonical_text"] == "Metro Gamma"
        for item in entity_records
    )
    person_epsilon_record = next(
        item for item in entity_records if item["canonical_text"] == "Person Epsilon"
    )
    assert "EpsilonSurname" not in person_epsilon_record["aliases"]
    assert "Person Epsilon Variant Three" in person_epsilon_record["aliases"]
    person_zeta_record = next(
        item for item in entity_records if item["canonical_text"] == "Person Zeta"
    )
    assert "ZetaSurname" not in person_zeta_record["aliases"]
    assert "Person Zeta Honorific" in person_zeta_record["aliases"]
    mira_record = next(
        item for item in entity_records if item["canonical_text"] == "Person Eta"
    )
    assert mira_record["aliases"] == ["Person Eta Variant"]
    ilya_record = next(
        item for item in entity_records if item["canonical_text"] == "Person Theta"
    )
    assert ilya_record["aliases"] == ["Person Theta Variant"]
    metro_gamma_record = next(
        item for item in entity_records if item["canonical_text"] == "Metro Gamma"
    )
    assert metro_gamma_record["aliases"] == ["Metro Gamma Variant"]

    rule_records = [
        json.loads(line)
        for line in Path(result.rules_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert {
        "name": "email_address",
        "label": "email_address",
        "kind": "regex",
        "pattern": r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
    } in rule_records
    assert {
        "name": "url",
        "label": "url",
        "kind": "regex",
        "pattern": r"https?://[^\s]+",
    } in rule_records


def test_iter_wikidata_general_entities_keeps_short_high_support_locations(
    tmp_path: Path,
) -> None:
    wikidata_path = tmp_path / "wikidata_general_entities.jsonl"
    wikidata_path.write_text(
        json.dumps(
            {
                "id": "Q794",
                "entity_type": "location",
                "label": "Iran",
                "aliases": ["Persia"],
                "popularity": 369,
                "source_features": {
                    "alias_count": 1,
                    "has_enwiki": True,
                    "popularity_signal": "sitelinks",
                    "sitelink_count": 369,
                },
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    records = list(_iter_wikidata_general_entities(wikidata_path))

    assert records == [
        {
            "aliases": [],
            "canonical_text": "Iran",
            "entity_id": "wikidata:Q794",
            "entity_type": "location",
            "popularity": 369,
            "source_features": {
                "alias_count": 1,
                "has_enwiki": True,
                "popularity_signal": "sitelinks",
                "sitelink_count": 369,
            },
            "source_id": "Q794",
            "source_name": "wikidata-general-entities",
        }
    ]


def test_build_general_source_bundle_prunes_low_signal_geonames_locations(
    tmp_path: Path,
) -> None:
    snapshots = create_general_raw_snapshots(tmp_path)
    snapshots["geonames_places"].write_text(
        "\n".join(
            [
                "geonameid|name|alternatenames|feature_class|feature_code|country_code|population",
                "100|Metro Beta|Metro Beta Alt|P|PPLC|GB|8900000",
                "101|Cook|Cook|P|PPL|US|50000",
                "102|AI|AI|P|PPL|US|1000000",
                "103|Store|store|P|PPL|US|75000",
                "104|Lima|Lima|P|PPLC|PE|10000000",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = build_general_source_bundle(
        wikidata_entities_path=snapshots["wikidata_entities"],
        geonames_places_path=snapshots["geonames_places"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    entity_records = [
        json.loads(line)
        for line in Path(result.entities_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    location_names = {
        item["canonical_text"]
        for item in entity_records
        if item["entity_type"] == "location"
    }
    assert "Metro Beta" in location_names
    assert "Lima" in location_names
    assert "Cook" not in location_names
    assert "AI" not in location_names
    assert "Store" not in location_names


def test_build_general_source_bundle_preserves_explicit_curated_entity_id_prefix(
    tmp_path: Path,
) -> None:
    snapshots = create_general_raw_snapshots(tmp_path)
    snapshots["curated_entities"].write_text(
        json.dumps(
            {
                "entities": [
                    {
                        "canonical_text": "Pauline Hanson",
                        "entity_type": "person",
                        "entity_id": "wikidata:Q466220",
                        "aliases": ["Pauline Lee Hanson"],
                        "description": "Australian politician",
                        "popularity": 17,
                        "source_features": {
                            "has_enwiki": True,
                            "identifier_count": 30,
                            "sitelink_count": 17,
                            "statement_count": 38,
                        },
                    }
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = build_general_source_bundle(
        wikidata_entities_path=snapshots["wikidata_entities"],
        geonames_places_path=snapshots["geonames_places"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    entity_records = [
        json.loads(line)
        for line in Path(result.entities_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    record = next(item for item in entity_records if item["canonical_text"] == "Pauline Hanson")

    assert record["entity_id"] == "wikidata:Q466220"
    assert record["source_id"] == "Q466220"
    assert record["source_name"] == "curated-general-entities"
    assert record["aliases"] == ["Pauline Lee Hanson"]
    assert record["description"] == "Australian politician"
    assert record["popularity"] == 17
    assert record["source_features"] == {
        "has_enwiki": True,
        "identifier_count": 30,
        "sitelink_count": 17,
        "statement_count": 38,
    }


def test_build_general_source_bundle_accepts_official_geonames_zip(
    tmp_path: Path,
) -> None:
    snapshots = create_general_raw_snapshots(tmp_path)
    geonames_zip_path = snapshots["geonames_places"].with_suffix(".zip")
    geonames_rows = "\n".join(
        [
            "745044\tMetro Alpha\tMetro Alpha\tMetro Alpha Historic,Byzantium\t41.0\t29.0\tP\tPPLA\tTR\t\t34\t\t\t\t15462452\t100\t100\tEurope/Istanbul\t2026-01-01",
            "2643743\tMetro Beta\tMetro Beta\tMetro Beta Alt\t51.5\t-0.1\tP\tPPLC\tGB\t\tENG\t\t\t\t8900000\t10\t10\tEurope/London\t2026-01-01",
        ]
    ) + "\n"
    with zipfile.ZipFile(geonames_zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("allCountries.txt", geonames_rows)

    result = build_general_source_bundle(
        wikidata_entities_path=snapshots["wikidata_entities"],
        geonames_places_path=geonames_zip_path,
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    entity_records = [
        json.loads(line)
        for line in Path(result.entities_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    location_names = {
        item["canonical_text"]
        for item in entity_records
        if item["entity_type"] == "location"
    }
    assert "Metro Alpha" in location_names
    assert "Metro Beta" in location_names


def test_build_general_source_bundle_keeps_only_stable_geonames_place_classes(
    tmp_path: Path,
) -> None:
    snapshots = create_general_raw_snapshots(tmp_path)
    snapshots["geonames_places"].write_text(
        "\n".join(
            [
                "geonameid|name|alternatenames|feature_class|feature_code|country_code|population",
                "100|Metro Capital|Metro Capital Alt|P|PPLC|TR|0",
                "101|Metro Large|Metro Large Alt|P|PPL|TR|150000",
                "102|Metro Tiny|Metro Tiny Alt|P|PPL|TR|10",
                "103|Region Alpha|Region Alpha Alt|A|ADM1|TR|0",
                "104|Mountain Alpha|Mountain Alpha Alt|T|MT|TR|0",
                "105|Lake Alpha|Lake Alpha Alt|H|LK|TR|0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = build_general_source_bundle(
        wikidata_entities_path=snapshots["wikidata_entities"],
        geonames_places_path=snapshots["geonames_places"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    entity_records = [
        json.loads(line)
        for line in Path(result.entities_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    location_names = {
        item["canonical_text"]
        for item in entity_records
        if item["entity_type"] == "location"
    }
    assert "Metro Capital" in location_names
    assert "Metro Large" in location_names
    assert "Region Alpha" in location_names
    assert "Metro Tiny" not in location_names
    assert "Mountain Alpha" not in location_names
    assert "Lake Alpha" not in location_names


def test_build_general_source_bundle_drops_low_support_real_wikidata_rows(
    tmp_path: Path,
) -> None:
    snapshots = create_general_raw_snapshots(tmp_path)
    snapshots["wikidata_entities"].write_text(
        json.dumps(
            {
                "entities": [
                    {
                        "id": "Q1",
                        "entity_type": "person",
                        "label": "Person Supported",
                        "aliases": [],
                        "popularity": 2,
                        "source_features": {
                            "sitelink_count": 2,
                            "alias_count": 0,
                        },
                    },
                    {
                        "id": "Q2",
                        "entity_type": "organization",
                        "label": "Org Supported",
                        "aliases": ["Org Supported Variant"],
                        "popularity": 0,
                        "source_features": {
                            "sitelink_count": 0,
                            "alias_count": 1,
                        },
                    },
                    {
                        "id": "Q3",
                        "entity_type": "person",
                        "label": "Person Unsupported",
                        "aliases": [],
                        "popularity": 0,
                        "source_features": {
                            "sitelink_count": 0,
                            "alias_count": 0,
                            "statement_count": 1,
                        },
                    },
                    {
                        "id": "Q4",
                        "entity_type": "organization",
                        "label": "Org Unsupported Group",
                        "aliases": [],
                        "popularity": 0,
                        "source_features": {
                            "sitelink_count": 0,
                            "alias_count": 0,
                        },
                    },
                    {
                        "id": "Q5",
                        "entity_type": "location",
                        "label": "Tiny",
                        "aliases": [],
                        "popularity": 0,
                        "source_features": {
                            "sitelink_count": 0,
                            "alias_count": 0,
                        },
                    },
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    result = build_general_source_bundle(
        wikidata_entities_path=snapshots["wikidata_entities"],
        geonames_places_path=snapshots["geonames_places"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    entity_records = [
        json.loads(line)
        for line in Path(result.entities_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    canonical_texts = {item["canonical_text"] for item in entity_records}
    assert "Person Supported" in canonical_texts
    assert "Org Supported" in canonical_texts
    assert "Org Unsupported Group" in canonical_texts
    assert "Person Unsupported" not in canonical_texts
    assert "Tiny" not in canonical_texts


def test_build_general_source_bundle_drops_low_support_single_token_locations(
    tmp_path: Path,
) -> None:
    snapshots = create_general_raw_snapshots(tmp_path)
    snapshots["wikidata_entities"].write_text(
        json.dumps(
            {
                "entities": [
                    {
                        "id": "Q1",
                        "entity_type": "location",
                        "label": "Letter",
                        "aliases": [],
                        "popularity": 9,
                        "source_features": {
                            "sitelink_count": 9,
                            "alias_count": 0,
                        },
                    },
                    {
                        "id": "Q2",
                        "entity_type": "location",
                        "label": "London",
                        "aliases": [],
                        "popularity": 10,
                        "source_features": {
                            "sitelink_count": 10,
                            "alias_count": 0,
                        },
                    },
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    result = build_general_source_bundle(
        wikidata_entities_path=snapshots["wikidata_entities"],
        geonames_places_path=snapshots["geonames_places"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )

    entity_records = [
        json.loads(line)
        for line in Path(result.entities_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    location_names = {
        item["canonical_text"]
        for item in entity_records
        if item["entity_type"] == "location"
    }
    assert "Letter" not in location_names
    assert "London" in location_names


def test_iter_wikidata_general_entities_jsonl_skips_filtered_rows(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "wikidata_general_entities.jsonl"
    snapshot_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "id": "Q1",
                        "entity_type": "person",
                        "label": "Person Alpha",
                        "aliases": ["Person Alpha Variant"],
                        "popularity": 5,
                        "source_features": {
                            "sitelink_count": 5,
                            "alias_count": 1,
                            "identifier_count": 1,
                            "statement_count": 10,
                        },
                    }
                ),
                json.dumps(
                    {
                        "id": "Q2",
                        "entity_type": "person",
                        "label": "Person Filtered",
                        "aliases": [],
                        "popularity": 0,
                        "source_features": {
                            "sitelink_count": 0,
                            "alias_count": 0,
                            "identifier_count": 0,
                            "statement_count": 1,
                        },
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    records = list(_iter_wikidata_general_entities(snapshot_path))

    assert [item["canonical_text"] for item in records] == ["Person Alpha"]


def _sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()
