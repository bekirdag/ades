import json
import os
from pathlib import Path
import sqlite3

import ades.packs.general_sources as general_sources
from ades.packs.general_bundle import build_general_source_bundle
from ades.packs.general_sources import fetch_general_source_snapshot
from tests.general_bundle_helpers import create_general_remote_sources


def test_fetch_general_source_snapshot_writes_immutable_snapshot_dir(tmp_path: Path) -> None:
    remote_sources = create_general_remote_sources(tmp_path / "remote")

    result = fetch_general_source_snapshot(
        output_dir=tmp_path / "raw" / "general-en",
        snapshot="2026-04-10",
        wikidata_url=remote_sources["wikidata_url"],
        geonames_places_url=remote_sources["geonames_places_url"],
    )

    assert result.pack_id == "general-en"
    assert result.snapshot == "2026-04-10"
    assert result.source_count == 3
    assert result.wikidata_entity_count == 12
    assert result.geonames_location_count == 2
    assert result.curated_entity_count == 0
    assert Path(result.snapshot_dir).exists()
    assert Path(result.wikidata_entities_path).exists()
    assert Path(result.geonames_places_path).exists()
    assert Path(result.curated_entities_path).exists()
    assert Path(result.source_manifest_path).exists()
    assert result.warnings == []

    manifest = json.loads(Path(result.source_manifest_path).read_text(encoding="utf-8"))
    assert manifest["pack_id"] == "general-en"
    assert manifest["snapshot"] == "2026-04-10"
    sources = {item["name"]: item for item in manifest["sources"]}
    assert sources["wikidata-general-entities"]["source_url"] == remote_sources["wikidata_url"]
    assert sources["geonames-places"]["source_url"] == remote_sources["geonames_places_url"]
    assert sources["curated-general-entities"]["source_url"].startswith("operator://")

    bundle = build_general_source_bundle(
        wikidata_entities_path=result.wikidata_entities_path,
        geonames_places_path=result.geonames_places_path,
        curated_entities_path=result.curated_entities_path,
        output_dir=tmp_path / "bundles",
    )
    assert bundle.wikidata_entity_count == 12
    assert bundle.geonames_location_count == 2
    assert bundle.curated_entity_count == 0


def test_fetch_general_source_snapshot_rejects_existing_snapshot_dir(
    tmp_path: Path,
) -> None:
    remote_sources = create_general_remote_sources(tmp_path / "remote")
    output_root = tmp_path / "raw" / "general-en"

    fetch_general_source_snapshot(
        output_dir=output_root,
        snapshot="2026-04-10",
        wikidata_url=remote_sources["wikidata_url"],
        geonames_places_url=remote_sources["geonames_places_url"],
    )

    try:
        fetch_general_source_snapshot(
            output_dir=output_root,
            snapshot="2026-04-10",
            wikidata_url=remote_sources["wikidata_url"],
            geonames_places_url=remote_sources["geonames_places_url"],
        )
    except FileExistsError as exc:
        assert "already exists" in str(exc)
    else:
        raise AssertionError("Expected FileExistsError for an immutable snapshot dir.")


def test_fetch_general_source_snapshot_preserves_existing_resume_dir_on_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    output_root = tmp_path / "raw" / "general-en"
    snapshot_dir = output_root / "2026-04-10"
    snapshot_dir.mkdir(parents=True)
    preserved_path = snapshot_dir / "preserve.txt"
    preserved_path.write_text("keep-me\n", encoding="utf-8")

    def raise_download_error(**_: object) -> object:
        raise RuntimeError("boom")

    monkeypatch.setattr(
        general_sources,
        "_download_wikidata_snapshot",
        raise_download_error,
    )

    try:
        fetch_general_source_snapshot(
            output_dir=output_root,
            snapshot="2026-04-10",
        )
    except RuntimeError as exc:
        assert "boom" in str(exc)
    else:
        raise AssertionError("Expected the injected download failure.")

    assert snapshot_dir.exists()
    assert preserved_path.read_text(encoding="utf-8") == "keep-me\n"


def test_download_source_file_truncates_oversized_http_resume(
    tmp_path: Path,
    monkeypatch,
) -> None:
    destination = tmp_path / "oversized-download.bin"
    destination.write_bytes(b"0123456789abc")

    def raise_range_not_satisfiable(
        request: general_sources.urllib.request.Request,
        timeout: float,
    ) -> object:
        raise general_sources.urllib.error.HTTPError(
            request.full_url,
            416,
            "Range Not Satisfiable",
            {"Content-Range": "bytes */11"},
            None,
        )

    monkeypatch.setattr(
        general_sources.urllib.request,
        "urlopen",
        raise_range_not_satisfiable,
    )

    general_sources._download_source_file(
        "https://example.com/oversized-download.bin",
        destination,
        user_agent="ades-test",
    )

    assert destination.read_bytes() == b"0123456789a"


def test_download_source_file_skips_copy_when_file_url_matches_destination(
    tmp_path: Path,
) -> None:
    destination = tmp_path / "wikidata_truthy.source.nt"
    destination.write_text("already-here\n", encoding="utf-8")

    general_sources._download_source_file(
        destination.resolve().as_uri(),
        destination,
        user_agent="ades-test",
    )

    assert destination.read_text(encoding="utf-8") == "already-here\n"


def test_download_source_file_skips_copy_for_existing_hardlink_destination(
    tmp_path: Path,
) -> None:
    source = tmp_path / "wikidata_truthy.source.nt"
    source.write_text("already-linked\n", encoding="utf-8")
    destination = tmp_path / "resume" / "wikidata_truthy.source.nt"
    destination.parent.mkdir(parents=True, exist_ok=True)
    os.link(source, destination)

    general_sources._download_source_file(
        source.resolve().as_uri(),
        destination,
        user_agent="ades-test",
    )

    assert destination.read_text(encoding="utf-8") == "already-linked\n"


def test_validate_wikidata_source_quality_rejects_large_zero_popularity_snapshot(
    tmp_path: Path,
) -> None:
    jsonl_path = tmp_path / "wikidata_general_entities.jsonl"
    with jsonl_path.open("w", encoding="utf-8") as handle:
        for index in range(
            general_sources._WIKIDATA_SOURCE_QUALITY_GATE_MIN_RECORDS
        ):
            handle.write(
                json.dumps(
                    {
                        "id": f"Q{index + 1}",
                        "entity_type": "person",
                        "label": f"Person {index + 1}",
                        "aliases": [],
                        "popularity": 0,
                        "source_features": {"sitelink_count": 0},
                    },
                    sort_keys=True,
                )
                + "\n"
            )

    summary = general_sources._summarize_wikidata_source_quality(jsonl_path)

    assert summary.record_count == general_sources._WIKIDATA_SOURCE_QUALITY_GATE_MIN_RECORDS
    assert summary.positive_popularity_count == 0
    try:
        general_sources._validate_wikidata_source_quality(
            summary,
            source_url="file:///tmp/wikidata_general_entities.jsonl",
        )
    except ValueError as exc:
        assert "quality is too weak for production" in str(exc)
    else:
        raise AssertionError("Expected degraded large Wikidata snapshot to fail validation.")


def test_validate_wikidata_source_quality_accepts_large_supported_snapshot(
    tmp_path: Path,
) -> None:
    jsonl_path = tmp_path / "wikidata_general_entities.jsonl"
    with jsonl_path.open("w", encoding="utf-8") as handle:
        supported_count = 0
        for index in range(
            general_sources._WIKIDATA_SOURCE_QUALITY_GATE_MIN_RECORDS
        ):
            supported = index < 500
            if supported:
                supported_count += 1
            handle.write(
                json.dumps(
                    {
                        "id": f"Q{index + 1}",
                        "entity_type": "organization",
                        "label": f"Organization {index + 1}",
                        "aliases": [f"Org {index + 1}"] if supported else [],
                        "popularity": 3 if supported else 0,
                        "source_features": {"sitelink_count": 3 if supported else 0},
                    },
                    sort_keys=True,
                )
                + "\n"
            )

    summary = general_sources._summarize_wikidata_source_quality(jsonl_path)

    assert summary.record_count == general_sources._WIKIDATA_SOURCE_QUALITY_GATE_MIN_RECORDS
    assert summary.alias_backed_count == supported_count
    assert summary.positive_popularity_count == supported_count
    general_sources._validate_wikidata_source_quality(
        summary,
        source_url="file:///tmp/wikidata_general_entities.jsonl",
    )


def test_fetch_general_source_snapshot_can_extract_from_truthy_dump(
    tmp_path: Path,
) -> None:
    remote_sources = create_general_remote_sources(tmp_path / "remote")
    truthy_path = tmp_path / "remote" / "wikidata_truthy.source.nt"
    truthy_path.write_text(
        "\n".join(
            [
                "<http://www.wikidata.org/entity/Q100> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q5> .",
                "<http://www.wikidata.org/entity/Q100> <http://www.w3.org/2000/01/rdf-schema#label> \"Person Lambda\"@en .",
                "<http://www.wikidata.org/entity/Q100> <http://www.w3.org/2004/02/skos/core#altLabel> \"Person Lambda Variant\"@en .",
                "<https://www.wikidata.org/wiki/Special:EntityData/Q100> <http://wikiba.se/ontology#sitelinks> \"200\"^^<http://www.w3.org/2001/XMLSchema#integer> .",
                "<http://www.wikidata.org/entity/Q200> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q43229> .",
                "<http://www.wikidata.org/entity/Q200> <http://www.w3.org/2000/01/rdf-schema#label> \"Org Tiny\"@en .",
                "<http://www.wikidata.org/wiki/Special:EntityData/Q200> <http://wikiba.se/ontology#sitelinks> \"1\"^^<http://www.w3.org/2001/XMLSchema#integer> .",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = fetch_general_source_snapshot(
        output_dir=tmp_path / "raw" / "general-en",
        snapshot="2026-04-10",
        wikidata_url=None,
        wikidata_truthy_url=truthy_path.resolve().as_uri(),
        wikidata_entities_url=None,
        wikidata_seed_url=None,
        geonames_places_url=remote_sources["geonames_places_url"],
    )

    rows = [
        json.loads(line)
        for line in Path(result.wikidata_entities_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert result.wikidata_entity_count == 1
    assert Path(result.wikidata_entities_path).suffix == ".jsonl"
    assert rows == [
        {
            "id": "Q100",
            "entity_type": "person",
            "label": "Person Lambda",
            "aliases": ["Person Lambda Variant"],
            "popularity": 200,
            "source_features": {
                "sitelink_count": 200,
                "alias_count": 1,
                "popularity_signal": "sitelinks",
            },
            "type_ids": ["Q5"],
        }
    ]


def test_fetch_general_source_snapshot_resolves_truthy_subclass_matches(
    tmp_path: Path,
) -> None:
    remote_sources = create_general_remote_sources(tmp_path / "remote")
    truthy_path = tmp_path / "remote" / "wikidata_truthy_subclass.source.nt"
    truthy_path.write_text(
        "\n".join(
            [
                "<http://www.wikidata.org/entity/Q300> <http://www.wikidata.org/prop/direct/P279> <http://www.wikidata.org/entity/Q43229> .",
                "<http://www.wikidata.org/entity/Q200> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q300> .",
                "<http://www.wikidata.org/entity/Q200> <http://www.w3.org/2000/01/rdf-schema#label> \"Org Tiny\"@en .",
                "<https://www.wikidata.org/wiki/Special:EntityData/Q200> <http://wikiba.se/ontology#sitelinks> \"30\"^^<http://www.w3.org/2001/XMLSchema#integer> .",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = fetch_general_source_snapshot(
        output_dir=tmp_path / "raw" / "general-en",
        snapshot="2026-04-10",
        wikidata_url=None,
        wikidata_truthy_url=truthy_path.resolve().as_uri(),
        wikidata_entities_url=None,
        wikidata_seed_url=None,
        geonames_places_url=remote_sources["geonames_places_url"],
    )

    rows = [
        json.loads(line)
        for line in Path(result.wikidata_entities_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert result.wikidata_entity_count == 1
    assert rows == [
        {
            "id": "Q200",
            "entity_type": "organization",
            "label": "Org Tiny",
            "aliases": [],
            "popularity": 30,
            "source_features": {
                "sitelink_count": 30,
                "alias_count": 0,
                "popularity_signal": "sitelinks",
            },
            "type_ids": ["Q300"],
        }
    ]


def test_fetch_general_source_snapshot_merges_non_contiguous_truthy_segments(
    tmp_path: Path,
) -> None:
    remote_sources = create_general_remote_sources(tmp_path / "remote")
    truthy_path = tmp_path / "remote" / "wikidata_truthy_segment_merge.source.nt"
    truthy_path.write_text(
        "\n".join(
            [
                "<https://www.wikidata.org/wiki/Special:EntityData/Q500> <http://schema.org/version> \"1\"^^<http://www.w3.org/2001/XMLSchema#integer> .",
                "<http://www.wikidata.org/entity/Q500> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q5> .",
                "<http://www.wikidata.org/entity/Q500> <http://www.w3.org/2000/01/rdf-schema#label> \"Person Segmented\"@en .",
                "<http://www.wikidata.org/entity/Q501> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q5> .",
                "<http://www.wikidata.org/entity/Q501> <http://www.w3.org/2000/01/rdf-schema#label> \"Person Low Signal\"@en .",
                "<https://www.wikidata.org/wiki/Special:EntityData/Q501> <http://wikiba.se/ontology#sitelinks> \"1\"^^<http://www.w3.org/2001/XMLSchema#integer> .",
                "<https://www.wikidata.org/wiki/Special:EntityData/Q500> <http://wikiba.se/ontology#sitelinks> \"200\"^^<http://www.w3.org/2001/XMLSchema#integer> .",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = fetch_general_source_snapshot(
        output_dir=tmp_path / "raw" / "general-en",
        snapshot="2026-04-10",
        wikidata_url=None,
        wikidata_truthy_url=truthy_path.resolve().as_uri(),
        wikidata_entities_url=None,
        wikidata_seed_url=None,
        geonames_places_url=remote_sources["geonames_places_url"],
    )

    rows = [
        json.loads(line)
        for line in Path(result.wikidata_entities_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert result.wikidata_entity_count == 1
    assert rows == [
        {
            "id": "Q500",
            "entity_type": "person",
            "label": "Person Segmented",
            "aliases": [],
            "popularity": 200,
            "source_features": {
                "sitelink_count": 200,
                "alias_count": 0,
                "popularity_signal": "sitelinks",
            },
            "type_ids": ["Q5"],
        }
    ]


def test_fetch_general_source_snapshot_keeps_truthy_article_titles_as_aliases(
    tmp_path: Path,
) -> None:
    remote_sources = create_general_remote_sources(tmp_path / "remote")
    truthy_path = tmp_path / "remote" / "wikidata_truthy_article_titles.source.nt"
    truthy_path.write_text(
        "\n".join(
            [
                "<http://www.wikidata.org/entity/Q700> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q5> .",
                "<http://www.wikidata.org/entity/Q700> <http://www.w3.org/2000/01/rdf-schema#label> \"Person Canonical\"@en .",
                "<https://www.wikidata.org/wiki/Special:EntityData/Q700> <http://wikiba.se/ontology#sitelinks> \"250\"^^<http://www.w3.org/2001/XMLSchema#integer> .",
                "<https://en.wikipedia.org/wiki/Person_Canonical> <http://schema.org/about> <http://www.wikidata.org/entity/Q700> .",
                "<https://en.wikipedia.org/wiki/Person_Canonical> <http://schema.org/name> \"Person Surface\"@en .",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = fetch_general_source_snapshot(
        output_dir=tmp_path / "raw" / "general-en",
        snapshot="2026-04-10",
        wikidata_url=None,
        wikidata_truthy_url=truthy_path.resolve().as_uri(),
        wikidata_entities_url=None,
        wikidata_seed_url=None,
        geonames_places_url=remote_sources["geonames_places_url"],
    )

    rows = [
        json.loads(line)
        for line in Path(result.wikidata_entities_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert rows == [
        {
            "id": "Q700",
            "entity_type": "person",
            "label": "Person Canonical",
            "aliases": ["Person Surface"],
            "popularity": 250,
            "source_features": {
                "sitelink_count": 250,
                "alias_count": 1,
                "popularity_signal": "sitelinks",
            },
            "type_ids": ["Q5"],
        }
    ]


def test_fetch_general_source_snapshot_keeps_public_broadcasters_as_organizations(
    tmp_path: Path,
) -> None:
    remote_sources = create_general_remote_sources(tmp_path / "remote")
    truthy_path = tmp_path / "remote" / "wikidata_truthy_public_broadcaster.source.nt"
    truthy_path.write_text(
        "\n".join(
            [
                "<http://www.wikidata.org/entity/Q9531> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q1126006> .",
                "<http://www.wikidata.org/entity/Q9531> <http://www.w3.org/2000/01/rdf-schema#label> \"BBC\"@en .",
                "<https://www.wikidata.org/wiki/Special:EntityData/Q9531> <http://wikiba.se/ontology#sitelinks> \"250\"^^<http://www.w3.org/2001/XMLSchema#integer> .",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = fetch_general_source_snapshot(
        output_dir=tmp_path / "raw" / "general-en",
        snapshot="2026-04-10",
        wikidata_url=None,
        wikidata_truthy_url=truthy_path.resolve().as_uri(),
        wikidata_entities_url=None,
        wikidata_seed_url=None,
        geonames_places_url=remote_sources["geonames_places_url"],
    )

    rows = [
        json.loads(line)
        for line in Path(result.wikidata_entities_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert rows == [
        {
            "id": "Q9531",
            "entity_type": "organization",
            "label": "BBC",
            "aliases": [],
            "popularity": 250,
            "source_features": {
                "sitelink_count": 250,
                "alias_count": 0,
                "popularity_signal": "sitelinks",
            },
            "type_ids": ["Q1126006"],
        }
    ]


def test_fetch_general_source_snapshot_keeps_explicit_seed_overlay_below_bulk_threshold(
    tmp_path: Path,
) -> None:
    remote_sources = create_general_remote_sources(tmp_path / "remote")
    truthy_path = tmp_path / "remote" / "wikidata_truthy_seed.source.nt"
    truthy_path.write_text(
        "\n".join(
            [
                "<http://www.wikidata.org/entity/Q265852> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q5> .",
                "<http://www.wikidata.org/entity/Q265852> <http://www.w3.org/2000/01/rdf-schema#label> \"Person Alpha\"@en .",
                "<http://www.wikidata.org/entity/Q265852> <http://www.w3.org/2004/02/skos/core#altLabel> \"Person Alpha Variant\"@en .",
                "<https://www.wikidata.org/wiki/Special:EntityData/Q265852> <http://wikiba.se/ontology#sitelinks> \"12\"^^<http://www.w3.org/2001/XMLSchema#integer> .",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    seed_payload_path = tmp_path / "remote" / "wikidata_seed_low_signal_overlay.source.json"
    seed_payload_path.write_text(
        json.dumps(
            {
                "entities": {
                    "Q265852": {
                        "id": "Q265852",
                        "entity_type": "person",
                        "label": "Person Alpha",
                        "aliases": ["Person Alpha Variant"],
                        "popularity": 12,
                    }
                }
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    result = fetch_general_source_snapshot(
        output_dir=tmp_path / "raw" / "general-en",
        snapshot="2026-04-10",
        wikidata_url=None,
        wikidata_truthy_url=truthy_path.resolve().as_uri(),
        wikidata_entities_url=None,
        wikidata_seed_url=seed_payload_path.resolve().as_uri(),
        geonames_places_url=remote_sources["geonames_places_url"],
    )

    rows = [
        json.loads(line)
        for line in Path(result.wikidata_entities_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert result.wikidata_entity_count == 1
    assert result.source_count == 4
    assert rows == [
        {
            "id": "Q265852",
            "entity_type": "person",
            "label": "Person Alpha",
            "aliases": ["Person Alpha Variant"],
            "popularity": 12,
                "source_features": {
                    "sitelink_count": 12,
                    "alias_count": 1,
                    "popularity_signal": "sitelinks",
                },
            "type_ids": ["Q5"],
        }
    ]


def test_normalize_wikidata_dump_entity_keeps_supported_low_sitelink_person() -> None:
    entity = {
        "id": "Q123",
        "labels": {"en": {"value": "Person Middle Example"}},
        "descriptions": {"en": {"value": "Example person"}},
        "claims": {
            "P31": [
                {
                    "mainsnak": {
                        "snaktype": "value",
                        "datavalue": {"value": {"id": "Q5"}},
                    }
                }
            ],
            "P106": [{}],
            "P569": [{}],
            "P19": [{}],
            "P27": [{}],
            "P734": [{}],
            "P735": [{}],
            "P18": [{}],
            "P214": [{}],
            "P646": [{}],
        },
        "aliases": {"en": [{"value": "Person Example"}]},
        "sitelinks": {"commonswiki": {"title": "Person_Middle_Example"}},
    }

    normalized = general_sources._normalize_wikidata_dump_entity(
        entity,
        type_matches={"Q5": (0, "person")},
    )

    assert normalized is not None
    assert normalized["entity_type"] == "person"
    assert normalized["label"] == "Person Middle Example"
    assert normalized["aliases"] == ["Person Example"]


def test_normalize_wikidata_dump_entity_rejects_low_signal_location() -> None:
    entity = {
        "id": "Q124",
        "labels": {"en": {"value": "North Example"}},
        "claims": {
            "P31": [
                {
                    "mainsnak": {
                        "snaktype": "value",
                        "datavalue": {"value": {"id": "Q486972"}},
                    }
                }
            ],
        },
        "aliases": {"en": [{"value": "North Example Alt"}]},
        "sitelinks": {"commonswiki": {"title": "North_Example"}},
    }

    normalized = general_sources._normalize_wikidata_dump_entity(
        entity,
        type_matches={"Q486972": (0, "location")},
    )

    assert normalized is None


def test_fetch_general_source_snapshot_merges_seed_overlay_into_truthy_dump(
    tmp_path: Path,
) -> None:
    remote_sources = create_general_remote_sources(tmp_path / "remote")
    truthy_path = tmp_path / "remote" / "wikidata_truthy_overlay.source.nt"
    truthy_path.write_text(
        "\n".join(
            [
                "<http://www.wikidata.org/entity/Q305177> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q5> .",
                "<http://www.wikidata.org/entity/Q305177> <http://www.w3.org/2000/01/rdf-schema#label> \"Person Gamma\"@en .",
                "<https://www.wikidata.org/wiki/Special:EntityData/Q305177> <http://wikiba.se/ontology#sitelinks> \"200\"^^<http://www.w3.org/2001/XMLSchema#integer> .",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    seed_payload_path = tmp_path / "remote" / "wikidata_seed_overlay.source.json"
    seed_payload_path.write_text(
        json.dumps(
            {
                "entities": {
                    "Q7426870": {
                        "id": "Q7426870",
                        "entity_type": "person",
                        "labels": {"en": {"value": "Person Beta"}},
                        "aliases": {
                            "en": [
                                {"value": "Person Beta Variant"},
                            ]
                        },
                    }
                }
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    result = fetch_general_source_snapshot(
        output_dir=tmp_path / "raw" / "general-en",
        snapshot="2026-04-10",
        wikidata_url=None,
        wikidata_truthy_url=truthy_path.resolve().as_uri(),
        wikidata_entities_url=None,
        wikidata_seed_url=seed_payload_path.resolve().as_uri(),
        geonames_places_url=remote_sources["geonames_places_url"],
    )

    rows = [
        json.loads(line)
        for line in Path(result.wikidata_entities_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert result.wikidata_entity_count == 2
    assert result.source_count == 4
    assert rows == [
        {
            "id": "Q305177",
            "entity_type": "person",
            "label": "Person Gamma",
            "aliases": [],
            "popularity": 200,
            "source_features": {
                "sitelink_count": 200,
                "alias_count": 0,
                "popularity_signal": "sitelinks",
            },
            "type_ids": ["Q5"],
        },
        {
            "id": "Q7426870",
            "entity_type": "person",
            "label": "Person Beta",
            "aliases": ["Person Beta Variant"],
            "popularity": None,
        },
    ]
    manifest = json.loads(Path(result.source_manifest_path).read_text(encoding="utf-8"))
    source_names = {item["name"] for item in manifest["sources"]}
    assert "wikidata-seed-entities" in source_names


def test_iter_text_lines_prefers_pigz_over_rapidgzip(tmp_path: Path, monkeypatch) -> None:
    gzip_path = tmp_path / "sample.txt.gz"
    gzip_path.write_bytes(b"placeholder")
    popen_calls: list[list[str]] = []

    class _FakeStream:
        def __iter__(self):
            yield "hello\n"

        def close(self) -> None:
            return None

        def read(self) -> str:
            return ""

    class _FakeProcess:
        def __init__(self, args: list[str], **_: object) -> None:
            popen_calls.append(args)
            self.stdout = _FakeStream()
            self.stderr = _FakeStream()

        def kill(self) -> None:
            return None

        def wait(self) -> int:
            return 0

    def fake_which(name: str) -> str | None:
        if name == "pigz":
            return "/usr/bin/pigz"
        if name == "rapidgzip":
            return "/home/wodo/.local/bin/rapidgzip"
        return None

    monkeypatch.setattr(general_sources.shutil, "which", fake_which)
    monkeypatch.setattr(general_sources.subprocess, "Popen", _FakeProcess)

    assert list(general_sources._iter_text_lines(gzip_path)) == ["hello\n"]
    assert popen_calls == [["/usr/bin/pigz", "-dc", str(gzip_path)]]


def test_iter_wikidata_truthy_relevant_lines_filters_expected_predicates(
    tmp_path: Path,
) -> None:
    nt_path = tmp_path / "wikidata_truthy.source.nt"
    label_predicate = sorted(general_sources._WIKIDATA_LABEL_PREDICATE_TERMS)[0]
    nt_path.write_text(
        "\n".join(
            [
                "ignored line",
                f"<http://www.wikidata.org/entity/Q1> {general_sources._WIKIDATA_DIRECT_P31_TERM} <http://www.wikidata.org/entity/Q5> .",
                f"<http://www.wikidata.org/entity/Q2> {general_sources._WIKIDATA_DIRECT_P279_TERM} <http://www.wikidata.org/entity/Q43229> .",
                f"<https://en.wikipedia.org/wiki/Q2> {general_sources._WIKIDATA_SCHEMA_ABOUT_PREDICATE_TERM} <http://www.wikidata.org/entity/Q2> .",
                f"<http://www.wikidata.org/entity/Q3> {general_sources._WIKIDATA_SITELINKS_PREDICATE_TERM} \"10\"^^<http://www.w3.org/2001/XMLSchema#integer> .",
                f"<http://www.wikidata.org/entity/Q4> {label_predicate} \"Label\"@en .",
                f"<http://www.wikidata.org/entity/Q5> {general_sources._WIKIDATA_ALT_LABEL_PREDICATE_TERM} \"Alias\"@en .",
                f"<http://www.wikidata.org/entity/Q6> {label_predicate} \"Etiqueta\"@es .",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    assert list(general_sources._iter_wikidata_truthy_relevant_lines(nt_path)) == [
        f"<http://www.wikidata.org/entity/Q1> {general_sources._WIKIDATA_DIRECT_P31_TERM} <http://www.wikidata.org/entity/Q5> .\n",
        f"<http://www.wikidata.org/entity/Q2> {general_sources._WIKIDATA_DIRECT_P279_TERM} <http://www.wikidata.org/entity/Q43229> .\n",
        f"<https://en.wikipedia.org/wiki/Q2> {general_sources._WIKIDATA_SCHEMA_ABOUT_PREDICATE_TERM} <http://www.wikidata.org/entity/Q2> .\n",
        f"<http://www.wikidata.org/entity/Q3> {general_sources._WIKIDATA_SITELINKS_PREDICATE_TERM} \"10\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n",
        f"<http://www.wikidata.org/entity/Q4> {label_predicate} \"Label\"@en .\n",
        f"<http://www.wikidata.org/entity/Q5> {general_sources._WIKIDATA_ALT_LABEL_PREDICATE_TERM} \"Alias\"@en .\n",
    ]


def test_iter_wikidata_truthy_relevant_lines_uses_grep_prefilter_for_plain_nt(
    tmp_path: Path,
    monkeypatch,
) -> None:
    nt_path = tmp_path / "wikidata_truthy.source.nt"
    nt_path.write_text("placeholder\n", encoding="utf-8")
    popen_calls: list[list[str]] = []

    class _FakeStream:
        def __init__(self, lines: list[str]) -> None:
            self._lines = lines

        def __iter__(self):
            return iter(self._lines)

        def close(self) -> None:
            return None

        def read(self) -> str:
            return ""

    class _FakeProcess:
        def __init__(self, args: list[str], **_: object) -> None:
            popen_calls.append(args)
            self.stdout = _FakeStream(
                [
                    f"<http://www.wikidata.org/entity/Q1> {general_sources._WIKIDATA_DIRECT_P31_TERM} <http://www.wikidata.org/entity/Q5> .\n",
                    f"<https://en.wikipedia.org/wiki/Q1> {general_sources._WIKIDATA_SCHEMA_ABOUT_PREDICATE_TERM} <http://www.wikidata.org/entity/Q1> .\n",
                    f"<https://en.wikipedia.org/wiki/Q1> <{general_sources._WIKIDATA_SCHEMA_NAME_PREDICATE}> \"Entity Surface\"@en .\n",
                    f"<http://www.wikidata.org/entity/Q1> <{general_sources._WIKIDATA_SCHEMA_NAME_PREDICATE}> \"Etiqueta\"@es .\n",
                ]
            )
            self.stderr = _FakeStream([])

        def wait(self) -> int:
            return 0

    def fake_which(name: str) -> str | None:
        if name == "grep":
            return "/usr/bin/grep"
        return None

    monkeypatch.setattr(general_sources.shutil, "which", fake_which)
    monkeypatch.setattr(general_sources.subprocess, "Popen", _FakeProcess)

    assert list(general_sources._iter_wikidata_truthy_relevant_lines(nt_path)) == [
        f"<http://www.wikidata.org/entity/Q1> {general_sources._WIKIDATA_DIRECT_P31_TERM} <http://www.wikidata.org/entity/Q5> .\n",
        f"<https://en.wikipedia.org/wiki/Q1> {general_sources._WIKIDATA_SCHEMA_ABOUT_PREDICATE_TERM} <http://www.wikidata.org/entity/Q1> .\n",
        f"<https://en.wikipedia.org/wiki/Q1> <{general_sources._WIKIDATA_SCHEMA_NAME_PREDICATE}> \"Entity Surface\"@en .\n",
    ]
    assert popen_calls == [[
        "/usr/bin/grep",
        "-a",
        "-F",
        *sum((["-e", term] for term in general_sources._WIKIDATA_GREP_PREDICATE_TERMS), []),
        str(nt_path),
    ]]


def test_download_target_name_avoids_duplicate_source_suffix() -> None:
    assert (
        general_sources._download_target_name(
            "wikidata_truthy.source",
            "file:///mnt/githubActions/ades_big_data/pack_sources/raw/general-en-official/2026-04-10/wikidata_truthy.source.nt",
        )
        == "wikidata_truthy.source.nt"
    )


def test_sha256_file_reuses_matching_sidecar_cache(
    tmp_path: Path, monkeypatch
) -> None:
    target = tmp_path / "wikidata_truthy.source.nt"
    target.write_text("cached payload\n", encoding="utf-8")
    cache_path = target.with_name(f"{target.name}.sha256.json")
    cache_path.write_text(
        json.dumps(
            {
                "sha256": "cached-digest",
                "size_bytes": target.stat().st_size,
                "mtime_ns": target.stat().st_mtime_ns,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    original_open = general_sources.Path.open

    def fake_open(self: Path, *args: object, **kwargs: object):
        mode = args[0] if args else kwargs.get("mode", "r")
        if self == target and mode == "rb":
            raise AssertionError("raw file should not be re-read when cache matches")
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(general_sources.Path, "open", fake_open)

    assert general_sources._sha256_file(target) == "cached-digest"


def test_truthy_stage_source_signature_prefers_sha256_cache(tmp_path: Path) -> None:
    target = tmp_path / "wikidata_truthy.source.nt"
    target.write_text("cached payload\n", encoding="utf-8")
    cache_path = target.with_name(f"{target.name}.sha256.json")
    cache_path.write_text(
        json.dumps(
            {
                "sha256": "cached-digest",
                "size_bytes": target.stat().st_size,
                "mtime_ns": target.stat().st_mtime_ns,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    assert (
        general_sources._wikidata_truthy_stage_source_signature(target)
        == "sha256:cached-digest"
    )


def test_truthy_stage_source_signature_uses_fast_stat_signature_for_large_local_files(
    tmp_path: Path,
    monkeypatch,
) -> None:
    target = tmp_path / "wikidata_truthy.source.nt"
    target.write_text("payload\n", encoding="utf-8")

    monkeypatch.setattr(
        general_sources,
        "_TRUTHY_STAGE_FAST_SIGNATURE_MIN_BYTES",
        1,
    )

    def fail_sha256(_: Path) -> str:
        raise AssertionError("sha256 should not run for large local truthy files")

    monkeypatch.setattr(general_sources, "_sha256_file", fail_sha256)

    signature = general_sources._wikidata_truthy_stage_source_signature(target)

    assert signature.startswith("stat:")


def test_raw_source_manifest_entry_uses_fast_signature_for_large_uncached_files(
    tmp_path: Path,
    monkeypatch,
) -> None:
    target = tmp_path / "wikidata_truthy.source.nt"
    target.write_text("payload\n", encoding="utf-8")

    monkeypatch.setattr(
        general_sources,
        "_TRUTHY_STAGE_FAST_SIGNATURE_MIN_BYTES",
        1,
    )

    def fail_sha256(_: Path) -> str:
        raise AssertionError("sha256 should not run for large uncached raw sources")

    monkeypatch.setattr(general_sources, "_sha256_file", fail_sha256)

    payload = general_sources._raw_source_manifest_entry(
        name="wikidata-truthy-dump",
        source_url="file:///tmp/wikidata_truthy.source.nt",
        path=target,
    )

    assert "sha256" not in payload
    assert payload["content_signature"].startswith("stat:")


def test_normalized_source_manifest_entry_uses_fast_raw_signature_for_large_uncached_files(
    tmp_path: Path,
    monkeypatch,
) -> None:
    raw_path = tmp_path / "wikidata_truthy.source.nt"
    raw_path.write_text("payload\n", encoding="utf-8")
    formatted_path = tmp_path / "wikidata_general_entities.jsonl"
    formatted_path.write_text('{"id":"Q1"}\n', encoding="utf-8")

    monkeypatch.setattr(
        general_sources,
        "_TRUTHY_STAGE_FAST_SIGNATURE_MIN_BYTES",
        1,
    )
    original_sha256_file = general_sources._sha256_file

    def selective_sha256(path: Path) -> str:
        if path == raw_path:
            raise AssertionError(
                "raw sha256 should not run for large uncached truthy files"
            )
        return original_sha256_file(path)

    monkeypatch.setattr(general_sources, "_sha256_file", selective_sha256)

    payload = general_sources._normalized_source_manifest_entry(
        name="wikidata-general-entities",
        source_url="file:///tmp/wikidata_truthy.source.nt",
        raw_path=raw_path,
        formatted_path=formatted_path,
        record_count=1,
    )

    assert payload["sha256"] == original_sha256_file(formatted_path)
    assert "raw_sha256" not in payload
    assert payload["raw_content_signature"].startswith("stat:")


def test_truthy_stage_matches_rejects_incomplete_cached_stage(tmp_path: Path) -> None:
    stage_path = tmp_path / ".wikidata_truthy.stage.sqlite"
    connection = sqlite3.connect(stage_path)
    try:
        connection.execute(
            """
            CREATE TABLE stage_metadata (
                schema_version INTEGER NOT NULL,
                source_sha256 TEXT NOT NULL,
                root_fingerprint TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO stage_metadata(schema_version, source_sha256, root_fingerprint)
            VALUES (?, ?, ?)
            """,
            (
                general_sources._WIKIDATA_TRUTHY_STAGE_SCHEMA_VERSION,
                "sha256:test-source",
                "test-roots",
            ),
        )
        connection.commit()
    finally:
        connection.close()

    assert (
        general_sources._wikidata_truthy_stage_matches(
            stage_path,
            source_sha256="sha256:test-source",
            root_fingerprint="test-roots",
        )
        is False
    )


def test_truthy_stage_matches_rejects_root_fingerprint_mismatch_for_valid_stage(
    tmp_path: Path,
) -> None:
    stage_path = tmp_path / ".wikidata_truthy.stage.sqlite"
    connection = sqlite3.connect(stage_path)
    try:
        general_sources._initialize_wikidata_truthy_work_store(
            connection,
            source_sha256="source-signature",
            root_fingerprint="root-fingerprint-a",
        )
        connection.execute(
            """
            INSERT INTO subclass_edges(subject_id, parent_id)
            VALUES (?, ?)
            """,
            ("Q10", "Q5"),
        )
        connection.execute(
            """
            INSERT INTO entity_segments(
                entity_id,
                label,
                aliases_json,
                sitelinks,
                type_ids_json
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                "Q100",
                "Example Entity",
                json.dumps([]),
                None,
                json.dumps(["Q10"]),
            ),
        )
        general_sources._finalize_wikidata_truthy_work_store(connection)
        connection.commit()
    finally:
        connection.close()

    assert (
        general_sources._wikidata_truthy_stage_matches(
            stage_path,
            source_sha256="source-signature",
            root_fingerprint="different-roots-now",
        )
        is False
    )


def test_truthy_stage_matches_rejects_unfinalized_initialized_stage(
    tmp_path: Path,
) -> None:
    stage_path = tmp_path / ".wikidata_truthy.stage.sqlite"
    connection = sqlite3.connect(stage_path)
    try:
        general_sources._initialize_wikidata_truthy_work_store(
            connection,
            source_sha256="source-signature",
            root_fingerprint="root-fingerprint",
        )
        connection.execute(
            """
            INSERT INTO subclass_edges(subject_id, parent_id)
            VALUES (?, ?)
            """,
            ("Q10", "Q5"),
        )
        connection.execute(
            """
            INSERT INTO entity_segments(
                entity_id,
                label,
                aliases_json,
                sitelinks,
                type_ids_json
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                "Q100",
                "Example Entity",
                json.dumps([]),
                None,
                json.dumps(["Q10"]),
            ),
        )
        connection.commit()
    finally:
        connection.close()

    assert (
        general_sources._wikidata_truthy_stage_matches(
            stage_path,
            source_sha256="source-signature",
            root_fingerprint="root-fingerprint",
        )
        is False
    )


def test_promote_wikidata_truthy_stage_upgrades_legacy_complete_stage(
    tmp_path: Path,
) -> None:
    legacy_stage_path = tmp_path / "legacy" / ".wikidata_truthy.stage.sqlite"
    legacy_stage_path.parent.mkdir(parents=True)
    connection = sqlite3.connect(legacy_stage_path)
    try:
        connection.executescript(
            """
            CREATE TABLE stage_metadata (
                schema_version INTEGER NOT NULL,
                source_sha256 TEXT NOT NULL,
                root_fingerprint TEXT NOT NULL
            );
            CREATE TABLE subclass_edges (
                subject_id TEXT NOT NULL,
                parent_id TEXT NOT NULL,
                PRIMARY KEY(subject_id, parent_id)
            ) WITHOUT ROWID;
            CREATE TABLE allowed_roots (
                root_id TEXT PRIMARY KEY,
                priority INTEGER NOT NULL,
                entity_type TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE entity_segments (
                entity_id TEXT NOT NULL,
                label TEXT,
                aliases_json TEXT NOT NULL,
                sitelinks INTEGER,
                type_ids_json TEXT NOT NULL
            );
            """
        )
        connection.execute(
            """
            INSERT INTO stage_metadata(schema_version, source_sha256, root_fingerprint)
            VALUES (?, ?, ?)
            """,
            (
                1,
                "source-signature",
                "root-fingerprint",
            ),
        )
        connection.execute(
            """
            INSERT INTO subclass_edges(subject_id, parent_id)
            VALUES (?, ?)
            """,
            ("Q10", "Q5"),
        )
        connection.execute(
            """
            INSERT INTO entity_segments(
                entity_id,
                label,
                aliases_json,
                sitelinks,
                type_ids_json
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                "Q100",
                "Example Entity",
                json.dumps(["Example Alias"]),
                12,
                json.dumps(["Q10"]),
            ),
        )
        connection.commit()
    finally:
        connection.close()

    promoted_stage_path = tmp_path / "current" / ".wikidata_truthy.stage.sqlite"
    promoted_stage_path.parent.mkdir(parents=True)
    general_sources._promote_wikidata_truthy_stage(
        legacy_stage_path,
        work_db_path=promoted_stage_path,
        source_sha256="source-signature",
        root_fingerprint="root-fingerprint",
    )

    assert (
        general_sources._wikidata_truthy_stage_matches(
            promoted_stage_path,
            source_sha256="source-signature",
            root_fingerprint="root-fingerprint",
        )
        is True
    )
    matches = general_sources._load_wikidata_truthy_type_matches(promoted_stage_path)
    assert matches["Q10"] == (0, "person")


def test_find_reusable_wikidata_truthy_stage_prefers_complete_legacy_stage(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    current_stage_path = raw_root / "general-en-json-primary" / "2026-04-12" / ".wikidata_truthy.stage.sqlite"
    current_stage_path.parent.mkdir(parents=True)
    legacy_stage_path = raw_root / "general-en-rebuild-truthy" / "2026-04-11" / ".wikidata_truthy.stage.sqlite"
    legacy_stage_path.parent.mkdir(parents=True)
    connection = sqlite3.connect(legacy_stage_path)
    try:
        connection.executescript(
            """
            CREATE TABLE stage_metadata (
                schema_version INTEGER NOT NULL,
                source_sha256 TEXT NOT NULL,
                root_fingerprint TEXT NOT NULL
            );
            CREATE TABLE subclass_edges (
                subject_id TEXT NOT NULL,
                parent_id TEXT NOT NULL,
                PRIMARY KEY(subject_id, parent_id)
            ) WITHOUT ROWID;
            CREATE TABLE allowed_roots (
                root_id TEXT PRIMARY KEY,
                priority INTEGER NOT NULL,
                entity_type TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE entity_segments (
                entity_id TEXT NOT NULL,
                label TEXT,
                aliases_json TEXT NOT NULL,
                sitelinks INTEGER,
                type_ids_json TEXT NOT NULL
            );
            """
        )
        connection.execute(
            """
            INSERT INTO stage_metadata(schema_version, source_sha256, root_fingerprint)
            VALUES (?, ?, ?)
            """,
            (
                1,
                "source-signature",
                "root-fingerprint",
            ),
        )
        connection.execute(
            """
            INSERT INTO subclass_edges(subject_id, parent_id)
            VALUES (?, ?)
            """,
            ("Q10", "Q5"),
        )
        connection.execute(
            """
            INSERT INTO entity_segments(
                entity_id,
                label,
                aliases_json,
                sitelinks,
                type_ids_json
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                "Q100",
                "Example Entity",
                json.dumps([]),
                None,
                json.dumps(["Q10"]),
            ),
        )
        connection.commit()
    finally:
        connection.close()

    reusable_stage_path = general_sources._find_reusable_wikidata_truthy_stage(
        current_stage_path,
        source_sha256="source-signature",
        root_fingerprint="root-fingerprint",
    )

    assert reusable_stage_path == legacy_stage_path


def test_find_reusable_wikidata_truthy_stage_allows_legacy_root_mismatch_for_entity_dump_path(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    current_stage_path = raw_root / "general-en-json-primary" / "2026-04-12" / ".wikidata_truthy.stage.sqlite"
    current_stage_path.parent.mkdir(parents=True)
    legacy_stage_path = raw_root / "general-en-rebuild-truthy" / "2026-04-11" / ".wikidata_truthy.stage.sqlite"
    legacy_stage_path.parent.mkdir(parents=True)
    connection = sqlite3.connect(legacy_stage_path)
    try:
        connection.executescript(
            """
            CREATE TABLE stage_metadata (
                schema_version INTEGER NOT NULL,
                source_sha256 TEXT NOT NULL,
                root_fingerprint TEXT NOT NULL
            );
            CREATE TABLE subclass_edges (
                subject_id TEXT NOT NULL,
                parent_id TEXT NOT NULL,
                PRIMARY KEY(subject_id, parent_id)
            ) WITHOUT ROWID;
            CREATE TABLE allowed_roots (
                root_id TEXT PRIMARY KEY,
                priority INTEGER NOT NULL,
                entity_type TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE entity_segments (
                entity_id TEXT NOT NULL,
                label TEXT,
                aliases_json TEXT NOT NULL,
                sitelinks INTEGER,
                type_ids_json TEXT NOT NULL
            );
            """
        )
        connection.execute(
            """
            INSERT INTO stage_metadata(schema_version, source_sha256, root_fingerprint)
            VALUES (?, ?, ?)
            """,
            (
                1,
                "source-signature",
                "older-root-fingerprint",
            ),
        )
        connection.execute(
            """
            INSERT INTO subclass_edges(subject_id, parent_id)
            VALUES (?, ?)
            """,
            ("Q10", "Q5"),
        )
        connection.execute(
            """
            INSERT INTO entity_segments(
                entity_id,
                label,
                aliases_json,
                sitelinks,
                type_ids_json
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                "Q100",
                "Example Entity",
                json.dumps([]),
                None,
                json.dumps(["Q10"]),
            ),
        )
        connection.commit()
    finally:
        connection.close()

    assert (
        general_sources._find_reusable_wikidata_truthy_stage(
            current_stage_path,
            source_sha256="source-signature",
            root_fingerprint="root-fingerprint",
        )
        is None
    )
    assert (
        general_sources._find_reusable_wikidata_truthy_stage(
            current_stage_path,
            source_sha256="source-signature",
            root_fingerprint="root-fingerprint",
            allow_legacy_root_mismatch=True,
        )
        == legacy_stage_path
    )


def test_load_wikidata_truthy_type_matches_prefers_earlier_root_priority(
    tmp_path: Path,
    monkeypatch,
) -> None:
    stage_path = tmp_path / ".wikidata_truthy.stage.sqlite"
    monkeypatch.setattr(
        general_sources,
        "DEFAULT_WIKIDATA_ALLOWED_TYPE_ROOTS",
        {
            "Q1": "person",
            "Q2": "organization",
        },
    )
    connection = sqlite3.connect(stage_path)
    try:
        general_sources._initialize_wikidata_truthy_work_store(
            connection,
            source_sha256="source-signature",
            root_fingerprint="root-fingerprint",
        )
        connection.executemany(
            """
            INSERT INTO subclass_edges(subject_id, parent_id)
            VALUES (?, ?)
            """,
            [
                ("Q10", "Q1"),
                ("Q11", "Q10"),
                ("Q11", "Q2"),
                ("Q20", "Q2"),
            ],
        )
        general_sources._finalize_wikidata_truthy_work_store(connection)
        connection.commit()
    finally:
        connection.close()

    matches = general_sources._load_wikidata_truthy_type_matches(stage_path)

    assert matches["Q1"] == (0, "person")
    assert matches["Q10"] == (0, "person")
    assert matches["Q11"] == (0, "person")
    assert matches["Q2"] == (1, "organization")
    assert matches["Q20"] == (1, "organization")


def test_load_wikidata_truthy_type_matches_prefers_location_over_organization_for_shared_descendants(
    tmp_path: Path,
    monkeypatch,
) -> None:
    stage_path = tmp_path / ".wikidata_truthy.stage.sqlite"
    monkeypatch.setattr(
        general_sources,
        "DEFAULT_WIKIDATA_ALLOWED_TYPE_ROOTS",
        {
            "Q100": "location",
            "Q200": "organization",
        },
    )
    connection = sqlite3.connect(stage_path)
    try:
        general_sources._initialize_wikidata_truthy_work_store(
            connection,
            source_sha256="source-signature",
            root_fingerprint="root-fingerprint",
        )
        connection.executemany(
            """
            INSERT INTO subclass_edges(subject_id, parent_id)
            VALUES (?, ?)
            """,
            [
                ("Q300", "Q100"),
                ("Q300", "Q200"),
            ],
        )
        general_sources._finalize_wikidata_truthy_work_store(connection)
        connection.commit()
    finally:
        connection.close()

    matches = general_sources._load_wikidata_truthy_type_matches(stage_path)

    assert matches["Q300"] == (0, "location")


def test_iter_selected_wikidata_truthy_candidates_uses_sql_type_resolution(
    tmp_path: Path,
    monkeypatch,
) -> None:
    stage_path = tmp_path / ".wikidata_truthy.stage.sqlite"
    monkeypatch.setattr(
        general_sources,
        "DEFAULT_WIKIDATA_ALLOWED_TYPE_ROOTS",
        {
            "Q1": "person",
            "Q2": "organization",
        },
    )
    connection = sqlite3.connect(stage_path)
    try:
        general_sources._initialize_wikidata_truthy_work_store(
            connection,
            source_sha256="source-signature",
            root_fingerprint="root-fingerprint",
        )
        connection.executemany(
            """
            INSERT INTO subclass_edges(subject_id, parent_id)
            VALUES (?, ?)
            """,
            [
                ("Q10", "Q1"),
                ("Q20", "Q2"),
            ],
        )
        connection.executemany(
            """
            INSERT INTO entity_segments(
                entity_id,
                label,
                aliases_json,
                sitelinks,
                type_ids_json
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    "Q100",
                    "Example Person",
                    json.dumps(["Example Alias"]),
                    12,
                    json.dumps(["Q10"]),
                ),
                (
                    "Q100",
                    None,
                    json.dumps(["Second Alias"]),
                    12,
                    json.dumps(["Q20"]),
                ),
                (
                    "Q200",
                    "Example Org",
                    json.dumps(["Org Alias"]),
                    8,
                    json.dumps(["Q20"]),
                ),
            ],
        )
        general_sources._finalize_wikidata_truthy_work_store(connection)
        connection.commit()
    finally:
        connection.close()

    candidates = list(general_sources._iter_selected_wikidata_truthy_candidates(stage_path))

    assert candidates == [
        {
            "id": "Q100",
            "entity_type": "person",
            "label": "Example Person",
            "aliases": ["Example Alias", "Second Alias"],
            "popularity": 12,
            "type_ids": ["Q10", "Q20"],
            "source_features": {
                "sitelink_count": 12,
                "alias_count": 2,
                "popularity_signal": "sitelinks",
            },
        },
        {
            "id": "Q200",
            "entity_type": "organization",
            "label": "Example Org",
            "aliases": ["Org Alias"],
            "popularity": 8,
            "type_ids": ["Q20"],
            "source_features": {
                "sitelink_count": 8,
                "alias_count": 1,
                "popularity_signal": "sitelinks",
            },
        },
    ]


def test_iter_selected_wikidata_truthy_candidates_keeps_multi_token_entities_without_sitelinks(
    tmp_path: Path,
    monkeypatch,
) -> None:
    stage_path = tmp_path / ".wikidata_truthy.stage.sqlite"
    monkeypatch.setattr(
        general_sources,
        "DEFAULT_WIKIDATA_ALLOWED_TYPE_ROOTS",
        {
            "Q1": "person",
            "Q2": "organization",
            "Q3": "location",
        },
    )
    monkeypatch.setattr(
        general_sources,
        "DEFAULT_WIKIDATA_MIN_SITELINKS_BY_ENTITY_TYPE",
        {
            "person": 5,
            "organization": 3,
            "location": 3,
        },
    )
    connection = sqlite3.connect(stage_path)
    try:
        general_sources._initialize_wikidata_truthy_work_store(
            connection,
            source_sha256="source-signature",
            root_fingerprint="root-fingerprint",
        )
        connection.executemany(
            """
            INSERT INTO entity_segments(
                entity_id,
                label,
                aliases_json,
                sitelinks,
                type_ids_json
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    "Q100",
                    "Example Person",
                    json.dumps(["Example Alias"]),
                    None,
                    json.dumps(["Q1"]),
                ),
                (
                    "Q200",
                    "Example Org",
                    json.dumps([]),
                    None,
                    json.dumps(["Q2"]),
                ),
                (
                    "Q300",
                    "New Harbor",
                    json.dumps([]),
                    None,
                    json.dumps(["Q3"]),
                ),
                (
                    "Q400",
                    "Rio",
                    json.dumps([]),
                    None,
                    json.dumps(["Q3"]),
                ),
            ],
        )
        general_sources._finalize_wikidata_truthy_work_store(connection)
        connection.commit()
    finally:
        connection.close()

    candidates = list(general_sources._iter_selected_wikidata_truthy_candidates(stage_path))

    assert candidates == [
        {
            "id": "Q100",
            "entity_type": "person",
            "label": "Example Person",
            "aliases": ["Example Alias"],
            "popularity": 0,
            "type_ids": ["Q1"],
            "source_features": {
                "sitelink_count": 0,
                "alias_count": 1,
                "popularity_signal": "fallback",
            },
        },
        {
            "id": "Q200",
            "entity_type": "organization",
            "label": "Example Org",
            "aliases": [],
            "popularity": 0,
            "type_ids": ["Q2"],
            "source_features": {
                "sitelink_count": 0,
                "alias_count": 0,
                "popularity_signal": "fallback",
            },
        },
        {
            "id": "Q300",
            "entity_type": "location",
            "label": "New Harbor",
            "aliases": [],
            "popularity": 0,
            "type_ids": ["Q3"],
            "source_features": {
                "sitelink_count": 0,
                "alias_count": 0,
                "popularity_signal": "fallback",
            },
        },
    ]


def test_merge_wikidata_truthy_jsonl_with_seed_entities_streams_overlay(
    tmp_path: Path,
) -> None:
    jsonl_path = tmp_path / "wikidata_general_entities.jsonl"
    jsonl_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "id": "Q100",
                        "entity_type": "person",
                        "label": "Example Person",
                        "aliases": ["Example Alias"],
                        "popularity": 10,
                    },
                    sort_keys=True,
                ),
                json.dumps(
                    {
                        "id": "Q200",
                        "entity_type": "organization",
                        "label": "Example Org",
                        "aliases": [],
                        "popularity": 4,
                    },
                    sort_keys=True,
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    count = general_sources._merge_wikidata_truthy_jsonl_with_seed_entities(
        jsonl_path,
        seed_entities=[
            {
                "id": "Q100",
                "entity_type": "person",
                "label": "Example Person",
                "aliases": ["Example Seed Alias"],
                "popularity": None,
            },
            {
                "id": "Q300",
                "entity_type": "location",
                "label": "New Harbor",
                "aliases": [],
                "popularity": None,
            },
        ],
    )

    rows = [
        json.loads(line)
        for line in jsonl_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert count == 3
    assert rows == [
        {
            "aliases": ["Example Alias", "Example Seed Alias"],
            "entity_type": "person",
            "id": "Q100",
            "label": "Example Person",
            "popularity": 10,
        },
        {
            "aliases": [],
            "entity_type": "organization",
            "id": "Q200",
            "label": "Example Org",
            "popularity": 4,
        },
        {
            "aliases": [],
            "entity_type": "location",
            "id": "Q300",
            "label": "New Harbor",
            "popularity": None,
        },
    ]


def test_append_missing_wikidata_truthy_candidates_restores_stage_only_entities(
    tmp_path: Path,
    monkeypatch,
) -> None:
    jsonl_path = tmp_path / "wikidata_general_entities.jsonl"
    jsonl_path.write_text(
        json.dumps(
            {
                "id": "Q100",
                "entity_type": "organization",
                "label": "Newswire",
                "aliases": [],
                "popularity": 10,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    stage_path = tmp_path / ".wikidata_truthy.stage.sqlite"
    monkeypatch.setattr(
        general_sources,
        "DEFAULT_WIKIDATA_ALLOWED_TYPE_ROOTS",
        {
            "Q1": "organization",
            "Q2": "location",
        },
    )
    monkeypatch.setattr(
        general_sources,
        "DEFAULT_WIKIDATA_MIN_SITELINKS_BY_ENTITY_TYPE",
        {
            "organization": 3,
            "location": 3,
        },
    )
    connection = sqlite3.connect(stage_path)
    try:
        general_sources._initialize_wikidata_truthy_work_store(
            connection,
            source_sha256="source-signature",
            root_fingerprint="root-fingerprint",
        )
        connection.executemany(
            """
            INSERT INTO entity_segments(
                entity_id,
                label,
                aliases_json,
                sitelinks,
                type_ids_json
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    "Q100",
                    "Newswire",
                    json.dumps([]),
                    10,
                    json.dumps(["Q1"]),
                ),
                (
                    "Q200",
                    "Third Point Management",
                    json.dumps([]),
                    None,
                    json.dumps(["Q1"]),
                ),
                (
                    "Q300",
                    "New York",
                    json.dumps(["State of New York"]),
                    None,
                    json.dumps(["Q2"]),
                ),
                (
                    "Q400",
                    "Loeb's",
                    json.dumps([]),
                    None,
                    json.dumps(["Q1"]),
                ),
            ],
        )
        general_sources._finalize_wikidata_truthy_work_store(connection)
        connection.commit()
    finally:
        connection.close()

    count = general_sources._append_missing_wikidata_truthy_candidates(
        jsonl_path,
        work_db_path=stage_path,
    )

    rows = [
        json.loads(line)
        for line in jsonl_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert count == 3
    assert rows == [
        {
            "aliases": [],
            "entity_type": "organization",
            "id": "Q100",
            "label": "Newswire",
            "popularity": 10,
        },
        {
            "aliases": [],
            "entity_type": "organization",
            "id": "Q200",
            "label": "Third Point Management",
            "popularity": 0,
            "source_features": {
                "alias_count": 0,
                "popularity_signal": "fallback",
                "sitelink_count": 0,
            },
            "type_ids": ["Q1"],
        },
        {
            "aliases": ["State of New York"],
            "entity_type": "location",
            "id": "Q300",
            "label": "New York",
            "popularity": 0,
            "source_features": {
                "alias_count": 1,
                "popularity_signal": "fallback",
                "sitelink_count": 0,
            },
            "type_ids": ["Q2"],
        },
    ]


def test_should_keep_wikidata_dump_entity_rejects_weak_single_token_organization() -> None:
    assert (
        general_sources._should_keep_wikidata_dump_entity(
            entity_type="organization",
            label="Loeb's",
            aliases=["Loeb's (department store)"],
            sitelink_count=3,
            has_enwiki=False,
            identifier_count=0,
            statement_count=7,
        )
        is False
    )


def test_should_keep_wikidata_dump_entity_keeps_supported_single_token_organization() -> None:
    assert (
        general_sources._should_keep_wikidata_dump_entity(
            entity_type="organization",
            label="Reuters",
            aliases=["Reuters Group"],
            sitelink_count=12,
            has_enwiki=True,
            identifier_count=1,
            statement_count=20,
        )
        is True
    )


def test_should_keep_wikidata_dump_entity_rejects_weak_single_token_person() -> None:
    assert (
        general_sources._should_keep_wikidata_dump_entity(
            entity_type="person",
            label="Daniel",
            aliases=[],
            sitelink_count=5,
            has_enwiki=False,
            identifier_count=17,
            statement_count=24,
        )
        is False
    )


def test_should_keep_wikidata_dump_entity_rejects_weak_single_token_location() -> None:
    assert (
        general_sources._should_keep_wikidata_dump_entity(
            entity_type="location",
            label="Salvage",
            aliases=["Salvage, Newfoundland and Labrador"],
            sitelink_count=10,
            has_enwiki=True,
            identifier_count=16,
            statement_count=17,
        )
        is False
    )


def test_should_keep_wikidata_dump_entity_rejects_plain_multi_token_organization_without_strong_support() -> None:
    assert (
        general_sources._should_keep_wikidata_dump_entity(
            entity_type="organization",
            label="Real Estate",
            aliases=["Real Estate (band)"],
            sitelink_count=16,
            has_enwiki=True,
            identifier_count=39,
            statement_count=45,
        )
        is False
    )


def test_should_keep_wikidata_dump_entity_keeps_plain_multi_token_organization_with_strong_support() -> None:
    assert (
        general_sources._should_keep_wikidata_dump_entity(
            entity_type="organization",
            label="Goldman Sachs",
            aliases=[],
            sitelink_count=90,
            has_enwiki=True,
            identifier_count=50,
            statement_count=80,
        )
        is True
    )


def test_extract_wikidata_entity_dump_aliases_ignores_namespace_titles() -> None:
    aliases = general_sources._extract_wikidata_entity_dump_aliases(
        {
            "aliases": {"en": [{"value": "Beacon"}]},
            "sitelinks": {
                "enwiki": {"title": "Beacon_Group"},
                "enwikiquote": {"title": "Category:Beacon_Group"},
                "enwikivoyage": {"title": "Portal:Beacon_Group"},
            },
        },
        label="Beacon Group",
    )

    assert aliases == ["Beacon"]


def test_resolve_wikidata_entity_type_supports_broader_organization_roots() -> None:
    type_matches = {
        "Q2385804": (0, "organization"),
        "Q2659904": (0, "organization"),
        "Q4438121": (0, "organization"),
        "Q7210356": (0, "organization"),
        "Q7278": (0, "organization"),
        "Q1331793": (0, "organization"),
    }

    for type_id in type_matches:
        assert (
            general_sources._resolve_wikidata_entity_type(
                [type_id],
                type_matches=type_matches,
            )
            == "organization"
        )


def test_resolve_wikidata_entity_type_supports_state_location_root() -> None:
    assert (
        general_sources._resolve_wikidata_entity_type(
            {"Q35657"},
            type_matches={"Q35657": (0, "location")},
        )
        == "location"
    )
