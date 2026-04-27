import json
from pathlib import Path

from ades.packs.registry import PackRegistry
from ades.pipeline.tagger import tag_text
from ades.storage.paths import build_storage_layout, ensure_storage_layout
from tests.pack_registry_helpers import create_pack_source


def _install_overlap_pack(storage_root: Path) -> str:
    layout = ensure_storage_layout(build_storage_layout(storage_root))
    pack_dir = create_pack_source(
        layout.packs_dir,
        pack_id="overlap-en",
        domain="general",
        labels=("organization",),
        aliases=(("Entity Alpha", "organization"),),
        rules=(("entity_alpha_holdings", "organization", r"Entity Alpha Holdings"),),
    )
    PackRegistry(storage_root).sync_pack_from_disk(pack_dir.name)
    return pack_dir.name


def _install_duplicate_entity_pack(storage_root: Path) -> str:
    layout = ensure_storage_layout(build_storage_layout(storage_root))
    pack_dir = create_pack_source(
        layout.packs_dir,
        pack_id="duplicate-entity-en",
        domain="general",
        labels=("organization",),
    )
    (pack_dir / "aliases.json").write_text(
        json.dumps(
            {
                "aliases": [
                    {
                        "text": "Org Beta",
                        "label": "organization",
                        "canonical_text": "Organization Beta",
                        "entity_id": "wikidata:Q123",
                        "source_domain": "general",
                    },
                    {
                        "text": "Beta Org",
                        "label": "organization",
                        "canonical_text": "Organization Beta",
                        "entity_id": "wikidata:Q123",
                        "source_domain": "general",
                    },
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    PackRegistry(storage_root).sync_pack_from_disk(pack_dir.name)
    return pack_dir.name


def _install_duration_alias_pack(storage_root: Path) -> str:
    layout = ensure_storage_layout(build_storage_layout(storage_root))
    pack_dir = create_pack_source(
        layout.packs_dir,
        pack_id="general-en",
        domain="general",
        labels=("organization",),
    )
    (pack_dir / "aliases.json").write_text(
        json.dumps(
            {
                "aliases": [
                    {
                        "text": "5 min",
                        "label": "organization",
                        "canonical_text": "5 min",
                        "entity_id": "wikidata:Q4641186",
                        "source_domain": "general",
                    },
                    {
                        "text": "15 Minutes",
                        "label": "organization",
                        "canonical_text": "15 Minutes",
                        "entity_id": "wikidata:Q115272423",
                        "source_domain": "general",
                    },
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    PackRegistry(storage_root).sync_pack_from_disk(pack_dir.name)
    return pack_dir.name


def _install_runtime_cleanup_pack(storage_root: Path) -> str:
    layout = ensure_storage_layout(build_storage_layout(storage_root))
    pack_dir = create_pack_source(
        layout.packs_dir,
        pack_id="general-en",
        domain="general",
        labels=("organization",),
    )
    (pack_dir / "aliases.json").write_text(
        json.dumps(
            {
                "aliases": [
                    {
                        "text": "Atlantic Ocean",
                        "label": "organization",
                        "canonical_text": "Atlantic Ocean",
                        "entity_id": "wikidata:Q31956345",
                        "source_domain": "general",
                    },
                    {
                        "text": "As ever",
                        "label": "organization",
                        "canonical_text": "As Ever",
                        "entity_id": "wikidata:Q127104762",
                        "source_domain": "general",
                    },
                    {
                        "text": "King's",
                        "label": "organization",
                        "canonical_text": "King's Company",
                        "entity_id": "wikidata:Q2197437",
                        "source_domain": "general",
                    },
                    {
                        "text": "Defense Minister",
                        "label": "person",
                        "canonical_text": "Jerry Codiñera",
                        "entity_id": "wikidata:Q8853429",
                        "source_domain": "general",
                    },
                    {
                        "text": "May. Just",
                        "label": "person",
                        "canonical_text": "May Evelyn Cornwell",
                        "entity_id": "wikidata:Q75523417",
                        "source_domain": "general",
                    },
                    {
                        "text": "New Town",
                        "label": "location",
                        "canonical_text": "New Town",
                        "entity_id": "wikidata:Q14935638",
                        "source_domain": "general",
                    },
                    {
                        "text": "The Highest Court in the Land",
                        "label": "location",
                        "canonical_text": "The Highest Court in the Land",
                        "entity_id": "wikidata:Q136804782",
                        "source_domain": "general",
                    },
                    {
                        "text": "Supreme Court",
                        "label": "location",
                        "canonical_text": "Supreme Court",
                        "entity_id": "wikidata:Q9990001",
                        "source_domain": "general",
                    },
                    {
                        "text": "Banks",
                        "label": "person",
                        "canonical_text": "Banks",
                        "entity_id": "wikidata:Q9990002",
                        "source_domain": "general",
                    },
                    {
                        "text": "Noise",
                        "label": "organization",
                        "canonical_text": "Noise",
                        "entity_id": "wikidata:Q7047669",
                        "source_domain": "general",
                    },
                    {
                        "text": "Isle",
                        "label": "location",
                        "canonical_text": "Isle",
                        "entity_id": "wikidata:Q935535",
                        "source_domain": "general",
                    },
                    {
                        "text": "Welsh",
                        "label": "location",
                        "canonical_text": "Welsh",
                        "entity_id": "wikidata:Q2459054",
                        "source_domain": "general",
                    },
                    {
                        "text": "Santander",
                        "label": "location",
                        "canonical_text": "Santander",
                        "entity_id": "wikidata:Q12233",
                        "source_domain": "general",
                    },
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    PackRegistry(storage_root).sync_pack_from_disk(pack_dir.name)
    return pack_dir.name


def test_tagger_debug_payload_reports_discarded_overlaps(tmp_path: Path) -> None:
    pack_id = _install_overlap_pack(tmp_path)

    response = tag_text(
        text="Entity Alpha Holdings moved.",
        pack=pack_id,
        content_type="text/plain",
        storage_root=tmp_path,
        debug=True,
    )

    assert [entity.text for entity in response.entities] == ["Entity Alpha Holdings"]
    assert response.debug is not None
    assert response.debug.discarded_span_count >= 1
    assert any(
        item.text in {"Entity Alpha", "Entity Alpha Holdings"}
        and item.reason in {"overlap_replaced", "exact_duplicate_replaced"}
        for item in response.debug.span_decisions
    )


def test_tagger_emits_low_density_warning_from_pack_metadata(tmp_path: Path) -> None:
    pack_id = _install_overlap_pack(tmp_path)
    layout = ensure_storage_layout(build_storage_layout(tmp_path))
    manifest_path = layout.packs_dir / pack_id / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["min_entities_per_100_tokens_warning"] = 5.0
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    PackRegistry(tmp_path).sync_pack_from_disk(pack_id)

    response = tag_text(
        text=("plain token " * 60) + "Entity Alpha Holdings.",
        pack=pack_id,
        content_type="text/plain",
        storage_root=tmp_path,
    )

    assert any(warning.startswith("low_entity_density:") for warning in response.warnings)


def test_tagger_merges_duplicate_linked_entities_in_response(tmp_path: Path) -> None:
    pack_id = _install_duplicate_entity_pack(tmp_path)

    response = tag_text(
        text="Org Beta, also Beta Org, expanded.",
        pack=pack_id,
        content_type="text/plain",
        storage_root=tmp_path,
    )

    assert len(response.entities) == 1
    entity = response.entities[0]
    assert entity.text == "Org Beta"
    assert entity.aliases == ["Org Beta", "Beta Org"]
    assert entity.link is not None
    assert entity.link.entity_id == "wikidata:Q123"
    assert response.metrics.entity_count == 1


def test_tagger_drops_runtime_generic_duration_aliases(tmp_path: Path) -> None:
    pack_id = _install_duration_alias_pack(tmp_path)

    response = tag_text(
        text="5 min and 15 Minutes were mentioned.",
        pack=pack_id,
        content_type="text/plain",
        storage_root=tmp_path,
        debug=True,
    )

    assert response.entities == []
    assert response.metrics.entity_count == 0
    assert response.debug is not None
    assert any(
        item.reason == "runtime_generic_duration_alias"
        and item.text in {"5 min", "15 Minutes"}
        for item in response.debug.span_decisions
    )


def test_tagger_drops_runtime_structural_geo_titleish_and_phrase_aliases(
    tmp_path: Path,
) -> None:
    pack_id = _install_runtime_cleanup_pack(tmp_path)

    response = tag_text(
        text=(
            "The islands lie in the south-west Atlantic Ocean. "
            "As ever, campaign rhetoric grew louder. "
            "The King's visit was delayed. "
            "The Defense Minister spoke later. "
            "May. Just moved on. "
            "A New Town plan followed. "
            "The highest court in the land ruled yesterday. "
            "The Supreme Court ruled in August. "
            "Banks have also hit back at the ruling. "
            "The association counts Santander and the financial arms of BMW and Volkswagen among its members. "
            "Noise faded. "
            "Welsh rhetoric sharpened. "
            "The Isle route closed."
        ),
        pack=pack_id,
        content_type="text/plain",
        storage_root=tmp_path,
        debug=True,
    )

    assert response.entities == []
    assert response.metrics.entity_count == 0
    assert response.debug is not None
    assert any(
        item.reason == "runtime_structural_geo_feature_label_mismatch"
        and item.text == "Atlantic Ocean"
        for item in response.debug.span_decisions
    )
    assert all(entity.text != "As ever" for entity in response.entities)
    assert all(entity.text != "King's" for entity in response.entities)
    assert all(entity.text != "May. Just" for entity in response.entities)
    assert all(entity.text != "New Town" for entity in response.entities)
    assert all(entity.text != "highest court in the land" for entity in response.entities)
    assert all(entity.text != "Supreme Court" for entity in response.entities)
    assert all(entity.text != "Banks" for entity in response.entities)
    assert all(entity.text != "Santander" for entity in response.entities)
    assert all(entity.text != "Noise" for entity in response.entities)
    assert all(entity.text != "Welsh" for entity in response.entities)
    assert all(entity.text != "Isle" for entity in response.entities)
    assert any(
        item.reason == "runtime_office_title_person_alias_artifact"
        and item.text == "Defense Minister"
        for item in response.debug.span_decisions
    )
