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
