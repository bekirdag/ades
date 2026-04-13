from pathlib import Path

from ades import build_general_source_bundle, build_registry, generate_pack_source, pull_pack, tag
from tests.general_bundle_helpers import create_general_raw_snapshots


def test_public_api_can_build_general_bundle_then_generate_install_and_tag(
    tmp_path: Path,
) -> None:
    snapshots = create_general_raw_snapshots(tmp_path / "snapshots")

    bundle = build_general_source_bundle(
        wikidata_entities_path=snapshots["wikidata_entities"],
        geonames_places_path=snapshots["geonames_places"],
        curated_entities_path=snapshots["curated_entities"],
        output_dir=tmp_path / "bundles",
    )
    assert Path(bundle.sources_lock_path).exists()
    generated = generate_pack_source(
        bundle.bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )
    registry = build_registry([generated.pack_dir], output_dir=tmp_path / "registry")

    install_root = tmp_path / "install"
    pull_pack(
        "general-en",
        storage_root=install_root,
        registry_url=registry.index_url,
    )
    response = tag(
        "Person Alpha emailed contact@example.test about Org Alpha in Metro Alpha. Visit https://portal.example.test now.",
        pack="general-en",
        storage_root=install_root,
    )

    entities = {(entity.text, entity.label) for entity in response.entities}
    assert ("Person Alpha", "person") in entities
    assert ("contact@example.test", "email_address") in entities
    assert ("Org Alpha", "organization") in entities
    assert ("Metro Alpha", "location") in entities
    assert ("https://portal.example.test", "url") in entities
