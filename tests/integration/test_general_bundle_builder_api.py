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
        "Tim Cook emailed jane@example.com about OpenAI in Istanbul. Visit https://openai.com now.",
        pack="general-en",
        storage_root=install_root,
    )

    entities = {(entity.text, entity.label) for entity in response.entities}
    assert ("Tim Cook", "person") in entities
    assert ("jane@example.com", "email_address") in entities
    assert ("OpenAI", "organization") in entities
    assert ("Istanbul", "location") in entities
    assert ("https://openai.com", "url") in entities
