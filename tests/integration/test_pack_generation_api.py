from pathlib import Path
import shutil

from ades import build_registry, generate_pack_source, pull_pack, tag
from ades import get_pack
from tests.pack_generation_helpers import (
    create_bundle_backed_general_pack_source,
    create_bundle_backed_general_pack_source_with_alias_collision,
    create_finance_generation_bundle,
    create_general_generation_bundle,
    create_noisy_general_generation_bundle,
    create_pre_resolved_general_generation_bundle,
)


def test_public_api_can_generate_build_install_and_tag_generated_pack(
    tmp_path: Path,
    monkeypatch,
) -> None:
    bundle_dir = create_finance_generation_bundle(tmp_path / "bundle")

    generated = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )
    assert generated.matcher_algorithm == "aho_corasick"
    assert Path(generated.matcher_artifact_path).exists()
    assert Path(generated.matcher_entries_path).exists()
    registry = build_registry(
        [generated.pack_dir],
        output_dir=tmp_path / "registry",
    )

    install_root = tmp_path / "install"
    result = pull_pack(
        "finance-en",
        storage_root=install_root,
        registry_url=registry.index_url,
    )
    monkeypatch.setattr(
        "ades.pipeline.tagger._iter_candidate_windows",
        lambda _value: (_ for _ in ()).throw(
            AssertionError("matcher-enabled packs should not fall back to window lookup")
        ),
    )
    response = tag(
        "Issuer Alpha said TICKA traded on EXCHX after USD 12.5 guidance.",
        pack="finance-en",
        storage_root=install_root,
    )
    installed_pack = get_pack("finance-en", storage_root=install_root)

    entities = {(entity.text, entity.label) for entity in response.entities}
    assert result.installed == ["finance-en"]
    assert installed_pack is not None
    assert installed_pack.matcher_ready is True
    assert installed_pack.matcher_algorithm == "aho_corasick"
    assert ("Issuer Alpha", "organization") in entities
    assert ("TICKA", "ticker") in entities
    assert ("EXCHX", "exchange") in entities
    assert ("USD 12.5", "currency_amount") in entities


def test_public_api_can_generate_and_tag_general_pack_variants(tmp_path: Path) -> None:
    bundle_dir = create_general_generation_bundle(tmp_path / "bundle")

    generated = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )
    registry = build_registry(
        [generated.pack_dir],
        output_dir=tmp_path / "registry",
    )

    install_root = tmp_path / "install"
    result = pull_pack(
        "general-en",
        storage_root=install_root,
        registry_url=registry.index_url,
    )
    response = tag(
        "Jordan Vale from North Harbor said Beacon expanded.",
        pack="general-en",
        storage_root=install_root,
    )

    entities = {(entity.text, entity.label) for entity in response.entities}
    assert result.installed == ["general-en"]
    assert ("Jordan Vale", "person") in entities
    assert ("North Harbor", "location") in entities
    assert ("Beacon", "organization") in entities


def test_public_api_can_generate_and_tag_pre_resolved_general_pack(tmp_path: Path) -> None:
    bundle_dir = create_pre_resolved_general_generation_bundle(tmp_path / "bundle")

    generated = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )
    registry = build_registry(
        [generated.pack_dir],
        output_dir=tmp_path / "registry",
    )

    install_root = tmp_path / "install"
    result = pull_pack(
        "general-en",
        storage_root=install_root,
        registry_url=registry.index_url,
    )
    response = tag(
        "Jordan Vale from North Harbor said Beacon expanded.",
        pack="general-en",
        storage_root=install_root,
    )

    entities = {(entity.text, entity.label) for entity in response.entities}
    assert result.installed == ["general-en"]
    assert ("Jordan Vale", "person") in entities
    assert ("North Harbor", "location") in entities
    assert ("Beacon", "organization") in entities


def test_public_api_can_tag_bundle_backed_general_pack_source(tmp_path: Path) -> None:
    pack_dir = create_bundle_backed_general_pack_source(tmp_path / "bundle-pack")
    install_pack_dir = tmp_path / "install" / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    response = tag(
        "Jordan Vale from North Harbor said Beacon expanded.",
        pack="general-en",
        storage_root=tmp_path / "install",
    )

    entities = {(entity.text, entity.label) for entity in response.entities}
    assert ("Jordan Vale", "person") in entities
    assert ("North Harbor", "location") in entities
    assert ("Beacon", "organization") in entities


def test_public_api_bundle_backed_pack_resolves_alias_collisions(tmp_path: Path) -> None:
    pack_dir = create_bundle_backed_general_pack_source_with_alias_collision(
        tmp_path / "bundle-pack"
    )
    install_pack_dir = tmp_path / "install" / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    response = tag(
        "Jordan Vale arrived in North Harbor.",
        pack="general-en",
        storage_root=tmp_path / "install",
    )

    entity = next(
        item for item in response.entities if item.text == "Jordan Vale" and item.label == "person"
    )
    assert entity.link is not None
    assert entity.link.entity_id == "person:jordan-elliott-vale-primary"
    assert entity.link.canonical_text == "Jordan Elliott Vale Prime"


def test_public_api_generated_general_pack_suppresses_noisy_generic_matches(
    tmp_path: Path,
) -> None:
    bundle_dir = create_noisy_general_generation_bundle(tmp_path / "bundle")

    generated = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )
    registry = build_registry(
        [generated.pack_dir],
        output_dir=tmp_path / "registry",
    )

    install_root = tmp_path / "install"
    pull_pack(
        "general-en",
        storage_root=install_root,
        registry_url=registry.index_url,
    )
    response = tag(
        "Daniel Loeb said Beacon expanded after the letter about real estate.",
        pack="general-en",
        storage_root=install_root,
    )

    entities = {(entity.text, entity.label) for entity in response.entities}
    assert ("Daniel Loeb", "person") in entities
    assert ("Beacon", "organization") in entities
    assert ("Daniel", "person") not in entities
    assert ("letter", "location") not in entities
    assert ("real estate", "organization") not in entities
