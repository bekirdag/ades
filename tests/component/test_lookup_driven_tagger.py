from pathlib import Path
import shutil

from ades.packs.generation import generate_pack_source
from ades.packs.installer import PackInstaller
from ades.pipeline.tagger import tag_text
from tests.pack_generation_helpers import (
    create_bundle_backed_general_pack_source,
    create_bundle_backed_general_pack_source_with_alias_collision,
    create_general_generation_bundle,
    create_noisy_general_generation_bundle,
    create_pre_resolved_general_generation_bundle,
)
from tests.pack_registry_helpers import delete_installed_pack_metadata


def test_tagger_uses_lookup_for_multi_token_general_aliases(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("general-en")

    response = tag_text(
        text="Person Alpha spoke about Org Beta.",
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("Person Alpha", "person") in pairs
    assert ("Org Beta", "organization") in pairs


def test_tagger_recovers_finance_alias_lookup_after_metadata_row_deletion(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")

    delete_installed_pack_metadata(tmp_path, "finance-en")

    response = tag_text(
        text="TICKA traded on EXCHX.",
        pack="finance-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("TICKA", "ticker") in pairs
    assert ("EXCHX", "exchange") in pairs


def test_tagger_uses_generated_general_pack_variants(
    tmp_path: Path,
    monkeypatch,
) -> None:
    bundle_dir = create_general_generation_bundle(tmp_path / "bundle")
    generated = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )
    pack_dir = Path(generated.pack_dir)
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)
    monkeypatch.setattr(
        "ades.pipeline.tagger._iter_candidate_windows",
        lambda _value: (_ for _ in ()).throw(
            AssertionError("matcher-enabled packs should not fall back to window lookup")
        ),
    )

    response = tag_text(
        text="Jordan Vale from North Harbor said Beacon expanded.",
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("Jordan Vale", "person") in pairs
    assert ("North Harbor", "location") in pairs
    assert ("Beacon", "organization") in pairs


def test_tagger_uses_pre_resolved_general_pack_aliases(tmp_path: Path) -> None:
    bundle_dir = create_pre_resolved_general_generation_bundle(tmp_path / "bundle")
    generated = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )
    pack_dir = Path(generated.pack_dir)
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    response = tag_text(
        text="Jordan Vale from North Harbor said Beacon expanded.",
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("Jordan Vale", "person") in pairs
    assert ("North Harbor", "location") in pairs
    assert ("Beacon", "organization") in pairs


def test_tagger_uses_bundle_backed_general_pack_source(tmp_path: Path) -> None:
    pack_dir = create_bundle_backed_general_pack_source(tmp_path / "bundle-pack")
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    response = tag_text(
        text="Jordan Vale from North Harbor said Beacon expanded.",
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("Jordan Vale", "person") in pairs
    assert ("North Harbor", "location") in pairs
    assert ("Beacon", "organization") in pairs


def test_tagger_bundle_backed_pack_resolves_alias_collisions(tmp_path: Path) -> None:
    pack_dir = create_bundle_backed_general_pack_source_with_alias_collision(
        tmp_path / "bundle-pack"
    )
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    response = tag_text(
        text="Jordan Vale arrived in North Harbor.",
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    entity = next(
        item for item in response.entities if item.text == "Jordan Vale" and item.label == "person"
    )
    assert entity.link is not None
    assert entity.link.entity_id == "person:jordan-elliott-vale-primary"
    assert entity.link.canonical_text == "Jordan Elliott Vale Prime"


def test_tagger_skips_low_information_single_token_people_and_orgs(tmp_path: Path) -> None:
    pack_dir = create_bundle_backed_general_pack_source(tmp_path / "bundle-pack")
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    response = tag_text(
        text="april investor matter Beacon",
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("Beacon", "organization") in pairs
    assert ("investor", "organization") not in pairs
    assert ("matter", "organization") not in pairs


def test_tagger_suppresses_noisy_general_aliases_in_generic_prose(tmp_path: Path) -> None:
    bundle_dir = create_noisy_general_generation_bundle(tmp_path / "bundle")
    generated = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )
    pack_dir = Path(generated.pack_dir)
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    response = tag_text(
        text="Daniel Loeb said Beacon expanded after the letter about real estate.",
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("Daniel Loeb", "person") in pairs
    assert ("Beacon", "organization") in pairs
    assert ("Daniel", "person") not in pairs
    assert ("letter", "location") not in pairs
    assert ("real estate", "organization") not in pairs
