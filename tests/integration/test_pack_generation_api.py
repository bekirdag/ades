from pathlib import Path
import shutil

from ades import build_registry, generate_pack_source, pull_pack, tag
from ades import get_pack
from tests.pack_generation_helpers import (
    create_acronym_general_generation_bundle,
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
    assert generated.matcher_algorithm == "token_trie_v1"
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
    assert installed_pack.matcher_algorithm == "token_trie_v1"
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
        (
            "Alden Voss said Beacon expanded. Jiajia Yang, an associate professor at "
            "James Cook University, said the letter about real estate was overblown. "
            "The founder filed the report in April may still bring changes. "
            "We are near the University. This downgrade arrived in March. "
            "Lindsay James said Stock markets in Europe were volatile."
        ),
        pack="general-en",
        storage_root=install_root,
    )

    entities = {(entity.text, entity.label) for entity in response.entities}
    lowered = {entity.text.casefold() for entity in response.entities}
    assert ("Alden Voss", "person") in entities
    assert ("Beacon", "organization") in entities
    assert ("James Cook University", "organization") in entities
    assert ("Europe", "location") in entities
    assert ("Alden", "person") not in entities
    assert ("Europe", "organization") not in entities
    assert ("James", "organization") not in entities
    assert ("letter", "location") not in entities
    assert ("This", "location") not in entities
    assert ("March", "location") not in entities
    assert ("Stock", "location") not in entities
    assert ("real estate", "organization") not in entities
    assert "yang, an" not in lowered
    assert "the founder" not in lowered
    assert "the report" not in lowered
    assert "april may" not in lowered
    assert "we are" not in lowered
    assert "university" not in lowered


def test_public_api_generated_general_pack_prefers_longer_valid_spans(
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
        (
            "The Strait of Talora reopened after the National Bank of Talora "
            "published new guidance."
        ),
        pack="general-en",
        storage_root=install_root,
    )

    entities = {(entity.text, entity.label) for entity in response.entities}
    assert ("Strait of Talora", "location") in entities
    assert ("Talora", "location") not in entities
    assert ("National Bank of Talora", "organization") in entities
    assert ("Bank of Talora", "organization") not in entities


def test_public_api_generated_general_pack_backfills_document_defined_acronyms(
    tmp_path: Path,
) -> None:
    bundle_dir = create_acronym_general_generation_bundle(tmp_path / "bundle")

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
        (
            "International Energy Agency (IEA) officials met delegates from the "
            "United States (US). Later, the IEA said the US would respond."
        ),
        pack="general-en",
        storage_root=install_root,
    )

    entities = {(entity.text, entity.label) for entity in response.entities}
    aliases = {entity.text: set(entity.aliases) for entity in response.entities}
    assert ("International Energy Agency", "organization") in entities
    assert ("United States", "location") in entities
    assert response.metrics.entity_count == 2
    assert aliases["International Energy Agency"] == {
        "International Energy Agency",
        "IEA",
    }
    assert aliases["United States"] == {"United States", "US"}


def test_public_api_generated_general_pack_extracts_standalone_org_initialisms(
    tmp_path: Path,
) -> None:
    bundle_dir = create_acronym_general_generation_bundle(tmp_path / "bundle")

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
        "The IEA said energy markets would respond.",
        pack="general-en",
        storage_root=install_root,
    )

    entities = {(entity.text, entity.label) for entity in response.entities}
    assert ("IEA", "organization") in entities


def test_public_api_generated_general_pack_prefers_geopolitical_location_acronyms_over_two_letter_org_noise(
    tmp_path: Path,
) -> None:
    bundle_dir = create_acronym_general_generation_bundle(tmp_path / "bundle")

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
        "US officials said the UK and EU would respond while CI Simpson filed a report.",
        pack="general-en",
        storage_root=install_root,
    )

    entities = {(entity.text, entity.label) for entity in response.entities}
    assert ("US", "location") in entities
    assert ("UK", "location") in entities
    assert ("EU", "location") in entities
    assert ("CI", "organization") not in entities


def test_public_api_generated_general_pack_recovers_geopolitical_and_structured_org_spans(
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
        (
            "Four analysts in China said Israel would rely on Shanghai-based "
            "Northwind Solutions while Harbor China Information reviewed the "
            "supply outlook."
        ),
        pack="general-en",
        storage_root=install_root,
    )

    entities = {(entity.text, entity.label) for entity in response.entities}
    assert ("China", "location") in entities
    assert ("Israel", "location") in entities
    assert ("Shanghai", "location") in entities
    assert ("Northwind Solutions", "organization") in entities
    assert ("Harbor China Information", "organization") in entities
    assert ("Four", "location") not in entities
    assert ("Harbor", "organization") not in entities


def test_public_api_generated_general_pack_backfills_structured_orgs_and_skips_datelines(
    tmp_path: Path,
) -> None:
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
    pull_pack(
        "general-en",
        storage_root=install_root,
        registry_url=registry.index_url,
    )
    response = tag(
        (
            "LONDON (Reuters) - Russian Foreign Ministry officials met Columbia University "
            "and Opinion Research Corporation advisers."
        ),
        pack="general-en",
        storage_root=install_root,
    )

    entities = {(entity.text, entity.label) for entity in response.entities}

    assert ("Russian Foreign Ministry", "organization") in entities
    assert ("Columbia University", "organization") in entities
    assert ("Opinion Research Corporation", "organization") in entities
    assert ("LONDON", "organization") not in entities
    assert ("LONDON", "location") not in entities
