import json
from pathlib import Path
import shutil

import pytest

from ades.packs.generation import generate_pack_source
from ades.packs.installer import PackInstaller
from ades.pipeline.tagger import tag_text
from tests.pack_generation_helpers import (
    create_acronym_general_generation_bundle,
    create_bundle_backed_general_pack_source,
    create_bundle_backed_general_pack_source_with_alias_collision,
    create_general_generation_bundle,
    create_noisy_general_generation_bundle,
    create_pre_resolved_general_generation_bundle,
)
from tests.pack_registry_helpers import delete_installed_pack_metadata


def _create_structural_general_generation_bundle(root: Path) -> Path:
    bundle_dir = create_general_generation_bundle(root)
    entities_path = bundle_dir / "normalized" / "entities.jsonl"
    records = [
        json.loads(line)
        for line in entities_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    records.extend(
        [
            {
                "entity_id": "location:mexico",
                "entity_type": "location",
                "canonical_text": "Mexico",
                "aliases": [],
                "source_name": "curated-general",
                "population": 126_000_000,
            },
            {
                "entity_id": "organization:state-attorney",
                "entity_type": "organization",
                "canonical_text": "State Attorney",
                "aliases": [],
                "source_name": "curated-general",
                "popularity": 0.61,
            },
        ]
    )
    entities_path.write_text(
        "".join(json.dumps(item) + "\n" for item in records),
        encoding="utf-8",
    )
    return bundle_dir


def _create_singleton_noise_general_generation_bundle(root: Path) -> Path:
    bundle_dir = create_general_generation_bundle(root)
    entities_path = bundle_dir / "normalized" / "entities.jsonl"
    records = [
        json.loads(line)
        for line in entities_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    records.extend(
        [
            {
                "entity_id": "organization:nimbus",
                "entity_type": "organization",
                "canonical_text": "Nimbus",
                "aliases": [],
                "source_name": "curated-general",
                "popularity": 0.84,
            },
            {
                "entity_id": "organization:but",
                "entity_type": "organization",
                "canonical_text": "But",
                "aliases": [],
                "source_name": "curated-general",
                "popularity": 0.61,
            },
            {
                "entity_id": "organization:riverton",
                "entity_type": "organization",
                "canonical_text": "Riverton",
                "aliases": [],
                "source_name": "curated-general",
                "popularity": 0.67,
            },
        ]
    )
    entities_path.write_text(
        "".join(json.dumps(item) + "\n" for item in records),
        encoding="utf-8",
    )
    return bundle_dir


def _create_person_surname_conflict_general_pack_source(root: Path) -> Path:
    pack_dir = create_bundle_backed_general_pack_source(root)
    entities_path = pack_dir / "normalized" / "entities.jsonl"
    records = [
        json.loads(line)
        for line in entities_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    records.extend(
        [
            {
                "entity_id": "person:lisa-d-cook",
                "entity_type": "person",
                "canonical_text": "Lisa D. Cook",
                "aliases": ["Governor Lisa Cook"],
                "source_name": "curated-general",
                "popularity": 0.88,
            },
            {
                "entity_id": "person:jerome-powell",
                "entity_type": "person",
                "canonical_text": "Jerome Powell",
                "aliases": ["Jerome H. Powell"],
                "source_name": "curated-general",
                "popularity": 0.94,
            },
            {
                "entity_id": "person:doug-ford",
                "entity_type": "person",
                "canonical_text": "Doug Ford",
                "aliases": [],
                "source_name": "curated-general",
                "popularity": 0.86,
            },
            {
                "entity_id": "person:bill-davis",
                "entity_type": "person",
                "canonical_text": "Bill Davis",
                "aliases": [],
                "source_name": "curated-general",
                "popularity": 0.84,
            },
            {
                "entity_id": "person:byington-ford",
                "entity_type": "person",
                "canonical_text": "Byington Ford",
                "aliases": ["By Ford"],
                "source_name": "curated-general",
                "popularity": 0.41,
            },
            {
                "entity_id": "location:cook",
                "entity_type": "location",
                "canonical_text": "Cook",
                "aliases": [],
                "source_name": "curated-general",
                "popularity": 0.52,
            },
            {
                "entity_id": "location:powell",
                "entity_type": "location",
                "canonical_text": "Powell",
                "aliases": [],
                "source_name": "curated-general",
                "popularity": 0.53,
            },
            {
                "entity_id": "location:davis",
                "entity_type": "location",
                "canonical_text": "Davis",
                "aliases": [],
                "source_name": "curated-general",
                "popularity": 0.52,
            },
            {
                "entity_id": "location:ford",
                "entity_type": "location",
                "canonical_text": "Ford",
                "aliases": [],
                "source_name": "curated-general",
                "popularity": 0.45,
            },
        ]
    )
    entities_path.write_text(
        "".join(json.dumps(item) + "\n" for item in records),
        encoding="utf-8",
    )
    return pack_dir


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


def test_tagger_resolves_matcher_entries_without_registry_exact_lookup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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

    def _fail_lookup(self, candidate_text: str, **kwargs):  # type: ignore[no-untyped-def]
        if kwargs.get("exact_alias"):
            raise AssertionError(
                "matcher-enabled packs should resolve exact matches from matcher payloads"
            )
        return []

    monkeypatch.setattr(
        "ades.packs.registry.PackRegistry.lookup_candidates",
        _fail_lookup,
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


def test_tagger_resolves_hyphenated_matcher_entries_without_registry_exact_lookup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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

    def _fail_lookup(self, candidate_text: str, **kwargs):  # type: ignore[no-untyped-def]
        if kwargs.get("exact_alias"):
            raise AssertionError(
                "matcher-enabled hyphenated recovery should resolve exact matches from matcher payloads"
            )
        return []

    monkeypatch.setattr(
        "ades.packs.registry.PackRegistry.lookup_candidates",
        _fail_lookup,
    )

    response = tag_text(
        text="Beacon-based teams gathered in North Harbor.",
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("Beacon", "organization") in pairs
    assert ("North Harbor", "location") in pairs


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


def test_tagger_trims_structural_org_role_suffix_and_skips_plural_generic_phrase(
    tmp_path: Path,
) -> None:
    pack_dir = create_bundle_backed_general_pack_source(tmp_path / "bundle-pack")
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    response = tag_text(
        text=(
            '"Budget airlines are like weights when it comes to airfares," '
            "said Atmosphere Research Group airline analyst Henry Harteveldt."
        ),
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}
    texts = {entity.text for entity in response.entities}

    assert "Budget airlines" not in texts
    assert "Atmosphere Research Group airline" not in texts
    assert ("Atmosphere Research Group", "organization") in pairs


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
        text=(
            "Meridia said Harborview opened lower after Alden Voss said Beacon expanded. "
            "Jiajia Yang, an associate professor at James Cook University, said the letter "
            "about real estate was overblown. The founder filed the report in April may still "
            "bring changes. We are near the University. This downgrade arrived in March. "
            "Lindsay James said Stock markets in Europe were volatile."
        ),
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}
    lowered = {entity.text.casefold() for entity in response.entities}

    assert ("Meridia", "organization") in pairs
    assert ("Harborview", "location") in pairs
    assert ("Alden Voss", "person") in pairs
    assert ("Beacon", "organization") in pairs
    assert ("James Cook University", "organization") in pairs
    assert ("Europe", "location") in pairs
    assert ("Alden", "person") not in pairs
    assert ("Europe", "organization") not in pairs
    assert ("James", "organization") not in pairs
    assert ("letter", "location") not in pairs
    assert ("This", "location") not in pairs
    assert ("March", "location") not in pairs
    assert ("Stock", "location") not in pairs
    assert ("real estate", "organization") not in pairs
    assert "yang, an" not in lowered
    assert "the founder" not in lowered
    assert "the report" not in lowered
    assert "april may" not in lowered
    assert "we are" not in lowered
    assert "university" not in lowered


def test_tagger_backfills_document_defined_acronyms(tmp_path: Path) -> None:
    bundle_dir = create_acronym_general_generation_bundle(tmp_path / "bundle")
    generated = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )
    pack_dir = Path(generated.pack_dir)
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    response = tag_text(
        text=(
            "International Energy Agency (IEA) officials met delegates from the "
            "United States (US). Later, the IEA said the US would respond."
        ),
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}
    aliases = {entity.text: set(entity.aliases) for entity in response.entities}

    assert ("International Energy Agency", "organization") in pairs
    assert ("United States", "location") in pairs
    assert response.metrics.entity_count == 2
    assert aliases["International Energy Agency"] == {
        "International Energy Agency",
        "IEA",
    }
    assert aliases["United States"] == {"United States", "US"}


def test_tagger_backfills_contextual_org_acronyms_and_skips_noise(tmp_path: Path) -> None:
    bundle_dir = create_general_generation_bundle(tmp_path / "bundle")
    generated = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )
    pack_dir = Path(generated.pack_dir)
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    response = tag_text(
        text=(
            "He told CNBC that WWF's centre had expanded. "
            "Julian Knight MP praised the TV show and backed DACA, the act for students. "
            "LGBT (lesbian, gay, bisexual and transgender) refugees arrived."
        ),
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("WWF", "organization") in pairs
    assert ("CNBC", "organization") not in pairs
    assert ("MP", "organization") not in pairs
    assert ("TV", "organization") not in pairs
    assert ("DACA", "organization") not in pairs
    assert ("LGBT", "organization") not in pairs


def test_tagger_backfills_article_led_and_slash_paired_org_acronyms(tmp_path: Path) -> None:
    bundle_dir = create_general_generation_bundle(tmp_path / "bundle")
    generated = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )
    pack_dir = Path(generated.pack_dir)
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    response = tag_text(
        text=(
            "The Court of Arbitration for Sport rejected the appeal. "
            "The FA has contacted the full-back. "
            "A BBC/ICM poll found support. "
            "The NUT/ATL school cuts website tracked updates."
        ),
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("Court of Arbitration for Sport", "organization") in pairs
    assert ("FA", "organization") in pairs
    assert ("ICM", "organization") in pairs
    assert ("ATL", "organization") in pairs
    assert ("NUT", "organization") not in pairs


def test_tagger_extracts_generated_standalone_org_initialisms(tmp_path: Path) -> None:
    bundle_dir = create_acronym_general_generation_bundle(tmp_path / "bundle")
    generated = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )
    pack_dir = Path(generated.pack_dir)
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    response = tag_text(
        text="The IEA said energy markets would respond.",
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("IEA", "organization") in pairs


def test_tagger_prefers_longer_valid_spans_over_embedded_fragments(
    tmp_path: Path,
) -> None:
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
        text=(
            "The Strait of Talora reopened after the National Bank of Talora "
            "published new guidance."
        ),
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("Strait of Talora", "location") in pairs
    assert ("Talora", "location") not in pairs
    assert ("National Bank of Talora", "organization") in pairs
    assert ("Bank of Talora", "organization") not in pairs


def test_tagger_recovers_geopolitical_and_structured_org_newswire_spans(
    tmp_path: Path,
) -> None:
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
        text=(
            "Four analysts in China said Israel would rely on Shanghai-based "
            "Northwind Solutions while Harbor China Information reviewed the "
            "supply outlook."
        ),
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("China", "location") in pairs
    assert ("Israel", "location") in pairs
    assert ("Shanghai", "location") in pairs
    assert ("Northwind Solutions", "organization") in pairs
    assert ("Harbor China Information", "organization") in pairs
    assert ("Four", "location") not in pairs
    assert ("Harbor", "organization") not in pairs


def test_tagger_keeps_exact_all_caps_expansion_acronyms(tmp_path: Path) -> None:
    bundle_dir = create_acronym_general_generation_bundle(tmp_path / "bundle")
    entities_path = bundle_dir / "normalized" / "entities.jsonl"
    records = [
        json.loads(line)
        for line in entities_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    for record in records:
        if record.get("entity_id") == "organization:international-energy-agency":
            aliases = list(record.get("aliases") or [])
            aliases.append("IEA")
            record["aliases"] = aliases
            break
    entities_path.write_text(
        "".join(json.dumps(item) + "\n" for item in records),
        encoding="utf-8",
    )
    generated = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )
    pack_dir = Path(generated.pack_dir)
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    response = tag_text(
        text="The IEA said the BP traders were watching Iran.",
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("IEA", "organization") in pairs


def test_tagger_prefers_geopolitical_location_acronyms_over_two_letter_org_noise(
    tmp_path: Path,
) -> None:
    bundle_dir = create_acronym_general_generation_bundle(tmp_path / "bundle")
    generated = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )
    pack_dir = Path(generated.pack_dir)
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    response = tag_text(
        text="US officials said the UK and EU would respond while CI Simpson filed a report.",
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("US", "location") in pairs
    assert ("UK", "location") in pairs
    assert ("EU", "location") in pairs
    assert ("CI", "organization") not in pairs


def test_tagger_backfills_heuristic_definition_and_contextual_acronyms(
    tmp_path: Path,
) -> None:
    bundle_dir = create_acronym_general_generation_bundle(tmp_path / "bundle")
    generated = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )
    pack_dir = Path(generated.pack_dir)
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    response = tag_text(
        text=(
            "Image Source: PTI/ Reuters. The Indian Youth Congress (IYC) said members of the IYC would respond. "
            "She later received a scholarship from MIT. "
            "The Local Organising Committee (LOC) said the National Sports Festival (NSF) would continue."
        ),
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("PTI", "organization") in pairs
    assert ("IYC", "organization") in pairs
    assert ("MIT", "organization") in pairs
    assert ("LOC", "organization") in pairs
    assert ("NSF", "organization") in pairs


def test_tagger_backfills_contextual_acronym_with_following_railroad_noun(
    tmp_path: Path,
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

    response = tag_text(
        text="CSX railroad's profit jumped after the company cut expenses.",
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("CSX", "organization") in pairs


def test_tagger_extends_trailing_structural_org_suffixes(tmp_path: Path) -> None:
    pack_dir = create_bundle_backed_general_pack_source(tmp_path / "bundle-pack")
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    response = tag_text(
        text="Signal Harbor Partners LLC said Beacon Group Inc expanded.",
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("Signal Harbor Partners LLC", "organization") in pairs
    assert ("Signal Harbor Partners", "organization") not in pairs
    assert ("Beacon Group Inc", "organization") in pairs
    assert ("Beacon Group", "organization") not in pairs


def test_tagger_extends_structural_locations_and_skips_generic_office_titles(
    tmp_path: Path,
) -> None:
    bundle_dir = _create_structural_general_generation_bundle(tmp_path / "bundle")
    generated = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )
    pack_dir = Path(generated.pack_dir)
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    response = tag_text(
        text="State Attorney General Douglas Chin said the Gulf of Mexico remained busy.",
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("Gulf of Mexico", "location") in pairs
    assert ("Mexico", "location") not in pairs
    assert ("State Attorney", "organization") not in pairs


def test_tagger_suppresses_singleton_org_noise_while_keeping_repeated_mentions(
    tmp_path: Path,
) -> None:
    bundle_dir = _create_singleton_noise_general_generation_bundle(tmp_path / "bundle")
    generated = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )
    pack_dir = Path(generated.pack_dir)
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    response = tag_text(
        text="But this plan failed. I posted on Nimbus. Nimbus users replied from Riverton, North Harbor.",
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("Nimbus", "organization") in pairs
    assert ("But", "organization") not in pairs
    assert ("Riverton", "organization") not in pairs


def test_tagger_suppresses_surname_coreference_location_false_positives(
    tmp_path: Path,
) -> None:
    pack_dir = _create_person_surname_conflict_general_pack_source(tmp_path / "bundle-pack")
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    response = tag_text(
        text=(
            "Governor Lisa Cook said rates could stay high. Later, Cook said policy remained tight. "
            "Jerome Powell said the committee would wait. Later, Powell said inflation was still elevated."
        ),
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}
    lowered = {entity.text.casefold() for entity in response.entities}

    assert ("Governor Lisa Cook", "person") in pairs
    assert ("Jerome Powell", "person") in pairs
    assert ("Cook", "location") not in pairs
    assert ("Powell", "location") not in pairs
    assert "cook" not in lowered
    assert "powell" not in lowered


def test_tagger_suppresses_additional_surname_coreference_mention_types(
    tmp_path: Path,
) -> None:
    pack_dir = _create_person_surname_conflict_general_pack_source(tmp_path / "bundle-pack")
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    response = tag_text(
        text=(
            "Doug Ford defended the purchase. Premier Ford said the province would keep flying. "
            "Travel conducted by Ford across Canada continued through the weekend. "
            "Bill Davis later came under fire. The Davis government sold the jet."
        ),
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}
    lowered = {entity.text.casefold() for entity in response.entities}

    assert ("Doug Ford", "person") in pairs
    assert ("Bill Davis", "person") in pairs
    assert ("Ford", "location") not in pairs
    assert ("Davis", "location") not in pairs
    assert ("by Ford", "person") not in pairs
    assert "ford" not in lowered
    assert "davis" not in lowered
    assert "by ford" not in lowered


def test_tagger_suppresses_reported_object_and_leadership_surname_coreference(
    tmp_path: Path,
) -> None:
    pack_dir = _create_person_surname_conflict_general_pack_source(tmp_path / "bundle-pack")
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    response = tag_text(
        text=(
            "Jerome Powell spoke earlier in the day. "
            "Analysts told Powell to stay in place. "
            "Later, Powell led a first round of talks with staff."
        ),
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}
    lowered = {entity.text.casefold() for entity in response.entities}

    assert ("Jerome Powell", "person") in pairs
    assert ("Powell", "location") not in pairs
    assert "powell" not in lowered


def test_tagger_suppresses_went_public_surname_coreference_location_false_positive(
    tmp_path: Path,
) -> None:
    pack_dir = _create_person_surname_conflict_general_pack_source(tmp_path / "bundle-pack")
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    response = tag_text(
        text=(
            "Premier Doug Ford criticized the proposal earlier. "
            "Ford went public in April with his concerns."
        ),
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}
    lowered = {entity.text.casefold() for entity in response.entities}

    assert ("Doug Ford", "person") in pairs
    assert ("Ford", "location") not in pairs
    assert "ford" not in lowered


def test_tagger_backfills_structured_organizations_and_skips_dateline_noise(
    tmp_path: Path,
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

    response = tag_text(
        text=(
            "LONDON (Reuters) - Russian Foreign Ministry officials met Columbia University "
            "and Opinion Research Corporation advisers."
        ),
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("Russian Foreign Ministry", "organization") in pairs
    assert ("Columbia University", "organization") in pairs
    assert ("Opinion Research Corporation", "organization") in pairs
    assert ("LONDON", "organization") not in pairs
    assert ("LONDON", "location") not in pairs


def test_tagger_prefers_structural_org_spans_over_short_alias_fragments(
    tmp_path: Path,
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

    response = tag_text(
        text=(
            "Glasgow City Council said it planned to raise council tax. "
            "In the schoolyard of the Shmisani Institute for Girls, teachers gathered."
        ),
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("Glasgow City Council", "organization") in pairs
    assert ("Glasgow City", "location") not in pairs
    assert ("Shmisani Institute for Girls", "organization") in pairs
    assert ("Girls", "organization") not in pairs


def test_tagger_extends_college_spans_and_backfills_unhcr(
    tmp_path: Path,
) -> None:
    bundle_dir = create_general_generation_bundle(tmp_path / "bundle")
    entities_path = bundle_dir / "normalized" / "entities.jsonl"
    records = [
        json.loads(line)
        for line in entities_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    records.append(
        {
            "entity_id": "organization:college-of-physicians",
            "entity_type": "organization",
            "canonical_text": "College of Physicians",
            "aliases": [],
            "source_name": "curated-general",
            "popularity": 0.63,
        }
    )
    entities_path.write_text(
        "".join(json.dumps(item) + "\n" for item in records),
        encoding="utf-8",
    )
    generated = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )
    pack_dir = Path(generated.pack_dir)
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    response = tag_text(
        text=(
            "The College of Physicians of Philadelphia hosted the exhibit. "
            "Roads to Goma were crowded with refugees, the UNHCR reported. "
            "The U.N. High Commissioner for Refugees warned of danger."
        ),
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("College of Physicians of Philadelphia", "organization") in pairs
    assert ("College of Physicians", "organization") not in pairs
    assert ("UNHCR", "organization") in pairs


def test_tagger_recovers_connector_head_and_hospital_structural_spans(
    tmp_path: Path,
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

    response = tag_text(
        text=(
            '"Our offices are basically destroyed now, nothing works," UNHCR spokesman '
            "Ron Redmond said from Geneva. "
            "WHO Collaborating Centre for pharmacovigilance launched at IPC Ghaziabad. "
            "Evans told the Society of Editors meeting. "
            "The 26-year-old commando served with the Special Operations Task Group. "
            "A patient was taken to Sunshine Coast University Hospital."
        ),
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("UNHCR", "organization") in pairs
    assert ("WHO Collaborating Centre for pharmacovigilance", "organization") in pairs
    assert ("WHO", "organization") not in pairs
    assert ("Society of Editors", "organization") in pairs
    assert ("Editors", "organization") not in pairs
    assert ("Special Operations Task Group", "organization") in pairs
    assert ("Special Operations", "organization") not in pairs
    assert ("Sunshine Coast University Hospital", "organization") in pairs
    assert ("Sunshine Coast", "location") not in pairs


def test_tagger_prefers_structural_orgs_over_article_led_generic_overlap_winners(
    tmp_path: Path,
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

    response = tag_text(
        text=(
            "City of Edinburgh Council met the National Fire Chiefs Council "
            "after a safety review."
        ),
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}
    normalized_pairs = {(entity.text.casefold(), entity.label) for entity in response.entities}

    assert ("City of Edinburgh Council", "organization") in pairs
    assert ("National Fire Chiefs Council", "organization") in pairs
    assert ("the national", "organization") not in normalized_pairs


def test_tagger_recovers_solutions_suffix_locations_and_or_acronym_definitions(
    tmp_path: Path,
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

    response = tag_text(
        text=(
            "Dave Ramsey is CEO of Ramsey Solutions. "
            "The men were held on the island of Nauru. "
            "Deferred Action for Childhood Arrivals, or DACA, program supporters gathered."
        ),
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("Ramsey Solutions", "organization") in pairs
    assert ("island of Nauru", "location") in pairs
    assert ("DACA", "organization") in pairs


def test_tagger_recovers_media_suffix_and_parliamentary_house_spans(
    tmp_path: Path,
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

    response = tag_text(
        text=(
            "Newsmax Media published the interview after members of the House of Commons met. "
            "Greek media later repeated the claim."
        ),
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("Newsmax Media", "organization") in pairs
    assert ("House of Commons", "organization") in pairs
    assert ("Greek media", "organization") not in pairs


def test_tagger_recovers_hyphenated_locations_and_broadcaster_acronyms(
    tmp_path: Path,
) -> None:
    pack_dir = create_bundle_backed_general_pack_source(tmp_path / "bundle-pack")
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    response = tag_text(
        text=(
            "The London-based regulator joined a Vermont-based advocacy group. "
            "Local broadcaster WISN reported the decision."
        ),
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("London", "location") in pairs
    assert ("Vermont", "location") in pairs
    assert ("WISN", "organization") in pairs


def test_tagger_recovers_contextual_archive_acronyms_and_multi_token_hyphenated_locations(
    tmp_path: Path,
) -> None:
    pack_dir = create_bundle_backed_general_pack_source(tmp_path / "bundle-pack")
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    response = tag_text(
        text=(
            "The trade association ABICAB reported new figures while state news agency KCNA "
            "released a statement. The LGBT community then praised the Networked Quantum "
            "Information Technologies Hub (NQIT). A Hong Kong-based analyst later commented."
        ),
        pack="general-en",
        content_type="text/plain",
        storage_root=tmp_path,
    )

    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert ("ABICAB", "organization") in pairs
    assert ("KCNA", "organization") in pairs
    assert ("LGBT", "organization") in pairs
    assert ("NQIT", "organization") in pairs
    assert ("Hong Kong", "location") in pairs
