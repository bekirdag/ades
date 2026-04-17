import json
from pathlib import Path
import shutil

from fastapi.testclient import TestClient

from ades.packs.generation import generate_pack_source
from ades.packs.installer import PackInstaller
from ades.service.app import create_app
from tests.pack_generation_helpers import (
    create_acronym_general_generation_bundle,
    create_bundle_backed_general_pack_source,
    create_general_generation_bundle,
    create_noisy_general_generation_bundle,
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


def test_tag_endpoint_uses_lookup_for_dependency_aliases(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    client = TestClient(create_app(storage_root=tmp_path))

    response = client.post(
        "/v0/tag",
        json={
            "text": "Org Beta said TICKA traded on EXCHX.",
            "pack": "finance-en",
            "content_type": "text/plain",
        },
    )

    assert response.status_code == 200
    pairs = {(entity["text"], entity["label"]) for entity in response.json()["entities"]}

    assert ("Org Beta", "organization") in pairs
    assert ("TICKA", "ticker") in pairs
    assert ("EXCHX", "exchange") in pairs


def test_tag_endpoint_backfills_structured_organizations_and_skips_dateline_noise(
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

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag",
        json={
            "text": (
                "LONDON (Reuters) - Russian Foreign Ministry officials met Columbia University "
                "and Opinion Research Corporation advisers."
            ),
            "pack": "general-en",
            "content_type": "text/plain",
        },
    )

    assert response.status_code == 200
    pairs = {(entity["text"], entity["label"]) for entity in response.json()["entities"]}

    assert ("Russian Foreign Ministry", "organization") in pairs
    assert ("Columbia University", "organization") in pairs
    assert ("Opinion Research Corporation", "organization") in pairs
    assert ("LONDON", "organization") not in pairs
    assert ("LONDON", "location") not in pairs


def test_tag_endpoint_recovers_after_metadata_row_deletion(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    delete_installed_pack_metadata(tmp_path, "finance-en")
    client = TestClient(create_app(storage_root=tmp_path))

    response = client.post(
        "/v0/tag",
        json={
            "text": "TICKA traded on EXCHX.",
            "pack": "finance-en",
            "content_type": "text/plain",
        },
    )

    assert response.status_code == 200
    pairs = {(entity["text"], entity["label"]) for entity in response.json()["entities"]}

    assert ("TICKA", "ticker") in pairs
    assert ("EXCHX", "exchange") in pairs


def test_tag_endpoint_uses_generated_general_pack_variants(
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

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag",
        json={
            "text": "Jordan Vale from North Harbor said Beacon expanded.",
            "pack": "general-en",
            "content_type": "text/plain",
        },
    )

    assert response.status_code == 200
    pairs = {(entity["text"], entity["label"]) for entity in response.json()["entities"]}

    assert ("Jordan Vale", "person") in pairs
    assert ("North Harbor", "location") in pairs
    assert ("Beacon", "organization") in pairs


def test_tag_endpoint_suppresses_noisy_general_aliases_in_generic_prose(
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

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag",
        json={
            "text": (
                "Meridia said Harborview opened lower after Alden Voss said Beacon expanded. "
                "Jiajia Yang, an associate professor at James Cook University, said the letter "
                "about real estate was overblown. The founder filed the report in April may still "
                "bring changes. We are near the University. This downgrade arrived in March. "
                "Lindsay James said Stock markets in Europe were volatile."
            ),
            "pack": "general-en",
            "content_type": "text/plain",
        },
    )

    assert response.status_code == 200
    pairs = {(entity["text"], entity["label"]) for entity in response.json()["entities"]}
    lowered = {entity["text"].casefold() for entity in response.json()["entities"]}

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


def test_tag_endpoint_backfills_document_defined_acronyms(tmp_path: Path) -> None:
    bundle_dir = create_acronym_general_generation_bundle(tmp_path / "bundle")
    generated = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )
    pack_dir = Path(generated.pack_dir)
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag",
        json={
            "text": (
                "International Energy Agency (IEA) officials met delegates from the "
                "United States (US). Later, the IEA said the US would respond."
            ),
            "pack": "general-en",
            "content_type": "text/plain",
        },
    )

    assert response.status_code == 200
    pairs = {(entity["text"], entity["label"]) for entity in response.json()["entities"]}

    assert ("International Energy Agency", "organization") in pairs
    assert ("United States", "location") in pairs
    assert ("IEA", "organization") in pairs
    assert ("US", "location") in pairs


def test_tag_endpoint_backfills_contextual_org_acronyms_and_skips_noise(
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

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag",
        json={
            "text": (
                "He told CNBC that WWF's centre had expanded. "
                "Julian Knight MP praised the TV show and backed DACA, the act for students. "
                "LGBT (lesbian, gay, bisexual and transgender) refugees arrived."
            ),
            "pack": "general-en",
            "content_type": "text/plain",
        },
    )

    assert response.status_code == 200
    pairs = {(entity["text"], entity["label"]) for entity in response.json()["entities"]}

    assert ("CNBC", "organization") in pairs
    assert ("WWF", "organization") in pairs
    assert ("MP", "organization") not in pairs
    assert ("TV", "organization") not in pairs
    assert ("DACA", "organization") not in pairs
    assert ("LGBT", "organization") not in pairs


def test_tag_endpoint_extracts_generated_standalone_org_initialisms(tmp_path: Path) -> None:
    bundle_dir = create_acronym_general_generation_bundle(tmp_path / "bundle")
    generated = generate_pack_source(
        bundle_dir,
        output_dir=tmp_path / "generated-packs",
    )
    pack_dir = Path(generated.pack_dir)
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag",
        json={
            "text": "The IEA said energy markets would respond.",
            "pack": "general-en",
            "content_type": "text/plain",
        },
    )

    assert response.status_code == 200
    pairs = {(entity["text"], entity["label"]) for entity in response.json()["entities"]}

    assert ("IEA", "organization") in pairs


def test_tag_endpoint_prefers_longer_valid_spans_over_embedded_fragments(
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

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag",
        json={
            "text": (
                "The Strait of Talora reopened after the National Bank of Talora "
                "published new guidance."
            ),
            "pack": "general-en",
            "content_type": "text/plain",
        },
    )

    assert response.status_code == 200
    pairs = {(entity["text"], entity["label"]) for entity in response.json()["entities"]}

    assert ("Strait of Talora", "location") in pairs
    assert ("Talora", "location") not in pairs
    assert ("National Bank of Talora", "organization") in pairs
    assert ("Bank of Talora", "organization") not in pairs


def test_tag_endpoint_recovers_geopolitical_and_structured_org_newswire_spans(
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

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag",
        json={
            "text": (
                "Four analysts in China said Israel would rely on Shanghai-based "
                "Northwind Solutions while Harbor China Information reviewed the "
                "supply outlook."
            ),
            "pack": "general-en",
            "content_type": "text/plain",
        },
    )

    assert response.status_code == 200
    pairs = {(entity["text"], entity["label"]) for entity in response.json()["entities"]}

    assert ("China", "location") in pairs
    assert ("Israel", "location") in pairs
    assert ("Shanghai", "location") in pairs
    assert ("Northwind Solutions", "organization") in pairs
    assert ("Harbor China Information", "organization") in pairs
    assert ("Four", "location") not in pairs
    assert ("Harbor", "organization") not in pairs


def test_tag_endpoint_prefers_geopolitical_location_acronyms_over_two_letter_org_noise(
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

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag",
        json={
            "text": "US officials said the UK and EU would respond while CI Simpson filed a report.",
            "pack": "general-en",
            "content_type": "text/plain",
        },
    )

    assert response.status_code == 200
    pairs = {(entity["text"], entity["label"]) for entity in response.json()["entities"]}

    assert ("US", "location") in pairs
    assert ("UK", "location") in pairs
    assert ("EU", "location") in pairs
    assert ("CI", "organization") not in pairs


def test_tag_endpoint_backfills_heuristic_definition_and_contextual_acronyms(
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

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag",
        json={
            "text": (
                "Image Source: PTI/ Reuters. The Indian Youth Congress (IYC) said members of the IYC would respond. "
                "She later received a scholarship from MIT. "
                "The Local Organising Committee (LOC) said the National Sports Festival (NSF) would continue."
            ),
            "pack": "general-en",
            "content_type": "text/plain",
        },
    )

    assert response.status_code == 200
    pairs = {(entity["text"], entity["label"]) for entity in response.json()["entities"]}

    assert ("PTI", "organization") in pairs
    assert ("IYC", "organization") in pairs
    assert ("MIT", "organization") in pairs
    assert ("LOC", "organization") in pairs
    assert ("NSF", "organization") in pairs


def test_tag_endpoint_extends_trailing_structural_org_suffixes(tmp_path: Path) -> None:
    pack_dir = create_bundle_backed_general_pack_source(tmp_path / "bundle-pack")
    install_pack_dir = tmp_path / "packs" / "general-en"
    install_pack_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(pack_dir, install_pack_dir)

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag",
        json={
            "text": "Signal Harbor Partners LLC said Beacon Group Inc expanded.",
            "pack": "general-en",
            "content_type": "text/plain",
        },
    )

    assert response.status_code == 200
    pairs = {(entity["text"], entity["label"]) for entity in response.json()["entities"]}

    assert ("Signal Harbor Partners LLC", "organization") in pairs
    assert ("Signal Harbor Partners", "organization") not in pairs
    assert ("Beacon Group Inc", "organization") in pairs
    assert ("Beacon Group", "organization") not in pairs


def test_tag_endpoint_extends_structural_locations_and_skips_generic_office_titles(
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

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag",
        json={
            "text": "State Attorney General Douglas Chin said the Gulf of Mexico remained busy.",
            "pack": "general-en",
            "content_type": "text/plain",
        },
    )

    assert response.status_code == 200
    pairs = {(entity["text"], entity["label"]) for entity in response.json()["entities"]}

    assert ("Gulf of Mexico", "location") in pairs
    assert ("Mexico", "location") not in pairs
    assert ("State Attorney", "organization") not in pairs


def test_tag_endpoint_suppresses_singleton_org_noise_while_keeping_repeated_mentions(
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

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag",
        json={
            "text": "But this plan failed. I posted on Nimbus. Nimbus users replied from Riverton, North Harbor.",
            "pack": "general-en",
            "content_type": "text/plain",
        },
    )

    assert response.status_code == 200
    pairs = {(entity["text"], entity["label"]) for entity in response.json()["entities"]}

    assert ("Nimbus", "organization") in pairs
    assert ("But", "organization") not in pairs
    assert ("Riverton", "organization") not in pairs
