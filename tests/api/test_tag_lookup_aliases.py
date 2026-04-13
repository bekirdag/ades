from pathlib import Path
import shutil

from fastapi.testclient import TestClient

from ades.packs.generation import generate_pack_source
from ades.packs.installer import PackInstaller
from ades.service.app import create_app
from tests.pack_generation_helpers import (
    create_general_generation_bundle,
    create_noisy_general_generation_bundle,
)
from tests.pack_registry_helpers import delete_installed_pack_metadata


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
            "text": "Daniel Loeb said Beacon expanded after the letter about real estate.",
            "pack": "general-en",
            "content_type": "text/plain",
        },
    )

    assert response.status_code == 200
    pairs = {(entity["text"], entity["label"]) for entity in response.json()["entities"]}

    assert ("Daniel Loeb", "person") in pairs
    assert ("Beacon", "organization") in pairs
    assert ("Daniel", "person") not in pairs
    assert ("letter", "location") not in pairs
    assert ("real estate", "organization") not in pairs
