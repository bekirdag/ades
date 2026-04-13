from pathlib import Path

from ades import lookup_candidates, pull_pack
from ades.storage.registry_db import PackMetadataStore


def test_public_api_lookup_supports_multi_term_cross_field_search(tmp_path: Path) -> None:
    pull_pack("finance-en", storage_root=tmp_path)

    response = lookup_candidates(
        "org beta organization",
        storage_root=tmp_path,
        limit=10,
    )

    assert response.candidates
    assert response.candidates[0].kind == "alias"
    assert response.candidates[0].value == "Org Beta"
    assert response.candidates[0].label == "organization"


def test_public_api_exact_lookup_does_not_sync_global_search_index(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pull_pack("finance-en", storage_root=tmp_path)

    def fail_sync(self: PackMetadataStore, connection: object) -> None:
        raise AssertionError("public exact lookup must not rebuild the global search index")

    monkeypatch.setattr(PackMetadataStore, "_sync_search_index", fail_sync)

    response = lookup_candidates(
        "TICKA",
        storage_root=tmp_path,
        exact_alias=True,
        limit=10,
    )

    assert response.candidates


def test_public_api_lookup_supports_explicit_fuzzy_mode(tmp_path: Path) -> None:
    pull_pack("finance-en", storage_root=tmp_path)

    response = lookup_candidates(
        "org beta",
        storage_root=tmp_path,
        fuzzy=True,
        limit=10,
    )

    assert response.fuzzy is True
    assert response.candidates
