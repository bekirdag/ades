import pytest

from ades import build_registry, list_packs, pull_pack
from tests.pack_registry_helpers import create_pack_source, write_general_pack_build


def test_public_api_pull_rejects_pathological_general_pack_artifact(tmp_path) -> None:
    general_dir = create_pack_source(tmp_path / "sources", pack_id="general-en", domain="general")
    write_general_pack_build(
        general_dir,
        person_count=26,
        organization_count=14,
        location_count=1_417_881,
    )
    registry = build_registry([general_dir], output_dir=tmp_path / "registry")
    install_root = tmp_path / "install"

    with pytest.raises(ValueError, match="General-en install verification failed"):
        pull_pack(
            "general-en",
            storage_root=install_root,
            registry_url=registry.index_url,
        )

    assert list_packs(storage_root=install_root) == []
