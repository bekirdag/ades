from fastapi.testclient import TestClient

from ades import build_registry
from ades.packs.installer import PackInstaller
from ades.service.app import create_app
from tests.pack_registry_helpers import create_pack_source, write_general_pack_build


def test_service_status_stays_empty_after_general_install_validation_failure(tmp_path) -> None:
    general_dir = create_pack_source(tmp_path / "sources", pack_id="general-en", domain="general")
    write_general_pack_build(
        general_dir,
        person_count=26,
        organization_count=14,
        location_count=1_417_881,
    )
    registry = build_registry([general_dir], output_dir=tmp_path / "registry")
    install_root = tmp_path / "install"

    client = TestClient(create_app(storage_root=install_root))

    try:
        PackInstaller(install_root, registry_url=registry.index_url).install("general-en")
    except ValueError as exc:
        assert "General-en install verification failed" in str(exc)
    else:
        raise AssertionError("Expected general-en install verification failure.")

    status_response = client.get("/v0/status")
    assert status_response.status_code == 200
    assert status_response.json()["installed_packs"] == []

    packs_response = client.get("/v0/packs")
    assert packs_response.status_code == 200
    assert packs_response.json() == []
