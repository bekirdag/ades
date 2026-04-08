from pathlib import Path

from fastapi.testclient import TestClient

from ades.service.app import create_app
from ades.version import __version__


def test_npm_installer_endpoint_reports_bootstrap_metadata() -> None:
    client = TestClient(create_app())

    response = client.get("/v0/installers/npm")

    assert response.status_code == 200
    payload = response.json()
    assert payload["npm_package"] == "ades-cli"
    assert payload["command"] == "ades"
    assert payload["wrapper_version"] == __version__
    assert payload["python_package_spec"] == f"ades=={__version__}"
    assert payload["runtime_dir"].endswith(f"/ades/npm-runtime/{__version__}")
