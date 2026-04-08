import json
from pathlib import Path

from fastapi.testclient import TestClient

from ades.packs.installer import PackInstaller
from ades.service.app import create_app


def test_tag_files_endpoint_can_write_batch_manifest_and_return_warnings(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    alpha_dir = tmp_path / "alpha"
    beta_dir = tmp_path / "beta"
    alpha_dir.mkdir()
    beta_dir.mkdir()
    first_input = alpha_dir / "report.html"
    second_input = beta_dir / "report.html"
    first_input.write_text("<p>Apple said AAPL traded on NASDAQ.</p>", encoding="utf-8")
    second_input.write_text("<p>NASDAQ said Apple moved AAPL guidance.</p>", encoding="utf-8")
    output_dir = tmp_path / "outputs"

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag/files",
        json={
            "paths": [str(first_input), str(second_input)],
            "pack": "finance-en",
            "content_type": "application/json",
            "output": {
                "directory": str(output_dir),
                "write_manifest": True,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    manifest_path = output_dir.resolve() / "batch.finance-en.ades-manifest.json"
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert payload["saved_manifest_path"] == str(manifest_path)
    assert payload["warnings"] == ["unsupported_content_type:application/json"]
    assert manifest_payload["warnings"] == ["unsupported_content_type:application/json"]
    assert manifest_payload["items"][0]["saved_output_path"] == str(
        output_dir.resolve() / "report.finance-en.ades.json"
    )
