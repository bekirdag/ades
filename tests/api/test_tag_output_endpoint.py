import json
from pathlib import Path

from fastapi.testclient import TestClient

from ades.packs.installer import PackInstaller
from ades.service.app import create_app


def test_tag_file_endpoint_can_persist_results(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    input_path = tmp_path / "report.html"
    input_path.write_text("<p>Org Beta said TICKA traded on EXCHX.</p>", encoding="utf-8")
    output_dir = tmp_path / "outputs"

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag/file",
        json={
            "path": str(input_path),
            "pack": "finance-en",
            "output": {
                "directory": str(output_dir),
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    saved_path = output_dir.resolve() / "report.finance-en.ades.json"

    assert payload["saved_output_path"] == str(saved_path)
    persisted = json.loads(saved_path.read_text(encoding="utf-8"))
    assert persisted["saved_output_path"] == str(saved_path)
    assert persisted["source_path"] == str(input_path.resolve())
