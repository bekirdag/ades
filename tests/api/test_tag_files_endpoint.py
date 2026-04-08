from pathlib import Path

from fastapi.testclient import TestClient

from ades.packs.installer import PackInstaller
from ades.service.app import create_app


def test_tag_files_endpoint_tags_multiple_local_documents(tmp_path: Path) -> None:
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
            "output": {
                "directory": str(output_dir),
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    saved_paths = {item["saved_output_path"] for item in payload["items"]}

    assert payload["pack"] == "finance-en"
    assert payload["item_count"] == 2
    assert saved_paths == {
        str(output_dir.resolve() / "report.finance-en.ades.json"),
        str(output_dir.resolve() / "report.finance-en.ades-2.json"),
    }
