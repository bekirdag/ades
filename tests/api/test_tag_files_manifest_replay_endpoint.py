from pathlib import Path

from fastapi.testclient import TestClient

from ades.api import tag_files
from ades.packs.installer import PackInstaller
from ades.service.app import create_app


def test_tag_files_endpoint_can_replay_all_items_from_saved_manifest(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    first_input = tmp_path / "alpha.html"
    second_input = tmp_path / "beta.html"
    first_input.write_text("<p>Org Beta said TICKA rallied.</p>", encoding="utf-8")
    second_input.write_text("<p>EXCHX guidance moved.</p>", encoding="utf-8")
    output_dir = tmp_path / "outputs"

    initial = tag_files(
        [first_input, second_input],
        pack="finance-en",
        storage_root=tmp_path,
        output_dir=output_dir,
        write_manifest=True,
        max_files=1,
    )
    manifest_path = Path(initial.saved_manifest_path)

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag/files",
        json={
            "manifest_input_path": str(manifest_path),
            "manifest_replay_mode": "all",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["pack"] == "finance-en"
    assert payload["item_count"] == 2
    assert payload["summary"]["manifest_input_path"] == str(manifest_path.resolve())
    assert payload["summary"]["manifest_replay_mode"] == "all"
    assert payload["summary"]["manifest_candidate_count"] == 2
    assert payload["summary"]["manifest_selected_count"] == 2
    assert [item["source_path"] for item in payload["items"]] == [
        str(first_input.resolve()),
        str(second_input.resolve()),
    ]
