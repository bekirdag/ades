from pathlib import Path

from fastapi.testclient import TestClient

from ades.api import tag_files
from ades.packs.installer import PackInstaller
from ades.service.app import create_app


def test_tag_files_endpoint_can_repair_missing_reused_outputs(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()
    stable_input = corpus_dir / "stable.html"
    changed_input = corpus_dir / "changed.html"
    stable_input.write_text("<p>Apple said AAPL rallied.</p>", encoding="utf-8")
    changed_input.write_text("<p>NASDAQ guidance moved.</p>", encoding="utf-8")
    output_dir = tmp_path / "outputs"

    initial = tag_files(
        directories=[corpus_dir],
        pack="finance-en",
        storage_root=tmp_path,
        output_dir=output_dir,
        write_manifest=True,
    )
    stable_output = output_dir / "stable.finance-en.ades.json"
    stable_output.unlink()
    manifest_path = Path(initial.saved_manifest_path)

    changed_input.write_text("<p>NASDAQ guidance moved again.</p>", encoding="utf-8")
    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag/files",
        json={
            "directories": [str(corpus_dir)],
            "manifest_input_path": str(manifest_path),
            "skip_unchanged": True,
            "reuse_unchanged_outputs": True,
            "repair_missing_reused_outputs": True,
            "pack": "finance-en",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    repair_warning = f"repaired_reused_output:{stable_output.resolve()}"
    assert payload["summary"]["repaired_reused_output_count"] == 1
    assert payload["summary"]["reused_output_missing_count"] == 0
    assert repair_warning in payload["warnings"]
    assert payload["reused_items"][0]["saved_output_exists"] is True
    assert repair_warning in payload["reused_items"][0]["warnings"]
    assert stable_output.exists()
