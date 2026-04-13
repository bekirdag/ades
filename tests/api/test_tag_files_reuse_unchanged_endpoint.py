from pathlib import Path

from fastapi.testclient import TestClient

from ades.api import tag_files
from ades.packs.installer import PackInstaller
from ades.service.app import create_app


def test_tag_files_endpoint_can_reuse_unchanged_output_metadata_from_manifest(
    tmp_path: Path,
) -> None:
    PackInstaller(tmp_path).install("finance-en")
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()
    stable_input = corpus_dir / "stable.html"
    changed_input = corpus_dir / "changed.html"
    stable_input.write_text("<p>Org Beta said TICKA rallied.</p>", encoding="utf-8")
    changed_input.write_text("<p>EXCHX guidance moved.</p>", encoding="utf-8")
    output_dir = tmp_path / "outputs"

    initial = tag_files(
        directories=[corpus_dir],
        pack="finance-en",
        storage_root=tmp_path,
        output_dir=output_dir,
        write_manifest=True,
    )
    manifest_path = Path(initial.saved_manifest_path)

    changed_input.write_text("<p>EXCHX guidance moved again.</p>", encoding="utf-8")
    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag/files",
        json={
            "directories": [str(corpus_dir)],
            "manifest_input_path": str(manifest_path),
            "skip_unchanged": True,
            "reuse_unchanged_outputs": True,
            "pack": "finance-en",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["item_count"] == 1
    assert payload["summary"]["unchanged_skipped_count"] == 1
    assert payload["summary"]["unchanged_reused_count"] == 1
    assert payload["items"][0]["source_path"] == str(changed_input.resolve())
    assert payload["reused_items"][0]["source_path"] == str(stable_input.resolve())
    assert payload["reused_items"][0]["saved_output_path"] == str(
        output_dir.resolve() / "stable.finance-en.ades.json"
    )
