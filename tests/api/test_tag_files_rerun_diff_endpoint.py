from pathlib import Path

from fastapi.testclient import TestClient

from ades.api import tag_files
from ades.packs.installer import PackInstaller
from ades.service.app import create_app


def test_tag_files_endpoint_reports_rerun_diff(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()
    stable_input = corpus_dir / "01-stable.html"
    changed_input = corpus_dir / "02-changed.html"
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
    stable_output = output_dir / "01-stable.finance-en.ades.json"
    stable_output.unlink()
    changed_input.write_text("<p>EXCHX guidance moved again.</p>", encoding="utf-8")
    new_input = corpus_dir / "03-new.html"
    skipped_input = corpus_dir / "04-skipped.html"
    new_input.write_text("<p>Org Beta traded on EXCHX.</p>", encoding="utf-8")
    skipped_input.write_text("<p>Late file.</p>", encoding="utf-8")

    client = TestClient(create_app(storage_root=tmp_path))
    response = client.post(
        "/v0/tag/files",
        json={
            "directories": [str(corpus_dir)],
            "manifest_input_path": str(initial.saved_manifest_path),
            "skip_unchanged": True,
            "reuse_unchanged_outputs": True,
            "repair_missing_reused_outputs": True,
            "max_files": 3,
            "pack": "finance-en",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["limit_skipped_count"] == 1
    assert payload["rerun_diff"]["changed"] == [str(changed_input.resolve())]
    assert payload["rerun_diff"]["newly_processed"] == [str(new_input.resolve())]
    assert payload["rerun_diff"]["reused"] == [str(stable_input.resolve())]
    assert payload["rerun_diff"]["repaired"] == [str(stable_input.resolve())]
    assert payload["rerun_diff"]["skipped"] == [
        {
            "reference": str(skipped_input.resolve()),
            "reason": "max_files_limit",
            "source_kind": "limit",
            "size_bytes": skipped_input.stat().st_size,
        }
    ]
