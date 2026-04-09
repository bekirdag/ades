import json
from pathlib import Path

from fastapi.testclient import TestClient

from ades.packs.installer import PackInstaller
from ades.service.app import create_app


def test_tag_files_endpoint_reports_manifest_lineage(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()
    source_path = corpus_dir / "report.html"
    source_path.write_text("<p>Apple said AAPL rallied.</p>", encoding="utf-8")
    output_dir = tmp_path / "outputs"

    client = TestClient(create_app(storage_root=tmp_path))
    initial = client.post(
        "/v0/tag/files",
        json={
            "directories": [str(corpus_dir)],
            "pack": "finance-en",
            "output": {
                "directory": str(output_dir),
                "write_manifest": True,
            },
        },
    )

    assert initial.status_code == 200
    initial_payload = initial.json()
    manifest_path = output_dir.resolve() / "batch.finance-en.ades-manifest.json"
    assert initial_payload["saved_manifest_path"] == str(manifest_path)
    assert initial_payload["lineage"]["run_id"].startswith("ades-run-")
    assert initial_payload["lineage"]["root_run_id"] == initial_payload["lineage"]["run_id"]
    assert initial_payload["lineage"]["parent_run_id"] is None
    assert initial_payload["lineage"]["source_manifest_path"] is None
    assert initial_payload["lineage"]["created_at"]

    child_manifest_path = output_dir.resolve() / "child.finance-en.ades-manifest.json"
    replay = client.post(
        "/v0/tag/files",
        json={
            "manifest_input_path": str(manifest_path),
            "manifest_replay_mode": "processed",
            "pack": "finance-en",
            "output": {
                "directory": str(output_dir),
                "write_manifest": True,
                "manifest_path": str(child_manifest_path),
            },
        },
    )

    assert replay.status_code == 200
    replay_payload = replay.json()
    assert replay_payload["saved_manifest_path"] == str(child_manifest_path)
    assert replay_payload["lineage"]["parent_run_id"] == initial_payload["lineage"]["run_id"]
    assert replay_payload["lineage"]["root_run_id"] == initial_payload["lineage"]["root_run_id"]
    assert replay_payload["lineage"]["source_manifest_path"] == str(manifest_path)
    assert replay_payload["lineage"]["created_at"]
    assert (
        replay_payload["lineage"]["created_at"]
        > initial_payload["lineage"]["created_at"]
    )

    child_manifest_payload = json.loads(child_manifest_path.read_text(encoding="utf-8"))
    assert child_manifest_payload["lineage"] == replay_payload["lineage"]
