import json
import sys
from pathlib import Path

from ades.artifact_release import (
    ArtifactDescriptor,
    write_artifact_release_manifest,
)


def test_write_artifact_release_manifest_requires_kinds_and_runs_gates(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "recent_news_support.json"
    artifact_path.write_text(
        json.dumps({"schema_version": 2, "generated_at": "2026-05-13T00:00:00Z"}),
        encoding="utf-8",
    )
    manifest_path = tmp_path / "release_manifest.json"

    manifest = write_artifact_release_manifest(
        [
            ArtifactDescriptor(
                kind="recent_news",
                name="recent-news-v2",
                path=artifact_path,
            )
        ],
        output_path=manifest_path,
        require_kinds=["recent_news"],
        release_gate_commands=[f'{sys.executable} -c "print(\\\"ok\\\")"'],
        release_gate_working_dir=tmp_path,
        rollback_instructions=["restore previous recent-news artifact"],
    )

    assert manifest["passed"] is True
    assert manifest["release_gate_passed"] is True
    assert manifest["artifacts"][0]["schema_version"] == 2
    assert manifest["artifacts"][0]["sha256"]
    assert json.loads(manifest_path.read_text(encoding="utf-8"))["passed"] is True


def test_write_artifact_release_manifest_fails_when_required_kind_missing(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "qid_graph_index_manifest.json"
    artifact_path.write_text("{}", encoding="utf-8")

    manifest = write_artifact_release_manifest(
        [ArtifactDescriptor(kind="qid_graph", name="qid", path=artifact_path)],
        output_path=tmp_path / "release_manifest.json",
        require_kinds=["qid_graph", "qdrant_vector"],
    )

    assert manifest["passed"] is False
    assert manifest["missing_required_kinds"] == ["qdrant_vector"]
