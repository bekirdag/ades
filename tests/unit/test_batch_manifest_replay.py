import json
from pathlib import Path

from ades.service.models import (
    BatchManifest,
    BatchManifestItem,
    BatchSkippedInput,
    BatchSourceSummary,
)
from ades.storage.results import build_batch_manifest_replay_plan


def _write_manifest(path: Path) -> Path:
    manifest = BatchManifest(
        version="0.1.0",
        pack="finance-en",
        item_count=1,
        summary=BatchSourceSummary(
            explicit_path_count=2,
            directory_match_count=0,
            glob_match_count=0,
            discovered_count=2,
            included_count=2,
            processed_count=1,
            excluded_count=0,
            skipped_count=1,
            rejected_count=0,
            duplicate_count=0,
            generated_output_skipped_count=0,
            discovered_input_bytes=10,
            included_input_bytes=10,
            processed_input_bytes=5,
            recursive=True,
        ),
        skipped=[
            BatchSkippedInput(
                reference="/tmp/beta.html",
                reason="max_files_limit",
                source_kind="limit",
                size_bytes=5,
            )
        ],
        rejected=[],
        reused_items=[
            BatchManifestItem(
                source_path="/tmp/reused.html",
                saved_output_path="/tmp/reused.finance-en.ades.json",
                content_type="text/html",
                input_size_bytes=7,
                warning_count=1,
                warnings=["empty_input"],
                entity_count=0,
                topic_count=0,
                timing_ms=1,
            )
        ],
        items=[
            BatchManifestItem(
                source_path="/tmp/alpha.html",
                saved_output_path=None,
                content_type="text/html",
                input_size_bytes=5,
                warning_count=0,
                warnings=[],
                entity_count=1,
                topic_count=0,
                timing_ms=1,
            )
        ],
    )
    path.write_text(
        json.dumps(manifest.model_dump(mode="json"), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def test_build_batch_manifest_replay_plan_supports_resume_and_all_modes(tmp_path: Path) -> None:
    manifest_path = _write_manifest(tmp_path / "batch.finance-en.ades-manifest.json")

    resume_plan = build_batch_manifest_replay_plan(manifest_path, mode="resume")
    assert resume_plan.candidate_count == 1
    assert resume_plan.selected_count == 1
    assert resume_plan.paths == [Path("/tmp/beta.html").resolve()]

    all_plan = build_batch_manifest_replay_plan(manifest_path, mode="all")
    assert all_plan.candidate_count == 3
    assert all_plan.selected_count == 3
    assert all_plan.paths == [
        Path("/tmp/alpha.html").resolve(),
        Path("/tmp/reused.html").resolve(),
        Path("/tmp/beta.html").resolve(),
    ]
    assert all_plan.content_type_overrides[Path("/tmp/alpha.html").resolve()] == "text/html"
    assert all_plan.item_index[Path("/tmp/reused.html").resolve()].saved_output_path == "/tmp/reused.finance-en.ades.json"
