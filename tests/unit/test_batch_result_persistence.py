import json
from pathlib import Path

from ades.service.models import (
    BatchManifest,
    BatchManifestItem,
    BatchRunLineage,
    BatchRerunDiff,
    BatchSkippedInput,
    BatchSourceSummary,
    BatchTagResponse,
    SourceFingerprint,
    TagResponse,
)
from ades.storage.results import (
    aggregate_batch_warnings,
    build_batch_manifest_item_from_tag_response,
    build_batch_run_lineage,
    persist_batch_tag_manifest_json,
    persist_tag_responses_json,
    verify_reused_output_items,
)


def _response(source_path: str) -> TagResponse:
    return TagResponse(
        version="0.1.0",
        pack="finance-en",
        language="en",
        content_type="text/html",
        source_path=source_path,
        input_size_bytes=12,
        source_fingerprint=SourceFingerprint(
            size_bytes=12,
            modified_time_ns=123,
            sha256="a" * 64,
        ),
        entities=[],
        topics=[],
        warnings=[],
        timing_ms=1,
    )


def test_persist_tag_responses_json_deduplicates_conflicting_filenames(tmp_path: Path) -> None:
    responses = [
        _response("/tmp/alpha/report.html"),
        _response("/tmp/beta/report.html"),
    ]

    persisted = persist_tag_responses_json(
        responses,
        pack_id="finance-en",
        output_dir=tmp_path,
    )

    first_path = tmp_path.resolve() / "report.finance-en.ades.json"
    second_path = tmp_path.resolve() / "report.finance-en.ades-2.json"

    assert [item.saved_output_path for item in persisted] == [str(first_path), str(second_path)]
    first_payload = json.loads(first_path.read_text(encoding="utf-8"))
    second_payload = json.loads(second_path.read_text(encoding="utf-8"))
    assert first_payload["saved_output_path"] == str(first_path)
    assert second_payload["saved_output_path"] == str(second_path)


def test_persist_batch_manifest_json_writes_aggregated_audit_artifact(tmp_path: Path) -> None:
    persisted_items = persist_tag_responses_json(
        [
            _response("/tmp/alpha/report.html").model_copy(
                update={"warnings": ["unsupported_content_type:application/json"]}
            ),
            _response("/tmp/beta/report.html").model_copy(
                update={"warnings": ["unsupported_content_type:application/json", "empty_input"]}
            ),
        ],
        pack_id="finance-en",
        output_dir=tmp_path / "outputs",
    )
    response = BatchTagResponse(
        pack="finance-en",
        item_count=2,
        summary=BatchSourceSummary(
            explicit_path_count=2,
            directory_match_count=0,
            glob_match_count=0,
            discovered_count=2,
            included_count=2,
            processed_count=2,
            excluded_count=0,
            skipped_count=0,
            rejected_count=0,
            duplicate_count=0,
            generated_output_skipped_count=0,
            discovered_input_bytes=24,
            included_input_bytes=24,
            processed_input_bytes=24,
            recursive=True,
        ),
        warnings=aggregate_batch_warnings(persisted_items),
        skipped=[],
        rejected=[],
        items=persisted_items,
    )

    persisted = persist_batch_tag_manifest_json(
        response,
        pack_id="finance-en",
        output_dir=tmp_path / "outputs",
    )

    manifest_path = tmp_path.resolve() / "outputs" / "batch.finance-en.ades-manifest.json"
    assert persisted.saved_manifest_path == str(manifest_path)
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert payload["pack"] == "finance-en"
    assert payload["warnings"] == ["empty_input", "unsupported_content_type:application/json"]
    assert payload["items"][0]["saved_output_path"] == str(
        tmp_path.resolve() / "outputs" / "report.finance-en.ades.json"
    )
    assert payload["items"][0]["saved_output_exists"] is True
    assert payload["items"][0]["input_size_bytes"] == 12
    assert payload["items"][0]["source_fingerprint"]["sha256"] == "a" * 64


def test_persist_batch_manifest_json_includes_reused_items(tmp_path: Path) -> None:
    reused_item = BatchManifestItem(
        source_path="/tmp/alpha/report.html",
        saved_output_path=str(tmp_path / "outputs" / "report.finance-en.ades.json"),
        content_type="text/html",
        input_size_bytes=12,
        source_fingerprint=SourceFingerprint(
            size_bytes=12,
            modified_time_ns=123,
            sha256="a" * 64,
        ),
        warning_count=0,
        warnings=[],
        entity_count=0,
        topic_count=0,
        timing_ms=1,
    )
    response = BatchTagResponse(
        pack="finance-en",
        item_count=0,
        summary=BatchSourceSummary(
            explicit_path_count=1,
            directory_match_count=0,
            glob_match_count=0,
            discovered_count=1,
            included_count=1,
            processed_count=0,
            excluded_count=0,
            skipped_count=1,
            rejected_count=0,
            unchanged_skipped_count=1,
            unchanged_reused_count=1,
            duplicate_count=0,
            generated_output_skipped_count=0,
            discovered_input_bytes=12,
            included_input_bytes=12,
            processed_input_bytes=0,
            recursive=True,
        ),
        warnings=aggregate_batch_warnings([], reused_items=[reused_item]),
        skipped=[],
        rejected=[],
        reused_items=[reused_item],
        items=[],
    )

    persisted = persist_batch_tag_manifest_json(
        response,
        pack_id="finance-en",
        output_dir=tmp_path / "outputs",
    )

    manifest_path = Path(persisted.saved_manifest_path)
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload["summary"]["unchanged_reused_count"] == 1
    assert payload["summary"]["reused_output_missing_count"] == 1
    assert payload["reused_items"][0]["source_path"] == "/tmp/alpha/report.html"
    assert payload["reused_items"][0]["saved_output_exists"] is False


def test_verify_reused_output_items_marks_missing_output_paths(tmp_path: Path) -> None:
    missing_output = tmp_path / "missing.finance-en.ades.json"
    reused_item = BatchManifestItem(
        source_path="/tmp/alpha/report.html",
        saved_output_path=str(missing_output),
        content_type="text/html",
        input_size_bytes=12,
        source_fingerprint=SourceFingerprint(
            size_bytes=12,
            modified_time_ns=123,
            sha256="a" * 64,
        ),
        warning_count=0,
        warnings=[],
        entity_count=0,
        topic_count=0,
        timing_ms=1,
    )

    verified_items, missing_count = verify_reused_output_items([reused_item])

    assert missing_count == 1
    assert verified_items[0].saved_output_exists is False
    assert verified_items[0].warnings == [f"reused_output_missing_file:{missing_output.resolve()}"]
    assert verified_items[0].warning_count == 1


def test_build_batch_manifest_item_from_tag_response_marks_repaired_output(tmp_path: Path) -> None:
    output_path = tmp_path / "stable.finance-en.ades.json"
    output_path.write_text("{}", encoding="utf-8")
    response = _response("/tmp/alpha/report.html").model_copy(
        update={"saved_output_path": str(output_path)}
    )

    item = build_batch_manifest_item_from_tag_response(
        response,
        extra_warnings=[f"repaired_reused_output:{output_path.resolve()}"],
    )

    assert item.saved_output_path == str(output_path.resolve())
    assert item.saved_output_exists is True
    assert item.warnings == [f"repaired_reused_output:{output_path.resolve()}"]
    assert item.warning_count == 1


def test_persist_batch_manifest_json_includes_rerun_diff(tmp_path: Path) -> None:
    persisted_items = persist_tag_responses_json(
        [_response("/tmp/changed/report.html")],
        pack_id="finance-en",
        output_dir=tmp_path / "outputs",
    )
    response = BatchTagResponse(
        pack="finance-en",
        item_count=1,
        summary=BatchSourceSummary(
            explicit_path_count=0,
            directory_match_count=1,
            glob_match_count=0,
            discovered_count=3,
            included_count=3,
            processed_count=1,
            excluded_count=0,
            skipped_count=2,
            rejected_count=0,
            limit_skipped_count=1,
            unchanged_skipped_count=1,
            unchanged_reused_count=1,
            repaired_reused_output_count=1,
            duplicate_count=0,
            generated_output_skipped_count=0,
            discovered_input_bytes=36,
            included_input_bytes=36,
            processed_input_bytes=12,
            manifest_input_path="/tmp/outputs/batch.finance-en.ades-manifest.json",
            manifest_replay_mode="resume",
            recursive=True,
        ),
        warnings=[],
        rerun_diff=BatchRerunDiff(
            manifest_input_path="/tmp/outputs/batch.finance-en.ades-manifest.json",
            changed=["/tmp/changed/report.html"],
            newly_processed=["/tmp/new/report.html"],
            reused=["/tmp/stable/report.html"],
            repaired=["/tmp/stable/report.html"],
            skipped=[
                BatchSkippedInput(
                    reference="/tmp/late/report.html",
                    reason="max_files_limit",
                    source_kind="limit",
                    size_bytes=12,
                )
            ],
        ),
        skipped=[
            BatchSkippedInput(
                reference="/tmp/stable/report.html",
                reason="unchanged_since_manifest",
                source_kind="fingerprint",
                size_bytes=12,
            ),
            BatchSkippedInput(
                reference="/tmp/late/report.html",
                reason="max_files_limit",
                source_kind="limit",
                size_bytes=12,
            ),
        ],
        rejected=[],
        items=persisted_items,
    )

    persisted = persist_batch_tag_manifest_json(
        response,
        pack_id="finance-en",
        output_dir=tmp_path / "outputs",
    )

    manifest_path = Path(persisted.saved_manifest_path)
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert payload["rerun_diff"]["manifest_input_path"] == "/tmp/outputs/batch.finance-en.ades-manifest.json"
    assert payload["rerun_diff"]["changed"] == ["/tmp/changed/report.html"]
    assert payload["rerun_diff"]["newly_processed"] == ["/tmp/new/report.html"]
    assert payload["rerun_diff"]["reused"] == ["/tmp/stable/report.html"]
    assert payload["rerun_diff"]["repaired"] == ["/tmp/stable/report.html"]
    assert payload["rerun_diff"]["skipped"] == [
        {
            "reference": "/tmp/late/report.html",
            "reason": "max_files_limit",
            "source_kind": "limit",
            "size_bytes": 12,
        }
    ]


def test_build_batch_run_lineage_creates_root_run_for_initial_batch() -> None:
    lineage = build_batch_run_lineage()

    assert lineage.run_id.startswith("ades-run-")
    assert lineage.root_run_id == lineage.run_id
    assert lineage.parent_run_id is None
    assert lineage.source_manifest_path is None
    assert lineage.created_at.tzinfo is not None


def test_build_batch_run_lineage_inherits_parent_chain(tmp_path: Path) -> None:
    parent_manifest = BatchManifest(
        version="0.1.0",
        pack="finance-en",
        item_count=0,
        summary=BatchSourceSummary(
            explicit_path_count=0,
            directory_match_count=0,
            glob_match_count=0,
            discovered_count=0,
            included_count=0,
            processed_count=0,
            excluded_count=0,
            skipped_count=0,
            rejected_count=0,
            duplicate_count=0,
            generated_output_skipped_count=0,
            discovered_input_bytes=0,
            included_input_bytes=0,
            processed_input_bytes=0,
            recursive=True,
        ),
        lineage=BatchRunLineage(
            run_id="ades-run-parent",
            root_run_id="ades-run-root",
            parent_run_id=None,
            source_manifest_path=None,
            created_at="2026-04-08T12:00:00Z",
        ),
        warnings=[],
        skipped=[],
        rejected=[],
        reused_items=[],
        items=[],
    )

    lineage = build_batch_run_lineage(
        parent_manifest=parent_manifest,
        source_manifest_path=tmp_path / "batch.finance-en.ades-manifest.json",
    )

    assert lineage.run_id.startswith("ades-run-")
    assert lineage.run_id != "ades-run-parent"
    assert lineage.parent_run_id == "ades-run-parent"
    assert lineage.root_run_id == "ades-run-root"
    assert lineage.source_manifest_path == str(
        (tmp_path / "batch.finance-en.ades-manifest.json").resolve()
    )


def test_persist_batch_manifest_json_includes_lineage(tmp_path: Path) -> None:
    response = BatchTagResponse(
        pack="finance-en",
        item_count=0,
        summary=BatchSourceSummary(
            explicit_path_count=0,
            directory_match_count=0,
            glob_match_count=0,
            discovered_count=0,
            included_count=0,
            processed_count=0,
            excluded_count=0,
            skipped_count=0,
            rejected_count=0,
            duplicate_count=0,
            generated_output_skipped_count=0,
            discovered_input_bytes=0,
            included_input_bytes=0,
            processed_input_bytes=0,
            recursive=True,
        ),
        lineage=BatchRunLineage(
            run_id="ades-run-child",
            root_run_id="ades-run-root",
            parent_run_id="ades-run-parent",
            source_manifest_path="/tmp/parent-manifest.json",
            created_at="2026-04-08T12:00:00Z",
        ),
        warnings=[],
        skipped=[],
        rejected=[],
        reused_items=[],
        items=[],
    )

    persisted = persist_batch_tag_manifest_json(
        response,
        pack_id="finance-en",
        output_dir=tmp_path / "outputs",
    )

    manifest_payload = json.loads(
        Path(persisted.saved_manifest_path).read_text(encoding="utf-8")
    )
    assert manifest_payload["lineage"] == {
        "created_at": "2026-04-08T12:00:00Z",
        "parent_run_id": "ades-run-parent",
        "root_run_id": "ades-run-root",
        "run_id": "ades-run-child",
        "source_manifest_path": "/tmp/parent-manifest.json",
    }
