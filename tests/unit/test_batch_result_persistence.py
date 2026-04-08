import json
from pathlib import Path

from ades.service.models import (
    BatchManifestItem,
    BatchSourceSummary,
    BatchTagResponse,
    SourceFingerprint,
    TagResponse,
)
from ades.storage.results import (
    aggregate_batch_warnings,
    persist_batch_tag_manifest_json,
    persist_tag_responses_json,
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
    assert payload["reused_items"][0]["source_path"] == "/tmp/alpha/report.html"
