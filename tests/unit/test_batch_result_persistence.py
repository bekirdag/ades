import json
from pathlib import Path

from ades.service.models import TagResponse
from ades.storage.results import persist_tag_responses_json


def _response(source_path: str) -> TagResponse:
    return TagResponse(
        version="0.1.0",
        pack="finance-en",
        language="en",
        content_type="text/html",
        source_path=source_path,
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
