import json
from pathlib import Path

from ades.service.models import TagResponse
from ades.storage.results import build_tag_output_path, persist_tag_response_json


def test_build_and_persist_tag_output_paths(tmp_path: Path) -> None:
    output_path = build_tag_output_path(
        pack_id="finance-en",
        output_dir=tmp_path,
        source_path="/tmp/report.html",
    )
    assert output_path == tmp_path.resolve() / "report.finance-en.ades.json"

    response = TagResponse(
        version="0.1.0",
        pack="finance-en",
        language="en",
        content_type="text/html",
        source_path="/tmp/report.html",
        entities=[],
        topics=[],
        warnings=[],
        timing_ms=1,
    )
    persisted = persist_tag_response_json(
        response,
        pack_id="finance-en",
        output_dir=tmp_path,
    )

    assert persisted.saved_output_path == str(output_path)
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["pack"] == "finance-en"
    assert payload["saved_output_path"] == str(output_path)
