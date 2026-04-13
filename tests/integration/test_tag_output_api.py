import json
from pathlib import Path

from ades import pull_pack, tag


def test_public_api_can_persist_inline_tag_results(tmp_path: Path) -> None:
    pull_pack("finance-en", storage_root=tmp_path)
    output_dir = tmp_path / "outputs"

    response = tag(
        "Org Beta said TICKA traded on EXCHX.",
        pack="finance-en",
        storage_root=tmp_path,
        output_dir=output_dir,
    )

    saved_path = output_dir.resolve() / "ades.finance-en.ades.json"
    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert response.saved_output_path == str(saved_path)
    assert ("Org Beta", "organization") in pairs
    persisted = json.loads(saved_path.read_text(encoding="utf-8"))
    assert persisted["saved_output_path"] == str(saved_path)
    assert persisted["content_type"] == "text/plain"
