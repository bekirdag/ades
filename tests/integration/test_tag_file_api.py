from pathlib import Path

from ades import pull_pack, tag_file


def test_public_api_can_tag_local_html_files(tmp_path: Path) -> None:
    pull_pack("finance-en", storage_root=tmp_path)
    input_path = tmp_path / "report.html"
    input_path.write_text("<p>Org Beta said TICKA traded on EXCHX.</p>", encoding="utf-8")

    response = tag_file(input_path, pack="finance-en", storage_root=tmp_path)
    pairs = {(entity.text, entity.label) for entity in response.entities}

    assert response.source_path == str(input_path.resolve())
    assert response.content_type == "text/html"
    assert ("Org Beta", "organization") in pairs
    assert ("TICKA", "ticker") in pairs
    assert ("EXCHX", "exchange") in pairs
