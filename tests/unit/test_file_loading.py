from pathlib import Path

from ades.pipeline.files import infer_content_type, load_tag_file


def test_infer_content_type_and_load_tag_file(tmp_path: Path) -> None:
    html_path = tmp_path / "report.html"
    html_path.write_text("<p>Org Beta on EXCHX</p>", encoding="utf-8")

    assert infer_content_type(html_path) == "text/html"

    resolved_path, text, content_type, input_size_bytes, source_fingerprint = load_tag_file(
        html_path
    )

    assert resolved_path == html_path.resolve()
    assert text == "<p>Org Beta on EXCHX</p>"
    assert content_type == "text/html"
    assert input_size_bytes == len("<p>Org Beta on EXCHX</p>".encode("utf-8"))
    assert source_fingerprint.size_bytes == len("<p>Org Beta on EXCHX</p>".encode("utf-8"))
    assert source_fingerprint.modified_time_ns > 0
    assert len(source_fingerprint.sha256) == 64
