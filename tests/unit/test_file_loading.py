from pathlib import Path

from ades.pipeline.files import infer_content_type, load_tag_file


def test_infer_content_type_and_load_tag_file(tmp_path: Path) -> None:
    html_path = tmp_path / "report.html"
    html_path.write_text("<p>Apple on NASDAQ</p>", encoding="utf-8")

    assert infer_content_type(html_path) == "text/html"

    resolved_path, text, content_type = load_tag_file(html_path)

    assert resolved_path == html_path.resolve()
    assert text == "<p>Apple on NASDAQ</p>"
    assert content_type == "text/html"
