from pathlib import Path

from ades.pipeline.files import resolve_tag_file_sources


def test_resolve_tag_file_sources_supports_directories_and_globs(tmp_path: Path) -> None:
    corpus_dir = tmp_path / "corpus"
    nested_dir = corpus_dir / "nested"
    glob_dir = tmp_path / "globbed"
    nested_dir.mkdir(parents=True)
    glob_dir.mkdir()

    explicit_path = tmp_path / "explicit.html"
    nested_path = nested_dir / "report.html"
    globbed_path = glob_dir / "bulletin.html"
    generated_output = corpus_dir / "report.finance-en.ades.json"

    explicit_path.write_text("<p>Apple</p>", encoding="utf-8")
    nested_path.write_text("<p>NASDAQ</p>", encoding="utf-8")
    globbed_path.write_text("<p>AAPL</p>", encoding="utf-8")
    generated_output.write_text("{}", encoding="utf-8")

    resolved = resolve_tag_file_sources(
        paths=[explicit_path, globbed_path],
        directories=[corpus_dir],
        glob_patterns=[str(glob_dir / "*.html")],
    )

    assert resolved == [
        explicit_path.resolve(),
        globbed_path.resolve(),
        nested_path.resolve(),
    ]
