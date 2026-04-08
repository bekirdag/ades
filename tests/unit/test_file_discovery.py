from pathlib import Path

from ades.pipeline.files import discover_tag_file_sources, resolve_tag_file_sources


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


def test_discover_tag_file_sources_applies_include_exclude_filters_and_tracks_summary(
    tmp_path: Path,
) -> None:
    corpus_dir = tmp_path / "corpus"
    nested_dir = corpus_dir / "nested"
    reports_dir = corpus_dir / "reports"
    nested_dir.mkdir(parents=True)
    reports_dir.mkdir()

    explicit_path = tmp_path / "explicit.txt"
    keep_path = reports_dir / "keep-report.html"
    drop_path = nested_dir / "skip-report.html"
    generated_output = reports_dir / "keep-report.finance-en.ades.json"

    explicit_path.write_text("alpha", encoding="utf-8")
    keep_path.write_text("<p>Apple</p>", encoding="utf-8")
    drop_path.write_text("<p>NASDAQ</p>", encoding="utf-8")
    generated_output.write_text("{}", encoding="utf-8")

    discovery = discover_tag_file_sources(
        paths=[explicit_path],
        directories=[corpus_dir],
        glob_patterns=[str(keep_path), str(reports_dir / "*.html")],
        include_patterns=["*report.html"],
        exclude_patterns=["skip*"],
    )

    assert discovery.paths == [keep_path.resolve()]
    assert discovery.summary.explicit_path_count == 1
    assert discovery.summary.directory_match_count == 2
    assert discovery.summary.glob_match_count == 2
    assert discovery.summary.discovered_count == 3
    assert discovery.summary.included_count == 1
    assert discovery.summary.excluded_count == 2
    assert discovery.summary.duplicate_count == 2
    assert discovery.summary.generated_output_skipped_count == 1
    assert discovery.summary.include_patterns == ("*report.html",)
    assert discovery.summary.exclude_patterns == ("skip*",)
