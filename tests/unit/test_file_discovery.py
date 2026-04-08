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
    assert discovery.summary.processed_count == 1
    assert discovery.summary.excluded_count == 2
    assert discovery.summary.skipped_count == 5
    assert discovery.summary.rejected_count == 0
    assert discovery.summary.limit_skipped_count == 0
    assert discovery.summary.duplicate_count == 2
    assert discovery.summary.generated_output_skipped_count == 1
    assert discovery.summary.discovered_input_bytes == (
        len("alpha".encode("utf-8"))
        + len("<p>Apple</p>".encode("utf-8"))
        + len("<p>NASDAQ</p>".encode("utf-8"))
    )
    assert discovery.summary.included_input_bytes == len("<p>Apple</p>".encode("utf-8"))
    assert discovery.summary.processed_input_bytes == len("<p>Apple</p>".encode("utf-8"))
    assert discovery.summary.include_patterns == ("*report.html",)
    assert discovery.summary.exclude_patterns == ("skip*",)
    skipped_reasons = {(entry.reference, entry.reason) for entry in discovery.skipped}
    assert (str(generated_output.resolve()), "generated_output") in skipped_reasons
    assert (str(keep_path.resolve()), "duplicate") in skipped_reasons
    assert (str(explicit_path.resolve()), "include_filter_miss") in skipped_reasons
    assert (str(drop_path.resolve()), "exclude_filter_match") in skipped_reasons


def test_discover_tag_file_sources_reports_rejected_inputs_and_empty_results(tmp_path: Path) -> None:
    missing_file = tmp_path / "missing.html"
    missing_dir = tmp_path / "missing-dir"

    discovery = discover_tag_file_sources(
        paths=[missing_file],
        directories=[missing_dir],
        glob_patterns=[str(tmp_path / "*.html")],
    )

    assert discovery.paths == []
    assert discovery.summary.discovered_count == 0
    assert discovery.summary.rejected_count == 2
    assert discovery.summary.skipped_count == 1
    rejected_pairs = {(entry.reference, entry.reason) for entry in discovery.rejected}
    assert (str(missing_file.expanduser()), "file_not_found") in rejected_pairs
    assert (str(missing_dir.resolve()), "directory_not_found") in rejected_pairs
    skipped_pairs = {(entry.reference, entry.reason) for entry in discovery.skipped}
    assert (str(tmp_path / "*.html"), "glob_no_matches") in skipped_pairs


def test_discover_tag_file_sources_applies_max_files_guardrail_in_discovery_order(
    tmp_path: Path,
) -> None:
    first = tmp_path / "alpha.html"
    second = tmp_path / "beta.html"
    third = tmp_path / "gamma.html"
    first.write_text("<p>Apple</p>", encoding="utf-8")
    second.write_text("<p>NASDAQ</p>", encoding="utf-8")
    third.write_text("<p>AAPL</p>", encoding="utf-8")

    discovery = discover_tag_file_sources(
        paths=[first, second, third],
        max_files=1,
    )

    assert discovery.paths == [first.resolve()]
    assert discovery.summary.max_files == 1
    assert discovery.summary.included_count == 3
    assert discovery.summary.processed_count == 1
    assert discovery.summary.limit_skipped_count == 2
    assert discovery.summary.processed_input_bytes == len("<p>Apple</p>".encode("utf-8"))
    skipped_pairs = {(entry.reference, entry.reason) for entry in discovery.skipped}
    assert (str(second.resolve()), "max_files_limit") in skipped_pairs
    assert (str(third.resolve()), "max_files_limit") in skipped_pairs


def test_discover_tag_file_sources_applies_max_input_bytes_guardrail_in_discovery_order(
    tmp_path: Path,
) -> None:
    first = tmp_path / "alpha.html"
    second = tmp_path / "beta.html"
    third = tmp_path / "gamma.html"
    first.write_text("alpha", encoding="utf-8")
    second.write_text("bravo", encoding="utf-8")
    third.write_text("charlie", encoding="utf-8")

    discovery = discover_tag_file_sources(
        paths=[first, second, third],
        max_input_bytes=5,
    )

    assert discovery.paths == [first.resolve()]
    assert discovery.summary.max_input_bytes == 5
    assert discovery.summary.included_count == 3
    assert discovery.summary.processed_count == 1
    assert discovery.summary.limit_skipped_count == 2
    assert discovery.summary.processed_input_bytes == len("alpha".encode("utf-8"))
    skipped_pairs = {(entry.reference, entry.reason) for entry in discovery.skipped}
    assert (str(second.resolve()), "max_input_bytes_limit") in skipped_pairs
    assert (str(third.resolve()), "max_input_bytes_limit") in skipped_pairs
