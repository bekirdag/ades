from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app


class _TagResponse:
    def model_dump(self, *, mode: str = "json") -> dict[str, object]:
        return {
            "version": "0.1.0",
            "pack": "general-en",
            "pack_version": "0.2.0",
            "language": "en",
            "content_type": "text/plain",
            "entities": [],
            "topics": [],
            "warnings": [],
            "timing_ms": 1,
        }


class _BatchResponse:
    def model_dump(self, *, mode: str = "json") -> dict[str, object]:
        return {
            "pack": "general-en",
            "item_count": 0,
            "summary": {
                "explicit_path_count": 0,
                "directory_match_count": 0,
                "glob_match_count": 0,
                "discovered_count": 0,
                "included_count": 0,
                "processed_count": 0,
                "excluded_count": 0,
                "skipped_count": 0,
                "rejected_count": 0,
                "duplicate_count": 0,
                "generated_output_skipped_count": 0,
                "discovered_input_bytes": 0,
                "included_input_bytes": 0,
                "processed_input_bytes": 0,
                "recursive": True,
            },
            "items": [],
            "warnings": [],
        }


def test_cli_tag_forwards_vector_flags(monkeypatch) -> None:
    runner = CliRunner()
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "ades.cli._resolve_tag_pack_or_exit",
        lambda pack, *, manifest_input=None: "general-en",
    )

    def _fake_tag_text_response(
        text: str,
        *,
        pack: str,
        content_type: str,
        output_path,
        output_dir,
        pretty_output: bool,
        include_related_entities: bool = False,
        include_graph_support: bool = False,
        refine_links: bool = False,
        refinement_depth: str = "light",
    ):
        captured.update(
            {
                "text": text,
                "pack": pack,
                "include_related_entities": include_related_entities,
                "include_graph_support": include_graph_support,
                "refine_links": refine_links,
                "refinement_depth": refinement_depth,
            }
        )
        return _TagResponse()

    monkeypatch.setattr("ades.cli._tag_text_response", _fake_tag_text_response)

    result = runner.invoke(
        app,
        [
            "tag",
            "Entity Alpha Holdings moved.",
            "--pack",
            "general-en",
            "--include-related-entities",
            "--include-graph-support",
            "--refine-links",
            "--refinement-depth",
            "deep",
        ],
    )

    assert result.exit_code == 0
    assert captured["include_related_entities"] is True
    assert captured["include_graph_support"] is True
    assert captured["refine_links"] is True
    assert captured["refinement_depth"] == "deep"


def test_cli_tag_files_forwards_vector_flags(monkeypatch, tmp_path: Path) -> None:
    runner = CliRunner()
    captured: dict[str, object] = {}
    sample = tmp_path / "sample.txt"
    sample.write_text("Entity Alpha Holdings moved.", encoding="utf-8")

    monkeypatch.setattr(
        "ades.cli._resolve_tag_pack_or_exit",
        lambda pack, *, manifest_input=None: "general-en",
    )

    def _fake_tag_files_response(
        files,
        *,
        pack,
        content_type,
        output_dir,
        pretty_output,
        directories,
        glob_patterns,
        manifest_input,
        manifest_mode,
        skip_unchanged,
        reuse_unchanged_outputs,
        repair_missing_reused_outputs,
        recursive,
        include_patterns,
        exclude_patterns,
        max_files,
        max_input_bytes,
        write_manifest,
        manifest_output,
        include_related_entities: bool = False,
        include_graph_support: bool = False,
        refine_links: bool = False,
        refinement_depth: str = "light",
    ):
        captured.update(
            {
                "include_related_entities": include_related_entities,
                "include_graph_support": include_graph_support,
                "refine_links": refine_links,
                "refinement_depth": refinement_depth,
            }
        )
        return _BatchResponse()

    monkeypatch.setattr("ades.cli._tag_files_response", _fake_tag_files_response)

    result = runner.invoke(
        app,
        [
            "tag-files",
            str(sample),
            "--pack",
            "general-en",
            "--include-related-entities",
            "--include-graph-support",
            "--refine-links",
            "--refinement-depth",
            "deep",
        ],
    )

    assert result.exit_code == 0
    assert captured["include_related_entities"] is True
    assert captured["include_graph_support"] is True
    assert captured["refine_links"] is True
    assert captured["refinement_depth"] == "deep"
