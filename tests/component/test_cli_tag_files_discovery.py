import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from ades.packs.installer import PackInstaller


def test_cli_tag_files_supports_directory_and_glob_discovery(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    corpus_dir = tmp_path / "corpus"
    nested_dir = corpus_dir / "nested"
    glob_dir = tmp_path / "globbed"
    nested_dir.mkdir(parents=True)
    glob_dir.mkdir()

    nested_input = nested_dir / "report.html"
    globbed_input = glob_dir / "bulletin.html"
    nested_input.write_text("<p>Org Beta said TICKA traded on EXCHX.</p>", encoding="utf-8")
    globbed_input.write_text("<p>EXCHX said Org Beta moved TICKA guidance.</p>", encoding="utf-8")
    output_dir = tmp_path / "outputs"

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "tag-files",
            "--directory",
            str(corpus_dir),
            "--glob",
            str(glob_dir / "*.html"),
            "--pack",
            "finance-en",
            "--output-dir",
            str(output_dir),
        ],
        env={"ADES_STORAGE_ROOT": str(tmp_path)},
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    source_paths = {item["source_path"] for item in payload["items"]}

    assert payload["pack"] == "finance-en"
    assert payload["item_count"] == 2
    assert source_paths == {
        str(nested_input.resolve()),
        str(globbed_input.resolve()),
    }
