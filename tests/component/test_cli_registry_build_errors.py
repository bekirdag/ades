from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app


def test_cli_registry_build_reports_missing_pack_directory(tmp_path: Path) -> None:
    runner = CliRunner()
    missing_pack_dir = tmp_path / "missing-pack"

    result = runner.invoke(
        app,
        [
            "registry",
            "build",
            str(missing_pack_dir),
            "--output-dir",
            str(tmp_path / "registry"),
        ],
    )

    assert result.exit_code == 1
    assert result.stdout == ""
    assert result.stderr == f"Pack directory not found: {missing_pack_dir.resolve()}\n"


def test_cli_registry_build_reports_non_directory_pack_input(tmp_path: Path) -> None:
    runner = CliRunner()
    pack_file = tmp_path / "not-a-pack.txt"
    pack_file.write_text("not a pack directory", encoding="utf-8")

    result = runner.invoke(
        app,
        [
            "registry",
            "build",
            str(pack_file),
            "--output-dir",
            str(tmp_path / "registry"),
        ],
    )

    assert result.exit_code == 1
    assert result.stdout == ""
    assert result.stderr == f"Pack directory is not a directory: {pack_file.resolve()}\n"
