from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app


def test_cli_status_reports_missing_explicit_config(tmp_path: Path) -> None:
    runner = CliRunner()
    missing_config = tmp_path / "missing.toml"

    result = runner.invoke(
        app,
        ["status"],
        env={"ADES_CONFIG_FILE": str(missing_config)},
    )

    assert result.exit_code == 1
    assert "ades config file not found" in getattr(result, "stderr", result.output)


def test_cli_status_reports_invalid_runtime_target(tmp_path: Path) -> None:
    runner = CliRunner()
    config_path = tmp_path / "ades.toml"
    config_path.write_text("", encoding="utf-8")

    result = runner.invoke(
        app,
        ["status"],
        env={
            "ADES_CONFIG_FILE": str(config_path),
            "ADES_RUNTIME_TARGET": "broken-runtime",
        },
    )

    assert result.exit_code == 1
    assert "Unsupported ades runtime target" in getattr(result, "stderr", result.output)
