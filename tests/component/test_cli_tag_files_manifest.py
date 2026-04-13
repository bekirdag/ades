import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from ades.packs.installer import PackInstaller


def test_cli_tag_files_can_write_manifest_and_aggregate_warnings(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    alpha_dir = tmp_path / "alpha"
    beta_dir = tmp_path / "beta"
    alpha_dir.mkdir()
    beta_dir.mkdir()
    first_input = alpha_dir / "report.html"
    second_input = beta_dir / "report.html"
    first_input.write_text("<p>Org Beta said TICKA traded on EXCHX.</p>", encoding="utf-8")
    second_input.write_text("<p>EXCHX said Org Beta moved TICKA guidance.</p>", encoding="utf-8")
    output_dir = tmp_path / "outputs"

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "tag-files",
            str(first_input),
            str(second_input),
            "--pack",
            "finance-en",
            "--content-type",
            "application/json",
            "--output-dir",
            str(output_dir),
            "--write-manifest",
        ],
        env={"ADES_STORAGE_ROOT": str(tmp_path)},
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    manifest_path = output_dir.resolve() / "batch.finance-en.ades-manifest.json"
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert payload["saved_manifest_path"] == str(manifest_path)
    assert payload["warnings"] == ["unsupported_content_type:application/json"]
    assert manifest_payload["warnings"] == ["unsupported_content_type:application/json"]
    assert manifest_payload["items"][0]["saved_output_path"] == str(
        output_dir.resolve() / "report.finance-en.ades.json"
    )
