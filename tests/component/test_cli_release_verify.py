import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from tests.release_helpers import (
    build_fake_release_runner,
    create_release_project,
    patch_release_runner,
)


def test_cli_release_verify_reports_smoke_install_results(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())
    runner = CliRunner()

    verify_result = runner.invoke(
        app,
        ["release", "verify", "--output-dir", str(tmp_path / "dist")],
    )

    assert verify_result.exit_code == 0
    verify_payload = json.loads(verify_result.stdout)
    assert verify_payload["overall_success"] is True
    assert verify_payload["smoke_install"] is True
    assert verify_payload["python_install_smoke"]["passed"] is True
    assert verify_payload["python_install_smoke"]["pull"]["passed"] is True
    assert verify_payload["python_install_smoke"]["tag"]["passed"] is True
    assert verify_payload["python_install_smoke"]["recovery_status"]["passed"] is True
    assert verify_payload["python_install_smoke"]["serve"]["passed"] is True
    assert verify_payload["python_install_smoke"]["serve_healthz"]["passed"] is True
    assert verify_payload["python_install_smoke"]["serve_status"]["passed"] is True
    assert verify_payload["python_install_smoke"]["serve_tag"]["passed"] is True
    assert verify_payload["python_install_smoke"]["serve_tag_file"]["passed"] is True
    assert verify_payload["python_install_smoke"]["serve_tag_files"]["passed"] is True
    assert {"general-en", "finance-en"} <= set(
        verify_payload["python_install_smoke"]["pulled_pack_ids"]
    )
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        verify_payload["python_install_smoke"]["tagged_labels"]
    )
    assert {"general-en", "finance-en"} <= set(
        verify_payload["python_install_smoke"]["recovered_pack_ids"]
    )
    assert {"general-en", "finance-en"} <= set(
        verify_payload["python_install_smoke"]["served_pack_ids"]
    )
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        verify_payload["python_install_smoke"]["serve_tagged_labels"]
    )
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        verify_payload["python_install_smoke"]["serve_tag_file_labels"]
    )
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        verify_payload["python_install_smoke"]["serve_tag_files_labels"]
    )
    python_batch_payload = json.loads(
        verify_payload["python_install_smoke"]["serve_tag_files"]["stdout"]
    )
    assert str(python_batch_payload["saved_manifest_path"]).endswith(
        "batch.finance-en.ades-manifest.json"
    )
    assert len(
        [
            item["saved_output_path"]
            for item in python_batch_payload["items"]
            if item.get("saved_output_path")
        ]
    ) == 2
    assert verify_payload["npm_install_smoke"]["passed"] is True
    assert verify_payload["npm_install_smoke"]["pull"]["passed"] is True
    assert verify_payload["npm_install_smoke"]["tag"]["passed"] is True
    assert verify_payload["npm_install_smoke"]["recovery_status"]["passed"] is True
    assert verify_payload["npm_install_smoke"]["serve"]["passed"] is True
    assert verify_payload["npm_install_smoke"]["serve_healthz"]["passed"] is True
    assert verify_payload["npm_install_smoke"]["serve_status"]["passed"] is True
    assert verify_payload["npm_install_smoke"]["serve_tag"]["passed"] is True
    assert verify_payload["npm_install_smoke"]["serve_tag_file"]["passed"] is True
    assert verify_payload["npm_install_smoke"]["serve_tag_files"]["passed"] is True
    assert {"general-en", "finance-en"} <= set(
        verify_payload["npm_install_smoke"]["pulled_pack_ids"]
    )
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        verify_payload["npm_install_smoke"]["tagged_labels"]
    )
    assert {"general-en", "finance-en"} <= set(
        verify_payload["npm_install_smoke"]["recovered_pack_ids"]
    )
    assert {"general-en", "finance-en"} <= set(
        verify_payload["npm_install_smoke"]["served_pack_ids"]
    )
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        verify_payload["npm_install_smoke"]["serve_tagged_labels"]
    )
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        verify_payload["npm_install_smoke"]["serve_tag_file_labels"]
    )
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        verify_payload["npm_install_smoke"]["serve_tag_files_labels"]
    )
    npm_batch_payload = json.loads(
        verify_payload["npm_install_smoke"]["serve_tag_files"]["stdout"]
    )
    assert str(npm_batch_payload["saved_manifest_path"]).endswith(
        "batch.finance-en.ades-manifest.json"
    )
    assert len(
        [item["saved_output_path"] for item in npm_batch_payload["items"] if item.get("saved_output_path")]
    ) == 2


def test_cli_release_commands_sync_versions_and_write_manifest(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(
        tmp_path / "repo",
        version_file_version="0.1.0",
        pyproject_version="0.0.9",
        npm_version="0.0.8",
    )
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(
        monkeypatch,
        build_fake_release_runner(python_version="0.2.0", npm_version="0.2.0"),
    )
    runner = CliRunner()

    sync_result = runner.invoke(app, ["release", "sync-version", "0.2.0"])
    assert sync_result.exit_code == 0
    sync_payload = json.loads(sync_result.stdout)
    assert sync_payload["target_version"] == "0.2.0"
    assert sync_payload["version_state"]["synchronized"] is True

    manifest_result = runner.invoke(
        app,
        [
            "release",
            "manifest",
            "--output-dir",
            str(tmp_path / "dist"),
            "--no-smoke-install",
        ],
    )
    assert manifest_result.exit_code == 0
    manifest_payload = json.loads(manifest_result.stdout)
    assert manifest_payload["release_version"] == "0.2.0"
    assert manifest_payload["verification"]["overall_success"] is True
    assert manifest_payload["verification"]["smoke_install"] is False
    assert manifest_payload["verification"]["python_install_smoke"] is None
    assert manifest_payload["verification"]["npm_install_smoke"] is None
    assert manifest_payload["verification"]["wheel"]["file_name"].endswith(".whl")
    assert Path(manifest_payload["manifest_path"]).exists()
