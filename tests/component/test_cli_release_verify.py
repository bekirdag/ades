import json
from pathlib import Path

from typer.testing import CliRunner

from ades.cli import app
from tests.release_helpers import (
    build_expected_batch_manifest_path,
    build_expected_batch_output_paths,
    build_expected_batch_source_paths,
    build_expected_batch_replay_manifest_path,
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
    assert verify_payload["python_install_smoke"]["serve_tag_files_replay"]["passed"] is True
    assert verify_payload["python_install_smoke"]["remove_guardrail"]["passed"] is False
    assert (
        verify_payload["python_install_smoke"]["remove_guardrail"]["stderr"]
        == "Cannot remove pack general-en while installed dependent packs exist: finance-en"
    )
    assert verify_payload["python_install_smoke"]["remove"]["passed"] is True
    assert verify_payload["python_install_smoke"]["remove_status"]["passed"] is True
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
    assert verify_payload["python_install_smoke"]["remaining_pack_ids"] == ["general-en"]
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        verify_payload["python_install_smoke"]["serve_tagged_labels"]
    )
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        verify_payload["python_install_smoke"]["serve_tag_file_labels"]
    )
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        verify_payload["python_install_smoke"]["serve_tag_files_labels"]
    )
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        verify_payload["python_install_smoke"]["serve_tag_files_replay_labels"]
    )
    python_working_dir = Path(verify_payload["python_install_smoke"]["working_dir"])
    python_expected_source_paths = build_expected_batch_source_paths(python_working_dir)
    python_expected_output_paths = build_expected_batch_output_paths(python_working_dir)
    python_batch_payload = json.loads(
        verify_payload["python_install_smoke"]["serve_tag_files"]["stdout"]
    )
    assert (
        python_batch_payload["saved_manifest_path"]
        == build_expected_batch_manifest_path(python_working_dir)
    )
    assert python_batch_payload["pack"] == "finance-en"
    assert python_batch_payload["item_count"] == 2
    assert python_batch_payload["lineage"]["run_id"] == "ades-run-parent-smoke"
    assert python_batch_payload["lineage"]["root_run_id"] == "ades-run-parent-smoke"
    assert python_batch_payload["lineage"].get("parent_run_id") is None
    assert python_batch_payload["lineage"].get("source_manifest_path") is None
    assert python_batch_payload["lineage"]["created_at"] == "2026-04-09T14:57:00Z"
    assert [item["source_path"] for item in python_batch_payload["items"]] == (
        python_expected_source_paths
    )
    assert [item["saved_output_path"] for item in python_batch_payload["items"]] == (
        python_expected_output_paths
    )
    python_replay_payload = json.loads(
        verify_payload["python_install_smoke"]["serve_tag_files_replay"]["stdout"]
    )
    assert (
        python_replay_payload["saved_manifest_path"]
        == build_expected_batch_replay_manifest_path(python_working_dir)
    )
    assert python_replay_payload["pack"] == "finance-en"
    assert python_replay_payload["item_count"] == 2
    assert (
        python_replay_payload["summary"]["manifest_input_path"]
        == python_batch_payload["saved_manifest_path"]
    )
    assert (
        python_replay_payload["rerun_diff"]["manifest_input_path"]
        == python_batch_payload["saved_manifest_path"]
    )
    assert python_replay_payload["rerun_diff"]["changed"] == [
        item["source_path"] for item in python_batch_payload["items"]
    ]
    assert python_replay_payload["rerun_diff"]["newly_processed"] == []
    assert python_replay_payload["rerun_diff"]["reused"] == []
    assert python_replay_payload["rerun_diff"]["repaired"] == []
    assert python_replay_payload["rerun_diff"]["skipped"] == []
    assert python_replay_payload["summary"]["manifest_replay_mode"] == "processed"
    assert python_replay_payload["summary"]["manifest_candidate_count"] == 2
    assert python_replay_payload["summary"]["manifest_selected_count"] == 2
    assert (
        python_replay_payload["lineage"]["source_manifest_path"]
        == python_batch_payload["saved_manifest_path"]
    )
    assert python_replay_payload["lineage"]["run_id"] == "ades-run-replay-smoke"
    assert python_replay_payload["lineage"]["run_id"] != python_batch_payload["lineage"]["run_id"]
    assert python_replay_payload["lineage"]["root_run_id"] == "ades-run-parent-smoke"
    assert python_replay_payload["lineage"]["parent_run_id"] == "ades-run-parent-smoke"
    assert python_replay_payload["lineage"]["created_at"] == "2026-04-09T14:57:01Z"
    assert (
        python_replay_payload["lineage"]["created_at"]
        > python_batch_payload["lineage"]["created_at"]
    )
    assert [item["source_path"] for item in python_replay_payload["items"]] == (
        python_expected_source_paths
    )
    assert [item["saved_output_path"] for item in python_replay_payload["items"]] == (
        python_expected_output_paths
    )
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
    assert verify_payload["npm_install_smoke"]["serve_tag_files_replay"]["passed"] is True
    assert verify_payload["npm_install_smoke"]["remove_guardrail"]["passed"] is False
    assert (
        verify_payload["npm_install_smoke"]["remove_guardrail"]["stderr"]
        == "Cannot remove pack general-en while installed dependent packs exist: finance-en"
    )
    assert verify_payload["npm_install_smoke"]["remove"]["passed"] is True
    assert verify_payload["npm_install_smoke"]["remove_status"]["passed"] is True
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
    assert verify_payload["npm_install_smoke"]["remaining_pack_ids"] == ["general-en"]
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        verify_payload["npm_install_smoke"]["serve_tagged_labels"]
    )
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        verify_payload["npm_install_smoke"]["serve_tag_file_labels"]
    )
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        verify_payload["npm_install_smoke"]["serve_tag_files_labels"]
    )
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        verify_payload["npm_install_smoke"]["serve_tag_files_replay_labels"]
    )
    npm_working_dir = Path(verify_payload["npm_install_smoke"]["working_dir"])
    npm_expected_source_paths = build_expected_batch_source_paths(npm_working_dir)
    npm_expected_output_paths = build_expected_batch_output_paths(npm_working_dir)
    npm_batch_payload = json.loads(
        verify_payload["npm_install_smoke"]["serve_tag_files"]["stdout"]
    )
    assert (
        npm_batch_payload["saved_manifest_path"]
        == build_expected_batch_manifest_path(npm_working_dir)
    )
    assert npm_batch_payload["pack"] == "finance-en"
    assert npm_batch_payload["item_count"] == 2
    assert npm_batch_payload["lineage"]["run_id"] == "ades-run-parent-smoke"
    assert npm_batch_payload["lineage"]["root_run_id"] == "ades-run-parent-smoke"
    assert npm_batch_payload["lineage"].get("parent_run_id") is None
    assert npm_batch_payload["lineage"].get("source_manifest_path") is None
    assert npm_batch_payload["lineage"]["created_at"] == "2026-04-09T14:57:00Z"
    assert [item["source_path"] for item in npm_batch_payload["items"]] == (
        npm_expected_source_paths
    )
    assert [item["saved_output_path"] for item in npm_batch_payload["items"]] == (
        npm_expected_output_paths
    )
    npm_replay_payload = json.loads(
        verify_payload["npm_install_smoke"]["serve_tag_files_replay"]["stdout"]
    )
    assert (
        npm_replay_payload["saved_manifest_path"]
        == build_expected_batch_replay_manifest_path(npm_working_dir)
    )
    assert npm_replay_payload["pack"] == "finance-en"
    assert npm_replay_payload["item_count"] == 2
    assert (
        npm_replay_payload["summary"]["manifest_input_path"]
        == npm_batch_payload["saved_manifest_path"]
    )
    assert (
        npm_replay_payload["rerun_diff"]["manifest_input_path"]
        == npm_batch_payload["saved_manifest_path"]
    )
    assert npm_replay_payload["rerun_diff"]["changed"] == [
        item["source_path"] for item in npm_batch_payload["items"]
    ]
    assert npm_replay_payload["rerun_diff"]["newly_processed"] == []
    assert npm_replay_payload["rerun_diff"]["reused"] == []
    assert npm_replay_payload["rerun_diff"]["repaired"] == []
    assert npm_replay_payload["rerun_diff"]["skipped"] == []
    assert npm_replay_payload["summary"]["manifest_replay_mode"] == "processed"
    assert npm_replay_payload["summary"]["manifest_candidate_count"] == 2
    assert npm_replay_payload["summary"]["manifest_selected_count"] == 2
    assert (
        npm_replay_payload["lineage"]["source_manifest_path"]
        == npm_batch_payload["saved_manifest_path"]
    )
    assert npm_replay_payload["lineage"]["run_id"] == "ades-run-replay-smoke"
    assert npm_replay_payload["lineage"]["run_id"] != npm_batch_payload["lineage"]["run_id"]
    assert npm_replay_payload["lineage"]["root_run_id"] == "ades-run-parent-smoke"
    assert npm_replay_payload["lineage"]["parent_run_id"] == "ades-run-parent-smoke"
    assert npm_replay_payload["lineage"]["created_at"] == "2026-04-09T14:57:01Z"
    assert (
        npm_replay_payload["lineage"]["created_at"]
        > npm_batch_payload["lineage"]["created_at"]
    )
    assert [item["source_path"] for item in npm_replay_payload["items"]] == (
        npm_expected_source_paths
    )
    assert [item["saved_output_path"] for item in npm_replay_payload["items"]] == (
        npm_expected_output_paths
    )


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


def test_cli_release_verify_reports_missing_release_pyproject(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())
    missing_pyproject = (project_root / "pyproject.toml").resolve()
    missing_pyproject.unlink()
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["release", "verify", "--output-dir", str(tmp_path / "dist")],
    )

    assert result.exit_code == 1
    assert result.stdout == ""
    assert result.stderr == f"Release coordination requires {missing_pyproject}.\n"


def test_cli_release_verify_reports_invalid_version_file(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())
    (project_root / "src" / "ades" / "version.py").write_text(
        '"""Package version."""\n\nVERSION = "broken"\n',
        encoding="utf-8",
    )
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["release", "verify", "--output-dir", str(tmp_path / "dist")],
    )

    assert result.exit_code == 1
    assert result.stdout == ""
    assert result.stderr == "Could not parse __version__ from src/ades/version.py.\n"


def test_cli_release_manifest_reports_missing_release_pyproject(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())
    missing_pyproject = (project_root / "pyproject.toml").resolve()
    missing_pyproject.unlink()
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "release",
            "manifest",
            "--output-dir",
            str(tmp_path / "dist"),
            "--no-smoke-install",
        ],
    )

    assert result.exit_code == 1
    assert result.stdout == ""
    assert result.stderr == f"Release coordination requires {missing_pyproject}.\n"


def test_cli_release_manifest_reports_invalid_version_file(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())
    (project_root / "src" / "ades" / "version.py").write_text(
        '"""Package version."""\n\nVERSION = "broken"\n',
        encoding="utf-8",
    )
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "release",
            "manifest",
            "--output-dir",
            str(tmp_path / "dist"),
            "--no-smoke-install",
        ],
    )

    assert result.exit_code == 1
    assert result.stdout == ""
    assert result.stderr == "Could not parse __version__ from src/ades/version.py.\n"


def test_cli_release_validate_reports_missing_release_pyproject(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())
    missing_pyproject = (project_root / "pyproject.toml").resolve()
    missing_pyproject.unlink()
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "release",
            "validate",
            "--output-dir",
            str(tmp_path / "dist"),
            "--no-smoke-install",
        ],
    )

    assert result.exit_code == 1
    assert result.stdout == ""
    assert result.stderr == f"Release coordination requires {missing_pyproject}.\n"


def test_cli_release_validate_reports_invalid_version_file(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())
    (project_root / "src" / "ades" / "version.py").write_text(
        '"""Package version."""\n\nVERSION = "broken"\n',
        encoding="utf-8",
    )
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "release",
            "validate",
            "--output-dir",
            str(tmp_path / "dist"),
            "--no-smoke-install",
        ],
    )

    assert result.exit_code == 1
    assert result.stdout == ""
    assert result.stderr == "Could not parse __version__ from src/ades/version.py.\n"


def test_cli_release_versions_reports_missing_release_pyproject(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    missing_pyproject = (project_root / "pyproject.toml").resolve()
    missing_pyproject.unlink()
    runner = CliRunner()

    result = runner.invoke(app, ["release", "versions"])

    assert result.exit_code == 1
    assert result.stdout == ""
    assert result.stderr == f"Release coordination requires {missing_pyproject}.\n"


def test_cli_release_versions_reports_invalid_version_file(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    (project_root / "src" / "ades" / "version.py").write_text(
        '"""Package version."""\n\nVERSION = "broken"\n',
        encoding="utf-8",
    )
    runner = CliRunner()

    result = runner.invoke(app, ["release", "versions"])

    assert result.exit_code == 1
    assert result.stdout == ""
    assert result.stderr == "Could not parse __version__ from src/ades/version.py.\n"


def test_cli_release_sync_version_reports_missing_release_pyproject(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    missing_pyproject = (project_root / "pyproject.toml").resolve()
    missing_pyproject.unlink()
    runner = CliRunner()

    result = runner.invoke(app, ["release", "sync-version", "0.2.0"])

    assert result.exit_code == 1
    assert result.stdout == ""
    assert result.stderr == f"Release coordination requires {missing_pyproject}.\n"


def test_cli_release_sync_version_reports_invalid_version_file(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    (project_root / "src" / "ades" / "version.py").write_text(
        '"""Package version."""\n\nVERSION = "broken"\n',
        encoding="utf-8",
    )
    runner = CliRunner()

    result = runner.invoke(app, ["release", "sync-version", "0.2.0"])

    assert result.exit_code == 1
    assert result.stdout == ""
    assert result.stderr == "Could not update __version__ in src/ades/version.py.\n"
