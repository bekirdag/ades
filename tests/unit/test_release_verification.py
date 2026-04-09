import json
from pathlib import Path

import pytest

from ades.release import (
    _parse_status_version,
    release_versions,
    sync_release_version,
    verify_release_artifacts,
    write_release_manifest,
)
from ades.service.models import ReleaseCommandResult
from ades.version import __version__
from tests.release_helpers import (
    build_failed_release_runner,
    build_fake_release_runner,
    create_release_project,
    patch_release_runner,
)


def _served_batch_manifest_payload(
    working_dir: Path,
    *,
    version: str = __version__,
    include_manifest_path: bool = True,
    manifest_file_name: str = "serve-smoke-batch-manifest.finance-en.ades-manifest.json",
    output_count: int = 2,
) -> dict[str, object]:
    """Return one deterministic live `/v0/tag/files` payload with manifest output paths."""

    output_dir = (working_dir / "serve-smoke-batch-outputs").resolve()
    items: list[dict[str, object]] = [
        {
            "version": version,
            "pack": "finance-en",
            "language": "en",
            "content_type": "text/html",
            "source_path": str((working_dir / "serve-smoke-batch-alpha.html").resolve()),
            "saved_output_path": str(
                (output_dir / "serve-smoke-batch-alpha.finance-en.ades.json").resolve()
            ),
            "entities": [{"label": "organization"}, {"label": "ticker"}],
        },
        {
            "version": version,
            "pack": "finance-en",
            "language": "en",
            "content_type": "text/html",
            "source_path": str((working_dir / "serve-smoke-batch-beta.html").resolve()),
            "saved_output_path": str(
                (output_dir / "serve-smoke-batch-beta.finance-en.ades.json").resolve()
            ),
            "entities": [{"label": "exchange"}, {"label": "currency_amount"}],
        },
    ]
    payload: dict[str, object] = {
        "pack": "finance-en",
        "item_count": len(items),
        "warnings": [],
        "items": items[:output_count],
    }
    if include_manifest_path:
        payload["saved_manifest_path"] = str((output_dir / manifest_file_name).resolve())
    return payload


def _served_batch_manifest_replay_payload(
    working_dir: Path,
    *,
    version: str = __version__,
    pack: str = "finance-en",
    include_manifest_input_path: bool = True,
    include_lineage_source_manifest_path: bool = True,
    include_manifest_candidate_count: bool = True,
    include_manifest_selected_count: bool = True,
    include_rerun_diff: bool = True,
    include_rerun_diff_manifest_input_path: bool = True,
    include_rerun_diff_changed: bool = True,
    include_rerun_diff_newly_processed: bool = True,
    include_rerun_diff_reused: bool = True,
    include_rerun_diff_repaired: bool = True,
    include_rerun_diff_skipped: bool = True,
    manifest_input_file_name: str = "serve-smoke-batch-manifest.finance-en.ades-manifest.json",
    manifest_file_name: str = "serve-smoke-batch-replay.finance-en.ades-manifest.json",
    rerun_diff_manifest_input_file_name: str | None = None,
    replay_mode: str = "processed",
    manifest_candidate_count: int = 2,
    manifest_selected_count: int = 2,
    changed_paths: list[str] | None = None,
    rerun_diff_newly_processed: list[str] | None = None,
    rerun_diff_reused: list[str] | None = None,
    rerun_diff_repaired: list[str] | None = None,
    rerun_diff_skipped: list[object] | None = None,
    item_count: int | None = None,
    output_count: int = 2,
) -> dict[str, object]:
    """Return one deterministic live replay `/v0/tag/files` payload."""

    output_dir = (working_dir / "serve-smoke-batch-outputs").resolve()
    manifest_input_path = str((output_dir / manifest_input_file_name).resolve())
    items: list[dict[str, object]] = [
        {
            "version": version,
            "pack": "finance-en",
            "language": "en",
            "content_type": "text/html",
            "source_path": str((working_dir / "serve-smoke-batch-alpha.html").resolve()),
            "saved_output_path": str(
                (output_dir / "serve-smoke-batch-alpha.finance-en.ades.json").resolve()
            ),
            "entities": [{"label": "organization"}, {"label": "ticker"}],
        },
        {
            "version": version,
            "pack": "finance-en",
            "language": "en",
            "content_type": "text/html",
            "source_path": str((working_dir / "serve-smoke-batch-beta.html").resolve()),
            "saved_output_path": str(
                (output_dir / "serve-smoke-batch-beta.finance-en.ades.json").resolve()
            ),
            "entities": [{"label": "exchange"}, {"label": "currency_amount"}],
        },
    ]
    default_changed_paths = [
        str((working_dir / "serve-smoke-batch-alpha.html").resolve()),
        str((working_dir / "serve-smoke-batch-beta.html").resolve()),
    ]
    payload: dict[str, object] = {
        "pack": pack,
        "item_count": len(items) if item_count is None else item_count,
        "saved_manifest_path": str((output_dir / manifest_file_name).resolve()),
        "summary": {
            "manifest_replay_mode": replay_mode,
        },
        "lineage": {},
        "warnings": [],
        "items": items[:output_count],
    }
    if include_manifest_input_path:
        summary = payload["summary"]
        assert isinstance(summary, dict)
        summary["manifest_input_path"] = manifest_input_path
    if include_manifest_candidate_count:
        summary = payload["summary"]
        assert isinstance(summary, dict)
        summary["manifest_candidate_count"] = manifest_candidate_count
    if include_manifest_selected_count:
        summary = payload["summary"]
        assert isinstance(summary, dict)
        summary["manifest_selected_count"] = manifest_selected_count
    if include_lineage_source_manifest_path:
        lineage = payload["lineage"]
        assert isinstance(lineage, dict)
        lineage["source_manifest_path"] = manifest_input_path
    if include_rerun_diff:
        rerun_diff: dict[str, object] = {}
        if include_rerun_diff_manifest_input_path:
            rerun_diff["manifest_input_path"] = str(
                (
                    output_dir
                    / (
                        rerun_diff_manifest_input_file_name
                        if rerun_diff_manifest_input_file_name is not None
                        else manifest_input_file_name
                    )
                ).resolve()
            )
        if include_rerun_diff_changed:
            rerun_diff["changed"] = (
                default_changed_paths if changed_paths is None else changed_paths
            )
        if include_rerun_diff_newly_processed:
            rerun_diff["newly_processed"] = (
                [] if rerun_diff_newly_processed is None else rerun_diff_newly_processed
            )
        if include_rerun_diff_reused:
            rerun_diff["reused"] = [] if rerun_diff_reused is None else rerun_diff_reused
        if include_rerun_diff_repaired:
            rerun_diff["repaired"] = (
                [] if rerun_diff_repaired is None else rerun_diff_repaired
            )
        if include_rerun_diff_skipped:
            rerun_diff["skipped"] = [] if rerun_diff_skipped is None else rerun_diff_skipped
        payload["rerun_diff"] = rerun_diff
    return payload


def test_release_versions_reports_synchronized_state(monkeypatch, tmp_path: Path) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)

    response = release_versions()

    assert response.version_file_version == __version__
    assert response.pyproject_version == __version__
    assert response.npm_version == __version__
    assert response.synchronized is True


def test_sync_release_version_updates_all_release_files(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo", pyproject_version="0.0.9")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)

    response = sync_release_version("0.2.0")

    assert response.target_version == "0.2.0"
    assert response.version_state.synchronized is True
    assert response.version_state.version_file_version == "0.2.0"
    assert response.version_state.pyproject_version == "0.2.0"
    assert response.version_state.npm_version == "0.2.0"
    assert len(response.updated_files) == 3


def test_verify_release_artifacts_builds_and_hashes_expected_outputs(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())

    response = verify_release_artifacts(output_dir=tmp_path / "dist")

    assert response.python_version == __version__
    assert response.npm_version == __version__
    assert response.version_state.synchronized is True
    assert response.versions_match is True
    assert response.smoke_install is True
    assert response.overall_success is True
    assert response.wheel.file_name.endswith(".whl")
    assert response.sdist.file_name.endswith(".tar.gz")
    assert response.npm_tarball.file_name.endswith(".tgz")
    assert response.python_install_smoke is not None
    assert response.python_install_smoke.passed is True
    assert response.python_install_smoke.reported_version == __version__
    assert response.python_install_smoke.pull is not None
    assert response.python_install_smoke.pull.passed is True
    assert response.python_install_smoke.tag is not None
    assert response.python_install_smoke.tag.passed is True
    assert response.python_install_smoke.recovery_status is not None
    assert response.python_install_smoke.recovery_status.passed is True
    assert response.python_install_smoke.serve is not None
    assert response.python_install_smoke.serve.passed is True
    assert response.python_install_smoke.serve_healthz is not None
    assert response.python_install_smoke.serve_healthz.passed is True
    assert response.python_install_smoke.serve_status is not None
    assert response.python_install_smoke.serve_status.passed is True
    assert response.python_install_smoke.serve_tag is not None
    assert response.python_install_smoke.serve_tag.passed is True
    assert response.python_install_smoke.serve_tag_file is not None
    assert response.python_install_smoke.serve_tag_file.passed is True
    assert response.python_install_smoke.serve_tag_files is not None
    assert response.python_install_smoke.serve_tag_files.passed is True
    assert response.python_install_smoke.serve_tag_files_replay is not None
    assert response.python_install_smoke.serve_tag_files_replay.passed is True
    assert {"general-en", "finance-en"} <= set(response.python_install_smoke.pulled_pack_ids)
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        response.python_install_smoke.tagged_labels
    )
    assert {"general-en", "finance-en"} <= set(response.python_install_smoke.recovered_pack_ids)
    assert {"general-en", "finance-en"} <= set(response.python_install_smoke.served_pack_ids)
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        response.python_install_smoke.serve_tagged_labels
    )
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        response.python_install_smoke.serve_tag_file_labels
    )
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        response.python_install_smoke.serve_tag_files_labels
    )
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        response.python_install_smoke.serve_tag_files_replay_labels
    )
    python_batch_payload = json.loads(response.python_install_smoke.serve_tag_files.stdout)
    assert str(python_batch_payload["saved_manifest_path"]).endswith(
        "serve-smoke-batch-manifest.finance-en.ades-manifest.json"
    )
    assert len(
        [
            item["saved_output_path"]
            for item in python_batch_payload["items"]
            if item.get("saved_output_path")
        ]
    ) == 2
    python_replay_payload = json.loads(response.python_install_smoke.serve_tag_files_replay.stdout)
    assert str(python_replay_payload["saved_manifest_path"]).endswith(
        "serve-smoke-batch-replay.finance-en.ades-manifest.json"
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
    assert len(
        [
            item["saved_output_path"]
            for item in python_replay_payload["items"]
            if item.get("saved_output_path")
        ]
    ) == 2
    assert response.npm_install_smoke is not None
    assert response.npm_install_smoke.passed is True
    assert response.npm_install_smoke.reported_version == __version__
    assert response.npm_install_smoke.pull is not None
    assert response.npm_install_smoke.pull.passed is True
    assert response.npm_install_smoke.tag is not None
    assert response.npm_install_smoke.tag.passed is True
    assert response.npm_install_smoke.recovery_status is not None
    assert response.npm_install_smoke.recovery_status.passed is True
    assert response.npm_install_smoke.serve is not None
    assert response.npm_install_smoke.serve.passed is True
    assert response.npm_install_smoke.serve_healthz is not None
    assert response.npm_install_smoke.serve_healthz.passed is True
    assert response.npm_install_smoke.serve_status is not None
    assert response.npm_install_smoke.serve_status.passed is True
    assert response.npm_install_smoke.serve_tag is not None
    assert response.npm_install_smoke.serve_tag.passed is True
    assert response.npm_install_smoke.serve_tag_file is not None
    assert response.npm_install_smoke.serve_tag_file.passed is True
    assert response.npm_install_smoke.serve_tag_files is not None
    assert response.npm_install_smoke.serve_tag_files.passed is True
    assert response.npm_install_smoke.serve_tag_files_replay is not None
    assert response.npm_install_smoke.serve_tag_files_replay.passed is True
    assert {"general-en", "finance-en"} <= set(response.npm_install_smoke.pulled_pack_ids)
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        response.npm_install_smoke.tagged_labels
    )
    assert {"general-en", "finance-en"} <= set(response.npm_install_smoke.recovered_pack_ids)
    assert {"general-en", "finance-en"} <= set(response.npm_install_smoke.served_pack_ids)
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        response.npm_install_smoke.serve_tagged_labels
    )
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        response.npm_install_smoke.serve_tag_file_labels
    )
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        response.npm_install_smoke.serve_tag_files_labels
    )
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        response.npm_install_smoke.serve_tag_files_replay_labels
    )
    npm_batch_payload = json.loads(response.npm_install_smoke.serve_tag_files.stdout)
    assert str(npm_batch_payload["saved_manifest_path"]).endswith(
        "serve-smoke-batch-manifest.finance-en.ades-manifest.json"
    )
    assert len(
        [item["saved_output_path"] for item in npm_batch_payload["items"] if item.get("saved_output_path")]
    ) == 2
    npm_replay_payload = json.loads(response.npm_install_smoke.serve_tag_files_replay.stdout)
    assert str(npm_replay_payload["saved_manifest_path"]).endswith(
        "serve-smoke-batch-replay.finance-en.ades-manifest.json"
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
    assert len(
        [item["saved_output_path"] for item in npm_replay_payload["items"] if item.get("saved_output_path")]
    ) == 2
    assert len(response.wheel.sha256) == 64
    assert len(response.sdist.sha256) == 64
    assert len(response.npm_tarball.sha256) == 64


def test_verify_release_artifacts_reports_version_mismatch_warning(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(
        tmp_path / "repo",
        pyproject_version="0.1.1",
        npm_version="0.1.2",
    )
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(
        monkeypatch,
        build_fake_release_runner(python_version="0.1.1", npm_version="0.1.2"),
    )

    response = verify_release_artifacts(output_dir=tmp_path / "dist")

    assert response.versions_match is False
    assert response.overall_success is False
    assert response.version_state.synchronized is False
    assert response.warnings == [
        f"version_mismatch:version_file={__version__},pyproject=0.1.1,npm=0.1.2",
        "python_wheel_version_mismatch:0.1.1",
        "npm_tarball_version_mismatch:0.1.1",
    ]


def test_verify_release_artifacts_raises_on_python_build_failure(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(
        monkeypatch,
        build_failed_release_runner(stderr="python build failed"),
    )

    with pytest.raises(ValueError, match="python build failed"):
        verify_release_artifacts(output_dir=tmp_path / "dist")


def test_verify_release_artifacts_can_skip_smoke_install(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())

    response = verify_release_artifacts(
        output_dir=tmp_path / "dist",
        smoke_install=False,
    )

    assert response.smoke_install is False
    assert response.overall_success is True
    assert response.python_install_smoke is None
    assert response.npm_install_smoke is None
    assert response.warnings == []


def test_verify_release_artifacts_reports_smoke_install_failures(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(
        monkeypatch,
        build_fake_release_runner(
            npm_install_returncode=4,
            npm_install_stderr="npm install failed",
        ),
    )

    response = verify_release_artifacts(output_dir=tmp_path / "dist")

    assert response.overall_success is False
    assert response.python_install_smoke is not None
    assert response.python_install_smoke.passed is True
    assert response.npm_install_smoke is not None
    assert response.npm_install_smoke.passed is False
    assert response.npm_install_smoke.install.exit_code == 4
    assert response.npm_install_smoke.install.stderr == "npm install failed"
    assert (
        response.npm_install_smoke.invoke.stderr
        == "Skipped because npm tarball installation failed."
    )
    assert response.warnings == ["npm_tarball_install_failed:4"]


def test_verify_release_artifacts_reports_tag_smoke_failures(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(
        monkeypatch,
        build_fake_release_runner(
            npm_tag_returncode=7,
            npm_tag_stderr="tag failed",
        ),
    )

    response = verify_release_artifacts(output_dir=tmp_path / "dist")

    assert response.overall_success is False
    assert response.npm_install_smoke is not None
    assert response.npm_install_smoke.pull is not None
    assert response.npm_install_smoke.pull.passed is True
    assert response.npm_install_smoke.tag is not None
    assert response.npm_install_smoke.tag.passed is False
    assert response.npm_install_smoke.tag.exit_code == 7
    assert response.npm_install_smoke.tag.stderr == "tag failed"
    assert response.warnings == ["npm_tarball_tag_failed:7"]


def test_verify_release_artifacts_reports_serve_smoke_failures(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())

    def fake_service_smoke(*, executable, working_dir, storage_root, expected_version, extra_env=None):
        if "node_modules/.bin" not in str(executable):
            return (
                ReleaseCommandResult(
                    command=[str(executable), "serve"],
                    exit_code=0,
                    passed=True,
                    stdout="Using storage root\n",
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["GET", "http://127.0.0.1:8734/healthz"],
                    exit_code=0,
                    passed=True,
                    stdout='{"status":"ok","version":"0.1.0"}',
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["GET", "http://127.0.0.1:8734/v0/status"],
                    exit_code=0,
                    passed=True,
                    stdout=(
                        '{"service":"ades","version":"0.1.0","installed_packs":'
                        '["general-en","finance-en"]}'
                    ),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(
                        {
                            "entities": [
                                {"label": "organization"},
                                {"label": "ticker"},
                                {"label": "exchange"},
                                {"label": "currency_amount"},
                            ]
                        }
                    ),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag/file"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(
                        {
                            "entities": [
                                {"label": "organization"},
                                {"label": "ticker"},
                                {"label": "exchange"},
                                {"label": "currency_amount"},
                            ]
                        }
                    ),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(_served_batch_manifest_payload(working_dir)),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(_served_batch_manifest_replay_payload(working_dir)),
                    stderr="",
                ),
                ["general-en", "finance-en"],
                ["organization", "ticker", "exchange", "currency_amount"],
                ["organization", "ticker", "exchange", "currency_amount"],
                ["organization", "ticker", "exchange", "currency_amount"],
                ["organization", "ticker", "exchange", "currency_amount"],
            )
        return (
            ReleaseCommandResult(
                command=[str(executable), "serve"],
                exit_code=9,
                passed=False,
                stdout="",
                stderr="serve failed",
            ),
            ReleaseCommandResult(
                command=["GET", "http://127.0.0.1:8734/healthz"],
                exit_code=1,
                passed=False,
                stdout="",
                stderr="connection refused",
            ),
            ReleaseCommandResult(
                command=["GET", "http://127.0.0.1:8734/v0/status"],
                exit_code=-1,
                passed=False,
                stdout="",
                stderr="Skipped because service startup did not pass the health probe.",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag"],
                exit_code=-1,
                passed=False,
                stdout="",
                stderr="Skipped because service startup did not pass the health probe.",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/file"],
                exit_code=-1,
                passed=False,
                stdout="",
                stderr="Skipped because service startup did not pass the health probe.",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                exit_code=-1,
                passed=False,
                stdout="",
                stderr="Skipped because service startup did not pass the health probe.",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                exit_code=-1,
                passed=False,
                stdout="",
                stderr="Skipped because service startup did not pass the health probe.",
            ),
            [],
            [],
            [],
            [],
            [],
        )

    monkeypatch.setattr("ades.release._run_cli_service_smoke", fake_service_smoke)

    response = verify_release_artifacts(output_dir=tmp_path / "dist")

    assert response.overall_success is False
    assert response.npm_install_smoke is not None
    assert response.npm_install_smoke.serve is not None
    assert response.npm_install_smoke.serve.passed is False
    assert response.npm_install_smoke.serve.exit_code == 9
    assert response.npm_install_smoke.serve.stderr == "serve failed"
    assert response.warnings == ["npm_tarball_serve_failed:9"]


def test_verify_release_artifacts_reports_live_service_tag_failures(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())

    def fake_service_smoke(*, executable, working_dir, storage_root, expected_version, extra_env=None):
        if "node_modules/.bin" not in str(executable):
            return (
                ReleaseCommandResult(
                    command=[str(executable), "serve"],
                    exit_code=0,
                    passed=True,
                    stdout="Using storage root\n",
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["GET", "http://127.0.0.1:8734/healthz"],
                    exit_code=0,
                    passed=True,
                    stdout=f'{{"status":"ok","version":"{expected_version}"}}',
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["GET", "http://127.0.0.1:8734/v0/status"],
                    exit_code=0,
                    passed=True,
                    stdout=(
                        '{"service":"ades","version":"'
                        + expected_version
                        + '","installed_packs":["general-en","finance-en"]}'
                    ),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(
                        {
                            "entities": [
                                {"label": "organization"},
                                {"label": "ticker"},
                                {"label": "exchange"},
                                {"label": "currency_amount"},
                            ]
                        }
                    ),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag/file"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(
                        {
                            "entities": [
                                {"label": "organization"},
                                {"label": "ticker"},
                                {"label": "exchange"},
                                {"label": "currency_amount"},
                            ]
                        }
                    ),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(
                        _served_batch_manifest_payload(working_dir, version=expected_version)
                    ),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(
                        _served_batch_manifest_replay_payload(
                            working_dir,
                            version=expected_version,
                        )
                    ),
                    stderr="",
                ),
                ["general-en", "finance-en"],
                ["organization", "ticker", "exchange", "currency_amount"],
                ["organization", "ticker", "exchange", "currency_amount"],
                ["organization", "ticker", "exchange", "currency_amount"],
                ["organization", "ticker", "exchange", "currency_amount"],
            )
        return (
            ReleaseCommandResult(
                command=[str(executable), "serve"],
                exit_code=0,
                passed=True,
                stdout="Using storage root\n",
                stderr="",
            ),
            ReleaseCommandResult(
                command=["GET", "http://127.0.0.1:8734/healthz"],
                exit_code=0,
                passed=True,
                stdout=f'{{"status":"ok","version":"{expected_version}"}}',
                stderr="",
            ),
            ReleaseCommandResult(
                command=["GET", "http://127.0.0.1:8734/v0/status"],
                exit_code=0,
                passed=True,
                stdout=(
                    '{"service":"ades","version":"'
                    + expected_version
                    + '","installed_packs":["general-en","finance-en"]}'
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag"],
                exit_code=12,
                passed=False,
                stdout="",
                stderr="tag endpoint failed",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/file"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    {
                        "entities": [
                            {"label": "organization"},
                            {"label": "ticker"},
                            {"label": "exchange"},
                            {"label": "currency_amount"},
                        ]
                    }
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    _served_batch_manifest_payload(working_dir, version=expected_version)
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                exit_code=-1,
                passed=False,
                stdout="",
                stderr="Skipped because live service tag endpoint failed.",
            ),
            ["general-en", "finance-en"],
            [],
            ["organization", "ticker", "exchange", "currency_amount"],
            ["organization", "ticker", "exchange", "currency_amount"],
            [],
        )

    monkeypatch.setattr("ades.release._run_cli_service_smoke", fake_service_smoke)

    response = verify_release_artifacts(output_dir=tmp_path / "dist")

    assert response.overall_success is False
    assert response.npm_install_smoke is not None
    assert response.npm_install_smoke.serve_tag is not None
    assert response.npm_install_smoke.serve_tag.passed is False
    assert response.npm_install_smoke.serve_tag.exit_code == 12
    assert response.npm_install_smoke.serve_tag.stderr == "tag endpoint failed"
    assert response.warnings == ["npm_tarball_serve_tag_failed:12"]


def test_verify_release_artifacts_reports_live_service_file_tag_failures(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())

    def fake_service_smoke(*, executable, working_dir, storage_root, expected_version, extra_env=None):
        if "node_modules/.bin" not in str(executable):
            return (
                ReleaseCommandResult(
                    command=[str(executable), "serve"],
                    exit_code=0,
                    passed=True,
                    stdout="Using storage root\n",
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["GET", "http://127.0.0.1:8734/healthz"],
                    exit_code=0,
                    passed=True,
                    stdout=f'{{"status":"ok","version":"{expected_version}"}}',
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["GET", "http://127.0.0.1:8734/v0/status"],
                    exit_code=0,
                    passed=True,
                    stdout=(
                        '{"service":"ades","version":"'
                        + expected_version
                        + '","installed_packs":["general-en","finance-en"]}'
                    ),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(
                        {
                            "entities": [
                                {"label": "organization"},
                                {"label": "ticker"},
                                {"label": "exchange"},
                                {"label": "currency_amount"},
                            ]
                        }
                    ),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag/file"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(
                        {
                            "entities": [
                                {"label": "organization"},
                                {"label": "ticker"},
                                {"label": "exchange"},
                                {"label": "currency_amount"},
                            ]
                        }
                    ),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(
                        _served_batch_manifest_payload(working_dir, version=expected_version)
                    ),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(
                        _served_batch_manifest_replay_payload(
                            working_dir,
                            version=expected_version,
                        )
                    ),
                    stderr="",
                ),
                ["general-en", "finance-en"],
                ["organization", "ticker", "exchange", "currency_amount"],
                ["organization", "ticker", "exchange", "currency_amount"],
                ["organization", "ticker", "exchange", "currency_amount"],
                ["organization", "ticker", "exchange", "currency_amount"],
            )
        return (
            ReleaseCommandResult(
                command=[str(executable), "serve"],
                exit_code=0,
                passed=True,
                stdout="Using storage root\n",
                stderr="",
            ),
            ReleaseCommandResult(
                command=["GET", "http://127.0.0.1:8734/healthz"],
                exit_code=0,
                passed=True,
                stdout=f'{{"status":"ok","version":"{expected_version}"}}',
                stderr="",
            ),
            ReleaseCommandResult(
                command=["GET", "http://127.0.0.1:8734/v0/status"],
                exit_code=0,
                passed=True,
                stdout=(
                    '{"service":"ades","version":"'
                    + expected_version
                    + '","installed_packs":["general-en","finance-en"]}'
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    {
                        "entities": [
                            {"label": "organization"},
                            {"label": "ticker"},
                            {"label": "exchange"},
                            {"label": "currency_amount"},
                        ]
                    }
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/file"],
                exit_code=13,
                passed=False,
                stdout="",
                stderr="file tag endpoint failed",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    _served_batch_manifest_payload(working_dir, version=expected_version)
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                exit_code=-1,
                passed=False,
                stdout="",
                stderr="Skipped because live service file-tag endpoint failed.",
            ),
            ["general-en", "finance-en"],
            ["organization", "ticker", "exchange", "currency_amount"],
            [],
            ["organization", "ticker", "exchange", "currency_amount"],
            [],
        )

    monkeypatch.setattr("ades.release._run_cli_service_smoke", fake_service_smoke)

    response = verify_release_artifacts(output_dir=tmp_path / "dist")

    assert response.overall_success is False
    assert response.npm_install_smoke is not None
    assert response.npm_install_smoke.serve_tag_file is not None
    assert response.npm_install_smoke.serve_tag_file.passed is False
    assert response.npm_install_smoke.serve_tag_file.exit_code == 13
    assert response.npm_install_smoke.serve_tag_file.stderr == "file tag endpoint failed"
    assert response.warnings == ["npm_tarball_serve_tag_file_failed:13"]


def test_verify_release_artifacts_reports_live_service_batch_file_tag_failures(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())

    def fake_service_smoke(*, executable, working_dir, storage_root, expected_version, extra_env=None):
        if "node_modules/.bin" not in str(executable):
            return (
                ReleaseCommandResult(
                    command=[str(executable), "serve"],
                    exit_code=0,
                    passed=True,
                    stdout="Using storage root\n",
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["GET", "http://127.0.0.1:8734/healthz"],
                    exit_code=0,
                    passed=True,
                    stdout=f'{{"status":"ok","version":"{expected_version}"}}',
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["GET", "http://127.0.0.1:8734/v0/status"],
                    exit_code=0,
                    passed=True,
                    stdout=(
                        '{"service":"ades","version":"'
                        + expected_version
                        + '","installed_packs":["general-en","finance-en"]}'
                    ),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(
                        {
                            "entities": [
                                {"label": "organization"},
                                {"label": "ticker"},
                                {"label": "exchange"},
                                {"label": "currency_amount"},
                            ]
                        }
                    ),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag/file"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(
                        {
                            "entities": [
                                {"label": "organization"},
                                {"label": "ticker"},
                                {"label": "exchange"},
                                {"label": "currency_amount"},
                            ]
                        }
                    ),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(
                        _served_batch_manifest_payload(working_dir, version=expected_version)
                    ),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(
                        _served_batch_manifest_replay_payload(
                            working_dir,
                            version=expected_version,
                        )
                    ),
                    stderr="",
                ),
                ["general-en", "finance-en"],
                ["organization", "ticker", "exchange", "currency_amount"],
                ["organization", "ticker", "exchange", "currency_amount"],
                ["organization", "ticker", "exchange", "currency_amount"],
                ["organization", "ticker", "exchange", "currency_amount"],
            )
        return (
            ReleaseCommandResult(
                command=[str(executable), "serve"],
                exit_code=0,
                passed=True,
                stdout="Using storage root\n",
                stderr="",
            ),
            ReleaseCommandResult(
                command=["GET", "http://127.0.0.1:8734/healthz"],
                exit_code=0,
                passed=True,
                stdout=f'{{"status":"ok","version":"{expected_version}"}}',
                stderr="",
            ),
            ReleaseCommandResult(
                command=["GET", "http://127.0.0.1:8734/v0/status"],
                exit_code=0,
                passed=True,
                stdout=(
                    '{"service":"ades","version":"'
                    + expected_version
                    + '","installed_packs":["general-en","finance-en"]}'
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    {
                        "entities": [
                            {"label": "organization"},
                            {"label": "ticker"},
                            {"label": "exchange"},
                            {"label": "currency_amount"},
                        ]
                    }
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/file"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    {
                        "entities": [
                            {"label": "organization"},
                            {"label": "ticker"},
                            {"label": "exchange"},
                            {"label": "currency_amount"},
                        ]
                    }
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                exit_code=14,
                passed=False,
                stdout="",
                stderr="batch tag endpoint failed",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                exit_code=-1,
                passed=False,
                stdout="",
                stderr="Skipped because live service batch tag endpoint failed.",
            ),
            ["general-en", "finance-en"],
            ["organization", "ticker", "exchange", "currency_amount"],
            ["organization", "ticker", "exchange", "currency_amount"],
            [],
            [],
        )

    monkeypatch.setattr("ades.release._run_cli_service_smoke", fake_service_smoke)

    response = verify_release_artifacts(output_dir=tmp_path / "dist")

    assert response.overall_success is False
    assert response.npm_install_smoke is not None
    assert response.npm_install_smoke.serve_tag_files is not None
    assert response.npm_install_smoke.serve_tag_files.passed is False
    assert response.npm_install_smoke.serve_tag_files.exit_code == 14
    assert response.npm_install_smoke.serve_tag_files.stderr == "batch tag endpoint failed"
    assert response.warnings == ["npm_tarball_serve_tag_files_failed:14"]


def test_verify_release_artifacts_reports_live_service_batch_manifest_failures(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())

    def fake_service_smoke(*, executable, working_dir, storage_root, expected_version, extra_env=None):
        if "node_modules/.bin" not in str(executable):
            return (
                ReleaseCommandResult(
                    command=[str(executable), "serve"],
                    exit_code=0,
                    passed=True,
                    stdout="Using storage root\n",
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["GET", "http://127.0.0.1:8734/healthz"],
                    exit_code=0,
                    passed=True,
                    stdout=f'{{"status":"ok","version":"{expected_version}"}}',
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["GET", "http://127.0.0.1:8734/v0/status"],
                    exit_code=0,
                    passed=True,
                    stdout=(
                        '{"service":"ades","version":"'
                        + expected_version
                        + '","installed_packs":["general-en","finance-en"]}'
                    ),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(
                        {
                            "entities": [
                                {"label": "organization"},
                                {"label": "ticker"},
                                {"label": "exchange"},
                                {"label": "currency_amount"},
                            ]
                        }
                    ),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag/file"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(
                        {
                            "entities": [
                                {"label": "organization"},
                                {"label": "ticker"},
                                {"label": "exchange"},
                                {"label": "currency_amount"},
                            ]
                        }
                    ),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(
                        _served_batch_manifest_payload(working_dir, version=expected_version)
                    ),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(
                        _served_batch_manifest_replay_payload(
                            working_dir,
                            version=expected_version,
                        )
                    ),
                    stderr="",
                ),
                ["general-en", "finance-en"],
                ["organization", "ticker", "exchange", "currency_amount"],
                ["organization", "ticker", "exchange", "currency_amount"],
                ["organization", "ticker", "exchange", "currency_amount"],
                ["organization", "ticker", "exchange", "currency_amount"],
            )
        return (
            ReleaseCommandResult(
                command=[str(executable), "serve"],
                exit_code=0,
                passed=True,
                stdout="Using storage root\n",
                stderr="",
            ),
            ReleaseCommandResult(
                command=["GET", "http://127.0.0.1:8734/healthz"],
                exit_code=0,
                passed=True,
                stdout=f'{{"status":"ok","version":"{expected_version}"}}',
                stderr="",
            ),
            ReleaseCommandResult(
                command=["GET", "http://127.0.0.1:8734/v0/status"],
                exit_code=0,
                passed=True,
                stdout=(
                    '{"service":"ades","version":"'
                    + expected_version
                    + '","installed_packs":["general-en","finance-en"]}'
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    {
                        "entities": [
                            {"label": "organization"},
                            {"label": "ticker"},
                            {"label": "exchange"},
                            {"label": "currency_amount"},
                        ]
                    }
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/file"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    {
                        "entities": [
                            {"label": "organization"},
                            {"label": "ticker"},
                            {"label": "exchange"},
                            {"label": "currency_amount"},
                        ]
                    }
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    _served_batch_manifest_payload(
                        working_dir,
                        version=expected_version,
                        include_manifest_path=False,
                    )
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                exit_code=-1,
                passed=False,
                stdout="",
                stderr="Skipped because live batch tagging did not write a manifest.",
            ),
            ["general-en", "finance-en"],
            ["organization", "ticker", "exchange", "currency_amount"],
            ["organization", "ticker", "exchange", "currency_amount"],
            ["organization", "ticker", "exchange", "currency_amount"],
            [],
        )

    monkeypatch.setattr("ades.release._run_cli_service_smoke", fake_service_smoke)

    response = verify_release_artifacts(output_dir=tmp_path / "dist")

    assert response.overall_success is False
    assert response.npm_install_smoke is not None
    assert response.npm_install_smoke.passed is False
    assert response.npm_install_smoke.serve_tag_files is not None
    assert response.npm_install_smoke.serve_tag_files.passed is True
    assert response.warnings == ["npm_tarball_serve_tag_files_missing_manifest_path"]


def test_verify_release_artifacts_reports_live_service_batch_manifest_path_mismatches(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())

    def fake_service_smoke(*, executable, working_dir, storage_root, expected_version, extra_env=None):
        if "node_modules/.bin" not in str(executable):
            return (
                ReleaseCommandResult(
                    command=[str(executable), "serve"],
                    exit_code=0,
                    passed=True,
                    stdout="Using storage root\n",
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["GET", "http://127.0.0.1:8734/healthz"],
                    exit_code=0,
                    passed=True,
                    stdout=f'{{"status":"ok","version":"{expected_version}"}}',
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["GET", "http://127.0.0.1:8734/v0/status"],
                    exit_code=0,
                    passed=True,
                    stdout=(
                        '{"service":"ades","version":"'
                        + expected_version
                        + '","installed_packs":["general-en","finance-en"]}'
                    ),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(
                        {
                            "entities": [
                                {"label": "organization"},
                                {"label": "ticker"},
                                {"label": "exchange"},
                                {"label": "currency_amount"},
                            ]
                        }
                    ),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag/file"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(
                        {
                            "entities": [
                                {"label": "organization"},
                                {"label": "ticker"},
                                {"label": "exchange"},
                                {"label": "currency_amount"},
                            ]
                        }
                    ),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(
                        _served_batch_manifest_payload(working_dir, version=expected_version)
                    ),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(
                        _served_batch_manifest_replay_payload(
                            working_dir,
                            version=expected_version,
                        )
                    ),
                    stderr="",
                ),
                ["general-en", "finance-en"],
                ["organization", "ticker", "exchange", "currency_amount"],
                ["organization", "ticker", "exchange", "currency_amount"],
                ["organization", "ticker", "exchange", "currency_amount"],
                ["organization", "ticker", "exchange", "currency_amount"],
            )
        return (
            ReleaseCommandResult(
                command=[str(executable), "serve"],
                exit_code=0,
                passed=True,
                stdout="Using storage root\n",
                stderr="",
            ),
            ReleaseCommandResult(
                command=["GET", "http://127.0.0.1:8734/healthz"],
                exit_code=0,
                passed=True,
                stdout=f'{{"status":"ok","version":"{expected_version}"}}',
                stderr="",
            ),
            ReleaseCommandResult(
                command=["GET", "http://127.0.0.1:8734/v0/status"],
                exit_code=0,
                passed=True,
                stdout=(
                    '{"service":"ades","version":"'
                    + expected_version
                    + '","installed_packs":["general-en","finance-en"]}'
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    {
                        "entities": [
                            {"label": "organization"},
                            {"label": "ticker"},
                            {"label": "exchange"},
                            {"label": "currency_amount"},
                        ]
                    }
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/file"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    {
                        "entities": [
                            {"label": "organization"},
                            {"label": "ticker"},
                            {"label": "exchange"},
                            {"label": "currency_amount"},
                        ]
                    }
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    _served_batch_manifest_payload(
                        working_dir,
                        version=expected_version,
                        manifest_file_name="batch.finance-en.ades-manifest.json",
                    )
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                exit_code=-1,
                passed=False,
                stdout="",
                stderr="Skipped because live batch tagging wrote an unexpected manifest path.",
            ),
            ["general-en", "finance-en"],
            ["organization", "ticker", "exchange", "currency_amount"],
            ["organization", "ticker", "exchange", "currency_amount"],
            ["organization", "ticker", "exchange", "currency_amount"],
            [],
        )

    monkeypatch.setattr("ades.release._run_cli_service_smoke", fake_service_smoke)

    response = verify_release_artifacts(output_dir=tmp_path / "dist")

    assert response.overall_success is False
    assert response.npm_install_smoke is not None
    assert response.npm_install_smoke.passed is False
    assert response.npm_install_smoke.serve_tag_files is not None
    assert response.npm_install_smoke.serve_tag_files.passed is True
    assert response.warnings == ["npm_tarball_serve_tag_files_invalid_manifest_path"]


def test_verify_release_artifacts_reports_live_service_batch_replay_manifest_input_mismatches(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())

    def fake_service_smoke(*, executable, working_dir, storage_root, expected_version, extra_env=None):
        if "node_modules/.bin" not in str(executable):
            return (
                ReleaseCommandResult(
                    command=[str(executable), "serve"],
                    exit_code=0,
                    passed=True,
                    stdout="Using storage root\n",
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["GET", "http://127.0.0.1:8734/healthz"],
                    exit_code=0,
                    passed=True,
                    stdout=f'{{"status":"ok","version":"{expected_version}"}}',
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["GET", "http://127.0.0.1:8734/v0/status"],
                    exit_code=0,
                    passed=True,
                    stdout=(
                        '{"service":"ades","version":"'
                        + expected_version
                        + '","installed_packs":["general-en","finance-en"]}'
                    ),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(
                        {
                            "entities": [
                                {"label": "organization"},
                                {"label": "ticker"},
                                {"label": "exchange"},
                                {"label": "currency_amount"},
                            ]
                        }
                    ),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag/file"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(
                        {
                            "entities": [
                                {"label": "organization"},
                                {"label": "ticker"},
                                {"label": "exchange"},
                                {"label": "currency_amount"},
                            ]
                        }
                    ),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(
                        _served_batch_manifest_payload(working_dir, version=expected_version)
                    ),
                    stderr="",
                ),
                ReleaseCommandResult(
                    command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                    exit_code=0,
                    passed=True,
                    stdout=json.dumps(
                        _served_batch_manifest_replay_payload(
                            working_dir,
                            version=expected_version,
                        )
                    ),
                    stderr="",
                ),
                ["general-en", "finance-en"],
                ["organization", "ticker", "exchange", "currency_amount"],
                ["organization", "ticker", "exchange", "currency_amount"],
                ["organization", "ticker", "exchange", "currency_amount"],
                ["organization", "ticker", "exchange", "currency_amount"],
            )
        return (
            ReleaseCommandResult(
                command=[str(executable), "serve"],
                exit_code=0,
                passed=True,
                stdout="Using storage root\n",
                stderr="",
            ),
            ReleaseCommandResult(
                command=["GET", "http://127.0.0.1:8734/healthz"],
                exit_code=0,
                passed=True,
                stdout=f'{{"status":"ok","version":"{expected_version}"}}',
                stderr="",
            ),
            ReleaseCommandResult(
                command=["GET", "http://127.0.0.1:8734/v0/status"],
                exit_code=0,
                passed=True,
                stdout=(
                    '{"service":"ades","version":"'
                    + expected_version
                    + '","installed_packs":["general-en","finance-en"]}'
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    {
                        "entities": [
                            {"label": "organization"},
                            {"label": "ticker"},
                            {"label": "exchange"},
                            {"label": "currency_amount"},
                        ]
                    }
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/file"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    {
                        "entities": [
                            {"label": "organization"},
                            {"label": "ticker"},
                            {"label": "exchange"},
                            {"label": "currency_amount"},
                        ]
                    }
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    _served_batch_manifest_payload(working_dir, version=expected_version)
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    _served_batch_manifest_replay_payload(
                        working_dir,
                        version=expected_version,
                        manifest_input_file_name="wrong.finance-en.ades-manifest.json",
                        rerun_diff_manifest_input_file_name=(
                            "serve-smoke-batch-manifest.finance-en.ades-manifest.json"
                        ),
                    )
                ),
                stderr="",
            ),
            ["general-en", "finance-en"],
            ["organization", "ticker", "exchange", "currency_amount"],
            ["organization", "ticker", "exchange", "currency_amount"],
            ["organization", "ticker", "exchange", "currency_amount"],
            ["organization", "ticker", "exchange", "currency_amount"],
        )

    monkeypatch.setattr("ades.release._run_cli_service_smoke", fake_service_smoke)

    response = verify_release_artifacts(output_dir=tmp_path / "dist")

    assert response.overall_success is False
    assert response.npm_install_smoke is not None
    assert response.npm_install_smoke.passed is False
    assert response.npm_install_smoke.serve_tag_files_replay is not None
    assert response.npm_install_smoke.serve_tag_files_replay.passed is True
    assert response.warnings == ["npm_tarball_serve_tag_files_replay_invalid_manifest_input_path"]


@pytest.mark.parametrize(
    ("replay_kwargs", "expected_warning"),
    [
        (
            {"manifest_candidate_count": 1},
            "npm_tarball_serve_tag_files_replay_invalid_manifest_candidate_count:1",
        ),
        (
            {"manifest_selected_count": 1},
            "npm_tarball_serve_tag_files_replay_invalid_manifest_selected_count:1",
        ),
    ],
)
def test_verify_release_artifacts_reports_live_service_batch_replay_summary_count_mismatches(
    monkeypatch,
    tmp_path: Path,
    replay_kwargs: dict[str, int],
    expected_warning: str,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())

    def fake_service_smoke(*, executable, working_dir, storage_root, expected_version, extra_env=None):
        replay_payload_kwargs: dict[str, int] = (
            {} if "node_modules/.bin" not in str(executable) else replay_kwargs
        )
        return (
            ReleaseCommandResult(
                command=[str(executable), "serve", "--host", "127.0.0.1", "--port", "8734"],
                exit_code=0,
                passed=True,
                stdout="ready",
                stderr="",
            ),
            ReleaseCommandResult(
                command=["GET", "http://127.0.0.1:8734/healthz"],
                exit_code=0,
                passed=True,
                stdout=json.dumps({"status": "ok", "version": expected_version}),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["GET", "http://127.0.0.1:8734/v0/status"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    {
                        "service": "ades",
                        "version": expected_version,
                        "installed_packs": ["general-en", "finance-en"],
                    }
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    {
                        "entities": [
                            {"label": "organization"},
                            {"label": "ticker"},
                            {"label": "exchange"},
                            {"label": "currency_amount"},
                        ]
                    }
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/file"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    {
                        "entities": [
                            {"label": "organization"},
                            {"label": "ticker"},
                            {"label": "exchange"},
                            {"label": "currency_amount"},
                        ]
                    }
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    _served_batch_manifest_payload(working_dir, version=expected_version)
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    _served_batch_manifest_replay_payload(
                        working_dir,
                        version=expected_version,
                        **replay_payload_kwargs,
                    )
                ),
                stderr="",
            ),
            ["general-en", "finance-en"],
            ["organization", "ticker", "exchange", "currency_amount"],
            ["organization", "ticker", "exchange", "currency_amount"],
            ["organization", "ticker", "exchange", "currency_amount"],
            ["organization", "ticker", "exchange", "currency_amount"],
        )

    monkeypatch.setattr("ades.release._run_cli_service_smoke", fake_service_smoke)

    response = verify_release_artifacts(output_dir=tmp_path / "dist")

    assert response.overall_success is False
    assert response.npm_install_smoke is not None
    assert response.npm_install_smoke.passed is False
    assert response.npm_install_smoke.serve_tag_files_replay is not None
    assert response.npm_install_smoke.serve_tag_files_replay.passed is True
    assert response.warnings == [expected_warning]


@pytest.mark.parametrize(
    ("replay_kwargs", "expected_warning"),
    [
        (
            {"pack": "general-en"},
            "npm_tarball_serve_tag_files_replay_invalid_pack:general-en",
        ),
        (
            {"item_count": 1},
            "npm_tarball_serve_tag_files_replay_invalid_item_count:1",
        ),
    ],
)
def test_verify_release_artifacts_reports_live_service_batch_replay_identity_mismatches(
    monkeypatch,
    tmp_path: Path,
    replay_kwargs: dict[str, str | int],
    expected_warning: str,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())

    def fake_service_smoke(*, executable, working_dir, storage_root, expected_version, extra_env=None):
        replay_payload_kwargs: dict[str, str | int] = (
            {} if "node_modules/.bin" not in str(executable) else replay_kwargs
        )
        return (
            ReleaseCommandResult(
                command=[str(executable), "serve", "--host", "127.0.0.1", "--port", "8734"],
                exit_code=0,
                passed=True,
                stdout="ready",
                stderr="",
            ),
            ReleaseCommandResult(
                command=["GET", "http://127.0.0.1:8734/healthz"],
                exit_code=0,
                passed=True,
                stdout=json.dumps({"status": "ok", "version": expected_version}),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["GET", "http://127.0.0.1:8734/v0/status"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    {
                        "service": "ades",
                        "version": expected_version,
                        "installed_packs": ["general-en", "finance-en"],
                    }
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    {
                        "entities": [
                            {"label": "organization"},
                            {"label": "ticker"},
                            {"label": "exchange"},
                            {"label": "currency_amount"},
                        ]
                    }
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/file"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    {
                        "entities": [
                            {"label": "organization"},
                            {"label": "ticker"},
                            {"label": "exchange"},
                            {"label": "currency_amount"},
                        ]
                    }
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    _served_batch_manifest_payload(working_dir, version=expected_version)
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    _served_batch_manifest_replay_payload(
                        working_dir,
                        version=expected_version,
                        **replay_payload_kwargs,
                    )
                ),
                stderr="",
            ),
            ["general-en", "finance-en"],
            ["organization", "ticker", "exchange", "currency_amount"],
            ["organization", "ticker", "exchange", "currency_amount"],
            ["organization", "ticker", "exchange", "currency_amount"],
            ["organization", "ticker", "exchange", "currency_amount"],
        )

    monkeypatch.setattr("ades.release._run_cli_service_smoke", fake_service_smoke)

    response = verify_release_artifacts(output_dir=tmp_path / "dist")

    assert response.overall_success is False
    assert response.npm_install_smoke is not None
    assert response.npm_install_smoke.passed is False
    assert response.npm_install_smoke.serve_tag_files_replay is not None
    assert response.npm_install_smoke.serve_tag_files_replay.passed is True
    assert response.warnings == [expected_warning]


@pytest.mark.parametrize(
    ("replay_kwargs", "expected_warning"),
    [
        (
            {
                "rerun_diff_manifest_input_file_name": "wrong.finance-en.ades-manifest.json"
            },
            "npm_tarball_serve_tag_files_replay_invalid_rerun_diff_manifest_input_path",
        ),
        (
            {"changed_paths": []},
            "npm_tarball_serve_tag_files_replay_invalid_rerun_diff_changed_paths",
        ),
    ],
)
def test_verify_release_artifacts_reports_live_service_batch_replay_rerun_diff_mismatches(
    monkeypatch,
    tmp_path: Path,
    replay_kwargs: dict[str, object],
    expected_warning: str,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())

    def fake_service_smoke(*, executable, working_dir, storage_root, expected_version, extra_env=None):
        replay_payload_kwargs: dict[str, object] = (
            {} if "node_modules/.bin" not in str(executable) else replay_kwargs
        )
        return (
            ReleaseCommandResult(
                command=[str(executable), "serve", "--host", "127.0.0.1", "--port", "8734"],
                exit_code=0,
                passed=True,
                stdout="ready",
                stderr="",
            ),
            ReleaseCommandResult(
                command=["GET", "http://127.0.0.1:8734/healthz"],
                exit_code=0,
                passed=True,
                stdout=json.dumps({"status": "ok", "version": expected_version}),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["GET", "http://127.0.0.1:8734/v0/status"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    {
                        "service": "ades",
                        "version": expected_version,
                        "installed_packs": ["general-en", "finance-en"],
                    }
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    {
                        "entities": [
                            {"label": "organization"},
                            {"label": "ticker"},
                            {"label": "exchange"},
                            {"label": "currency_amount"},
                        ]
                    }
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/file"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    {
                        "entities": [
                            {"label": "organization"},
                            {"label": "ticker"},
                            {"label": "exchange"},
                            {"label": "currency_amount"},
                        ]
                    }
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    _served_batch_manifest_payload(working_dir, version=expected_version)
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    _served_batch_manifest_replay_payload(
                        working_dir,
                        version=expected_version,
                        **replay_payload_kwargs,
                    )
                ),
                stderr="",
            ),
            ["general-en", "finance-en"],
            ["organization", "ticker", "exchange", "currency_amount"],
            ["organization", "ticker", "exchange", "currency_amount"],
            ["organization", "ticker", "exchange", "currency_amount"],
            ["organization", "ticker", "exchange", "currency_amount"],
        )

    monkeypatch.setattr("ades.release._run_cli_service_smoke", fake_service_smoke)

    response = verify_release_artifacts(output_dir=tmp_path / "dist")

    assert response.overall_success is False
    assert response.npm_install_smoke is not None
    assert response.npm_install_smoke.passed is False
    assert response.npm_install_smoke.serve_tag_files_replay is not None
    assert response.npm_install_smoke.serve_tag_files_replay.passed is True
    assert response.warnings == [expected_warning]


@pytest.mark.parametrize(
    ("replay_kwargs", "expected_warning"),
    [
        (
            {"rerun_diff_newly_processed": ["/tmp/unexpected-new.html"]},
            "npm_tarball_serve_tag_files_replay_invalid_rerun_diff_newly_processed",
        ),
        (
            {"rerun_diff_reused": ["/tmp/unexpected-reused.html"]},
            "npm_tarball_serve_tag_files_replay_invalid_rerun_diff_reused",
        ),
        (
            {"rerun_diff_repaired": ["/tmp/unexpected-repaired.html"]},
            "npm_tarball_serve_tag_files_replay_invalid_rerun_diff_repaired",
        ),
        (
            {
                "rerun_diff_skipped": [
                    {
                        "reference": "/tmp/unexpected-skipped.html",
                        "reason": "unexpected",
                        "source_kind": "limit",
                        "size_bytes": 1,
                    }
                ]
            },
            "npm_tarball_serve_tag_files_replay_invalid_rerun_diff_skipped",
        ),
    ],
)
def test_verify_release_artifacts_reports_live_service_batch_replay_rerun_diff_empty_collection_mismatches(
    monkeypatch,
    tmp_path: Path,
    replay_kwargs: dict[str, object],
    expected_warning: str,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())

    def fake_service_smoke(*, executable, working_dir, storage_root, expected_version, extra_env=None):
        replay_payload_kwargs: dict[str, object] = (
            {} if "node_modules/.bin" not in str(executable) else replay_kwargs
        )
        return (
            ReleaseCommandResult(
                command=[str(executable), "serve", "--host", "127.0.0.1", "--port", "8734"],
                exit_code=0,
                passed=True,
                stdout="ready",
                stderr="",
            ),
            ReleaseCommandResult(
                command=["GET", "http://127.0.0.1:8734/healthz"],
                exit_code=0,
                passed=True,
                stdout=json.dumps({"status": "ok", "version": expected_version}),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["GET", "http://127.0.0.1:8734/v0/status"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    {
                        "service": "ades",
                        "version": expected_version,
                        "installed_packs": ["general-en", "finance-en"],
                    }
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    {
                        "entities": [
                            {"label": "organization"},
                            {"label": "ticker"},
                            {"label": "exchange"},
                            {"label": "currency_amount"},
                        ]
                    }
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/file"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    {
                        "entities": [
                            {"label": "organization"},
                            {"label": "ticker"},
                            {"label": "exchange"},
                            {"label": "currency_amount"},
                        ]
                    }
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    _served_batch_manifest_payload(working_dir, version=expected_version)
                ),
                stderr="",
            ),
            ReleaseCommandResult(
                command=["POST", "http://127.0.0.1:8734/v0/tag/files"],
                exit_code=0,
                passed=True,
                stdout=json.dumps(
                    _served_batch_manifest_replay_payload(
                        working_dir,
                        version=expected_version,
                        **replay_payload_kwargs,
                    )
                ),
                stderr="",
            ),
            ["general-en", "finance-en"],
            ["organization", "ticker", "exchange", "currency_amount"],
            ["organization", "ticker", "exchange", "currency_amount"],
            ["organization", "ticker", "exchange", "currency_amount"],
            ["organization", "ticker", "exchange", "currency_amount"],
        )

    monkeypatch.setattr("ades.release._run_cli_service_smoke", fake_service_smoke)

    response = verify_release_artifacts(output_dir=tmp_path / "dist")

    assert response.overall_success is False
    assert response.npm_install_smoke is not None
    assert response.npm_install_smoke.passed is False
    assert response.npm_install_smoke.serve_tag_files_replay is not None
    assert response.npm_install_smoke.serve_tag_files_replay.passed is True
    assert response.warnings == [expected_warning]


def test_verify_release_artifacts_reports_recovery_status_failures(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())

    def fake_recovery_status(*, executable, working_dir, storage_root, expected_version, extra_env=None):
        if "node_modules/.bin" not in str(executable):
            return (
                ReleaseCommandResult(
                    command=[str(executable), "status"],
                    exit_code=0,
                    passed=True,
                    stdout=(
                        '{"service":"ades","version":"'
                        + expected_version
                        + '","installed_packs":["general-en","finance-en"]}'
                    ),
                    stderr="",
                ),
                ["general-en", "finance-en"],
            )
        return (
            ReleaseCommandResult(
                command=[str(executable), "status"],
                exit_code=11,
                passed=False,
                stdout="",
                stderr="status failed after metadata reset",
            ),
            [],
        )

    monkeypatch.setattr("ades.release._run_cli_recovery_status_smoke", fake_recovery_status)

    response = verify_release_artifacts(output_dir=tmp_path / "dist")

    assert response.overall_success is False
    assert response.npm_install_smoke is not None
    assert response.npm_install_smoke.recovery_status is not None
    assert response.npm_install_smoke.recovery_status.passed is False
    assert response.npm_install_smoke.recovery_status.exit_code == 11
    assert response.npm_install_smoke.recovery_status.stderr == "status failed after metadata reset"
    assert response.warnings == ["npm_tarball_recovery_status_failed:11"]


def test_parse_status_version_accepts_wrapper_logs_before_json() -> None:
    result = ReleaseCommandResult(
        command=["ades", "status"],
        exit_code=0,
        passed=True,
        stdout=(
            "Collecting ades==0.1.0\n"
            "Successfully installed ades-0.1.0\n"
            "{\n"
            '  "service": "ades",\n'
            '  "version": "0.1.0"\n'
            "}\n"
        ),
        stderr="",
    )

    assert _parse_status_version(result) == "0.1.0"


def test_write_release_manifest_can_sync_then_persist_manifest(
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

    response = write_release_manifest(
        output_dir=tmp_path / "dist",
        version="0.2.0",
    )

    assert response.release_version == "0.2.0"
    assert response.version_sync is not None
    assert response.version_sync.target_version == "0.2.0"
    assert response.version_state.synchronized is True
    assert response.verification.smoke_install is True
    assert response.verification.python_install_smoke is not None
    assert response.verification.python_install_smoke.passed is True
    assert Path(response.manifest_path).exists()
