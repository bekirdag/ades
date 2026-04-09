import json
from pathlib import Path

from fastapi.testclient import TestClient

from ades.service.app import create_app
from tests.release_helpers import (
    build_expected_batch_input_sizes,
    build_expected_batch_manifest_path,
    build_expected_batch_output_paths,
    build_expected_batch_summary_item_counts,
    build_expected_batch_summary_input_bytes,
    build_expected_batch_zero_state_summary_counts,
    build_expected_batch_zero_state_reuse_summary_counts,
    build_expected_batch_source_fingerprints,
    build_expected_batch_source_paths,
    build_expected_batch_replay_manifest_path,
    build_fake_release_runner,
    create_release_project,
    patch_release_runner,
)


def test_release_verify_endpoint_reports_smoke_install_results(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())
    client = TestClient(create_app())

    verify_response = client.post(
        "/v0/release/verify",
        json={"output_dir": str(tmp_path / "dist"), "clean": True},
    )

    assert verify_response.status_code == 200
    verify_payload = verify_response.json()
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
    python_expected_input_sizes = build_expected_batch_input_sizes()
    python_expected_summary_counts = build_expected_batch_summary_item_counts()
    python_expected_zero_state_summary_counts = (
        build_expected_batch_zero_state_summary_counts()
    )
    python_expected_zero_state_reuse_summary_counts = (
        build_expected_batch_zero_state_reuse_summary_counts()
    )
    python_expected_summary_input_bytes = build_expected_batch_summary_input_bytes()
    python_expected_source_fingerprints = build_expected_batch_source_fingerprints()
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
    assert [item["input_size_bytes"] for item in python_batch_payload["items"]] == (
        python_expected_input_sizes
    )
    assert {
        field_name: python_batch_payload["summary"][field_name]
        for field_name in python_expected_summary_counts
    } == python_expected_summary_counts
    assert {
        field_name: python_batch_payload["summary"][field_name]
        for field_name in python_expected_zero_state_summary_counts
    } == python_expected_zero_state_summary_counts
    assert {
        field_name: python_batch_payload["summary"][field_name]
        for field_name in python_expected_zero_state_reuse_summary_counts
    } == python_expected_zero_state_reuse_summary_counts
    assert {
        field_name: python_batch_payload["summary"][field_name]
        for field_name in python_expected_summary_input_bytes
    } == python_expected_summary_input_bytes
    assert [item["source_fingerprint"] for item in python_batch_payload["items"]] == (
        python_expected_source_fingerprints
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
    assert {
        field_name: python_replay_payload["summary"][field_name]
        for field_name in python_expected_summary_counts
    } == python_expected_summary_counts
    assert {
        field_name: python_replay_payload["summary"][field_name]
        for field_name in python_expected_zero_state_summary_counts
    } == python_expected_zero_state_summary_counts
    assert {
        field_name: python_replay_payload["summary"][field_name]
        for field_name in python_expected_zero_state_reuse_summary_counts
    } == python_expected_zero_state_reuse_summary_counts
    assert {
        field_name: python_replay_payload["summary"][field_name]
        for field_name in python_expected_summary_input_bytes
    } == python_expected_summary_input_bytes
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
    assert [item["input_size_bytes"] for item in python_replay_payload["items"]] == (
        python_expected_input_sizes
    )
    assert [item["source_fingerprint"] for item in python_replay_payload["items"]] == (
        python_expected_source_fingerprints
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
    npm_expected_input_sizes = build_expected_batch_input_sizes()
    npm_expected_summary_counts = build_expected_batch_summary_item_counts()
    npm_expected_zero_state_summary_counts = (
        build_expected_batch_zero_state_summary_counts()
    )
    npm_expected_zero_state_reuse_summary_counts = (
        build_expected_batch_zero_state_reuse_summary_counts()
    )
    npm_expected_summary_input_bytes = build_expected_batch_summary_input_bytes()
    npm_expected_source_fingerprints = build_expected_batch_source_fingerprints()
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
    assert [item["input_size_bytes"] for item in npm_batch_payload["items"]] == (
        npm_expected_input_sizes
    )
    assert {
        field_name: npm_batch_payload["summary"][field_name]
        for field_name in npm_expected_summary_counts
    } == npm_expected_summary_counts
    assert {
        field_name: npm_batch_payload["summary"][field_name]
        for field_name in npm_expected_zero_state_summary_counts
    } == npm_expected_zero_state_summary_counts
    assert {
        field_name: npm_batch_payload["summary"][field_name]
        for field_name in npm_expected_zero_state_reuse_summary_counts
    } == npm_expected_zero_state_reuse_summary_counts
    assert {
        field_name: npm_batch_payload["summary"][field_name]
        for field_name in npm_expected_summary_input_bytes
    } == npm_expected_summary_input_bytes
    assert [item["source_fingerprint"] for item in npm_batch_payload["items"]] == (
        npm_expected_source_fingerprints
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
    assert {
        field_name: npm_replay_payload["summary"][field_name]
        for field_name in npm_expected_summary_counts
    } == npm_expected_summary_counts
    assert {
        field_name: npm_replay_payload["summary"][field_name]
        for field_name in npm_expected_zero_state_summary_counts
    } == npm_expected_zero_state_summary_counts
    assert {
        field_name: npm_replay_payload["summary"][field_name]
        for field_name in npm_expected_zero_state_reuse_summary_counts
    } == npm_expected_zero_state_reuse_summary_counts
    assert {
        field_name: npm_replay_payload["summary"][field_name]
        for field_name in npm_expected_summary_input_bytes
    } == npm_expected_summary_input_bytes
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
    assert [item["input_size_bytes"] for item in npm_replay_payload["items"]] == (
        npm_expected_input_sizes
    )
    assert [item["source_fingerprint"] for item in npm_replay_payload["items"]] == (
        npm_expected_source_fingerprints
    )
    assert [item["saved_output_path"] for item in npm_replay_payload["items"]] == (
        npm_expected_output_paths
    )


def test_release_endpoints_report_versions_sync_and_manifest(
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
        build_fake_release_runner(python_version="0.4.0", npm_version="0.4.0"),
    )
    client = TestClient(create_app())

    versions_response = client.get("/v0/release/versions")
    assert versions_response.status_code == 200
    assert versions_response.json()["synchronized"] is False

    sync_response = client.post("/v0/release/sync-version", json={"version": "0.4.0"})
    assert sync_response.status_code == 200
    assert sync_response.json()["version_state"]["synchronized"] is True

    manifest_response = client.post(
        "/v0/release/manifest",
        json={
            "output_dir": str(tmp_path / "dist"),
            "clean": True,
            "smoke_install": False,
        },
    )
    assert manifest_response.status_code == 200
    payload = manifest_response.json()
    assert payload["release_version"] == "0.4.0"
    assert payload["verification"]["overall_success"] is True
    assert payload["verification"]["smoke_install"] is False
    assert payload["verification"]["python_install_smoke"] is None
    assert payload["verification"]["npm_install_smoke"] is None
    assert payload["verification"]["wheel"]["file_name"].endswith(".whl")
    assert Path(payload["manifest_path"]).exists()


def test_release_verify_endpoint_reports_missing_release_pyproject(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())
    missing_pyproject = (project_root / "pyproject.toml").resolve()
    missing_pyproject.unlink()
    client = TestClient(create_app())

    verify_response = client.post(
        "/v0/release/verify",
        json={"output_dir": str(tmp_path / "dist"), "clean": True},
    )

    assert verify_response.status_code == 404
    assert verify_response.json() == {
        "detail": f"Release coordination requires {missing_pyproject}."
    }


def test_release_verify_endpoint_reports_invalid_version_file(
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
    client = TestClient(create_app())

    verify_response = client.post(
        "/v0/release/verify",
        json={"output_dir": str(tmp_path / "dist"), "clean": True},
    )

    assert verify_response.status_code == 400
    assert verify_response.json() == {
        "detail": "Could not parse __version__ from src/ades/version.py."
    }


def test_release_manifest_endpoint_reports_missing_release_pyproject(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())
    missing_pyproject = (project_root / "pyproject.toml").resolve()
    missing_pyproject.unlink()
    client = TestClient(create_app())

    manifest_response = client.post(
        "/v0/release/manifest",
        json={
            "output_dir": str(tmp_path / "dist"),
            "clean": True,
            "smoke_install": False,
        },
    )

    assert manifest_response.status_code == 404
    assert manifest_response.json() == {
        "detail": f"Release coordination requires {missing_pyproject}."
    }


def test_release_manifest_endpoint_reports_invalid_version_file(
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
    client = TestClient(create_app())

    manifest_response = client.post(
        "/v0/release/manifest",
        json={
            "output_dir": str(tmp_path / "dist"),
            "clean": True,
            "smoke_install": False,
        },
    )

    assert manifest_response.status_code == 400
    assert manifest_response.json() == {
        "detail": "Could not parse __version__ from src/ades/version.py."
    }


def test_release_validate_endpoint_reports_missing_release_pyproject(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    patch_release_runner(monkeypatch, build_fake_release_runner())
    missing_pyproject = (project_root / "pyproject.toml").resolve()
    missing_pyproject.unlink()
    client = TestClient(create_app())

    validate_response = client.post(
        "/v0/release/validate",
        json={
            "output_dir": str(tmp_path / "dist"),
            "clean": True,
            "smoke_install": False,
        },
    )

    assert validate_response.status_code == 404
    assert validate_response.json() == {
        "detail": f"Release coordination requires {missing_pyproject}."
    }


def test_release_validate_endpoint_reports_invalid_version_file(
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
    client = TestClient(create_app())

    validate_response = client.post(
        "/v0/release/validate",
        json={
            "output_dir": str(tmp_path / "dist"),
            "clean": True,
            "smoke_install": False,
        },
    )

    assert validate_response.status_code == 400
    assert validate_response.json() == {
        "detail": "Could not parse __version__ from src/ades/version.py."
    }


def test_release_versions_endpoint_reports_missing_release_pyproject(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    missing_pyproject = (project_root / "pyproject.toml").resolve()
    missing_pyproject.unlink()
    client = TestClient(create_app())

    versions_response = client.get("/v0/release/versions")

    assert versions_response.status_code == 404
    assert versions_response.json() == {
        "detail": f"Release coordination requires {missing_pyproject}."
    }


def test_release_versions_endpoint_reports_invalid_version_file(
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
    client = TestClient(create_app())

    versions_response = client.get("/v0/release/versions")

    assert versions_response.status_code == 400
    assert versions_response.json() == {
        "detail": "Could not parse __version__ from src/ades/version.py."
    }


def test_release_sync_version_endpoint_reports_missing_release_pyproject(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_root, npm_package_dir = create_release_project(tmp_path / "repo")
    monkeypatch.setattr("ades.release.resolve_project_root", lambda: project_root)
    monkeypatch.setattr("ades.release.resolve_npm_package_dir", lambda: npm_package_dir)
    missing_pyproject = (project_root / "pyproject.toml").resolve()
    missing_pyproject.unlink()
    client = TestClient(create_app())

    sync_response = client.post("/v0/release/sync-version", json={"version": "0.2.0"})

    assert sync_response.status_code == 404
    assert sync_response.json() == {
        "detail": f"Release coordination requires {missing_pyproject}."
    }


def test_release_sync_version_endpoint_reports_invalid_version_file(
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
    client = TestClient(create_app())

    sync_response = client.post("/v0/release/sync-version", json={"version": "0.2.0"})

    assert sync_response.status_code == 400
    assert sync_response.json() == {
        "detail": "Could not update __version__ in src/ades/version.py."
    }
