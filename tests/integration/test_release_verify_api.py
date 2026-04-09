from pathlib import Path

from ades import release_versions, sync_release_version, verify_release, write_release_manifest
from tests.release_helpers import (
    build_fake_release_runner,
    create_release_project,
    patch_release_runner,
)


def test_public_release_api_can_sync_versions_and_persist_manifest(
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
        build_fake_release_runner(python_version="0.3.0", npm_version="0.3.0"),
    )

    before = release_versions()
    sync = sync_release_version("0.3.0")
    verification = verify_release(output_dir=tmp_path / "dist")
    manifest = write_release_manifest(
        output_dir=tmp_path / "dist",
        smoke_install=False,
    )

    assert before.synchronized is False
    assert sync.version_state.version_file_version == "0.3.0"
    assert verification.version_state.synchronized is True
    assert verification.output_dir == str((tmp_path / "dist").resolve())
    assert verification.smoke_install is True
    assert verification.python_install_smoke is not None
    assert verification.python_install_smoke.passed is True
    assert verification.python_install_smoke.pull is not None
    assert verification.python_install_smoke.pull.passed is True
    assert verification.python_install_smoke.tag is not None
    assert verification.python_install_smoke.tag.passed is True
    assert verification.python_install_smoke.recovery_status is not None
    assert verification.python_install_smoke.recovery_status.passed is True
    assert verification.python_install_smoke.serve is not None
    assert verification.python_install_smoke.serve.passed is True
    assert verification.python_install_smoke.serve_healthz is not None
    assert verification.python_install_smoke.serve_healthz.passed is True
    assert verification.python_install_smoke.serve_status is not None
    assert verification.python_install_smoke.serve_status.passed is True
    assert verification.python_install_smoke.serve_tag is not None
    assert verification.python_install_smoke.serve_tag.passed is True
    assert verification.python_install_smoke.serve_tag_file is not None
    assert verification.python_install_smoke.serve_tag_file.passed is True
    assert verification.python_install_smoke.serve_tag_files is not None
    assert verification.python_install_smoke.serve_tag_files.passed is True
    assert {"general-en", "finance-en"} <= set(verification.python_install_smoke.pulled_pack_ids)
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        verification.python_install_smoke.tagged_labels
    )
    assert {"general-en", "finance-en"} <= set(
        verification.python_install_smoke.recovered_pack_ids
    )
    assert {"general-en", "finance-en"} <= set(verification.python_install_smoke.served_pack_ids)
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        verification.python_install_smoke.serve_tagged_labels
    )
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        verification.python_install_smoke.serve_tag_file_labels
    )
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        verification.python_install_smoke.serve_tag_files_labels
    )
    assert verification.npm_install_smoke is not None
    assert verification.npm_install_smoke.passed is True
    assert verification.npm_install_smoke.pull is not None
    assert verification.npm_install_smoke.pull.passed is True
    assert verification.npm_install_smoke.tag is not None
    assert verification.npm_install_smoke.tag.passed is True
    assert verification.npm_install_smoke.recovery_status is not None
    assert verification.npm_install_smoke.recovery_status.passed is True
    assert verification.npm_install_smoke.serve is not None
    assert verification.npm_install_smoke.serve.passed is True
    assert verification.npm_install_smoke.serve_healthz is not None
    assert verification.npm_install_smoke.serve_healthz.passed is True
    assert verification.npm_install_smoke.serve_status is not None
    assert verification.npm_install_smoke.serve_status.passed is True
    assert verification.npm_install_smoke.serve_tag is not None
    assert verification.npm_install_smoke.serve_tag.passed is True
    assert verification.npm_install_smoke.serve_tag_file is not None
    assert verification.npm_install_smoke.serve_tag_file.passed is True
    assert verification.npm_install_smoke.serve_tag_files is not None
    assert verification.npm_install_smoke.serve_tag_files.passed is True
    assert {"general-en", "finance-en"} <= set(verification.npm_install_smoke.pulled_pack_ids)
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        verification.npm_install_smoke.tagged_labels
    )
    assert {"general-en", "finance-en"} <= set(verification.npm_install_smoke.recovered_pack_ids)
    assert {"general-en", "finance-en"} <= set(verification.npm_install_smoke.served_pack_ids)
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        verification.npm_install_smoke.serve_tagged_labels
    )
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        verification.npm_install_smoke.serve_tag_file_labels
    )
    assert {"organization", "ticker", "exchange", "currency_amount"} <= set(
        verification.npm_install_smoke.serve_tag_files_labels
    )
    assert manifest.release_version == "0.3.0"
    assert manifest.verification.smoke_install is False
    assert manifest.verification.python_install_smoke is None
    assert manifest.verification.npm_install_smoke is None
    assert Path(manifest.manifest_path).exists()
