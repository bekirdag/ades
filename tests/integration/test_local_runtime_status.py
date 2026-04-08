from pathlib import Path

from ades import pull_pack, status


def test_local_runtime_status_reports_local_sqlite_mode(tmp_path: Path) -> None:
    pull_pack("finance-en", storage_root=tmp_path)

    runtime_status = status(storage_root=tmp_path)

    assert runtime_status.runtime_target == "local"
    assert runtime_status.metadata_backend == "sqlite"
    assert "finance-en" in runtime_status.installed_packs
