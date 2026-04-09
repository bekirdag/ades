from pathlib import Path

import pytest

from ades.api import build_registry


def test_api_registry_build_rejects_missing_pack_directory(tmp_path: Path) -> None:
    missing_pack_dir = tmp_path / "missing-pack"

    with pytest.raises(
        FileNotFoundError,
        match=f"Pack directory not found: {missing_pack_dir.resolve()}",
    ):
        build_registry([missing_pack_dir], output_dir=tmp_path / "registry")


def test_api_registry_build_requires_at_least_one_pack_directory(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="At least one pack directory is required."):
        build_registry([], output_dir=tmp_path / "registry")
