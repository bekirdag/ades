from pathlib import Path

import pytest

from ades import build_registry
from tests.pack_registry_helpers import create_pack_source


def test_public_registry_build_rejects_non_directory_pack_input(tmp_path: Path) -> None:
    pack_file = tmp_path / "not-a-pack.txt"
    pack_file.write_text("not a pack directory", encoding="utf-8")

    with pytest.raises(
        NotADirectoryError,
        match=f"Pack directory is not a directory: {pack_file.resolve()}",
    ):
        build_registry([pack_file], output_dir=tmp_path / "registry")


def test_public_registry_build_rejects_duplicate_pack_ids(tmp_path: Path) -> None:
    pack_dir = create_pack_source(tmp_path / "sources", pack_id="general-en", domain="general")

    with pytest.raises(ValueError, match="Duplicate pack id in registry build: general-en"):
        build_registry([pack_dir, pack_dir], output_dir=tmp_path / "registry")
