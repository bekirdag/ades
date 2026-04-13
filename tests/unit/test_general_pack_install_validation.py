from pathlib import Path

import pytest

from ades.packs.installer import PackInstaller
from tests.pack_registry_helpers import create_pack_source, write_general_pack_build


def test_validate_extracted_pack_accepts_balanced_small_general_build(tmp_path: Path) -> None:
    pack_dir = create_pack_source(tmp_path, pack_id="general-en", domain="general")
    write_general_pack_build(
        pack_dir,
        person_count=3,
        organization_count=3,
        location_count=1,
    )

    PackInstaller._validate_extracted_pack(pack_dir, "general-en")


def test_validate_extracted_pack_rejects_pathological_real_general_build(tmp_path: Path) -> None:
    pack_dir = create_pack_source(tmp_path, pack_id="general-en", domain="general")
    write_general_pack_build(
        pack_dir,
        person_count=26,
        organization_count=14,
        location_count=1_417_881,
    )

    with pytest.raises(ValueError, match="General-en install verification failed"):
        PackInstaller._validate_extracted_pack(pack_dir, "general-en")
