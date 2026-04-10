from pathlib import Path

from ades import publish_generated_registry_release
from tests.object_storage_publish_helpers import (
    create_reviewed_registry_dir,
    install_object_storage_publish_stub,
)


def test_public_api_can_publish_generated_registry_release(
    tmp_path: Path,
    monkeypatch,
) -> None:
    registry_dir = create_reviewed_registry_dir(tmp_path)
    install_object_storage_publish_stub(
        monkeypatch,
        registry_dir=registry_dir,
        prefix="generated-pack-releases/finance-general-medical-2026-04-10",
    )

    result = publish_generated_registry_release(
        registry_dir,
        prefix="generated-pack-releases/finance-general-medical-2026-04-10",
    )

    assert result.bucket == "ades-test"
    assert result.object_count == 3
    assert result.objects[0].key.endswith("index.json")
