from pathlib import Path

from ades.packs.publish import publish_registry_to_object_storage
from tests.object_storage_publish_helpers import (
    create_reviewed_registry_dir,
    install_object_storage_publish_stub,
)


def test_publish_registry_to_object_storage_uploads_reviewed_release(
    tmp_path: Path,
    monkeypatch,
) -> None:
    registry_dir = create_reviewed_registry_dir(tmp_path)
    calls = install_object_storage_publish_stub(
        monkeypatch,
        registry_dir=registry_dir,
        prefix="generated-pack-releases/finance-general-medical-2026-04-10",
    )

    result = publish_registry_to_object_storage(
        registry_dir,
        prefix="generated-pack-releases/finance-general-medical-2026-04-10",
        delete=False,
    )

    assert result.bucket == "ades-test"
    assert result.prefix == "generated-pack-releases/finance-general-medical-2026-04-10"
    assert result.storage_uri.endswith(
        "/generated-pack-releases/finance-general-medical-2026-04-10/"
    )
    assert result.index_storage_uri.endswith(
        "/generated-pack-releases/finance-general-medical-2026-04-10/index.json"
    )
    assert result.registry_url_candidates == [
        "https://ades-test.fsn1.your-objectstorage.com/generated-pack-releases/finance-general-medical-2026-04-10/index.json",
        "https://fsn1.your-objectstorage.com/ades-test/generated-pack-releases/finance-general-medical-2026-04-10/index.json",
    ]
    assert result.object_count == 3
    assert [item.key for item in result.objects] == [
        "generated-pack-releases/finance-general-medical-2026-04-10/index.json",
        "generated-pack-releases/finance-general-medical-2026-04-10/packs/general-en/manifest.json",
        "generated-pack-releases/finance-general-medical-2026-04-10/artifacts/general-en-0.2.0.tar.zst",
    ]
    assert "--delete" not in calls[0]


def test_publish_registry_to_object_storage_requires_bucket(
    tmp_path: Path,
    monkeypatch,
) -> None:
    registry_dir = create_reviewed_registry_dir(tmp_path)
    monkeypatch.delenv("ADES_PACK_OBJECT_STORAGE_BUCKET", raising=False)
    monkeypatch.delenv("ADES_PACK_OBJECT_STORAGE_ENDPOINT", raising=False)
    monkeypatch.delenv("ADES_PACK_OBJECT_STORAGE_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("ADES_PACK_OBJECT_STORAGE_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)

    try:
        publish_registry_to_object_storage(
            registry_dir,
            prefix="generated-pack-releases/finance-general-medical-2026-04-10",
        )
    except ValueError as exc:
        assert "bucket" in str(exc).lower()
    else:
        raise AssertionError("Expected ValueError when the bucket is missing.")
