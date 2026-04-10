import json
from pathlib import Path

from ades.packs.publish import prepare_registry_deploy_payload
from tests.published_registry_smoke_helpers import (
    create_working_published_registry_dir,
    serve_published_registry_dir,
    write_registry_promotion_spec,
)


def test_prepare_registry_deploy_payload_builds_bundled_registry_when_no_promotion_spec(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "ades.packs.publish.DEFAULT_REGISTRY_PROMOTION_SPEC_PATH",
        tmp_path / "missing-promoted-release.json",
    )

    result = prepare_registry_deploy_payload(
        [],
        output_dir=tmp_path / "deploy-registry",
    )

    assert result.mode == "bundled"
    assert result.promoted_registry_url is None
    assert result.consumer_smoke is None
    assert result.pack_count == 3
    assert [pack.pack_id for pack in result.packs] == [
        "finance-en",
        "general-en",
        "medical-en",
    ]
    assert Path(result.index_path).exists()


def test_prepare_registry_deploy_payload_materializes_promoted_registry(
    tmp_path: Path,
) -> None:
    registry_dir = create_working_published_registry_dir(tmp_path)

    with serve_published_registry_dir(registry_dir) as registry_url:
        promotion_spec_path = write_registry_promotion_spec(
            tmp_path,
            registry_url=registry_url,
        )
        result = prepare_registry_deploy_payload(
            [],
            output_dir=tmp_path / "deploy-registry",
            promotion_spec_path=promotion_spec_path,
        )

    assert result.mode == "promoted"
    assert result.promoted_registry_url == registry_url
    assert result.consumer_smoke is not None
    assert result.consumer_smoke.passed is True
    assert result.pack_count == 3
    assert sorted(pack.pack_id for pack in result.packs) == [
        "finance-en",
        "general-en",
        "medical-en",
    ]

    manifest_payload = json.loads(
        (tmp_path / "deploy-registry" / "packs" / "finance-en" / "manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest_payload["artifacts"][0]["url"].startswith("../../artifacts/")
    assert (tmp_path / "deploy-registry" / "artifacts").exists()
