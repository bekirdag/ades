from pathlib import Path

from ades import prepare_registry_deploy_release
from tests.published_registry_smoke_helpers import (
    create_working_published_registry_dir,
    serve_published_registry_dir,
    write_registry_promotion_spec,
)


def test_public_api_can_prepare_deploy_release_from_promoted_registry(
    tmp_path: Path,
) -> None:
    registry_dir = create_working_published_registry_dir(tmp_path)

    with serve_published_registry_dir(registry_dir) as registry_url:
        promotion_spec_path = write_registry_promotion_spec(
            tmp_path,
            registry_url=registry_url,
        )
        result = prepare_registry_deploy_release(
            output_dir=tmp_path / "deploy-registry",
            promotion_spec_path=promotion_spec_path,
        )

    assert result.mode == "promoted"
    assert result.promoted_registry_url == registry_url
    assert result.consumer_smoke is not None
    assert result.consumer_smoke.passed is True
    assert result.pack_count == 3
