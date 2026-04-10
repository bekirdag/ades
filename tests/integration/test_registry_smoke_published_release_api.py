from pathlib import Path

from ades import smoke_test_published_generated_registry
from tests.published_registry_smoke_helpers import (
    create_working_published_registry_dir,
    serve_published_registry_dir,
)


def test_public_api_can_smoke_published_generated_registry(tmp_path: Path) -> None:
    registry_dir = create_working_published_registry_dir(tmp_path)

    with serve_published_registry_dir(registry_dir) as registry_url:
        result = smoke_test_published_generated_registry(
            registry_url,
            pack_ids=["medical-en"],
        )

    assert result.passed is True
    assert result.pack_count == 1
    assert result.cases[0].pack_id == "medical-en"
    assert result.cases[0].installed_pack_ids == ["general-en", "medical-en"]
    assert "BRCA1" in result.cases[0].matched_entity_texts
