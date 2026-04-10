from pathlib import Path

import pytest

from ades.packs.publish import smoke_test_published_registry
from tests.published_registry_smoke_helpers import (
    create_working_published_registry_dir,
    serve_published_registry_dir,
)


def test_smoke_test_published_registry_validates_finance_and_medical(
    tmp_path: Path,
) -> None:
    registry_dir = create_working_published_registry_dir(tmp_path)

    with serve_published_registry_dir(registry_dir) as registry_url:
        result = smoke_test_published_registry(
            registry_url,
            pack_ids=["finance-en", "medical-en"],
        )

    assert result.passed is True
    assert result.pack_count == 2
    assert result.passed_case_count == 2
    finance_case = next(case for case in result.cases if case.pack_id == "finance-en")
    medical_case = next(case for case in result.cases if case.pack_id == "medical-en")
    assert finance_case.installed_pack_ids == ["finance-en"]
    assert finance_case.missing_entity_texts == []
    assert "AAPL" in finance_case.matched_entity_texts
    assert "ticker" in finance_case.matched_labels
    assert medical_case.installed_pack_ids == ["general-en", "medical-en"]
    assert medical_case.missing_entity_texts == []
    assert "BRCA1" in medical_case.matched_entity_texts
    assert "disease" in medical_case.matched_labels


def test_smoke_test_published_registry_rejects_unsupported_pack(
    tmp_path: Path,
) -> None:
    registry_dir = create_working_published_registry_dir(tmp_path)

    with serve_published_registry_dir(registry_dir) as registry_url:
        with pytest.raises(ValueError, match="Unsupported published-registry smoke pack"):
            smoke_test_published_registry(
                registry_url,
                pack_ids=["unsupported-pack"],
            )
